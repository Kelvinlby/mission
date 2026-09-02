use crate::{
    protocol::{self, ClientMessage, ServerMessage},
    session::{self, SessionEntry},
};
use anyhow::{Context, Result, bail};
use nix::{sys::signal as unix_signal, unistd::Pid};
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use std::{
    borrow::Cow,
    collections::VecDeque,
    env,
    fs::{self, OpenOptions},
    io::{Read, Write},
    os::unix::fs::PermissionsExt,
    os::unix::net::{UnixListener, UnixStream},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const TRANSCRIPT_LIMIT: usize = 4 * 1024 * 1024;

#[derive(Clone)]
struct OutputChunk {
    bytes: Vec<u8>,
    timestamp_ms: u64,
}

#[derive(Clone)]
struct ClientContext {
    client: Arc<Mutex<Option<UnixStream>>>,
    active_generation: Arc<AtomicU64>,
    writer: Arc<Mutex<Box<dyn Write + Send>>>,
    master: Arc<Mutex<Box<dyn portable_pty::MasterPty + Send>>>,
    running: Arc<AtomicBool>,
    stop_requested: Arc<AtomicBool>,
    remove_on_exit: Arc<AtomicBool>,
    pid: u32,
}

pub fn run(session_dir: PathBuf, command: Vec<String>) -> Result<()> {
    if command.is_empty() {
        bail!("supervisor received an empty command");
    }
    fs::create_dir_all(&session_dir)?;
    let socket_path = session_dir.join("control.sock");
    let _ = fs::remove_file(&socket_path);

    let pty = native_pty_system().openpty(PtySize {
        rows: 30,
        cols: 120,
        pixel_width: 0,
        pixel_height: 0,
    })?;
    let program = compatible_program(&command[0]);
    let mut builder = CommandBuilder::new(program.as_ref());
    builder.args(&command[1..]);
    builder.env_remove("MISSION_SUPERVISOR");
    builder.env("MISSION_ACTIVE_SESSION", "1");
    builder.env("TERM", "xterm-256color");
    let mut child = pty
        .slave
        .spawn_command(builder)
        .with_context(|| format!("start {}", command[0]))?;
    drop(pty.slave);
    let pid = child.process_id().context("child process has no pid")?;
    let mut reader = pty.master.try_clone_reader()?;
    let writer = Arc::new(Mutex::new(pty.master.take_writer()?));
    let master = Arc::new(Mutex::new(pty.master));

    let created_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let entry = SessionEntry {
        id: session_dir
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned(),
        command: command.clone(),
        pid,
        created_at,
        running: true,
        exit_code: None,
        dir: session_dir.clone(),
    };
    session::write_entry(&entry)?;

    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("bind {}", socket_path.display()))?;
    listener.set_nonblocking(true)?;
    let client: Arc<Mutex<Option<UnixStream>>> = Arc::new(Mutex::new(None));
    let transcript = Arc::new(Mutex::new(VecDeque::<OutputChunk>::new()));
    let running = Arc::new(AtomicBool::new(true));
    let stop_requested = Arc::new(AtomicBool::new(false));
    let remove_on_exit = Arc::new(AtomicBool::new(false));
    let client_generation = Arc::new(AtomicU64::new(0));

    {
        let client = Arc::clone(&client);
        let transcript = Arc::clone(&transcript);
        let log_path = entry.log_path();
        thread::spawn(move || {
            let mut log = OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_path)
                .ok();
            let mut buffer = [0_u8; 16 * 1024];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(count) => {
                        let bytes = &buffer[..count];
                        let timestamp_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map_or(0, |duration| duration.as_millis() as u64);
                        if let Some(file) = log.as_mut() {
                            let _ = file.write_all(bytes);
                        }
                        {
                            let mut saved =
                                transcript.lock().unwrap_or_else(|error| error.into_inner());
                            saved.push_back(OutputChunk {
                                bytes: bytes.to_vec(),
                                timestamp_ms,
                            });
                            let mut retained: usize =
                                saved.iter().map(|chunk| chunk.bytes.len()).sum();
                            while retained > TRANSCRIPT_LIMIT {
                                if let Some(removed) = saved.pop_front() {
                                    retained = retained.saturating_sub(removed.bytes.len());
                                }
                            }
                        }
                        send_to_client(
                            &client,
                            &ServerMessage::Output {
                                bytes: bytes.to_vec(),
                                timestamp_ms,
                            },
                        );
                    }
                }
            }
        });
    }

    {
        let client = Arc::clone(&client);
        let running = Arc::clone(&running);
        let mut finished_entry = entry.clone();
        thread::spawn(move || {
            let code = child.wait().ok().map(|status| status.exit_code() as i32);
            running.store(false, Ordering::Release);
            finished_entry.running = false;
            finished_entry.exit_code = code;
            let _ = session::write_entry(&finished_entry);
            send_to_client(&client, &ServerMessage::Exited(code));
        });
    }

    let mut idle_after_exit = None;
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                let mut sink = stream.try_clone()?;
                protocol::send(
                    &mut sink,
                    &ServerMessage::Hello {
                        pid,
                        command: command.clone(),
                        running: running.load(Ordering::Acquire),
                    },
                )?;
                let saved: Vec<OutputChunk> = transcript
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .iter()
                    .cloned()
                    .collect();
                for chunk in saved {
                    protocol::send(
                        &mut sink,
                        &ServerMessage::Output {
                            bytes: chunk.bytes,
                            timestamp_ms: chunk.timestamp_ms,
                        },
                    )?;
                }
                let generation = client_generation.fetch_add(1, Ordering::AcqRel) + 1;
                let mut active = client.lock().unwrap_or_else(|error| error.into_inner());
                if let Some(previous) = active.take() {
                    let _ = previous.shutdown(std::net::Shutdown::Both);
                }
                *active = Some(sink);
                drop(active);
                spawn_client_reader(
                    stream,
                    generation,
                    ClientContext {
                        client: Arc::clone(&client),
                        active_generation: Arc::clone(&client_generation),
                        writer: Arc::clone(&writer),
                        master: Arc::clone(&master),
                        running: Arc::clone(&running),
                        stop_requested: Arc::clone(&stop_requested),
                        remove_on_exit: Arc::clone(&remove_on_exit),
                        pid,
                    },
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50))
            }
            Err(error) => return Err(error.into()),
        }
        if !running.load(Ordering::Acquire)
            && client
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_none()
        {
            if remove_on_exit.load(Ordering::Acquire) {
                break;
            }
            let since = idle_after_exit.get_or_insert_with(std::time::Instant::now);
            if since.elapsed() >= Duration::from_secs(30) {
                break;
            }
        } else {
            idle_after_exit = None;
        }
    }
    let _ = fs::remove_file(socket_path);
    if remove_on_exit.load(Ordering::Acquire) {
        let _ = fs::remove_dir_all(session_dir);
    }
    Ok(())
}

fn compatible_program(program: &str) -> Cow<'_, str> {
    if program == "python" {
        Cow::Borrowed(python_program(
            program_exists("python"),
            program_exists("python3"),
        ))
    } else {
        Cow::Borrowed(program)
    }
}

fn python_program(has_python: bool, has_python3: bool) -> &'static str {
    if !has_python && has_python3 {
        "python3"
    } else {
        "python"
    }
}

fn program_exists(program: &str) -> bool {
    let path = std::path::Path::new(program);
    if path.components().count() > 1 {
        return path.is_file();
    }
    env::var_os("PATH").is_some_and(|path| {
        env::split_paths(&path).any(|directory| {
            let candidate = directory.join(program);
            candidate.metadata().is_ok_and(|metadata| {
                metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
            })
        })
    })
}

fn spawn_client_reader(mut stream: UnixStream, generation: u64, context: ClientContext) {
    thread::spawn(move || {
        while let Ok(message) = protocol::receive::<ClientMessage>(&mut stream) {
            match message {
                ClientMessage::Input(bytes) => {
                    let mut writer = context
                        .writer
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    if writer
                        .write_all(&bytes)
                        .and_then(|_| writer.flush())
                        .is_err()
                    {
                        break;
                    }
                }
                ClientMessage::Resize { rows, cols } => {
                    let _ = context
                        .master
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .resize(PtySize {
                            rows: rows.max(1),
                            cols: cols.max(1),
                            pixel_width: 0,
                            pixel_height: 0,
                        });
                }
                ClientMessage::Stop => {
                    request_stop(context.pid, &context.running, &context.stop_requested);
                }
                ClientMessage::Close => {
                    context.remove_on_exit.store(true, Ordering::Release);
                    request_stop(context.pid, &context.running, &context.stop_requested);
                    break;
                }
                ClientMessage::Detach => break,
            }
        }
        if context.active_generation.load(Ordering::Acquire) == generation {
            *context
                .client
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = None;
        }
    });
}

fn request_stop(pid: u32, running: &Arc<AtomicBool>, stop_requested: &AtomicBool) {
    if !running.load(Ordering::Acquire) {
        return;
    }
    if stop_requested
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        let running = Arc::clone(running);
        thread::spawn(move || stop_process(pid, &running));
    }
}

fn stop_process(pid: u32, running: &AtomicBool) {
    let process_group = Pid::from_raw(pid as i32);
    send_process_signal(process_group, unix_signal::Signal::SIGINT);
    if wait_until_stopped(running, Duration::from_secs(2)) {
        return;
    }
    send_process_signal(process_group, unix_signal::Signal::SIGTERM);
    if wait_until_stopped(running, Duration::from_secs(2)) {
        return;
    }
    send_process_signal(process_group, unix_signal::Signal::SIGKILL);
}

fn send_process_signal(process_group: Pid, signal: unix_signal::Signal) {
    if unix_signal::killpg(process_group, signal).is_err() {
        let _ = unix_signal::kill(process_group, signal);
    }
}

fn wait_until_stopped(running: &AtomicBool, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while running.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(50));
    }
    !running.load(Ordering::Acquire)
}

fn send_to_client(client: &Mutex<Option<UnixStream>>, message: &ServerMessage) {
    let mut guard = client.lock().unwrap_or_else(|error| error.into_inner());
    if let Some(stream) = guard.as_mut()
        && protocol::send(stream, message).is_err()
    {
        *guard = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_falls_back_only_when_python3_is_the_available_name() {
        assert_eq!(python_program(false, true), "python3");
        assert_eq!(python_program(true, true), "python");
        assert_eq!(python_program(false, false), "python");
    }

    #[test]
    fn detached_pty_accepts_interactive_editing_keys() {
        let dir = std::env::temp_dir().join(format!(
            "mission-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let supervisor_dir = dir.clone();
        thread::spawn(move || {
            let _ = run(
                supervisor_dir,
                vec![
                    "/bin/bash".into(),
                    "-c".into(),
                    "IFS= read -r -e line; printf 'GOT:<%s>\\n' \"$line\"; sleep 30".into(),
                ],
            );
        });

        let socket = dir.join("control.sock");
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while !socket.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "supervisor did not start"
            );
            thread::sleep(Duration::from_millis(10));
        }
        let mut stream = UnixStream::connect(socket).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        assert!(matches!(
            protocol::receive::<ServerMessage>(&mut stream).unwrap(),
            ServerMessage::Hello { running: true, .. }
        ));
        protocol::send(&mut stream, &ClientMessage::Input(b"ac\x1b[Db\r".to_vec())).unwrap();
        let mut output = Vec::new();
        for _ in 0..4 {
            match protocol::receive::<ServerMessage>(&mut stream).unwrap() {
                ServerMessage::Output { bytes, .. } => output.extend(bytes),
                ServerMessage::Exited(_) => break,
                _ => {}
            }
            if String::from_utf8_lossy(&output).contains("GOT:<abc>") {
                break;
            }
        }
        assert!(String::from_utf8_lossy(&output).contains("GOT:<abc>"));
        protocol::send(&mut stream, &ClientMessage::Stop).unwrap();
        let mut exited = false;
        for _ in 0..4 {
            if matches!(
                protocol::receive::<ServerMessage>(&mut stream).unwrap(),
                ServerMessage::Exited(_)
            ) {
                exited = true;
                break;
            }
        }
        assert!(
            exited,
            "the graceful stop sequence did not stop the PTY process"
        );
        protocol::send(&mut stream, &ClientMessage::Close).unwrap();
        let cleanup_deadline = std::time::Instant::now() + Duration::from_secs(2);
        while dir.exists() && std::time::Instant::now() < cleanup_deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(!dir.exists(), "closing did not remove the finished session");
    }
}
