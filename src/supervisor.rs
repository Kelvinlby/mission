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
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
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

/// Everything a session owns for the lifetime of the supervisor. The PTY, child
/// pid and run state are all replaceable so the command can be rerun in place.
#[derive(Clone)]
struct Runtime {
    command: Vec<String>,
    working_dir: PathBuf,
    log_path: PathBuf,
    client: Arc<Mutex<Option<UnixStream>>>,
    transcript: Arc<Mutex<VecDeque<OutputChunk>>>,
    entry: Arc<Mutex<SessionEntry>>,
    active_generation: Arc<AtomicU64>,
    writer: Arc<Mutex<Box<dyn Write + Send>>>,
    master: Arc<Mutex<Box<dyn portable_pty::MasterPty + Send>>>,
    running: Arc<AtomicBool>,
    stop_requested: Arc<AtomicBool>,
    remove_on_exit: Arc<AtomicBool>,
    restarting: Arc<AtomicBool>,
    pid: Arc<AtomicU32>,
}

struct StartedChild {
    child: Box<dyn portable_pty::Child + Send + Sync>,
    reader: Box<dyn Read + Send>,
    writer: Box<dyn Write + Send>,
    master: Box<dyn portable_pty::MasterPty + Send>,
    pid: u32,
}

fn default_pty_size() -> PtySize {
    PtySize {
        rows: 30,
        cols: 120,
        pixel_width: 0,
        pixel_height: 0,
    }
}

fn spawn_child(command: &[String], size: PtySize, working_dir: &Path) -> Result<StartedChild> {
    let pty = native_pty_system().openpty(size)?;
    let program = compatible_program(&command[0]);
    let mut builder = CommandBuilder::new(program.as_ref());
    builder.args(&command[1..]);
    // Without this the child inherits nothing and portable-pty starts it in the
    // home directory instead of where mission was invoked.
    builder.cwd(working_dir);
    builder.env_remove("MISSION_SUPERVISOR");
    builder.env("MISSION_ACTIVE_SESSION", "1");
    builder.env("TERM", "xterm-256color");
    let child = pty
        .slave
        .spawn_command(builder)
        .with_context(|| format!("start {}", command[0]))?;
    drop(pty.slave);
    let pid = child.process_id().context("child process has no pid")?;
    let reader = pty.master.try_clone_reader()?;
    let writer = pty.master.take_writer()?;
    Ok(StartedChild {
        child,
        reader,
        writer,
        master: pty.master,
        pid,
    })
}

impl Runtime {
    fn lock_client(&self) -> std::sync::MutexGuard<'_, Option<UnixStream>> {
        self.client
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    /// Pump one child's output into the log, the transcript and the attached
    /// client, and publish its exit status when it finishes.
    fn watch(
        &self,
        mut child: Box<dyn portable_pty::Child + Send + Sync>,
        mut reader: Box<dyn Read + Send>,
        pid: u32,
    ) {
        let runtime = self.clone();
        thread::spawn(move || {
            let mut log = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&runtime.log_path)
                .ok();
            let mut buffer = [0_u8; 16 * 1024];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(count) => {
                        // A superseded run may still be draining; its output must not
                        // land in the transcript of the run that replaced it.
                        if runtime.pid.load(Ordering::Acquire) != pid {
                            break;
                        }
                        let bytes = &buffer[..count];
                        let timestamp_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map_or(0, |duration| duration.as_millis() as u64);
                        if let Some(file) = log.as_mut() {
                            let _ = file.write_all(bytes);
                        }
                        runtime.record(bytes, timestamp_ms);
                        send_to_client(
                            &runtime.client,
                            &ServerMessage::Output {
                                bytes: bytes.to_vec(),
                                timestamp_ms,
                            },
                        );
                    }
                }
            }
        });

        let runtime = self.clone();
        thread::spawn(move || {
            let code = child.wait().ok().map(|status| status.exit_code() as i32);
            if runtime.pid.load(Ordering::Acquire) != pid {
                return;
            }
            runtime.running.store(false, Ordering::Release);
            runtime.update_entry(|entry| {
                entry.running = false;
                entry.exit_code = code;
            });
            send_to_client(&runtime.client, &ServerMessage::Exited(code));
        });
    }

    fn record(&self, bytes: &[u8], timestamp_ms: u64) {
        let mut saved = self
            .transcript
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        saved.push_back(OutputChunk {
            bytes: bytes.to_vec(),
            timestamp_ms,
        });
        let mut retained: usize = saved.iter().map(|chunk| chunk.bytes.len()).sum();
        while retained > TRANSCRIPT_LIMIT {
            if let Some(removed) = saved.pop_front() {
                retained = retained.saturating_sub(removed.bytes.len());
            }
        }
    }

    fn update_entry(&self, edit: impl FnOnce(&mut SessionEntry)) {
        let mut entry = self.entry.lock().unwrap_or_else(|error| error.into_inner());
        edit(&mut entry);
        let _ = session::write_entry(&entry);
    }

    /// Stop the current child if it is still alive, then start the command again
    /// on a fresh PTY of the same size.
    fn restart(&self) {
        if self
            .restarting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let runtime = self.clone();
        thread::spawn(move || {
            if runtime.running.load(Ordering::Acquire) {
                request_stop(&runtime);
                if !wait_until_stopped(&runtime.running, Duration::from_secs(8)) {
                    runtime.fail_restart("cannot rerun: the process did not stop");
                    return;
                }
            }
            let size = runtime
                .master
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get_size()
                .unwrap_or_else(|_| default_pty_size());
            let StartedChild {
                child,
                reader,
                writer,
                master,
                pid,
            } = match spawn_child(&runtime.command, size, &runtime.working_dir) {
                Ok(started) => started,
                Err(error) => {
                    runtime.fail_restart(&format!("rerun failed: {error}"));
                    return;
                }
            };
            *runtime
                .writer
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = writer;
            *runtime
                .master
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = master;
            // The client resets its screen, so replayed output from the previous
            // run would not match what it is showing.
            runtime
                .transcript
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clear();
            runtime.mark_log_rerun();
            runtime.pid.store(pid, Ordering::Release);
            runtime.stop_requested.store(false, Ordering::Release);
            runtime.running.store(true, Ordering::Release);
            runtime.update_entry(|entry| {
                entry.pid = pid;
                entry.running = true;
                entry.exit_code = None;
            });
            send_to_client(&runtime.client, &ServerMessage::Restarted { pid });
            runtime.restarting.store(false, Ordering::Release);
            runtime.watch(child, reader, pid);
        });
    }

    fn fail_restart(&self, reason: &str) {
        send_to_client(&self.client, &ServerMessage::Error(reason.to_owned()));
        self.restarting.store(false, Ordering::Release);
    }

    fn mark_log_rerun(&self) {
        if let Ok(mut log) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
        {
            let _ = log.write_all(b"\r\n\r\n-- rerun --\r\n\r\n");
        }
    }
}

pub fn run(session_dir: PathBuf, command: Vec<String>) -> Result<()> {
    if command.is_empty() {
        bail!("supervisor received an empty command");
    }
    fs::create_dir_all(&session_dir)?;
    let socket_path = session_dir.join("control.sock");
    let _ = fs::remove_file(&socket_path);

    // The supervisor is spawned from the mission process and never changes
    // directory, so its own cwd is the directory the user launched from.
    let working_dir = env::current_dir().context("resolve the launch directory")?;
    let StartedChild {
        child,
        reader,
        writer,
        master,
        pid,
    } = spawn_child(&command, default_pty_size(), &working_dir)?;

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
    let runtime = Runtime {
        command: command.clone(),
        working_dir,
        log_path: entry.log_path(),
        client: Arc::new(Mutex::new(None)),
        transcript: Arc::new(Mutex::new(VecDeque::new())),
        entry: Arc::new(Mutex::new(entry)),
        active_generation: Arc::new(AtomicU64::new(0)),
        writer: Arc::new(Mutex::new(writer)),
        master: Arc::new(Mutex::new(master)),
        running: Arc::new(AtomicBool::new(true)),
        stop_requested: Arc::new(AtomicBool::new(false)),
        remove_on_exit: Arc::new(AtomicBool::new(false)),
        restarting: Arc::new(AtomicBool::new(false)),
        pid: Arc::new(AtomicU32::new(pid)),
    };
    runtime.watch(child, reader, pid);

    let mut idle_after_exit = None;
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                let mut sink = stream.try_clone()?;
                protocol::send(
                    &mut sink,
                    &ServerMessage::Hello {
                        pid: runtime.pid.load(Ordering::Acquire),
                        command: command.clone(),
                        running: runtime.running.load(Ordering::Acquire),
                    },
                )?;
                let saved: Vec<OutputChunk> = runtime
                    .transcript
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
                let generation = runtime.active_generation.fetch_add(1, Ordering::AcqRel) + 1;
                let mut active = runtime.lock_client();
                if let Some(previous) = active.take() {
                    let _ = previous.shutdown(std::net::Shutdown::Both);
                }
                *active = Some(sink);
                drop(active);
                spawn_client_reader(stream, generation, runtime.clone());
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50))
            }
            Err(error) => return Err(error.into()),
        }
        if !runtime.running.load(Ordering::Acquire) && runtime.lock_client().is_none() {
            if runtime.remove_on_exit.load(Ordering::Acquire) {
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
    if runtime.remove_on_exit.load(Ordering::Acquire) {
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

fn spawn_client_reader(mut stream: UnixStream, generation: u64, runtime: Runtime) {
    thread::spawn(move || {
        while let Ok(message) = protocol::receive::<ClientMessage>(&mut stream) {
            match message {
                ClientMessage::Input(bytes) => {
                    let mut writer = runtime
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
                    let _ = runtime
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
                ClientMessage::Stop => request_stop(&runtime),
                ClientMessage::Restart => runtime.restart(),
                ClientMessage::Close => {
                    runtime.remove_on_exit.store(true, Ordering::Release);
                    request_stop(&runtime);
                    break;
                }
                ClientMessage::Detach => break,
            }
        }
        if runtime.active_generation.load(Ordering::Acquire) == generation {
            *runtime.lock_client() = None;
        }
    });
}

fn request_stop(runtime: &Runtime) {
    if !runtime.running.load(Ordering::Acquire) {
        return;
    }
    if runtime
        .stop_requested
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        let pid = runtime.pid.load(Ordering::Acquire);
        let running = Arc::clone(&runtime.running);
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

    fn wait_for_socket(dir: &std::path::Path) -> UnixStream {
        let socket = dir.join("control.sock");
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while !socket.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "supervisor did not start"
            );
            thread::sleep(Duration::from_millis(10));
        }
        let stream = UnixStream::connect(socket).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
    }

    fn test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "mission-test-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn rerunning_starts_a_fresh_process_and_clears_the_replayed_transcript() {
        let dir = test_dir("rerun");
        let supervisor_dir = dir.clone();
        thread::spawn(move || {
            let _ = run(
                supervisor_dir,
                vec![
                    "/bin/bash".into(),
                    "-c".into(),
                    "echo RUN-MARKER; sleep 30".into(),
                ],
            );
        });

        let mut stream = wait_for_socket(&dir);
        let first_pid = match protocol::receive::<ServerMessage>(&mut stream).unwrap() {
            ServerMessage::Hello { pid, running, .. } => {
                assert!(running);
                pid
            }
            other => panic!("expected Hello, got {other:?}"),
        };

        // Wait for the first run to produce output.
        let mut output = Vec::new();
        for _ in 0..8 {
            if let ServerMessage::Output { bytes, .. } =
                protocol::receive::<ServerMessage>(&mut stream).unwrap()
            {
                output.extend(bytes);
            }
            if String::from_utf8_lossy(&output).contains("RUN-MARKER") {
                break;
            }
        }
        assert!(String::from_utf8_lossy(&output).contains("RUN-MARKER"));

        protocol::send(&mut stream, &ClientMessage::Restart).unwrap();
        let mut second_pid = None;
        for _ in 0..12 {
            match protocol::receive::<ServerMessage>(&mut stream).unwrap() {
                ServerMessage::Restarted { pid } => {
                    second_pid = Some(pid);
                    break;
                }
                ServerMessage::Error(error) => panic!("rerun reported: {error}"),
                _ => {}
            }
        }
        let second_pid = second_pid.expect("the supervisor never reported a rerun");
        assert_ne!(second_pid, first_pid, "rerun reused the old process");

        // The fresh run produces the marker again, and the session entry tracks it.
        let mut rerun_output = Vec::new();
        for _ in 0..8 {
            if let ServerMessage::Output { bytes, .. } =
                protocol::receive::<ServerMessage>(&mut stream).unwrap()
            {
                rerun_output.extend(bytes);
            }
            if String::from_utf8_lossy(&rerun_output).contains("RUN-MARKER") {
                break;
            }
        }
        assert!(
            String::from_utf8_lossy(&rerun_output).contains("RUN-MARKER"),
            "the rerun produced no output"
        );

        let entry = session::read_entry(&dir).unwrap();
        assert_eq!(entry.pid, second_pid);
        assert!(entry.running);
        assert_eq!(entry.exit_code, None);

        // A client attaching now replays only the current run, not both.
        let mut second = UnixStream::connect(dir.join("control.sock")).unwrap();
        second
            .set_read_timeout(Some(Duration::from_millis(400)))
            .unwrap();
        assert!(matches!(
            protocol::receive::<ServerMessage>(&mut second).unwrap(),
            ServerMessage::Hello { pid, running: true, .. } if pid == second_pid
        ));
        let mut replayed = Vec::new();
        while let Ok(ServerMessage::Output { bytes, .. }) =
            protocol::receive::<ServerMessage>(&mut second)
        {
            replayed.extend(bytes);
        }
        assert_eq!(
            String::from_utf8_lossy(&replayed)
                .matches("RUN-MARKER")
                .count(),
            1,
            "the replayed transcript still contains the previous run"
        );

        protocol::send(&mut second, &ClientMessage::Close).unwrap();
        let cleanup = std::time::Instant::now() + Duration::from_secs(6);
        while dir.exists() && std::time::Instant::now() < cleanup {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(!dir.exists(), "closing did not remove the rerun session");
    }

    fn read_until(stream: &mut UnixStream, marker: &str) -> String {
        let mut out = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < deadline
            && !String::from_utf8_lossy(&out).contains(marker)
        {
            match protocol::receive::<ServerMessage>(stream) {
                Ok(ServerMessage::Output { bytes, .. }) => out.extend(bytes),
                Ok(ServerMessage::Exited(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
        String::from_utf8_lossy(&out).into_owned()
    }

    #[test]
    fn commands_run_in_the_directory_mission_was_launched_from() {
        let expected = env::current_dir().unwrap().canonicalize().unwrap();
        let dir = test_dir("cwd");
        let supervisor_dir = dir.clone();
        thread::spawn(move || {
            let _ = run(
                supervisor_dir,
                vec![
                    "/bin/bash".into(),
                    "-c".into(),
                    "echo CWD=[$(pwd -P)]; sleep 30".into(),
                ],
            );
        });

        let mut stream = wait_for_socket(&dir);
        assert!(matches!(
            protocol::receive::<ServerMessage>(&mut stream).unwrap(),
            ServerMessage::Hello { .. }
        ));
        let marker = format!("CWD=[{}]", expected.display());
        let first = read_until(&mut stream, &marker);
        assert!(
            first.contains(&marker),
            "expected {marker} in the first run, got: {first}"
        );

        // A rerun must land in the same directory, not the supervisor's fallback.
        protocol::send(&mut stream, &ClientMessage::Restart).unwrap();
        let mut restarted = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < deadline && !restarted {
            match protocol::receive::<ServerMessage>(&mut stream) {
                Ok(ServerMessage::Restarted { .. }) => restarted = true,
                Ok(ServerMessage::Error(error)) => panic!("rerun reported: {error}"),
                Err(_) => break,
                Ok(_) => {}
            }
        }
        assert!(restarted, "the supervisor never reported a rerun");
        let second = read_until(&mut stream, &marker);
        assert!(
            second.contains(&marker),
            "expected {marker} after the rerun, got: {second}"
        );

        protocol::send(&mut stream, &ClientMessage::Close).unwrap();
        let cleanup = std::time::Instant::now() + Duration::from_secs(6);
        while dir.exists() && std::time::Instant::now() < cleanup {
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn rerunning_a_finished_command_starts_it_again() {
        let dir = test_dir("rerun-finished");
        let supervisor_dir = dir.clone();
        thread::spawn(move || {
            let _ = run(
                supervisor_dir,
                vec!["/bin/bash".into(), "-c".into(), "echo ONCE".into()],
            );
        });

        let mut stream = wait_for_socket(&dir);
        // The command may already have finished before this client attached, in
        // which case Hello reports it rather than replaying an Exited message.
        let mut finished = match protocol::receive::<ServerMessage>(&mut stream).unwrap() {
            ServerMessage::Hello { running, .. } => !running,
            other => panic!("expected Hello, got {other:?}"),
        };
        while !finished {
            if matches!(
                protocol::receive::<ServerMessage>(&mut stream).unwrap(),
                ServerMessage::Exited(_)
            ) {
                finished = true;
            }
        }

        protocol::send(&mut stream, &ClientMessage::Restart).unwrap();
        let mut restarted = false;
        for _ in 0..8 {
            match protocol::receive::<ServerMessage>(&mut stream).unwrap() {
                ServerMessage::Restarted { .. } => {
                    restarted = true;
                    break;
                }
                ServerMessage::Error(error) => panic!("rerun reported: {error}"),
                _ => {}
            }
        }
        assert!(restarted, "a finished command could not be rerun");
        assert!(session::read_entry(&dir).unwrap().running);

        protocol::send(&mut stream, &ClientMessage::Close).unwrap();
        let cleanup = std::time::Instant::now() + Duration::from_secs(6);
        while dir.exists() && std::time::Instant::now() < cleanup {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(!dir.exists(), "closing did not remove the session");
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

        let mut stream = wait_for_socket(&dir);
        assert!(matches!(
            protocol::receive::<ServerMessage>(&mut stream).unwrap(),
            ServerMessage::Hello { running: true, .. }
        ));
        protocol::send(&mut stream, &ClientMessage::Input(b"ac\x1b[Db\r".to_vec())).unwrap();
        // A busy machine splits the echo across an unpredictable number of
        // messages, so read until the marker appears rather than a fixed count.
        let mut output = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < deadline
            && !String::from_utf8_lossy(&output).contains("GOT:<abc>")
        {
            match protocol::receive::<ServerMessage>(&mut stream) {
                Ok(ServerMessage::Output { bytes, .. }) => output.extend(bytes),
                Ok(ServerMessage::Exited(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
        assert!(String::from_utf8_lossy(&output).contains("GOT:<abc>"));
        protocol::send(&mut stream, &ClientMessage::Stop).unwrap();
        let mut exited = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < deadline && !exited {
            match protocol::receive::<ServerMessage>(&mut stream) {
                Ok(ServerMessage::Exited(_)) => exited = true,
                Err(_) => break,
                Ok(_) => {}
            }
        }
        assert!(
            exited,
            "the graceful stop sequence did not stop the PTY process"
        );
        protocol::send(&mut stream, &ClientMessage::Close).unwrap();
        let cleanup_deadline = std::time::Instant::now() + Duration::from_secs(6);
        while dir.exists() && std::time::Instant::now() < cleanup_deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(!dir.exists(), "closing did not remove the finished session");
    }
}
