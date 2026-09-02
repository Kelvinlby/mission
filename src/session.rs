use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionEntry {
    pub id: String,
    pub command: Vec<String>,
    pub pid: u32,
    pub created_at: u64,
    pub running: bool,
    pub exit_code: Option<i32>,
    #[serde(skip)]
    pub dir: PathBuf,
}

impl SessionEntry {
    pub fn socket_path(&self) -> PathBuf {
        self.dir.join("control.sock")
    }
    pub fn log_path(&self) -> PathBuf {
        self.dir.join("terminal.log")
    }
    pub fn command_display(&self) -> String {
        shell_words::join(&self.command)
    }
}

pub fn root_dir() -> Result<PathBuf> {
    let base = dirs::state_dir()
        .or_else(dirs::data_local_dir)
        .context("cannot determine state directory")?;
    Ok(base.join("mission").join("sessions"))
}

pub fn launch(command: &[String]) -> Result<SessionEntry> {
    ensure_not_mission(command)?;
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let id = format!("{:x}-{}", stamp, std::process::id());
    let dir = root_dir()?.join(&id);
    fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    let executable = std::env::current_exe().context("locate mission executable")?;
    let mut child_command = Command::new(executable);
    child_command
        .env("MISSION_SUPERVISOR", "1")
        .arg(&dir)
        .args(command)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    unsafe {
        child_command.pre_exec(|| {
            nix::unistd::setsid().map_err(std::io::Error::other)?;
            Ok(())
        });
    }
    child_command
        .spawn()
        .context("start detached mission supervisor")?;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if dir.join("session.json").exists() && dir.join("control.sock").exists() {
            return read_entry(&dir);
        }
        let error_path = dir.join("supervisor.error");
        if error_path.exists() {
            let detail = fs::read_to_string(&error_path)
                .unwrap_or_else(|_| "detached supervisor failed without details".into());
            bail!("supervisor could not start the command: {}", detail.trim());
        }
        if std::time::Instant::now() >= deadline {
            bail!("supervisor did not start; inspect {}", error_path.display());
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn ensure_not_mission(command: &[String]) -> Result<()> {
    let Some(program) = command.first() else {
        return Ok(());
    };
    let path = Path::new(program);
    let named_mission = path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "mission" || name == "mission.exe");
    let resolves_to_self = resolve_program(program)
        .and_then(|resolved| resolved.canonicalize().ok())
        .zip(
            std::env::current_exe()
                .ok()
                .and_then(|path| path.canonicalize().ok()),
        )
        .is_some_and(|(program, current)| program == current);
    if named_mission || resolves_to_self {
        bail!("refusing to supervise mission itself; attach to the existing session instead");
    }
    Ok(())
}

fn resolve_program(program: &str) -> Option<PathBuf> {
    let path = Path::new(program);
    if path.components().count() > 1 {
        return Some(path.to_owned());
    }
    std::env::var_os("PATH")?
        .to_string_lossy()
        .split(':')
        .map(|directory| Path::new(directory).join(program))
        .find(|candidate| candidate.is_file())
}

pub fn write_entry(entry: &SessionEntry) -> Result<()> {
    let temp = entry.dir.join("session.json.tmp");
    fs::write(&temp, serde_json::to_vec_pretty(entry)?)?;
    fs::rename(temp, entry.dir.join("session.json"))?;
    Ok(())
}

pub fn read_entry(dir: &Path) -> Result<SessionEntry> {
    let mut entry: SessionEntry = serde_json::from_slice(&fs::read(dir.join("session.json"))?)?;
    entry.dir = dir.to_owned();
    Ok(entry)
}

pub fn sessions() -> Result<Vec<SessionEntry>> {
    let root = root_dir()?;
    fs::create_dir_all(&root)?;
    let mut entries = Vec::new();
    for child in fs::read_dir(root)? {
        let path = child?.path();
        if let Ok(mut entry) = read_entry(&path) {
            if entry.running
                && (!entry.socket_path().exists()
                    || nix::sys::signal::kill(nix::unistd::Pid::from_raw(entry.pid as i32), None)
                        .is_err())
            {
                entry.running = false;
            }
            entries.push(entry);
        }
    }
    entries.sort_by_key(|entry| std::cmp::Reverse(entry.created_at));
    Ok(entries)
}

pub fn resolve(prefix: &str) -> Result<SessionEntry> {
    let matches: Vec<_> = sessions()?
        .into_iter()
        .filter(|entry| entry.id.starts_with(prefix))
        .collect();
    match matches.as_slice() {
        [] => bail!("no session matches {prefix:?}"),
        [entry] => Ok(entry.clone()),
        _ => bail!("session prefix {prefix:?} is ambiguous"),
    }
}

pub fn print_sessions() -> Result<()> {
    let entries = sessions()?;
    if entries.is_empty() {
        println!("No mission sessions.");
        return Ok(());
    }
    println!("{:<20} {:<10} COMMAND", "ID", "STATUS");
    for entry in entries {
        let status = if entry.running {
            "running".into()
        } else {
            format!(
                "exit({})",
                entry.exit_code.map_or("?".into(), |c| c.to_string())
            )
        };
        println!(
            "{:<20} {:<10} {}",
            entry.id,
            status,
            entry.command_display()
        );
    }
    Ok(())
}

pub fn clean_stale() -> Result<usize> {
    let stale: Vec<_> = sessions()?
        .into_iter()
        .filter(|entry| !entry.running)
        .collect();
    for entry in &stale {
        fs::remove_dir_all(&entry.dir)?;
    }
    Ok(stale.len())
}

pub fn remove(entry: &SessionEntry) -> Result<()> {
    fs::remove_dir_all(&entry.dir).with_context(|| format!("remove session {}", entry.id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_direct_mission_recursion() {
        assert!(ensure_not_mission(&["mission".into()]).is_err());
        assert!(ensure_not_mission(&["/usr/local/bin/mission".into()]).is_err());
        assert!(ensure_not_mission(&["python".into(), "mission.py".into()]).is_ok());
    }

    #[test]
    fn rejects_the_current_binary_by_resolved_path() {
        let current = std::env::current_exe()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert!(ensure_not_mission(&[current]).is_err());
    }
}
