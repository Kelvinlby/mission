mod monitor;
mod protocol;
mod session;
mod supervisor;
mod ui;

use anyhow::{Context, Result, bail};
use clap::{CommandFactory, Parser};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "mission",
    version,
    about = "Keep commands alive in detachable, monitored terminal sessions",
    trailing_var_arg = true,
    disable_help_subcommand = true
)]
struct Cli {
    /// Reattach to a running session (an unambiguous id prefix is accepted)
    #[arg(long, value_name = "ID")]
    attach: Option<String>,
    /// List known sessions
    #[arg(long)]
    list: bool,
    /// Remove records for sessions which are no longer running
    #[arg(long)]
    clean: bool,
    /// Command and arguments to run; no shell quoting is added or removed
    #[arg(value_name = "COMMAND", allow_hyphen_values = true)]
    command: Vec<String>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("mission: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    if std::env::var_os("MISSION_SUPERVISOR").is_some() {
        let mut args = std::env::args_os().skip(1);
        let session_dir = PathBuf::from(args.next().context("missing supervisor session path")?);
        let command: Vec<String> = args.map(|arg| arg.to_string_lossy().into_owned()).collect();
        if let Err(error) = supervisor::run(session_dir.clone(), command) {
            let _ = std::fs::write(session_dir.join("supervisor.error"), format!("{error:#}\n"));
            return Err(error);
        }
        return Ok(());
    }
    if std::env::var_os("MISSION_ACTIVE_SESSION").is_some() {
        bail!("refusing to run mission from inside another mission session");
    }
    let cli = Cli::parse();
    if cli.list {
        return session::print_sessions();
    }
    if cli.clean {
        println!(
            "Removed {} stale session record(s).",
            session::clean_stale()?
        );
        return Ok(());
    }
    if let Some(prefix) = cli.attach {
        return ui::run(session::resolve(&prefix)?);
    }
    if cli.command.is_empty() {
        let entries = session::sessions()?;
        if entries.is_empty() {
            Cli::command().print_help()?;
            println!();
            return Ok(());
        }
        if let Some(entry) = ui::select_session(entries)? {
            return ui::run(entry);
        }
        return Ok(());
    }
    ui::run(session::launch(&cli.command)?)
}
