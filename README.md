# mission

`mission` runs a command in a real pseudo-terminal, keeps it alive after the dashboard is closed, and provides a modern terminal UI for interaction and resource monitoring.

```console
mission python train.py --epochs 100
```

Arguments are passed directly to the program. Your shell still handles quoting, so paths and arguments containing spaces work normally:

```console
mission python "experiments/long run.py" --output "model snapshots"
```

## Features

- Detachable PTY session with ANSI color, interactive input, paste, resize, and terminal logging.
- Reattachment from another terminal while the command continues in the background.
- Global CPU and RAM histories plus CPU/RAM totals for the complete spawned process tree.
- Runtime NVIDIA detection via NVML, with separate filled GPU-utilization and VRAM charts for every GPU, plus process-tree GPU utilization and process-tree VRAM allocation.
- On Hopper and newer NVIDIA hardware, best-effort NVML GPM charts for SM and Tensor Core utilization. Unsupported drivers and GPUs fall back cleanly to standard GPU/VRAM metrics.
- Filled Braille area charts with exact current values, keyboard navigation, mouse-clickable tabs/settings, and persistent refresh/history settings.
- Timestamped terminal rows with configurable `info`, `warning`, and `error` background highlighting plus case-insensitive inline status-keyword colors.
- One reliable stop action that escalates from `SIGINT` to `SIGTERM` and finally `SIGKILL` only when necessary.

AMD-specific GPU monitoring is not currently included. CPU/RAM monitoring works without any GPU libraries, and NVIDIA support is loaded dynamically, so the binary runs normally on machines without NVIDIA hardware.

## Install

Mission currently targets Unix terminals (Linux is the primary target).

```console
cargo install --path .
```

## Sessions

Detach with `Esc`. Detaching never stops the command. `Ctrl+Z` stops the command before closing the dashboard.

```console
mission --list
mission --attach <session-id-or-prefix>
mission --clean
```

Terminal output is retained at `$XDG_STATE_HOME/mission/sessions/<id>/terminal.log` (or the platform-equivalent local state directory). Completed supervisors shut down after a short grace period; attaching to an older completed session prints its retained terminal log.

## Controls

| Key | Action |
| --- | --- |
| `Tab`, `Shift+Tab` | Cycle through tabs |
| `Ctrl+C` | Copy the full terminal scrollback without ANSI styling or the timestamp gutter (system clipboard, falling back to OSC 52) |
| `Ctrl+X` | Stop the process, escalating from `SIGINT` to `SIGTERM` and then `SIGKILL` |
| `Ctrl+R` | Rerun the command in this session, stopping the current process first |
| `Ctrl+S` | Save the session log to the save directory (Settings tab; defaults to the platform data directory) |
| `Esc` | Detach safely and leave the process running |
| `Ctrl+Z` | Stop the process, close the dashboard, and detach |
| Mouse click | Select tabs and settings |
| Arrow keys | Navigate the Settings page |

When the Terminal tab is active, ordinary keys and control sequences go to the child process except for mission's four control shortcuts. Cursor keys follow the application-cursor mode requested by the child, modified keys use xterm-compatible parameters, and paste honors bracketed-paste mode.

Running `mission` without arguments opens a searchable session picker when saved sessions exist. If there are no sessions, it prints the CLI help instead.

Finished sessions remain attachable from the picker or with `mission --attach <id>`. Their retained output opens in the same dashboard as a read-only terminal log, with scrollback navigation and clipboard copying. Use `Ctrl+Z` from that view to remove the saved session.

Mission refuses to supervise itself, whether invoked directly, through an explicit path or symlink, or from a shell already running inside a mission session. Use the session picker or `mission --attach` instead of nesting sessions.

Because a real PTY combines stdout and stderr into one byte stream, preserving fully interactive terminal behavior and perfectly identifying the original stream are mutually exclusive. Mission keeps the PTY semantics, inherits the terminal's default background for ordinary output, and uses configurable keyword backgrounds for stderr-like severity highlighting.

## NVIDIA metric scope

Basic GPU load means the percentage of time at least one kernel was executing during NVML's sample period; it does not identify a particular execution unit. Fine-grained SM/Tensor metrics use NVML's GPM interface where supported (Hopper+ and a sufficiently recent driver). Mission does not inject CUPTI into the launched application or replay kernels, because doing so would turn lightweight monitoring into intrusive profiling.

## Architecture

The initial `mission` process launches a detached supervisor. The supervisor owns the PTY master, command process, rolling transcript, log, and Unix control socket. The TUI is only a client: it can come and go without affecting the workload. Session state and configuration use small JSON files, while terminal traffic uses a length-delimited binary protocol.
