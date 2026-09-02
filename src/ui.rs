use crate::{
    monitor::{Histories, Monitor, Sample},
    protocol::{self, ClientMessage, ServerMessage},
    session::{self, SessionEntry},
};
use anyhow::{Context, Result};
use crossterm::{
    clipboard::CopyToClipboard,
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyEventKind,
        KeyModifiers, MouseButton, MouseEventKind,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols::Marker,
    text::{Line, Span},
    widgets::{
        Axis, Block, BorderType, Borders, Chart, Dataset, GraphType, List, ListItem, ListState,
        Paragraph, Tabs, Widget, Wrap,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    fs,
    io::{self},
    os::unix::net::UnixStream,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

const ACCENT: Color = Color::Rgb(103, 232, 249);
const PURPLE: Color = Color::Rgb(192, 132, 252);
const GREEN: Color = Color::Rgb(74, 222, 128);
const RED: Color = Color::Rgb(248, 113, 113);
const YELLOW: Color = Color::Rgb(250, 204, 21);
const ORANGE: Color = Color::Rgb(251, 146, 60);
const MUTED: Color = Color::Rgb(113, 113, 122);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
struct Config {
    refresh_ms: u64,
    chart_points: usize,
    mouse_capture: bool,
    timestamps: bool,
    highlight_info: bool,
    highlight_warning: bool,
    highlight_error: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            refresh_ms: 500,
            chart_points: 120,
            mouse_capture: true,
            timestamps: true,
            highlight_info: true,
            highlight_warning: true,
            highlight_error: true,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Tab {
    Terminal,
    Resources,
    Settings,
    Help,
}

impl Tab {
    fn index(self) -> usize {
        match self {
            Self::Terminal => 0,
            Self::Resources => 1,
            Self::Settings => 2,
            Self::Help => 3,
        }
    }
    fn from_index(index: usize) -> Self {
        [Self::Terminal, Self::Resources, Self::Settings, Self::Help][index.min(3)]
    }
}

struct App {
    entry: SessionEntry,
    tab: Tab,
    parser: vt100::Parser,
    sample: Sample,
    histories: Histories,
    config: Config,
    settings_row: usize,
    running: bool,
    exit_code: Option<i32>,
    status: String,
    terminal_area: Rect,
    row_timestamps: VecDeque<String>,
}

pub fn run(entry: SessionEntry) -> Result<()> {
    let mut stream = UnixStream::connect(entry.socket_path()).ok();
    let (tx, rx) = mpsc::channel();
    if let Some(connection) = stream.as_ref() {
        let mut receiver = connection.try_clone()?;
        thread::spawn(move || {
            while let Ok(message) = protocol::receive::<ServerMessage>(&mut receiver) {
                if tx.send(message).is_err() {
                    break;
                }
            }
        });
    }

    let config = load_config();
    let connected = stream.is_some();
    let (window_columns, window_rows) = crossterm::terminal::size().unwrap_or((120, 34));
    let gutter = if config.timestamps { 9 } else { 0 };
    let terminal_rows = window_rows.saturating_sub(4).max(1);
    let terminal_columns = window_columns.saturating_sub(2 + gutter).max(1);
    let mut app = App {
        running: entry.running && connected,
        parser: vt100::Parser::new(terminal_rows, terminal_columns, 10_000),
        sample: Sample::default(),
        histories: Histories::default(),
        config,
        settings_row: 0,
        tab: Tab::Terminal,
        exit_code: entry.exit_code,
        status: String::new(),
        terminal_area: Rect::default(),
        row_timestamps: VecDeque::new(),
        entry,
    };
    if !connected {
        let log = fs::read(app.entry.log_path()).unwrap_or_default();
        let timestamp_ms = fs::metadata(app.entry.log_path())
            .and_then(|metadata| metadata.modified())
            .ok()
            .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(app.entry.created_at * 1_000, |duration| {
                duration.as_millis() as u64
            });
        process_terminal_output(&mut app, &log, timestamp_ms);
    }
    let mut monitor = Monitor::new(if app.running { app.entry.pid } else { u32::MAX });
    app.sample = monitor.sample();
    app.histories.push(&app.sample);

    let _guard = TerminalGuard::enter(app.config.mouse_capture)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    terminal.clear()?;
    let mut next_sample = Instant::now() + Duration::from_millis(app.config.refresh_ms);
    let mut last_size = (0, 0);

    loop {
        while let Ok(message) = rx.try_recv() {
            apply_server_message(&mut app, message);
        }
        if Instant::now() >= next_sample {
            app.sample = monitor.sample();
            app.histories.push(&app.sample);
            next_sample = Instant::now() + Duration::from_millis(app.config.refresh_ms);
        }
        terminal.draw(|frame| draw(frame, &mut app))?;
        let size = (app.terminal_area.height, app.terminal_area.width);
        if size != last_size && size.0 > 0 && size.1 > 0 {
            app.parser.screen_mut().set_size(size.0, size.1);
            app.row_timestamps.resize(size.0 as usize, String::new());
            if let Some(connection) = stream.as_mut() {
                send(
                    connection,
                    ClientMessage::Resize {
                        rows: size.0,
                        cols: size.1,
                    },
                )?;
            }
            last_size = size;
        }

        let timeout = next_sample
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(50));
        if event::poll(timeout)? {
            let event = event::read()?;
            if handle_event(&mut app, &mut stream, event)? {
                break;
            }
        }
    }
    if let Some(connection) = stream.as_mut() {
        let _ = send(connection, ClientMessage::Detach);
    }
    Ok(())
}

pub fn select_session(entries: Vec<SessionEntry>) -> Result<Option<SessionEntry>> {
    let config = load_config();
    let _guard = TerminalGuard::enter(config.mouse_capture)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    terminal.clear()?;
    let mut query = String::new();
    let mut selected = 0_usize;

    loop {
        let filtered = filtered_sessions(&entries, &query);
        selected = selected.min(filtered.len().saturating_sub(1));
        terminal.draw(|frame| draw_session_picker(frame, &filtered, &query, selected))?;

        if !event::poll(Duration::from_millis(100))? {
            continue;
        }
        match event::read()? {
            Event::Key(key)
                if key.code == KeyCode::Esc
                    || (matches!(key.code, KeyCode::Char('d' | 'q'))
                        && key.modifiers.contains(KeyModifiers::CONTROL)) =>
            {
                return Ok(None);
            }
            Event::Key(key) if key.code == KeyCode::Enter => {
                if let Some(entry) = filtered.get(selected) {
                    return Ok(Some((*entry).clone()));
                }
            }
            Event::Key(key) if key.code == KeyCode::Up => {
                selected = selected.saturating_sub(1);
            }
            Event::Key(key) if key.code == KeyCode::Down => {
                selected = (selected + 1).min(filtered.len().saturating_sub(1));
            }
            Event::Key(key) if key.code == KeyCode::Backspace => {
                query.pop();
                selected = 0;
            }
            Event::Key(key) => {
                if let KeyCode::Char(character) = key.code
                    && !key
                        .modifiers
                        .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
                {
                    query.push(character);
                    selected = 0;
                }
            }
            Event::Paste(text) => {
                query.push_str(&text);
                selected = 0;
            }
            Event::Mouse(mouse)
                if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left))
                    && mouse.row >= 7 =>
            {
                let index = mouse.row.saturating_sub(7) as usize;
                if index < filtered.len() {
                    selected = index;
                }
            }
            _ => {}
        }
    }
}

fn filtered_sessions<'a>(entries: &'a [SessionEntry], query: &str) -> Vec<&'a SessionEntry> {
    let query = query.to_ascii_lowercase();
    entries
        .iter()
        .filter(|entry| {
            query.is_empty()
                || entry.id.to_ascii_lowercase().contains(&query)
                || entry
                    .command_display()
                    .to_ascii_lowercase()
                    .contains(&query)
        })
        .collect()
}

fn draw_session_picker(frame: &mut Frame, entries: &[&SessionEntry], query: &str, selected: usize) {
    let areas = Layout::vertical([
        Constraint::Length(2),
        Constraint::Length(3),
        Constraint::Min(3),
        Constraint::Length(1),
    ])
    .margin(1)
    .split(frame.area());
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                "mission",
                Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
            ),
            Span::styled("  Resume a session", Style::default().fg(MUTED)),
        ])),
        areas[0],
    );
    frame.render_widget(
        Paragraph::new(format!("{query}▌"))
            .style(Style::default().fg(Color::White))
            .block(panel(" Search by command or id ")),
        areas[1],
    );
    let items: Vec<ListItem<'_>> = entries
        .iter()
        .map(|entry| {
            let status = if entry.running { "running" } else { "finished" };
            let color = if entry.running { GREEN } else { MUTED };
            ListItem::new(Line::from(vec![
                Span::styled(format!(" {status:<9}"), Style::default().fg(color)),
                Span::styled(format!("{:<20}", entry.id), Style::default().fg(ACCENT)),
                Span::raw(entry.command_display()),
            ]))
        })
        .collect();
    let mut state = ListState::default().with_selected((!entries.is_empty()).then_some(selected));
    frame.render_stateful_widget(
        List::new(items)
            .block(panel(&format!(" Sessions · {} matches ", entries.len())))
            .highlight_style(
                Style::default()
                    .bg(Color::Rgb(18, 36, 42))
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("›"),
        areas[2],
        &mut state,
    );
    frame.render_widget(
        Paragraph::new("type to search  ↑/↓ select  Enter resume  Esc close")
            .style(Style::default().fg(MUTED)),
        areas[3],
    );
}

fn apply_server_message(app: &mut App, message: ServerMessage) {
    match message {
        ServerMessage::Hello { running, .. } => {
            app.running = running;
        }
        ServerMessage::Output {
            bytes,
            timestamp_ms,
        } => process_terminal_output(app, &bytes, timestamp_ms),
        ServerMessage::Exited(code) => {
            app.running = false;
            app.exit_code = code;
            app.status = format!(
                "process exited ({})",
                code.map_or("signal".into(), |c| c.to_string())
            );
        }
        ServerMessage::Error(error) => app.status = error,
    }
}

fn process_terminal_output(app: &mut App, bytes: &[u8], timestamp_ms: u64) {
    let rows = app.parser.screen().size().0 as usize;
    app.row_timestamps.resize(rows, String::new());
    let timestamp = local_time(timestamp_ms);
    for byte in bytes {
        let before = app.parser.screen().cursor_position();
        app.parser.process(std::slice::from_ref(byte));
        let after = app.parser.screen().cursor_position();
        let scrolled = before.0 == rows.saturating_sub(1) as u16
            && after.0 == before.0
            && (*byte == b'\n' || after.1 < before.1);
        if scrolled {
            app.row_timestamps.pop_front();
            app.row_timestamps.push_back(timestamp.clone());
        } else {
            if let Some(mark) = app.row_timestamps.get_mut(before.0 as usize)
                && mark.is_empty()
            {
                *mark = timestamp.clone();
            }
            if after.0 != before.0
                && let Some(mark) = app.row_timestamps.get_mut(after.0 as usize)
            {
                *mark = timestamp.clone();
            }
        }
    }
}

fn local_time(timestamp_ms: u64) -> String {
    let seconds = (timestamp_ms / 1_000) as libc::time_t;
    let mut local = std::mem::MaybeUninit::<libc::tm>::uninit();
    let result = unsafe { libc::localtime_r(&seconds, local.as_mut_ptr()) };
    if result.is_null() {
        return "--:--:--".into();
    }
    let local = unsafe { local.assume_init() };
    format!(
        "{:02}:{:02}:{:02}",
        local.tm_hour, local.tm_min, local.tm_sec
    )
}

fn copy_terminal(app: &mut App) -> Result<()> {
    let contents = terminal_contents(app.parser.screen());
    execute!(
        io::stdout(),
        CopyToClipboard::to_clipboard_from(contents.as_bytes())
    )?;
    app.status = format!("copied {} characters", contents.chars().count());
    Ok(())
}

fn terminal_contents(screen: &vt100::Screen) -> String {
    let mut screen = screen.clone();
    let (rows, columns) = screen.size();
    screen.set_scrollback(usize::MAX);
    let maximum_offset = screen.scrollback();
    let page_size = usize::from(rows.max(1));
    let mut offset = maximum_offset;
    let mut copied_until = 0_usize;
    let mut contents = String::new();

    loop {
        screen.set_scrollback(offset);
        let page_start = maximum_offset.saturating_sub(offset);
        let skip = copied_until.saturating_sub(page_start).min(page_size);
        for (row, text) in screen.rows(0, columns).enumerate().skip(skip) {
            contents.push_str(&text);
            if !screen.row_wrapped(row as u16) {
                contents.push('\n');
            }
        }
        copied_until = page_start + page_size;
        if offset == 0 {
            break;
        }
        offset = offset.saturating_sub(page_size);
    }
    contents.trim_end_matches('\n').to_owned()
}

fn handle_event(app: &mut App, stream: &mut Option<UnixStream>, event: Event) -> Result<bool> {
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Release => {}
        Event::Key(key)
            if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) =>
        {
            copy_terminal(app)?;
        }
        Event::Key(key)
            if key.code == KeyCode::Char('x') && key.modifiers.contains(KeyModifiers::CONTROL) =>
        {
            if app.running {
                if let Some(connection) = stream.as_mut() {
                    send(connection, ClientMessage::Stop)?;
                }
                app.status = "stopping: interrupt → terminate → kill".into();
            } else {
                app.status = "process is not running".into();
            }
        }
        Event::Key(key)
            if key.code == KeyCode::Char('d') && key.modifiers.contains(KeyModifiers::CONTROL) =>
        {
            return Ok(true);
        }
        Event::Key(key)
            if key.code == KeyCode::Char('z') && key.modifiers.contains(KeyModifiers::CONTROL) =>
        {
            if let Some(connection) = stream.as_mut() {
                send(connection, ClientMessage::Close)?;
            } else if !app.entry.running {
                session::remove(&app.entry)?;
            } else {
                app.status = "cannot close: supervisor is unreachable".into();
                return Ok(false);
            }
            return Ok(true);
        }
        Event::Key(key) if key.code == KeyCode::Tab => {
            app.tab = Tab::from_index((app.tab.index() + 1) % 4)
        }
        Event::Key(key) if key.code == KeyCode::BackTab => {
            app.tab = Tab::from_index((app.tab.index() + 3) % 4)
        }
        Event::Key(key) if app.tab == Tab::Terminal && !app.running => match key.code {
            KeyCode::Up => scroll_terminal(app, 1, true),
            KeyCode::Down => scroll_terminal(app, 1, false),
            KeyCode::PageUp => scroll_terminal(app, app.terminal_area.height as usize, true),
            KeyCode::PageDown => scroll_terminal(app, app.terminal_area.height as usize, false),
            KeyCode::Home => app.parser.screen_mut().set_scrollback(usize::MAX),
            KeyCode::End => app.parser.screen_mut().set_scrollback(0),
            _ => {}
        },
        Event::Key(key) if app.tab == Tab::Terminal && app.running => {
            if let Some(bytes) = encode_key(key, app.parser.screen().application_cursor())
                && let Some(connection) = stream.as_mut()
            {
                send(connection, ClientMessage::Input(bytes))?;
            }
        }
        Event::Key(key) if app.tab == Tab::Settings => handle_settings(app, key)?,
        Event::Key(key) => match key.code {
            KeyCode::Tab | KeyCode::Right => app.tab = Tab::from_index((app.tab.index() + 1) % 4),
            KeyCode::BackTab | KeyCode::Left => {
                app.tab = Tab::from_index((app.tab.index() + 3) % 4)
            }
            _ => {}
        },
        Event::Paste(text) if app.tab == Tab::Terminal && app.running => {
            let bytes = if app.parser.screen().bracketed_paste() {
                format!("\x1b[200~{text}\x1b[201~").into_bytes()
            } else {
                text.into_bytes()
            };
            if let Some(connection) = stream.as_mut() {
                send(connection, ClientMessage::Input(bytes))?;
            }
        }
        Event::Mouse(mouse)
            if app.tab == Tab::Terminal && matches!(mouse.kind, MouseEventKind::ScrollUp) =>
        {
            scroll_terminal(app, 3, true);
        }
        Event::Mouse(mouse)
            if app.tab == Tab::Terminal && matches!(mouse.kind, MouseEventKind::ScrollDown) =>
        {
            scroll_terminal(app, 3, false);
        }
        Event::Mouse(mouse) if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) => {
            if mouse.row == 0 {
                if let Some(tab) = tab_at_column(mouse.column) {
                    app.tab = tab;
                }
            } else if app.tab == Tab::Settings {
                app.settings_row = mouse.row.saturating_sub(5).min(6) as usize;
            }
        }
        _ => {}
    }
    Ok(false)
}

fn scroll_terminal(app: &mut App, amount: usize, upward: bool) {
    let current = app.parser.screen().scrollback();
    app.parser.screen_mut().set_scrollback(if upward {
        current.saturating_add(amount)
    } else {
        current.saturating_sub(amount)
    });
    app.status = format!("log scrollback: {} lines", app.parser.screen().scrollback());
}

fn handle_settings(app: &mut App, key: KeyEvent) -> Result<()> {
    match key.code {
        KeyCode::Up => app.settings_row = app.settings_row.saturating_sub(1),
        KeyCode::Down => app.settings_row = (app.settings_row + 1).min(6),
        KeyCode::Left | KeyCode::Char('-') => adjust_setting(app, false),
        KeyCode::Right | KeyCode::Char('+') | KeyCode::Char('=') | KeyCode::Enter => {
            adjust_setting(app, true)
        }
        _ => return Ok(()),
    }
    save_config(&app.config)?;
    app.status = "settings saved".into();
    Ok(())
}

fn adjust_setting(app: &mut App, forward: bool) {
    match app.settings_row {
        0 => {
            app.config.refresh_ms = next_refresh_interval(app.config.refresh_ms, forward);
        }
        1 => {
            let values = [30, 60, 120, 180];
            let index = values
                .iter()
                .position(|value| *value == app.config.chart_points)
                .unwrap_or(2);
            app.config.chart_points = values[if forward {
                (index + 1).min(values.len() - 1)
            } else {
                index.saturating_sub(1)
            }];
        }
        2 => app.config.mouse_capture = !app.config.mouse_capture,
        3 => app.config.timestamps = !app.config.timestamps,
        4 => app.config.highlight_info = !app.config.highlight_info,
        5 => app.config.highlight_warning = !app.config.highlight_warning,
        6 => app.config.highlight_error = !app.config.highlight_error,
        _ => {}
    }
}

fn next_refresh_interval(current: u64, forward: bool) -> u64 {
    if forward {
        current.saturating_add(if current < 1_000 { 50 } else { 100 })
    } else {
        current
            .saturating_sub(if current <= 1_000 { 50 } else { 100 })
            .max(100)
    }
}

fn draw(frame: &mut Frame, app: &mut App) {
    let root = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(8),
        Constraint::Length(1),
    ])
    .split(frame.area());
    let titles = [" Terminal ", " Resources ", " Settings ", " Help "];
    frame.render_widget(
        Tabs::new(titles)
            .select(app.tab.index())
            .highlight_style(
                Style::default()
                    .fg(Color::Black)
                    .bg(ACCENT)
                    .add_modifier(Modifier::BOLD),
            )
            .style(Style::default().fg(MUTED))
            .padding("", "")
            .divider(" "),
        root[0],
    );
    match app.tab {
        Tab::Terminal => draw_terminal(frame, root[1], app),
        Tab::Resources => draw_resources(frame, root[1], app),
        Tab::Settings => draw_settings(frame, root[1], app),
        Tab::Help => draw_help(frame, root[1], app),
    }
    let state = if app.running {
        Span::styled("● RUNNING", Style::default().fg(GREEN))
    } else {
        Span::styled("● STOPPED", Style::default().fg(RED))
    };
    let shortcuts = if app.running {
        "^C copy  ^X stop  ^D detach  ^Z stop & close"
    } else {
        "^C copy  ↑/↓ PgUp/PgDn scroll  ^D detach  ^Z close"
    };
    let details = if app.status.is_empty() {
        format!("  pid {}  │  {shortcuts}", app.entry.pid)
    } else {
        format!("  pid {}  │  {}  │  {shortcuts}", app.entry.pid, app.status)
    };
    frame.render_widget(
        Paragraph::new(Line::from(vec![Span::raw("  "), state, Span::raw(details)]))
            .style(Style::default().fg(MUTED)),
        root[2],
    );
}

fn draw_terminal(frame: &mut Frame, area: Rect, app: &mut App) {
    let title = if app.running {
        format!(" {} ", app.entry.command_display())
    } else {
        format!(
            " {} · finished ({}) · read-only ",
            app.entry.command_display(),
            app.exit_code
                .map_or("signal".into(), |code| code.to_string())
        )
    };
    let block = panel(&title);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let columns = if app.config.timestamps {
        Layout::horizontal([Constraint::Length(9), Constraint::Min(1)]).split(inner)
    } else {
        Layout::horizontal([Constraint::Length(0), Constraint::Min(1)]).split(inner)
    };
    app.terminal_area = columns[1];
    if app.config.timestamps {
        let marks: Vec<Line<'_>> = app
            .row_timestamps
            .iter()
            .map(|timestamp| Line::styled(format!("{timestamp:>8}│"), Style::default().fg(MUTED)))
            .collect();
        frame.render_widget(Paragraph::new(marks), columns[0]);
    }
    frame.render_widget(
        VtWidget {
            screen: app.parser.screen(),
            config: &app.config,
        },
        columns[1],
    );
}

fn draw_resources(frame: &mut Frame, area: Rect, app: &App) {
    enum Card {
        SystemCpu,
        SystemRam,
        ProcessCpu,
        ProcessRam,
        Gpu(usize),
        Vram(usize),
        Sm(usize),
        Tensor(usize),
    }
    let mut cards = vec![
        Card::SystemCpu,
        Card::SystemRam,
        Card::ProcessCpu,
        Card::ProcessRam,
    ];
    for (index, gpu) in app.sample.gpus.iter().enumerate() {
        cards.push(Card::Gpu(index));
        cards.push(Card::Vram(index));
        if gpu.sm_percent.is_some() {
            cards.push(Card::Sm(index));
            cards.push(Card::Tensor(index));
        }
    }
    let card_count = cards.len();
    let row_count = card_count.div_ceil(2);
    let rows =
        Layout::vertical(vec![Constraint::Ratio(1, row_count as u32); row_count]).split(area);
    for (row_index, row) in rows.iter().enumerate() {
        let columns = Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(*row);
        for (column_index, column) in columns.iter().enumerate() {
            let card = row_index * 2 + column_index;
            match cards.get(card) {
                Some(Card::SystemCpu) => area_chart(
                    frame,
                    *column,
                    "CPU",
                    (app.sample.global_cpu, String::new()),
                    &app.histories.global_cpu,
                    ACCENT,
                    app.config.chart_points,
                ),
                Some(Card::SystemRam) => area_chart(
                    frame,
                    *column,
                    "RAM",
                    (
                        app.sample.global_memory,
                        format!(
                            "{} / {}",
                            bytes(app.sample.memory_used),
                            bytes(app.sample.memory_total)
                        ),
                    ),
                    &app.histories.global_memory,
                    PURPLE,
                    app.config.chart_points,
                ),
                Some(Card::ProcessCpu) => area_chart(
                    frame,
                    *column,
                    "PROCESS CPU",
                    (
                        app.sample.process_cpu,
                        format!("{} tasks", app.sample.process_count),
                    ),
                    &app.histories.process_cpu,
                    GREEN,
                    app.config.chart_points,
                ),
                Some(Card::ProcessRam) => area_chart(
                    frame,
                    *column,
                    "PROCESS RAM",
                    (
                        percent_of(app.sample.process_memory, app.sample.memory_total),
                        bytes(app.sample.process_memory),
                    ),
                    &app.histories.process_memory,
                    Color::Rgb(251, 146, 60),
                    app.config.chart_points,
                ),
                Some(Card::Gpu(index)) => {
                    let gpu = &app.sample.gpus[*index];
                    area_chart(
                        frame,
                        *column,
                        &indexed_metric("GPU", *index, app.sample.gpus.len()),
                        (
                            gpu.gpu_percent,
                            format!("mission {:5.1}%", gpu.process_gpu_percent),
                        ),
                        &app.histories.gpu[*index],
                        Color::Rgb(250, 204, 21),
                        app.config.chart_points,
                    );
                }
                Some(Card::Vram(index)) => {
                    let gpu = &app.sample.gpus[*index];
                    area_chart(
                        frame,
                        *column,
                        &indexed_metric("VRAM", *index, app.sample.gpus.len()),
                        (
                            gpu.memory_percent,
                            format!(
                                "{} / {} · mission {}",
                                bytes(gpu.memory_used),
                                bytes(gpu.memory_total),
                                bytes(gpu.process_memory)
                            ),
                        ),
                        &app.histories.vram[*index],
                        PURPLE,
                        app.config.chart_points,
                    );
                }
                Some(Card::Sm(index)) => area_chart(
                    frame,
                    *column,
                    &indexed_metric("SM", *index, app.sample.gpus.len()),
                    (
                        app.sample.gpus[*index].sm_percent.unwrap_or_default(),
                        String::new(),
                    ),
                    &app.histories.sm[*index],
                    ACCENT,
                    app.config.chart_points,
                ),
                Some(Card::Tensor(index)) => area_chart(
                    frame,
                    *column,
                    &indexed_metric("TENSOR", *index, app.sample.gpus.len()),
                    (
                        app.sample.gpus[*index].tensor_percent.unwrap_or_default(),
                        String::new(),
                    ),
                    &app.histories.tensor[*index],
                    GREEN,
                    app.config.chart_points,
                ),
                _ => {}
            }
        }
    }
}

fn area_chart(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    value: (f64, String),
    history: &VecDeque<f64>,
    color: Color,
    points: usize,
) {
    let (current, detail) = value;
    let y_upper = history.iter().copied().fold(100.0_f64, f64::max);
    let y_upper = (y_upper / 100.0).ceil() * 100.0;
    let data = filled_area_data(history, points, area.width, area.height, y_upper);
    let upper = points.max(2) as f64 - 1.0;
    let datasets = vec![
        Dataset::default()
            .marker(Marker::Braille)
            .graph_type(GraphType::Scatter)
            .style(Style::default().fg(color))
            .data(&data),
    ];
    let block = Block::default()
        .title(Line::styled(
            format!(" {title} "),
            Style::default()
                .fg(Color::Rgb(244, 244, 245))
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Rgb(63, 63, 70)));
    let widget = Chart::new(datasets)
        .block(block)
        .x_axis(Axis::default().bounds([0.0, upper]))
        .y_axis(Axis::default().bounds([0.0, y_upper]));
    frame.render_widget(widget, area);

    if area.width > 4 && area.height > 2 {
        let value = format!("{current:.1}%");
        let value_width = value.chars().count() as u16;
        frame.render_widget(
            Paragraph::new(value.clone()).style(
                Style::default()
                    .fg(Color::Rgb(250, 250, 250))
                    .add_modifier(Modifier::BOLD),
            ),
            Rect::new(area.x + 2, area.y + 1, value_width, 1),
        );
        let scale = format!("0–{y_upper:.0}%");
        let scale_width = scale.chars().count() as u16;
        if area.width > value_width + scale_width + 6 {
            frame.render_widget(
                Paragraph::new(scale).style(Style::default().fg(MUTED)),
                Rect::new(
                    area.right().saturating_sub(scale_width + 2),
                    area.y + 1,
                    scale_width,
                    1,
                ),
            );
        }
        if !detail.is_empty() && area.height > 3 {
            let width = (detail.chars().count() as u16).min(area.width.saturating_sub(4));
            frame.render_widget(
                Paragraph::new(detail).style(Style::default().fg(MUTED)),
                Rect::new(area.x + 2, area.y + 2, width, 1),
            );
        }
    }
}

fn indexed_metric(name: &str, index: usize, count: usize) -> String {
    if count > 1 {
        format!("{name} {index}")
    } else {
        name.to_owned()
    }
}

fn percent_of(used: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        used as f64 * 100.0 / total as f64
    }
}

fn filled_area_data(
    history: &VecDeque<f64>,
    points: usize,
    width: u16,
    height: u16,
    y_upper: f64,
) -> Vec<(f64, f64)> {
    let start = history.len().saturating_sub(points);
    let visible: Vec<_> = history.iter().skip(start).copied().collect();
    if visible.is_empty() {
        return Vec::new();
    }

    let upper = points.max(2) as f64 - 1.0;
    let x_offset = points.saturating_sub(visible.len()) as f64;
    let horizontal_dots = usize::from(width.saturating_sub(2).max(1)) * 2;
    let vertical_dots = usize::from(height.saturating_sub(2).max(1)) * 4;
    let vertical_step = y_upper / vertical_dots as f64;
    let mut data = Vec::with_capacity(horizontal_dots * vertical_dots);

    for dot in 0..horizontal_dots {
        let x = if horizontal_dots == 1 {
            upper
        } else {
            upper * dot as f64 / (horizontal_dots - 1) as f64
        };
        if x + f64::EPSILON < x_offset {
            continue;
        }
        let sample_position = (x - x_offset).clamp(0.0, (visible.len() - 1) as f64);
        let left = sample_position.floor() as usize;
        let right = (left + 1).min(visible.len() - 1);
        let fraction = sample_position - left as f64;
        let value = visible[left] + (visible[right] - visible[left]) * fraction;

        let mut y = 0.0;
        while y <= value {
            data.push((x, y));
            y += vertical_step;
        }
    }
    data
}

fn draw_settings(frame: &mut Frame, area: Rect, app: &App) {
    let body = Layout::vertical([
        Constraint::Length(2),
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Min(1),
    ])
    .margin(2)
    .split(area);
    frame.render_widget(
        Paragraph::new("Use ↑/↓ to select, ←/→ to change. Settings persist across sessions.")
            .style(Style::default().fg(MUTED)),
        body[0],
    );
    setting(
        frame,
        body[1],
        "Refresh interval",
        &format!("{} ms", app.config.refresh_ms),
        app.settings_row == 0,
    );
    setting(
        frame,
        body[2],
        "Chart history",
        &format!("{} samples", app.config.chart_points),
        app.settings_row == 1,
    );
    setting(
        frame,
        body[3],
        "Mouse capture (next launch)",
        if app.config.mouse_capture {
            "enabled"
        } else {
            "disabled"
        },
        app.settings_row == 2,
    );
    setting(
        frame,
        body[4],
        "Terminal timestamps",
        enabled(app.config.timestamps),
        app.settings_row == 3,
    );
    setting(
        frame,
        body[5],
        "Highlight info",
        enabled(app.config.highlight_info),
        app.settings_row == 4,
    );
    setting(
        frame,
        body[6],
        "Highlight warning",
        enabled(app.config.highlight_warning),
        app.settings_row == 5,
    );
    setting(
        frame,
        body[7],
        "Highlight error",
        enabled(app.config.highlight_error),
        app.settings_row == 6,
    );
}

fn setting(frame: &mut Frame, area: Rect, name: &str, value: &str, selected: bool) {
    let style = if selected {
        Style::default().fg(ACCENT).add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    let background = if selected {
        Color::Rgb(18, 36, 42)
    } else {
        Color::Reset
    };
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(format!("  {name:<30}"), style.bg(background)),
            Span::styled(
                format!("  ‹ {value} ›  "),
                Style::default().fg(PURPLE).bg(background),
            ),
        ])),
        area,
    );
}

fn enabled(value: bool) -> &'static str {
    if value { "enabled" } else { "disabled" }
}

fn tab_at_column(column: u16) -> Option<Tab> {
    let widths = [10_u16, 11, 10, 6];
    let mut start = 0;
    for (index, width) in widths.into_iter().enumerate() {
        if (start..start + width).contains(&column) {
            return Some(Tab::from_index(index));
        }
        start += width + 1;
    }
    None
}

fn draw_help(frame: &mut Frame, area: Rect, app: &App) {
    let help = vec![
        Line::from(vec![
            Span::styled(
                "MISSION  ",
                Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
            ),
            Span::raw(&app.entry.id),
        ]),
        Line::raw(""),
        Line::raw("Tab / Shift-Tab      switch tabs"),
        Line::raw("Ctrl+C               copy clean terminal text (timestamps are excluded)"),
        Line::raw("Ctrl+X               stop process: SIGINT → SIGTERM → SIGKILL"),
        Line::raw("Ctrl+D               detach and leave process running"),
        Line::raw("Ctrl+Z               stop process, close the dashboard, and detach"),
        Line::raw("↑/↓ or PgUp/PgDn     scroll a finished session's retained log"),
        Line::raw("mouse                click tabs/settings"),
        Line::raw(""),
        Line::raw("Terminal keys, control sequences, paste, and resize are forwarded to the PTY."),
        Line::raw("Detaching leaves the command alive. Reconnect with:"),
        Line::styled(
            format!("  mission --attach {}", app.entry.id),
            Style::default().fg(GREEN),
        ),
        Line::raw(""),
        Line::styled(
            "GPU note",
            Style::default().fg(PURPLE).add_modifier(Modifier::BOLD),
        ),
        Line::raw("NVIDIA utilization and VRAM come from NVML. On Hopper+ hardware, NVML GPM"),
        Line::raw(
            "also provides lightweight SM and Tensor utilization; older GPUs fall back cleanly.",
        ),
        Line::raw(""),
        Line::styled(
            "Terminal log styling",
            Style::default().fg(PURPLE).add_modifier(Modifier::BOLD),
        ),
        Line::raw(
            "A PTY combines stdout and stderr by design. Ordinary output inherits your terminal",
        ),
        Line::raw("background; configurable keywords add info/warning/error highlighting."),
    ];
    frame.render_widget(
        Paragraph::new(help)
            .block(panel(" Help & shortcuts "))
            .wrap(Wrap { trim: false }),
        area,
    );
}

struct VtWidget<'a> {
    screen: &'a vt100::Screen,
    config: &'a Config,
}

impl Widget for VtWidget<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        for row in 0..area.height {
            let content: String = (0..area.width)
                .filter_map(|col| self.screen.cell(row, col))
                .map(|cell| cell.contents())
                .collect();
            let lowercase = content.to_ascii_lowercase();
            let background = if self.config.highlight_error
                && (lowercase.contains("error")
                    || lowercase.contains("fatal")
                    || lowercase.contains("panic"))
            {
                Some(Color::Rgb(52, 18, 24))
            } else if self.config.highlight_warning
                && (lowercase.contains("warning") || lowercase.contains("warn"))
            {
                Some(Color::Rgb(52, 40, 14))
            } else if self.config.highlight_info && lowercase.contains("info") {
                Some(Color::Rgb(12, 40, 50))
            } else {
                None
            };
            let foregrounds = keyword_foregrounds(self.screen, row, area.width);
            for col in 0..area.width {
                let Some(cell) = self.screen.cell(row, col) else {
                    continue;
                };
                let symbol = cell.contents();
                let mut style = Style::default()
                    .fg(vt_color(cell.fgcolor()))
                    .bg(vt_color(cell.bgcolor()));
                if let Some(background) = background {
                    style = style.bg(background);
                }
                if let Some(foreground) = foregrounds[col as usize] {
                    style = style.fg(foreground);
                }
                if cell.bold() {
                    style = style.add_modifier(Modifier::BOLD);
                }
                if cell.italic() {
                    style = style.add_modifier(Modifier::ITALIC);
                }
                if cell.underline() {
                    style = style.add_modifier(Modifier::UNDERLINED);
                }
                if cell.inverse() {
                    style = style.add_modifier(Modifier::REVERSED);
                }
                buffer[(area.x + col, area.y + row)]
                    .set_symbol(if symbol.is_empty() { " " } else { symbol })
                    .set_style(style);
            }
        }
        if !self.screen.hide_cursor() {
            let (row, col) = self.screen.cursor_position();
            if row < area.height && col < area.width {
                buffer[(area.x + col, area.y + row)]
                    .set_style(Style::default().add_modifier(Modifier::REVERSED));
            }
        }
    }
}

fn keyword_foregrounds(screen: &vt100::Screen, row: u16, width: u16) -> Vec<Option<Color>> {
    let mut searchable = String::new();
    let mut byte_ranges = Vec::with_capacity(width as usize);
    for col in 0..width {
        let start = searchable.len();
        if let Some(cell) = screen.cell(row, col) {
            searchable.push_str(&cell.contents().to_ascii_lowercase());
        }
        byte_ranges.push((start, searchable.len()));
    }

    let keywords = [
        ("warning", YELLOW),
        ("starting", PURPLE),
        ("stopped", ORANGE),
        ("success", GREEN),
        ("succeed", GREEN),
        ("failed", RED),
        ("error", RED),
        ("warn", YELLOW),
        ("info", ACCENT),
        ("fail", RED),
        ("done", GREEN),
        ("start", PURPLE),
        ("stop", ORANGE),
    ];
    let mut colors = vec![None; width as usize];
    for (keyword, color) in keywords {
        for (start, matched) in searchable.match_indices(keyword) {
            let end = start + matched.len();
            for (column, &(cell_start, cell_end)) in byte_ranges.iter().enumerate() {
                if cell_start < end && cell_end > start {
                    colors[column] = Some(color);
                }
            }
        }
    }
    colors
}

fn vt_color(color: vt100::Color) -> Color {
    match color {
        vt100::Color::Default => Color::Reset,
        vt100::Color::Idx(index) => Color::Indexed(index),
        vt100::Color::Rgb(red, green, blue) => Color::Rgb(red, green, blue),
    }
}

fn encode_key(key: KeyEvent, application_cursor: bool) -> Option<Vec<u8>> {
    let modifiers = key.modifiers;
    let sequence = match key.code {
        KeyCode::Char(character) if modifiers.contains(KeyModifiers::CONTROL) => {
            let control = match character {
                ' ' | '@' | '`' => 0,
                'a'..='z' | 'A'..='Z' => (character.to_ascii_uppercase() as u8) & 0x1f,
                '[' | '3' | '{' => 0x1b,
                '\\' | '4' | '|' => 0x1c,
                ']' | '5' | '}' => 0x1d,
                '^' | '6' | '~' => 0x1e,
                '_' | '7' | '/' => 0x1f,
                '8' | '?' => 0x7f,
                _ => return None,
            };
            let mut bytes = Vec::new();
            if modifiers.contains(KeyModifiers::ALT) {
                bytes.push(0x1b);
            }
            bytes.push(control);
            bytes
        }
        KeyCode::Char(character) => {
            let mut bytes = Vec::new();
            if modifiers.contains(KeyModifiers::ALT) {
                bytes.push(0x1b);
            }
            let mut encoded = [0; 4];
            bytes.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
            bytes
        }
        KeyCode::Enter => b"\r".to_vec(),
        KeyCode::Tab => b"\t".to_vec(),
        KeyCode::BackTab => b"\x1b[Z".to_vec(),
        KeyCode::Backspace => vec![0x7f],
        KeyCode::Esc => vec![0x1b],
        KeyCode::Up => cursor_key('A', modifiers, application_cursor),
        KeyCode::Down => cursor_key('B', modifiers, application_cursor),
        KeyCode::Right => cursor_key('C', modifiers, application_cursor),
        KeyCode::Left => cursor_key('D', modifiers, application_cursor),
        KeyCode::Home => cursor_key('H', modifiers, application_cursor),
        KeyCode::End => cursor_key('F', modifiers, application_cursor),
        KeyCode::PageUp => tilde_key(5, modifiers),
        KeyCode::PageDown => tilde_key(6, modifiers),
        KeyCode::Delete => tilde_key(3, modifiers),
        KeyCode::Insert => tilde_key(2, modifiers),
        KeyCode::F(number) => function_key(number, modifiers)?,
        _ => return None,
    };
    Some(sequence)
}

fn modifier_parameter(modifiers: KeyModifiers) -> u8 {
    1 + u8::from(modifiers.contains(KeyModifiers::SHIFT))
        + 2 * u8::from(modifiers.contains(KeyModifiers::ALT))
        + 4 * u8::from(modifiers.contains(KeyModifiers::CONTROL))
}

fn cursor_key(final_byte: char, modifiers: KeyModifiers, application: bool) -> Vec<u8> {
    let parameter = modifier_parameter(modifiers);
    if parameter > 1 {
        format!("\x1b[1;{parameter}{final_byte}").into_bytes()
    } else if application {
        format!("\x1bO{final_byte}").into_bytes()
    } else {
        format!("\x1b[{final_byte}").into_bytes()
    }
}

fn tilde_key(code: u8, modifiers: KeyModifiers) -> Vec<u8> {
    let parameter = modifier_parameter(modifiers);
    if parameter > 1 {
        format!("\x1b[{code};{parameter}~").into_bytes()
    } else {
        format!("\x1b[{code}~").into_bytes()
    }
}

fn function_key(number: u8, modifiers: KeyModifiers) -> Option<Vec<u8>> {
    let parameter = modifier_parameter(modifiers);
    if number <= 4 {
        let final_byte = char::from(b'P' + number.saturating_sub(1));
        return Some(if parameter > 1 {
            format!("\x1b[1;{parameter}{final_byte}").into_bytes()
        } else {
            format!("\x1bO{final_byte}").into_bytes()
        });
    }
    let code = match number {
        5 => 15,
        6 => 17,
        7 => 18,
        8 => 19,
        9 => 20,
        10 => 21,
        11 => 23,
        12 => 24,
        _ => return None,
    };
    Some(tilde_key(code, modifiers))
}

fn panel<'a>(title: &'a str) -> Block<'a> {
    Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Rgb(63, 63, 70)))
}
fn send(stream: &mut UnixStream, message: ClientMessage) -> Result<()> {
    protocol::send(stream, &message)
}

fn bytes(value: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = value as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

fn config_path() -> Option<std::path::PathBuf> {
    session::root_dir()
        .ok()?
        .parent()
        .map(|parent| parent.join("config.json"))
}
fn load_config() -> Config {
    config_path()
        .and_then(|path| fs::read(path).ok())
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}
fn save_config(config: &Config) -> Result<()> {
    let path = config_path().context("cannot determine config path")?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(config)?)?;
    Ok(())
}

struct TerminalGuard {
    mouse: bool,
}
impl TerminalGuard {
    fn enter(mouse: bool) -> Result<Self> {
        enable_raw_mode()?;
        if mouse {
            execute!(io::stdout(), EnterAlternateScreen, EnableMouseCapture)?;
        } else {
            execute!(io::stdout(), EnterAlternateScreen)?;
        }
        Ok(Self { mouse })
    }
}
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        if self.mouse {
            let _ = execute!(io::stdout(), LeaveAlternateScreen, DisableMouseCapture);
        } else {
            let _ = execute!(io::stdout(), LeaveAlternateScreen);
        }
    }
}

#[cfg(test)]
mod input_tests {
    use super::*;

    fn key(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, modifiers)
    }

    #[test]
    fn cursor_keys_follow_terminal_mode_and_modifiers() {
        assert_eq!(
            encode_key(key(KeyCode::Right, KeyModifiers::NONE), false),
            Some(b"\x1b[C".to_vec())
        );
        assert_eq!(
            encode_key(key(KeyCode::Right, KeyModifiers::NONE), true),
            Some(b"\x1bOC".to_vec())
        );
        assert_eq!(
            encode_key(key(KeyCode::Left, KeyModifiers::CONTROL), false),
            Some(b"\x1b[1;5D".to_vec())
        );
        assert_eq!(
            encode_key(
                key(KeyCode::Up, KeyModifiers::SHIFT | KeyModifiers::ALT),
                false
            ),
            Some(b"\x1b[1;4A".to_vec())
        );
    }

    #[test]
    fn control_and_function_keys_use_xterm_sequences() {
        assert_eq!(
            encode_key(key(KeyCode::Char('c'), KeyModifiers::CONTROL), false),
            Some(vec![3])
        );
        assert_eq!(
            encode_key(key(KeyCode::Char(' '), KeyModifiers::CONTROL), false),
            Some(vec![0])
        );
        assert_eq!(
            encode_key(key(KeyCode::F(5), KeyModifiers::NONE), false),
            Some(b"\x1b[15~".to_vec())
        );
        assert_eq!(
            encode_key(key(KeyCode::F(12), KeyModifiers::CONTROL), false),
            Some(b"\x1b[24;5~".to_vec())
        );
    }

    #[test]
    fn compact_tab_hitboxes_match_rendered_labels() {
        assert_eq!(tab_at_column(1), Some(Tab::Terminal));
        assert_eq!(tab_at_column(12), Some(Tab::Resources));
        assert_eq!(tab_at_column(24), Some(Tab::Settings));
        assert_eq!(tab_at_column(35), Some(Tab::Help));
        assert_eq!(tab_at_column(60), None);
    }

    #[test]
    fn session_search_matches_id_and_command_case_insensitively() {
        let entries = vec![SessionEntry {
            id: "Alpha-123".into(),
            command: vec!["python".into(), "Train Model.py".into()],
            pid: 1,
            created_at: 0,
            running: true,
            exit_code: None,
            dir: std::path::PathBuf::new(),
        }];
        assert_eq!(filtered_sessions(&entries, "alpha").len(), 1);
        assert_eq!(filtered_sessions(&entries, "train model").len(), 1);
        assert!(filtered_sessions(&entries, "missing").is_empty());
    }

    #[test]
    fn clipboard_text_includes_scrollback_without_rendered_timestamps() {
        let mut parser = vt100::Parser::new(2, 20, 20);
        parser.process(b"first\r\nsecond\r\nthird");
        let text = terminal_contents(parser.screen());
        assert!(text.contains("first\nsecond\nthird"));
        assert!(!text.contains(':'));
    }

    #[test]
    fn ordinary_terminal_rows_keep_the_default_background() {
        let mut parser = vt100::Parser::new(1, 20, 0);
        parser.process(b"normal output");
        let area = Rect::new(0, 0, 20, 1);
        let mut buffer = Buffer::empty(area);
        VtWidget {
            screen: parser.screen(),
            config: &Config::default(),
        }
        .render(area, &mut buffer);

        assert!(buffer.content.iter().all(|cell| cell.bg == Color::Reset));
    }

    #[test]
    fn refresh_interval_uses_fine_steps_below_one_second() {
        assert_eq!(next_refresh_interval(100, false), 100);
        assert_eq!(next_refresh_interval(100, true), 150);
        assert_eq!(next_refresh_interval(950, true), 1_000);
        assert_eq!(next_refresh_interval(1_000, true), 1_100);
        assert_eq!(next_refresh_interval(1_100, false), 1_000);
        assert_eq!(next_refresh_interval(1_000, false), 950);
    }

    #[test]
    fn filled_chart_uses_every_braille_column() {
        let history = VecDeque::from(vec![20.0, 40.0, 60.0, 80.0]);
        let data = filled_area_data(&history, 4, 22, 8, 100.0);
        let baseline_columns = data.iter().filter(|(_, y)| *y == 0.0).count();

        assert_eq!(baseline_columns, 40);
    }

    #[test]
    fn resource_titles_are_short_and_gpu_indices_are_conditional() {
        assert_eq!(indexed_metric("GPU", 0, 1), "GPU");
        assert_eq!(indexed_metric("VRAM", 0, 1), "VRAM");
        assert_eq!(indexed_metric("GPU", 0, 2), "GPU 0");
        assert_eq!(indexed_metric("VRAM", 1, 2), "VRAM 1");
    }

    #[test]
    fn chart_renders_a_bright_title_and_current_value_overlay() {
        let history = VecDeque::from(vec![42.5]);
        let backend = ratatui::backend::TestBackend::new(30, 8);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                area_chart(
                    frame,
                    frame.area(),
                    "CPU",
                    (42.5, String::new()),
                    &history,
                    ACCENT,
                    30,
                );
            })
            .unwrap();
        let buffer = terminal.backend().buffer();
        let rendered: String = buffer.content().iter().map(|cell| cell.symbol()).collect();

        assert!(rendered.contains("CPU"));
        assert!(rendered.contains("42.5%"));
        assert_eq!(buffer[(2, 0)].fg, Color::Rgb(244, 244, 245));
    }

    #[test]
    fn keyword_colors_match_inside_a_line_case_insensitively() {
        let mut parser = vt100::Parser::new(1, 40, 0);
        parser.process(b"prefix InFo then FAILED and done");
        let area = Rect::new(0, 0, 40, 1);
        let mut buffer = Buffer::empty(area);
        VtWidget {
            screen: parser.screen(),
            config: &Config::default(),
        }
        .render(area, &mut buffer);

        assert_eq!(buffer[(7, 0)].fg, ACCENT);
        assert_eq!(buffer[(17, 0)].fg, RED);
        assert_eq!(buffer[(28, 0)].fg, GREEN);
        assert_eq!(buffer[(0, 0)].fg, Color::Reset);
    }

    #[test]
    fn finished_session_renders_inside_terminal_dashboard() {
        let entry = SessionEntry {
            id: "finished-test".into(),
            command: vec!["python".into(), "train.py".into()],
            pid: 42,
            created_at: 0,
            running: false,
            exit_code: Some(1),
            dir: std::path::PathBuf::new(),
        };
        let mut app = App {
            entry,
            tab: Tab::Terminal,
            parser: vt100::Parser::new(20, 80, 100),
            sample: Sample::default(),
            histories: Histories::default(),
            config: Config::default(),
            settings_row: 0,
            running: false,
            exit_code: Some(1),
            status: "finished · read-only log".into(),
            terminal_area: Rect::default(),
            row_timestamps: VecDeque::new(),
        };
        process_terminal_output(
            &mut app,
            b"INFO training started\r\nERROR training failed",
            0,
        );
        let backend = ratatui::backend::TestBackend::new(100, 26);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        let rendered: String = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect();
        assert!(rendered.contains("finished (1) · read-only"));
        assert!(rendered.contains("INFO training started"));
        assert!(rendered.contains("ERROR training failed"));
    }
}
