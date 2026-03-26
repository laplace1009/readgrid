mod app;
mod config;
mod db;
mod mcp;

use std::io;

use anyhow::Result;
use app::{App, CliArgs};
use clap::Parser;
use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{self, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{Terminal, backend::CrosstermBackend};

#[tokio::main]
async fn main() -> Result<()> {
    let args = CliArgs::parse();
    let config = config::ConfigStore::load()?;
    let mcp_context = mcp::McpContext::load(&args)?;
    let mut app = App::new(args, config, mcp_context)?;

    terminal::enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = app.run(&mut terminal).await;

    terminal::disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    result
}
