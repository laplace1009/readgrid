use std::{io::Stdout, path::PathBuf, time::Duration};

use anyhow::{Result, anyhow};
use clap::Parser;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, List, ListItem, ListState, Paragraph, Row, Table, Wrap},
};

use crate::{
    config::ConfigStore,
    db::{
        ConnectionProfile, DataPreview, DatabaseKind, ForeignKeyMeta, Session, SortState,
        TableDetail, TableRef,
    },
    mcp::McpContext,
};

#[derive(Debug, Clone, Parser)]
#[command(author, version, about = "ReadGrid: terminal database explorer")]
pub struct CliArgs {
    #[arg(long)]
    pub profile: Option<String>,
    #[arg(long)]
    pub pg_url: Option<String>,
    #[arg(long)]
    pub sqlite_path: Option<PathBuf>,
    #[arg(long)]
    pub schema: Option<String>,
    #[arg(long)]
    pub mcp_context_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Screen {
    Connections,
    Schemas,
    Browser,
    Detail,
}

#[derive(Debug, Clone)]
struct ConnectionCandidate {
    profile: ConnectionProfile,
    source: &'static str,
    preferred_schema: Option<String>,
}

pub struct App {
    config: ConfigStore,
    screen: Screen,
    candidates: Vec<ConnectionCandidate>,
    connection_index: usize,
    schemas: Vec<String>,
    schema_index: usize,
    tables: Vec<TableRef>,
    table_index: usize,
    filter: String,
    search_mode: bool,
    session: Option<Session>,
    active_profile: Option<ConnectionProfile>,
    active_schema: Option<String>,
    detail: Option<TableDetail>,
    preview: Option<DataPreview>,
    sort_index: usize,
    sort_desc: bool,
    status: String,
    example_config: String,
    pending_auto_connect: bool,
}

impl App {
    pub fn new(
        args: CliArgs,
        config: ConfigStore,
        mcp_context: Option<McpContext>,
    ) -> Result<Self> {
        if args.pg_url.is_some() && args.sqlite_path.is_some() {
            return Err(anyhow!("use either --pg-url or --sqlite-path, not both"));
        }

        let example_config = ConfigStore::example_profiles();
        let (candidates, selected_index, pending_auto_connect) =
            build_candidates(&args, &config, mcp_context);

        let status = if candidates.is_empty() {
            "No connection sources found. Use --pg-url/--sqlite-path or add profiles.toml.".into()
        } else {
            "Choose a connection profile and press Enter.".into()
        };

        Ok(Self {
            config,
            screen: Screen::Connections,
            candidates,
            connection_index: selected_index,
            schemas: Vec::new(),
            schema_index: 0,
            tables: Vec::new(),
            table_index: 0,
            filter: String::new(),
            search_mode: false,
            session: None,
            active_profile: None,
            active_schema: None,
            detail: None,
            preview: None,
            sort_index: 0,
            sort_desc: false,
            status,
            example_config,
            pending_auto_connect,
        })
    }

    pub async fn run(&mut self, terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> Result<()> {
        if self.pending_auto_connect && !self.candidates.is_empty() {
            self.activate_selected_connection().await?;
        }

        loop {
            terminal.draw(|frame| self.render(frame))?;

            if !event::poll(Duration::from_millis(150))? {
                continue;
            }

            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }

                if self.handle_key(key).await? {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Result<bool> {
        if matches!(key.code, KeyCode::Char('q')) && !self.search_mode {
            return Ok(true);
        }

        match self.screen {
            Screen::Connections => self.handle_connections_key(key).await,
            Screen::Schemas => self.handle_schemas_key(key).await,
            Screen::Browser => self.handle_browser_key(key).await,
            Screen::Detail => self.handle_detail_key(key).await,
        }
    }

    async fn handle_connections_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => self.move_connection(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_connection(1),
            KeyCode::Enter => {
                self.activate_selected_connection().await?;
            }
            _ => {}
        }
        Ok(false)
    }

    async fn handle_schemas_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => self.move_schema(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_schema(1),
            KeyCode::Enter => {
                self.load_tables_for_selected_schema().await?;
            }
            KeyCode::Esc => {
                self.screen = Screen::Connections;
                self.status = "Returned to connection selection.".into();
            }
            _ => {}
        }
        Ok(false)
    }

    async fn handle_browser_key(&mut self, key: KeyEvent) -> Result<bool> {
        if self.search_mode {
            self.handle_search_input(key);
            return Ok(false);
        }

        match key.code {
            KeyCode::Up | KeyCode::Char('k') => self.move_table(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_table(1),
            KeyCode::Char('/') => {
                self.search_mode = true;
                self.status = "Search tables: type to filter, Esc to clear.".into();
            }
            KeyCode::Char('r') => {
                self.reload_tables().await?;
            }
            KeyCode::Enter => {
                self.load_selected_table_detail().await?;
            }
            KeyCode::Esc => {
                if self.session_kind() == Some(DatabaseKind::Postgres) {
                    self.screen = Screen::Schemas;
                } else {
                    self.screen = Screen::Connections;
                }
                self.status = "Returned to previous screen.".into();
            }
            _ => {}
        }

        Ok(false)
    }

    async fn handle_detail_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Esc => {
                self.screen = Screen::Browser;
                self.status = "Returned to table browser.".into();
            }
            KeyCode::Char('[') => {
                if self.sort_index > 0 {
                    self.sort_index -= 1;
                    self.reload_preview().await?;
                }
            }
            KeyCode::Char(']') => {
                if let Some(detail) = &self.detail {
                    if self.sort_index + 1 < detail.columns.len() {
                        self.sort_index += 1;
                        self.reload_preview().await?;
                    }
                }
            }
            KeyCode::Char('s') => {
                self.sort_desc = !self.sort_desc;
                self.reload_preview().await?;
            }
            KeyCode::Char('n') => {
                if self
                    .preview
                    .as_ref()
                    .map(|preview| preview.has_more)
                    .unwrap_or(false)
                {
                    if let Some(preview) = &mut self.preview {
                        preview.page += 1;
                    }
                    self.reload_preview().await?;
                }
            }
            KeyCode::Char('p') => {
                if self
                    .preview
                    .as_ref()
                    .map(|preview| preview.page)
                    .unwrap_or(0)
                    > 0
                {
                    if let Some(preview) = &mut self.preview {
                        preview.page -= 1;
                    }
                    self.reload_preview().await?;
                }
            }
            KeyCode::Char('r') => {
                self.load_selected_table_detail().await?;
            }
            _ => {}
        }
        Ok(false)
    }

    fn handle_search_input(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                self.search_mode = false;
                self.filter.clear();
                self.table_index = 0;
                self.status = "Cleared table filter.".into();
            }
            KeyCode::Enter => {
                self.search_mode = false;
                self.status = format!("Filtering tables by '{}'.", self.filter);
            }
            KeyCode::Backspace => {
                self.filter.pop();
                self.clamp_table_index();
            }
            KeyCode::Char(ch) => {
                self.filter.push(ch);
                self.clamp_table_index();
            }
            _ => {}
        }
    }

    async fn activate_selected_connection(&mut self) -> Result<()> {
        let candidate = self
            .selected_candidate()
            .cloned()
            .ok_or_else(|| anyhow!("no connection candidate is selected"))?;

        self.status = format!("Connecting to {}...", candidate.profile.name);
        let session = Session::connect(&candidate.profile).await?;
        self.config.note_recent_profile(&candidate.profile.name)?;
        self.active_profile = Some(candidate.profile.clone());
        self.session = Some(session);
        self.detail = None;
        self.preview = None;
        self.sort_index = 0;
        self.sort_desc = false;

        if self.session_kind() == Some(DatabaseKind::Postgres) {
            self.schemas = self.session().unwrap().list_schemas().await?;
            self.schema_index = 0;
            self.screen = Screen::Schemas;
            if let Some(preferred) = candidate
                .preferred_schema
                .clone()
                .or_else(|| self.find_selected_schema_hint())
            {
                if let Some(index) = self.schemas.iter().position(|schema| schema == &preferred) {
                    self.schema_index = index;
                }
            }
            self.status = "Connected. Choose a schema and press Enter.".into();
        } else {
            self.active_schema = None;
            self.load_tables(None).await?;
            self.screen = Screen::Browser;
            self.status = "Connected. Browse tables and press Enter for details.".into();
        }

        Ok(())
    }

    async fn load_tables_for_selected_schema(&mut self) -> Result<()> {
        let schema = self.selected_schema().cloned();
        self.load_tables(schema).await?;
        self.screen = Screen::Browser;
        self.status = "Schema loaded. Browse tables and press Enter for details.".into();
        Ok(())
    }

    async fn load_tables(&mut self, schema: Option<String>) -> Result<()> {
        self.active_schema = schema.clone();
        self.tables = self
            .session()
            .unwrap()
            .list_tables(schema.as_deref())
            .await?;
        self.table_index = 0;
        self.filter.clear();
        self.search_mode = false;
        self.detail = None;
        self.preview = None;
        Ok(())
    }

    async fn reload_tables(&mut self) -> Result<()> {
        self.load_tables(self.active_schema.clone()).await?;
        self.status = "Reloaded table list.".into();
        Ok(())
    }

    async fn load_selected_table_detail(&mut self) -> Result<()> {
        let table = self
            .selected_table()
            .ok_or_else(|| anyhow!("no table is selected"))?;

        let detail = self.session().unwrap().load_detail(&table).await?;
        self.sort_index = 0;
        self.sort_desc = false;
        self.detail = Some(detail);
        self.preview = Some(DataPreview {
            columns: Vec::new(),
            rows: Vec::new(),
            page: 0,
            has_more: false,
        });
        self.reload_preview().await?;
        self.screen = Screen::Detail;
        self.status = format!("Viewing {}.", table.display_name());
        Ok(())
    }

    async fn reload_preview(&mut self) -> Result<()> {
        let table = self
            .detail
            .as_ref()
            .map(|detail| detail.table.clone())
            .ok_or_else(|| anyhow!("no table detail is loaded"))?;
        let page = self
            .preview
            .as_ref()
            .map(|preview| preview.page)
            .unwrap_or(0);
        let preview = self
            .session()
            .unwrap()
            .load_preview(&table, self.current_sort().as_ref(), page)
            .await?;
        self.preview = Some(preview);
        Ok(())
    }

    fn current_sort(&self) -> Option<SortState> {
        let detail = self.detail.as_ref()?;
        let column = detail.columns.get(self.sort_index)?;
        Some(SortState {
            column_name: column.name.clone(),
            descending: self.sort_desc,
        })
    }

    fn render(&self, frame: &mut Frame) {
        match self.screen {
            Screen::Connections => self.render_connections(frame),
            Screen::Schemas => self.render_schemas(frame),
            Screen::Browser => self.render_browser(frame),
            Screen::Detail => self.render_detail(frame),
        }
    }

    fn render_connections(&self, frame: &mut Frame) {
        let chunks = main_chunks(frame.area());
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
            .split(chunks[0]);

        let items = self
            .candidates
            .iter()
            .map(|candidate| {
                ListItem::new(format!("{} [{}]", candidate.profile.name, candidate.source))
            })
            .collect::<Vec<_>>();
        let mut state = ListState::default();
        state.select(Some(self.connection_index));
        let list = List::new(items)
            .block(Block::default().title("Connections").borders(Borders::ALL))
            .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
        frame.render_stateful_widget(list, body[0], &mut state);

        let detail = self
            .selected_candidate()
            .map(|candidate| {
                vec![
                    Line::from(format!("Kind: {}", candidate.profile.kind)),
                    Line::from(format!("Source: {}", candidate.source)),
                    Line::from(format!("Target: {}", candidate.profile.summary())),
                    Line::from(String::new()),
                    Line::from("Hints:"),
                    Line::from("- Enter: connect"),
                    Line::from("- q: quit"),
                    Line::from(format!(
                        "- Add saved profiles in {}",
                        self.config.profiles_path.display()
                    )),
                    Line::from(String::new()),
                    Line::from("Example config:"),
                    Line::from(self.example_config.clone()),
                ]
            })
            .unwrap_or_else(|| vec![Line::from("No connection candidates found.")]);

        let panel = Paragraph::new(detail)
            .block(Block::default().title("Selection").borders(Borders::ALL))
            .wrap(Wrap { trim: false });
        frame.render_widget(panel, body[1]);

        frame.render_widget(status_bar(&self.status), chunks[1]);
    }

    fn render_schemas(&self, frame: &mut Frame) {
        let chunks = main_chunks(frame.area());
        let items = self
            .schemas
            .iter()
            .map(|schema| ListItem::new(schema.as_str()))
            .collect::<Vec<_>>();
        let mut state = ListState::default();
        state.select(Some(self.schema_index));
        let list = List::new(items)
            .block(Block::default().title("Schemas").borders(Borders::ALL))
            .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
        frame.render_stateful_widget(list, chunks[0], &mut state);
        frame.render_widget(status_bar(&self.status), chunks[1]);
    }

    fn render_browser(&self, frame: &mut Frame) {
        let chunks = main_chunks(frame.area());
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
            .split(chunks[0]);
        let filtered = self.filtered_tables();
        let items = filtered
            .iter()
            .map(|table| ListItem::new(table.display_name()))
            .collect::<Vec<_>>();
        let mut state = ListState::default();
        state.select(Some(self.table_index.min(filtered.len().saturating_sub(1))));
        let title = if self.search_mode {
            format!("Tables (search: {})", self.filter)
        } else if self.filter.is_empty() {
            "Tables".into()
        } else {
            format!("Tables (filtered: {})", self.filter)
        };
        let list = List::new(items)
            .block(Block::default().title(title).borders(Borders::ALL))
            .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
        frame.render_stateful_widget(list, body[0], &mut state);

        let selected = self.selected_table();
        let summary = vec![
            Line::from(format!(
                "Connection: {}",
                self.active_profile
                    .as_ref()
                    .map(|profile| profile.name.as_str())
                    .unwrap_or("none")
            )),
            Line::from(format!(
                "Schema: {}",
                self.active_schema.as_deref().unwrap_or("main")
            )),
            Line::from(String::new()),
            Line::from(format!(
                "Selected table: {}",
                selected
                    .map(|table| table.display_name())
                    .unwrap_or_else(|| "none".into())
            )),
            Line::from(format!("Visible tables: {}", filtered.len())),
            Line::from(String::new()),
            Line::from("Keys:"),
            Line::from("- j/k or arrows: move"),
            Line::from("- /: filter tables"),
            Line::from("- Enter: open detail"),
            Line::from("- r: reload tables"),
            Line::from("- Esc: back"),
        ];
        let panel = Paragraph::new(summary)
            .block(Block::default().title("Browser").borders(Borders::ALL))
            .wrap(Wrap { trim: false });
        frame.render_widget(panel, body[1]);
        frame.render_widget(status_bar(&self.status), chunks[1]);
    }

    fn render_detail(&self, frame: &mut Frame) {
        let chunks = main_chunks(frame.area());
        let body = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Percentage(42),
                Constraint::Percentage(22),
                Constraint::Percentage(36),
            ])
            .split(chunks[0]);

        let detail = match &self.detail {
            Some(detail) => detail,
            None => {
                frame.render_widget(status_bar("No table detail loaded."), chunks[1]);
                return;
            }
        };

        let header = Row::new(vec!["Column", "Type", "Null", "PK", "Default"])
            .style(Style::default().add_modifier(Modifier::BOLD));
        let rows = detail.columns.iter().map(|column| {
            Row::new(vec![
                Cell::from(column.name.clone()),
                Cell::from(column.data_type.clone()),
                Cell::from(if column.nullable { "yes" } else { "no" }),
                Cell::from(if column.is_primary_key { "pk" } else { "" }),
                Cell::from(column.default_value.clone().unwrap_or_default()),
            ])
        });
        let columns = Table::new(
            rows,
            [
                Constraint::Length(24),
                Constraint::Length(18),
                Constraint::Length(6),
                Constraint::Length(4),
                Constraint::Min(20),
            ],
        )
        .header(header)
        .block(
            Block::default()
                .title(format!("Columns: {}", detail.table.display_name()))
                .borders(Borders::ALL),
        );
        frame.render_widget(columns, body[0]);

        let relationship_text = format_relationships(&detail.foreign_keys);
        let index_text = detail
            .indexes
            .iter()
            .map(|index| {
                format!(
                    "{}{}: {}",
                    if index.is_unique { "unique " } else { "" },
                    index.name,
                    index.details
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let metadata = Paragraph::new(vec![
            Line::from("Relationships:"),
            Line::from(relationship_text),
            Line::from(String::new()),
            Line::from("Indexes:"),
            Line::from(index_text),
        ])
        .block(
            Block::default()
                .title("Relations & Indexes")
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: false });
        frame.render_widget(metadata, body[1]);

        let preview = self.preview.as_ref();
        let preview_title = match self.current_sort() {
            Some(sort) => format!(
                "Preview page {} sorted by {} {}",
                preview.map(|data| data.page + 1).unwrap_or(1),
                sort.column_name,
                if sort.descending { "desc" } else { "asc" }
            ),
            None => "Preview".into(),
        };
        let preview_widget = render_preview(preview, &detail.columns, preview_title);
        frame.render_widget(preview_widget, body[2]);
        frame.render_widget(status_bar(&self.status), chunks[1]);
    }

    fn move_connection(&mut self, delta: isize) {
        self.connection_index = move_index(self.connection_index, self.candidates.len(), delta);
    }

    fn move_schema(&mut self, delta: isize) {
        self.schema_index = move_index(self.schema_index, self.schemas.len(), delta);
    }

    fn move_table(&mut self, delta: isize) {
        self.table_index = move_index(self.table_index, self.filtered_tables().len(), delta);
    }

    fn clamp_table_index(&mut self) {
        let len = self.filtered_tables().len();
        if len == 0 {
            self.table_index = 0;
        } else if self.table_index >= len {
            self.table_index = len - 1;
        }
    }

    fn filtered_tables(&self) -> Vec<TableRef> {
        if self.filter.is_empty() {
            return self.tables.clone();
        }

        let needle = self.filter.to_lowercase();
        self.tables
            .iter()
            .filter(|table| table.display_name().to_lowercase().contains(&needle))
            .cloned()
            .collect()
    }

    fn selected_candidate(&self) -> Option<&ConnectionCandidate> {
        self.candidates.get(self.connection_index)
    }

    fn selected_schema(&self) -> Option<&String> {
        self.schemas.get(self.schema_index)
    }

    fn selected_table(&self) -> Option<TableRef> {
        self.filtered_tables().get(self.table_index).cloned()
    }

    fn session(&self) -> Option<&Session> {
        self.session.as_ref()
    }

    fn session_kind(&self) -> Option<DatabaseKind> {
        self.session.as_ref().map(|session| session.kind())
    }

    fn find_selected_schema_hint(&self) -> Option<String> {
        None
    }
}

fn build_candidates(
    args: &CliArgs,
    config: &ConfigStore,
    mcp_context: Option<McpContext>,
) -> (Vec<ConnectionCandidate>, usize, bool) {
    let mut candidates = config
        .ordered_profiles()
        .into_iter()
        .map(|profile| ConnectionCandidate {
            profile,
            source: "saved",
            preferred_schema: None,
        })
        .collect::<Vec<_>>();

    if let Some(url) = &args.pg_url {
        candidates.insert(
            0,
            ConnectionCandidate {
                profile: ConnectionProfile {
                    name: "cli-postgres".into(),
                    kind: DatabaseKind::Postgres,
                    url: Some(url.clone()),
                    path: None,
                },
                source: "cli",
                preferred_schema: args.schema.clone(),
            },
        );
    }

    if let Some(path) = &args.sqlite_path {
        candidates.insert(
            0,
            ConnectionCandidate {
                profile: ConnectionProfile {
                    name: "cli-sqlite".into(),
                    kind: DatabaseKind::Sqlite,
                    url: None,
                    path: Some(path.clone()),
                },
                source: "cli",
                preferred_schema: None,
            },
        );
    }

    if let Some(context) = mcp_context {
        let preferred_schema = context.preferred_schema.clone();
        if let Some(profile) = context.into_profile() {
            candidates.insert(
                0,
                ConnectionCandidate {
                    profile,
                    source: "mcp",
                    preferred_schema,
                },
            );
        }
    }

    let selected_index = args
        .profile
        .as_ref()
        .and_then(|name| {
            candidates
                .iter()
                .position(|candidate| candidate.profile.name == *name)
        })
        .unwrap_or(0);
    let pending_auto_connect =
        args.profile.is_some() || args.pg_url.is_some() || args.sqlite_path.is_some();

    (candidates, selected_index, pending_auto_connect)
}

fn move_index(current: usize, len: usize, delta: isize) -> usize {
    if len == 0 {
        return 0;
    }

    let next = current as isize + delta;
    if next < 0 {
        0
    } else {
        (next as usize).min(len - 1)
    }
}

fn main_chunks(area: Rect) -> Vec<Rect> {
    Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(2)])
        .split(area)
        .to_vec()
}

fn status_bar(message: &str) -> Paragraph<'_> {
    Paragraph::new(Line::from(vec![
        Span::styled(" readgrid ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(message),
    ]))
    .block(Block::default().borders(Borders::TOP))
}

fn format_relationships(edges: &[ForeignKeyMeta]) -> String {
    if edges.is_empty() {
        return "No direct foreign-key relationships found.".into();
    }

    edges
        .iter()
        .map(|edge| {
            format!(
                "[{}] {} -> {}.{}",
                edge.direction.label(),
                edge.from_column,
                edge.to_table,
                edge.to_column
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_preview<'a>(
    preview: Option<&'a DataPreview>,
    fallback_columns: &'a [crate::db::ColumnMeta],
    title: String,
) -> Table<'a> {
    let columns = preview
        .map(|preview| preview.columns.clone())
        .filter(|cols| !cols.is_empty())
        .unwrap_or_else(|| {
            fallback_columns
                .iter()
                .map(|column| column.name.clone())
                .collect()
        });
    let header =
        Row::new(columns.iter().cloned()).style(Style::default().add_modifier(Modifier::BOLD));
    let rows = preview
        .map(|preview| preview.rows.clone())
        .unwrap_or_default()
        .into_iter()
        .map(Row::new);
    let widths = columns
        .iter()
        .map(|_| Constraint::Length(18))
        .collect::<Vec<_>>();

    Table::new(rows, widths)
        .header(header)
        .block(Block::default().title(title).borders(Borders::ALL))
}
