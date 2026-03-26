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
        ConnectionProfile, DataPreview, DatabaseKind, ForeignKeyMeta, RelationGraph, RelationNode,
        RelationNodeRole, Session, SortState, TableDetail, TableRef,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetailView {
    Table,
    Graph,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GraphLane {
    Incoming,
    Center,
    Outgoing,
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
    detail_view: DetailView,
    relation_graph: Option<RelationGraph>,
    graph_lane: GraphLane,
    graph_index: usize,
    graph_center_scroll: usize,
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
            detail_view: DetailView::Table,
            relation_graph: None,
            graph_lane: GraphLane::Center,
            graph_index: 0,
            graph_center_scroll: 0,
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
            KeyCode::Esc => {
                self.status = "Exited ReadGrid and returned to the terminal.".into();
                return Ok(true);
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
        if self.detail_view == DetailView::Graph {
            return self.handle_graph_key(key).await;
        }

        match key.code {
            KeyCode::Esc => {
                self.screen = Screen::Browser;
                self.status = "Returned to table browser.".into();
            }
            KeyCode::Char('g') => {
                self.enter_graph_view().await?;
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
                if let Some(table) = self.detail.as_ref().map(|detail| detail.table.clone()) {
                    self.load_table_detail(table, DetailView::Table).await?;
                }
            }
            _ => {}
        }
        Ok(false)
    }

    async fn handle_graph_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Esc | KeyCode::Char('g') => {
                self.detail_view = DetailView::Table;
                self.status = "Returned to table detail.".into();
            }
            KeyCode::Left | KeyCode::Char('h') => self.move_graph_lane(-1),
            KeyCode::Right | KeyCode::Char('l') => self.move_graph_lane(1),
            KeyCode::Up | KeyCode::Char('k') => self.move_graph_row(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_graph_row(1),
            KeyCode::Char('r') => {
                self.reload_relation_graph().await?;
                self.status = "Reloaded relationship graph.".into();
            }
            KeyCode::Enter => {
                if self.graph_lane == GraphLane::Center {
                    self.status = "Already centered on the current table.".into();
                } else if let Some(table) = self.focused_graph_table() {
                    self.load_table_detail(table, DetailView::Graph).await?;
                    self.status = "Centered relationship graph on selected table.".into();
                }
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
        self.detail_view = DetailView::Table;
        self.relation_graph = None;
        self.graph_lane = GraphLane::Center;
        self.graph_index = 0;
        self.graph_center_scroll = 0;
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
        self.detail_view = DetailView::Table;
        self.relation_graph = None;
        self.graph_lane = GraphLane::Center;
        self.graph_index = 0;
        self.graph_center_scroll = 0;
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
        self.load_table_detail(table, DetailView::Table).await
    }

    async fn load_table_detail(&mut self, table: TableRef, detail_view: DetailView) -> Result<()> {
        let detail = self.session().unwrap().load_detail(&table).await?;
        self.sort_index = 0;
        self.sort_desc = false;
        self.detail = Some(detail);
        self.detail_view = detail_view;
        self.graph_lane = GraphLane::Center;
        self.graph_index = 0;
        self.graph_center_scroll = 0;
        self.relation_graph = None;
        self.preview = Some(DataPreview {
            columns: Vec::new(),
            rows: Vec::new(),
            page: 0,
            has_more: false,
        });
        self.reload_preview().await?;
        if detail_view == DetailView::Graph {
            self.reload_relation_graph().await?;
        }
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

    async fn reload_relation_graph(&mut self) -> Result<()> {
        let table = self
            .detail
            .as_ref()
            .map(|detail| detail.table.clone())
            .ok_or_else(|| anyhow!("no table detail is loaded"))?;
        let graph = self.session().unwrap().load_relation_graph(&table).await?;
        self.relation_graph = Some(graph);
        self.graph_lane = GraphLane::Center;
        self.graph_index = 0;
        self.graph_center_scroll = 0;
        Ok(())
    }

    async fn enter_graph_view(&mut self) -> Result<()> {
        self.detail_view = DetailView::Graph;
        if self.relation_graph.is_none() {
            self.reload_relation_graph().await?;
        } else {
            self.graph_lane = GraphLane::Center;
            self.graph_index = 0;
            self.graph_center_scroll = 0;
        }
        self.status = "Viewing relationship graph.".into();
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

    fn controls_hint(&self) -> &'static str {
        match self.screen {
            Screen::Connections => "Enter connect | Esc quit | q quit",
            Screen::Schemas => "Enter open schema | Esc back | q quit",
            Screen::Browser if self.search_mode => "Type filter | Enter apply | Esc clear",
            Screen::Browser => "Enter detail | / filter | r reload | Esc back | q quit",
            Screen::Detail if self.detail_view == DetailView::Graph => {
                "Left/Right lane | Up/Down move-or-scroll | Enter center neighbor | g/Esc detail | r reload | q quit"
            }
            Screen::Detail => {
                "g graph | [ ] sort | s order | n/p page | r reload | Esc back | q quit"
            }
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
                    Line::from("- Esc: quit to terminal"),
                    Line::from("- q: quit to terminal"),
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

        frame.render_widget(status_bar(&self.status, self.controls_hint()), chunks[1]);
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
        frame.render_widget(status_bar(&self.status, self.controls_hint()), chunks[1]);
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
            Line::from("- q: quit to terminal"),
        ];
        let panel = Paragraph::new(summary)
            .block(Block::default().title("Browser").borders(Borders::ALL))
            .wrap(Wrap { trim: false });
        frame.render_widget(panel, body[1]);
        frame.render_widget(status_bar(&self.status, self.controls_hint()), chunks[1]);
    }

    fn render_detail(&self, frame: &mut Frame) {
        let chunks = main_chunks(frame.area());
        let detail = match &self.detail {
            Some(detail) => detail,
            None => {
                frame.render_widget(
                    status_bar("No table detail loaded.", self.controls_hint()),
                    chunks[1],
                );
                return;
            }
        };

        if self.detail_view == DetailView::Graph {
            self.render_graph_detail(frame, chunks[0], detail);
            frame.render_widget(status_bar(&self.status, self.controls_hint()), chunks[1]);
            return;
        }

        let body = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Percentage(42),
                Constraint::Percentage(22),
                Constraint::Percentage(36),
            ])
            .split(chunks[0]);

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
        frame.render_widget(status_bar(&self.status, self.controls_hint()), chunks[1]);
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

    fn graph_lane_nodes(&self, lane: GraphLane) -> Vec<&RelationNode> {
        self.relation_graph
            .as_ref()
            .map(|graph| {
                graph
                    .nodes
                    .iter()
                    .filter(|node| match lane {
                        GraphLane::Incoming => node.role == RelationNodeRole::Incoming,
                        GraphLane::Center => node.role == RelationNodeRole::Center,
                        GraphLane::Outgoing => node.role == RelationNodeRole::Outgoing,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn graph_lane_len(&self, lane: GraphLane) -> usize {
        self.graph_lane_nodes(lane).len()
    }

    fn move_graph_row(&mut self, delta: isize) {
        if self.graph_lane == GraphLane::Center {
            let len = self
                .detail
                .as_ref()
                .map(|detail| detail.columns.len())
                .unwrap_or(0);
            self.graph_center_scroll = move_index(self.graph_center_scroll, len, delta);
        } else {
            self.graph_index = move_index(
                self.graph_index,
                self.graph_lane_len(self.graph_lane),
                delta,
            );
        }
    }

    fn move_graph_lane(&mut self, delta: isize) {
        let lanes = [GraphLane::Incoming, GraphLane::Center, GraphLane::Outgoing];
        let Some(current) = lanes.iter().position(|lane| *lane == self.graph_lane) else {
            return;
        };
        let mut next = current as isize + delta;

        while next >= 0 && next < lanes.len() as isize {
            let lane = lanes[next as usize];
            if self.graph_lane_len(lane) > 0 {
                self.graph_lane = lane;
                self.graph_index = self
                    .graph_index
                    .min(self.graph_lane_len(lane).saturating_sub(1));
                return;
            }
            next += delta;
        }
    }

    fn focused_graph_table(&self) -> Option<TableRef> {
        self.graph_lane_nodes(self.graph_lane)
            .get(self.graph_index)
            .map(|node| node.table.clone())
    }

    fn render_graph_detail(&self, frame: &mut Frame, area: Rect, detail: &TableDetail) {
        let Some(graph) = &self.relation_graph else {
            let loading = Paragraph::new("Loading relationship graph...").block(
                Block::default()
                    .title("Relation Graph")
                    .borders(Borders::ALL),
            );
            frame.render_widget(loading, area);
            return;
        };

        if area.width < 100 || area.height < 20 {
            let fallback = Paragraph::new(graph_fallback_lines(
                graph,
                detail,
                self.graph_center_scroll,
                area.width.saturating_sub(2) as usize,
                area.height.saturating_sub(2) as usize,
            ))
            .block(
                Block::default()
                    .title(format!("Relation Graph: {}", detail.table.display_name()))
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false });
            frame.render_widget(fallback, area);
            return;
        }

        let sections = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(10), Constraint::Length(7)])
            .split(area);
        let lanes = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(25),
                Constraint::Percentage(50),
                Constraint::Percentage(25),
            ])
            .split(sections[0]);

        let incoming = graph_node_lines(
            "Incoming",
            self.graph_lane_nodes(GraphLane::Incoming),
            self.graph_lane == GraphLane::Incoming,
            self.graph_index,
        );
        let center = graph_center_lines(
            detail,
            self.graph_center_scroll,
            lanes[1].width.saturating_sub(2) as usize,
            lanes[1].height.saturating_sub(2) as usize,
        );
        let outgoing = graph_node_lines(
            "Outgoing",
            self.graph_lane_nodes(GraphLane::Outgoing),
            self.graph_lane == GraphLane::Outgoing,
            self.graph_index,
        );

        frame.render_widget(
            Paragraph::new(incoming)
                .block(
                    Block::default()
                        .title(graph_lane_title(GraphLane::Incoming))
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false }),
            lanes[0],
        );
        frame.render_widget(
            Paragraph::new(center)
                .block(
                    Block::default()
                        .title(graph_center_title(
                            detail,
                            self.graph_center_scroll,
                            lanes[1].height.saturating_sub(2) as usize,
                        ))
                        .borders(Borders::ALL)
                        .border_style(graph_center_border_style(
                            self.graph_lane == GraphLane::Center,
                        )),
                )
                .wrap(Wrap { trim: false }),
            lanes[1],
        );
        frame.render_widget(
            Paragraph::new(outgoing)
                .block(
                    Block::default()
                        .title(graph_lane_title(GraphLane::Outgoing))
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false }),
            lanes[2],
        );

        frame.render_widget(
            Paragraph::new(graph_edge_lines(graph))
                .block(Block::default().title("Connections").borders(Borders::ALL))
                .wrap(Wrap { trim: false }),
            sections[1],
        );
    }

    fn find_selected_schema_hint(&self) -> Option<String> {
        None
    }
}

fn graph_node_lines<'a>(
    lane_title: &'a str,
    nodes: Vec<&'a RelationNode>,
    lane_focused: bool,
    focus_index: usize,
) -> Vec<Line<'a>> {
    if nodes.is_empty() {
        return vec![Line::from(format!(
            "No {} tables.",
            lane_title.to_lowercase()
        ))];
    }

    let mut lines = Vec::new();
    for (index, node) in nodes.iter().enumerate() {
        if index > 0 {
            lines.push(Line::from(String::new()));
        }
        let focused = lane_focused && index == focus_index;
        lines.extend(render_graph_node(node, focused));
    }
    lines
}

fn render_graph_node<'a>(node: &'a RelationNode, focused: bool) -> Vec<Line<'a>> {
    let style = if focused {
        Style::default().add_modifier(Modifier::REVERSED)
    } else {
        Style::default()
    };
    let name_style = style.add_modifier(Modifier::BOLD);
    let mut lines = vec![
        Line::styled("+--------------------------+", style),
        Line::styled(
            format!(
                "| {:<24} |",
                truncate_for_box(&node.table.display_name(), 24)
            ),
            name_style,
        ),
        Line::styled("|--------------------------|", style),
    ];

    if node.key_columns.is_empty() {
        lines.push(Line::styled("| (no key columns)         |", style));
    } else {
        for column in &node.key_columns {
            lines.push(Line::styled(
                format!("| {:<24} |", truncate_for_box(column, 24)),
                style,
            ));
        }
    }

    lines.push(Line::styled("+--------------------------+", style));
    lines
}

fn truncate_for_box(input: &str, width: usize) -> String {
    let mut rendered = input.chars().take(width).collect::<String>();
    let len = rendered.chars().count();
    if len < width {
        rendered.push_str(&" ".repeat(width - len));
    }
    rendered
}

fn truncate_with_ellipsis(input: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }

    let len = input.chars().count();
    if len <= width {
        return input.to_string();
    }

    if width <= 3 {
        return input.chars().take(width).collect();
    }

    let mut rendered = input.chars().take(width - 3).collect::<String>();
    rendered.push_str("...");
    rendered
}

fn graph_center_lines(
    detail: &TableDetail,
    scroll: usize,
    width: usize,
    height: usize,
) -> Vec<Line<'static>> {
    if detail.columns.is_empty() {
        return vec![Line::from("No columns found for the current table.")];
    }

    let visible_rows = height.max(1);
    detail
        .columns
        .iter()
        .skip(scroll)
        .take(visible_rows)
        .map(|column| graph_center_column_line(column, &detail.foreign_keys, width))
        .collect()
}

fn graph_center_column_line(
    column: &crate::db::ColumnMeta,
    foreign_keys: &[ForeignKeyMeta],
    width: usize,
) -> Line<'static> {
    let mut badges = Vec::new();
    let has_incoming_fk = foreign_keys
        .iter()
        .any(|edge| edge.local_column() == column.name && edge.direction.label() == "in");
    let has_outgoing_fk = foreign_keys
        .iter()
        .any(|edge| edge.local_column() == column.name && edge.direction.label() == "out");

    if column.is_primary_key {
        badges.push("[pk]");
    }
    if has_incoming_fk {
        badges.push("[fk-in]");
    }
    if has_outgoing_fk {
        badges.push("[fk-out]");
    }
    if column.nullable {
        badges.push("[null]");
    }

    let mut rendered = format!("{} : {}", column.name, column.data_type);
    if !badges.is_empty() {
        rendered.push(' ');
        rendered.push_str(&badges.join(" "));
    }

    Line::from(truncate_with_ellipsis(&rendered, width))
}

fn graph_center_title(detail: &TableDetail, scroll: usize, height: usize) -> String {
    let total = detail.columns.len();
    if total == 0 {
        return format!("Center (current): {} [0/0]", detail.table.display_name());
    }

    let start = scroll.min(total.saturating_sub(1)) + 1;
    let end = (scroll + height.max(1)).min(total);
    format!(
        "Center (current): {} [{}-{} / {}]",
        detail.table.display_name(),
        start,
        end,
        total
    )
}

fn graph_center_border_style(focused: bool) -> Style {
    if focused {
        Style::default().add_modifier(Modifier::REVERSED)
    } else {
        Style::default()
    }
}

fn graph_edge_lines(graph: &RelationGraph) -> Vec<Line<'static>> {
    if graph.edges.is_empty() {
        return vec![Line::from("No direct relationships for this table.")];
    }

    graph
        .edges
        .iter()
        .map(|edge| {
            if edge.source_table == graph.center && edge.target_table == graph.center {
                Line::from(format!(
                    "self: {} -> {}",
                    edge.source_column, edge.target_column
                ))
            } else if edge.source_table == graph.center {
                Line::from(format!(
                    "out: {} -> {}.{}",
                    edge.source_column,
                    edge.target_table.display_name(),
                    edge.target_column
                ))
            } else {
                Line::from(format!(
                    "in: {}.{} -> {}",
                    edge.source_table.display_name(),
                    edge.source_column,
                    edge.target_column
                ))
            }
        })
        .collect()
}

fn graph_fallback_lines(
    graph: &RelationGraph,
    detail: &TableDetail,
    scroll: usize,
    width: usize,
    height: usize,
) -> Vec<Line<'static>> {
    let incoming = graph
        .nodes
        .iter()
        .filter(|node| node.role == RelationNodeRole::Incoming)
        .map(|node| node.table.display_name())
        .collect::<Vec<_>>();
    let outgoing = graph
        .nodes
        .iter()
        .filter(|node| node.role == RelationNodeRole::Outgoing)
        .map(|node| node.table.display_name())
        .collect::<Vec<_>>();

    let mut lines = vec![
        Line::from("Terminal too small for the full graph view."),
        Line::from(format!(
            "{}: {}",
            graph_lane_title(GraphLane::Center),
            graph.center.display_name()
        )),
        Line::from("Columns:"),
    ];

    let column_height = height.saturating_sub(5).max(1);
    lines.extend(graph_center_lines(detail, scroll, width, column_height));
    lines.push(Line::from(String::new()));
    lines.push(Line::from(format!(
        "Incoming: {}",
        if incoming.is_empty() {
            "none".into()
        } else {
            incoming.join(", ")
        }
    )));
    lines.push(Line::from(format!(
        "Outgoing: {}",
        if outgoing.is_empty() {
            "none".into()
        } else {
            outgoing.join(", ")
        }
    )));

    lines
}

fn graph_lane_title(lane: GraphLane) -> &'static str {
    match lane {
        GraphLane::Incoming => "Incoming (-> center)",
        GraphLane::Center => "Center (current)",
        GraphLane::Outgoing => "Outgoing (center ->)",
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

fn status_bar(message: impl Into<String>, controls: impl Into<String>) -> Paragraph<'static> {
    let message = message.into();
    let controls = controls.into();

    Paragraph::new(Line::from(vec![
        Span::styled(" readgrid ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(message),
        Span::raw("  "),
        Span::styled(controls, Style::default().add_modifier(Modifier::DIM)),
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
                edge.to_table.display_name(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyModifiers;

    fn test_app(screen: Screen, search_mode: bool) -> App {
        App {
            config: ConfigStore {
                profiles_path: PathBuf::from("profiles.toml"),
                state_path: PathBuf::from("state.toml"),
                file: crate::config::ConfigFile::default(),
                state: crate::config::StateFile::default(),
            },
            screen,
            candidates: vec![ConnectionCandidate {
                profile: ConnectionProfile {
                    name: "sample".into(),
                    kind: DatabaseKind::Sqlite,
                    url: None,
                    path: Some(PathBuf::from("sample.db")),
                },
                source: "saved",
                preferred_schema: None,
            }],
            connection_index: 0,
            schemas: vec!["public".into()],
            schema_index: 0,
            tables: vec![TableRef {
                schema: None,
                name: "widgets".into(),
            }],
            table_index: 0,
            filter: String::new(),
            search_mode,
            session: None,
            active_profile: None,
            active_schema: None,
            detail: None,
            detail_view: DetailView::Table,
            relation_graph: None,
            graph_lane: GraphLane::Center,
            graph_index: 0,
            graph_center_scroll: 0,
            preview: None,
            sort_index: 0,
            sort_desc: false,
            status: String::new(),
            example_config: String::new(),
            pending_auto_connect: false,
        }
    }

    fn sample_relation_graph() -> RelationGraph {
        RelationGraph {
            center: TableRef {
                schema: None,
                name: "tasks".into(),
            },
            nodes: vec![
                RelationNode {
                    table: TableRef {
                        schema: None,
                        name: "comments".into(),
                    },
                    key_columns: vec!["id".into(), "task_id".into()],
                    role: RelationNodeRole::Incoming,
                },
                RelationNode {
                    table: TableRef {
                        schema: None,
                        name: "tasks".into(),
                    },
                    key_columns: vec!["id".into(), "project_id".into()],
                    role: RelationNodeRole::Center,
                },
                RelationNode {
                    table: TableRef {
                        schema: None,
                        name: "projects".into(),
                    },
                    key_columns: vec!["id".into()],
                    role: RelationNodeRole::Outgoing,
                },
                RelationNode {
                    table: TableRef {
                        schema: None,
                        name: "users".into(),
                    },
                    key_columns: vec!["id".into()],
                    role: RelationNodeRole::Outgoing,
                },
            ],
            edges: vec![],
        }
    }

    fn sample_graph_detail() -> TableDetail {
        TableDetail {
            table: TableRef {
                schema: None,
                name: "tasks".into(),
            },
            columns: vec![
                crate::db::ColumnMeta {
                    name: "id".into(),
                    data_type: "INTEGER".into(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: true,
                },
                crate::db::ColumnMeta {
                    name: "project_id".into(),
                    data_type: "INTEGER".into(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: false,
                },
                crate::db::ColumnMeta {
                    name: "owner_id".into(),
                    data_type: "INTEGER".into(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                crate::db::ColumnMeta {
                    name: "title".into(),
                    data_type: "TEXT".into(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: false,
                },
            ],
            foreign_keys: vec![
                ForeignKeyMeta {
                    from_column: "project_id".into(),
                    to_table: TableRef {
                        schema: None,
                        name: "projects".into(),
                    },
                    to_column: "id".into(),
                    direction: crate::db::RelationshipDirection::Outgoing,
                },
                ForeignKeyMeta {
                    from_column: "tasks".into(),
                    to_table: TableRef {
                        schema: None,
                        name: "comments".into(),
                    },
                    to_column: "owner_id".into(),
                    direction: crate::db::RelationshipDirection::Incoming,
                },
            ],
            indexes: vec![],
        }
    }

    fn line_text(line: &Line<'_>) -> String {
        line.spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>()
    }

    #[test]
    fn graph_lane_title_marks_center_as_current() {
        assert_eq!(graph_lane_title(GraphLane::Center), "Center (current)");
    }

    #[test]
    fn render_graph_node_uses_name_header_and_separator() {
        let node = RelationNode {
            table: TableRef {
                schema: None,
                name: "tasks".into(),
            },
            key_columns: vec!["id".into(), "project_id".into()],
            role: RelationNodeRole::Center,
        };

        let lines = render_graph_node(&node, false);

        assert_eq!(line_text(&lines[1]), "| tasks                    |");
        assert_eq!(line_text(&lines[2]), "|--------------------------|");
        assert_eq!(
            lines[1].style,
            Style::default().add_modifier(Modifier::BOLD)
        );
    }

    #[test]
    fn render_graph_node_keeps_empty_state_below_name_header() {
        let node = RelationNode {
            table: TableRef {
                schema: None,
                name: "audit_log".into(),
            },
            key_columns: vec![],
            role: RelationNodeRole::Outgoing,
        };

        let lines = render_graph_node(&node, false);

        assert_eq!(line_text(&lines[2]), "|--------------------------|");
        assert_eq!(line_text(&lines[3]), "| (no key columns)         |");
    }

    #[test]
    fn render_graph_node_adds_bold_to_focused_name_row() {
        let node = RelationNode {
            table: TableRef {
                schema: None,
                name: "projects".into(),
            },
            key_columns: vec!["id".into()],
            role: RelationNodeRole::Outgoing,
        };

        let lines = render_graph_node(&node, true);

        assert_eq!(
            lines[1].style,
            Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD)
        );
    }

    #[test]
    fn graph_center_lines_show_type_and_relation_badges() {
        let detail = sample_graph_detail();

        let lines = graph_center_lines(&detail, 0, 80, 4);

        assert_eq!(line_text(&lines[0]), "id : INTEGER [pk]");
        assert_eq!(line_text(&lines[1]), "project_id : INTEGER [fk-out]");
        assert_eq!(line_text(&lines[2]), "owner_id : INTEGER [fk-in] [null]");
        assert_eq!(line_text(&lines[3]), "title : TEXT");
    }

    #[test]
    fn graph_fallback_lines_include_center_column_detail() {
        let graph = sample_relation_graph();
        let detail = sample_graph_detail();

        let lines = graph_fallback_lines(&graph, &detail, 0, 80, 12);
        let rendered = lines.iter().map(line_text).collect::<Vec<_>>().join("\n");

        assert!(rendered.contains("Columns:"));
        assert!(rendered.contains("project_id : INTEGER [fk-out]"));
        assert!(rendered.contains("owner_id : INTEGER [fk-in] [null]"));
    }

    #[tokio::test]
    async fn q_quits_globally_when_not_searching() {
        let mut app = test_app(Screen::Browser, false);

        let should_quit = app
            .handle_key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(should_quit);
    }

    #[tokio::test]
    async fn esc_quits_from_connections_screen() {
        let mut app = test_app(Screen::Connections, false);

        let should_quit = app
            .handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(should_quit);
    }

    #[tokio::test]
    async fn esc_goes_back_from_schemas_screen() {
        let mut app = test_app(Screen::Schemas, false);

        let should_quit = app
            .handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(!should_quit);
        assert_eq!(app.screen, Screen::Connections);
    }

    #[tokio::test]
    async fn esc_clears_search_without_quitting() {
        let mut app = test_app(Screen::Browser, true);
        app.filter = "wi".into();

        let should_quit = app
            .handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(!should_quit);
        assert_eq!(app.screen, Screen::Browser);
        assert!(!app.search_mode);
        assert!(app.filter.is_empty());
    }

    #[tokio::test]
    async fn q_is_search_input_while_filtering() {
        let mut app = test_app(Screen::Browser, true);

        let should_quit = app
            .handle_key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(!should_quit);
        assert_eq!(app.filter, "q");
    }

    #[tokio::test]
    async fn esc_returns_to_browser_from_detail() {
        let mut app = test_app(Screen::Detail, false);

        let should_quit = app
            .handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(!should_quit);
        assert_eq!(app.screen, Screen::Browser);
    }

    #[tokio::test]
    async fn g_toggles_to_graph_when_graph_is_cached() {
        let mut app = test_app(Screen::Detail, false);
        app.detail = Some(TableDetail {
            table: TableRef {
                schema: None,
                name: "tasks".into(),
            },
            columns: vec![],
            foreign_keys: vec![],
            indexes: vec![],
        });
        app.relation_graph = Some(sample_relation_graph());

        let should_quit = app
            .handle_key(KeyEvent::new(KeyCode::Char('g'), KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(!should_quit);
        assert_eq!(app.detail_view, DetailView::Graph);
        assert_eq!(app.graph_lane, GraphLane::Center);
    }

    #[tokio::test]
    async fn esc_returns_to_table_mode_from_graph() {
        let mut app = test_app(Screen::Detail, false);
        app.detail = Some(TableDetail {
            table: TableRef {
                schema: None,
                name: "tasks".into(),
            },
            columns: vec![],
            foreign_keys: vec![],
            indexes: vec![],
        });
        app.detail_view = DetailView::Graph;
        app.relation_graph = Some(sample_relation_graph());

        let should_quit = app
            .handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(!should_quit);
        assert_eq!(app.screen, Screen::Detail);
        assert_eq!(app.detail_view, DetailView::Table);
    }

    #[tokio::test]
    async fn graph_navigation_moves_focus_between_lanes() {
        let mut app = test_app(Screen::Detail, false);
        app.detail = Some(TableDetail {
            table: TableRef {
                schema: None,
                name: "tasks".into(),
            },
            columns: vec![],
            foreign_keys: vec![],
            indexes: vec![],
        });
        app.detail_view = DetailView::Graph;
        app.relation_graph = Some(sample_relation_graph());

        app.handle_key(KeyEvent::new(KeyCode::Left, KeyModifiers::NONE))
            .await
            .unwrap();
        assert_eq!(app.graph_lane, GraphLane::Incoming);
        assert_eq!(app.focused_graph_table().unwrap().name, "comments");

        app.handle_key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE))
            .await
            .unwrap();
        app.handle_key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE))
            .await
            .unwrap();
        assert_eq!(app.graph_lane, GraphLane::Outgoing);
        assert_eq!(app.focused_graph_table().unwrap().name, "projects");

        app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE))
            .await
            .unwrap();
        assert_eq!(app.focused_graph_table().unwrap().name, "users");
    }

    #[tokio::test]
    async fn center_lane_uses_up_down_for_column_scrolling() {
        let mut app = test_app(Screen::Detail, false);
        app.detail = Some(sample_graph_detail());
        app.detail_view = DetailView::Graph;
        app.relation_graph = Some(sample_relation_graph());

        app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE))
            .await
            .unwrap();
        app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE))
            .await
            .unwrap();

        assert_eq!(app.graph_lane, GraphLane::Center);
        assert_eq!(app.graph_index, 0);
        assert_eq!(app.graph_center_scroll, 2);

        app.handle_key(KeyEvent::new(KeyCode::Left, KeyModifiers::NONE))
            .await
            .unwrap();
        app.handle_key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE))
            .await
            .unwrap();
        assert_eq!(app.graph_center_scroll, 2);
    }

    #[tokio::test]
    async fn enter_is_noop_when_center_lane_is_selected() {
        let mut app = test_app(Screen::Detail, false);
        app.detail = Some(sample_graph_detail());
        app.detail_view = DetailView::Graph;
        app.relation_graph = Some(sample_relation_graph());
        app.graph_center_scroll = 1;

        let should_quit = app
            .handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(!should_quit);
        assert_eq!(app.graph_lane, GraphLane::Center);
        assert_eq!(app.graph_center_scroll, 1);
        assert_eq!(app.status, "Already centered on the current table.");
    }

    #[tokio::test]
    async fn enter_recenters_graph_using_sample_db() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("sample")
            .join("readgrid_demo.db");
        let session = Session::connect(&ConnectionProfile {
            name: "sample".into(),
            kind: DatabaseKind::Sqlite,
            url: None,
            path: Some(path),
        })
        .await
        .unwrap();

        let mut app = test_app(Screen::Detail, false);
        app.session = Some(session);
        app.detail = Some(
            app.session()
                .unwrap()
                .load_detail(&TableRef {
                    schema: None,
                    name: "tasks".into(),
                })
                .await
                .unwrap(),
        );
        app.relation_graph = Some(
            app.session()
                .unwrap()
                .load_relation_graph(&TableRef {
                    schema: None,
                    name: "tasks".into(),
                })
                .await
                .unwrap(),
        );
        app.detail_view = DetailView::Graph;
        app.graph_lane = GraphLane::Outgoing;
        app.graph_index = 0;
        app.graph_center_scroll = 3;

        let should_quit = app
            .handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(!should_quit);
        assert_eq!(app.detail_view, DetailView::Graph);
        assert_eq!(app.detail.as_ref().unwrap().table.name, "projects");
        assert_eq!(app.relation_graph.as_ref().unwrap().center.name, "projects");
        assert_eq!(app.graph_center_scroll, 0);
    }
}
