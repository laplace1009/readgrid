use std::{io::Stdout, path::PathBuf, time::Duration};

use anyhow::{Result, anyhow};
use clap::{Parser, ValueEnum};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
    terminal,
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, Cell, Clear, List, ListItem, ListState, Paragraph, Row, Table, TableState,
        Wrap,
    },
};
use serde::{Deserialize, Serialize};

use crate::{
    config::{BookmarkConnectionTarget, ConfigStore, FilterPreset, SavedBookmark},
    db::{
        ConnectionProfile, DataPreview, DatabaseKind, DrillThroughAction, ExportFormat,
        ExportRequest, ExportScope, ExportSummary, FilterOperator, ForeignKeyMeta,
        InvestigationSource, InvestigationState, PreviewFilter, RelationGraph, RelationNode,
        RelationNodeRole, Session, SortState, TableDetail, TableRef, build_drill_through_actions,
        write_preview_csv,
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
    pub table: Option<String>,
    #[arg(long, value_enum)]
    pub view: Option<StartupView>,
    #[arg(long)]
    pub bookmark: Option<String>,
    #[arg(long)]
    pub mcp_context_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartupView {
    Detail,
    Graph,
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
enum DetailFilterPrompt {
    SelectColumn {
        index: usize,
    },
    SelectOperator {
        column_index: usize,
        index: usize,
    },
    EnterValue {
        column_index: usize,
        operator: FilterOperator,
        value: String,
    },
}

#[derive(Debug, Clone)]
enum DetailExportPrompt {
    EnterPath {
        scope: ExportScope,
        value: String,
        edited: bool,
    },
    ConfirmOverwrite {
        scope: ExportScope,
        value: String,
        edited: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkspaceAction {
    SaveBookmark,
    OpenBookmark,
    SavePreset,
    ApplyPreset,
}

impl WorkspaceAction {
    fn label(&self) -> &'static str {
        match self {
            Self::SaveBookmark => "Save bookmark",
            Self::OpenBookmark => "Open bookmark",
            Self::SavePreset => "Save preset",
            Self::ApplyPreset => "Apply preset",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkspaceEntryKind {
    Bookmark,
    Preset,
}

impl WorkspaceEntryKind {
    fn label(&self) -> &'static str {
        match self {
            Self::Bookmark => "bookmark",
            Self::Preset => "preset",
        }
    }
}

#[derive(Debug, Clone)]
enum WorkspacePrompt {
    ChooseAction {
        actions: Vec<WorkspaceAction>,
        index: usize,
    },
    EnterName {
        kind: WorkspaceEntryKind,
        value: String,
    },
    ConfirmOverwrite {
        kind: WorkspaceEntryKind,
        value: String,
    },
    OpenBookmark {
        bookmarks: Vec<SavedBookmark>,
        index: usize,
    },
    ApplyPreset {
        presets: Vec<FilterPreset>,
        index: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetailFilterOutcome {
    None,
    ReloadPreview,
}

#[derive(Debug, Clone)]
struct ConnectionCandidate {
    profile: ConnectionProfile,
    bookmark_target: BookmarkConnectionTarget,
    source: &'static str,
}

#[derive(Debug, Clone, Default)]
struct StartupTarget {
    schema: Option<String>,
    table: Option<String>,
    view: Option<StartupView>,
    filters: Vec<PreviewFilter>,
    sort: Option<SortState>,
}

impl StartupTarget {
    fn is_empty(&self) -> bool {
        self.schema.is_none()
            && self.table.is_none()
            && self.view.is_none()
            && self.filters.is_empty()
            && self.sort.is_none()
    }

    fn investigation_for_table(&self, table: TableRef) -> InvestigationState {
        InvestigationState {
            source: InvestigationSource::Table(table),
            sort: self.sort.clone(),
            filters: self.filters.clone(),
            page: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct BookmarkStartupContext {
    bookmark: SavedBookmark,
    profile: ConnectionProfile,
}

#[derive(Debug, Clone, Default)]
struct BookmarkDefaults {
    cli: Option<BookmarkStartupContext>,
    mcp: Option<BookmarkStartupContext>,
    warning: Option<String>,
}

impl BookmarkDefaults {
    fn cli_bookmark(&self) -> Option<&SavedBookmark> {
        self.cli.as_ref().map(|context| &context.bookmark)
    }

    fn mcp_bookmark(&self) -> Option<&SavedBookmark> {
        self.mcp.as_ref().map(|context| &context.bookmark)
    }

    fn cli_profile(&self) -> Option<&ConnectionProfile> {
        self.cli.as_ref().map(|context| &context.profile)
    }

    fn mcp_profile(&self) -> Option<&ConnectionProfile> {
        self.mcp.as_ref().map(|context| &context.profile)
    }
}

#[derive(Debug, Clone)]
struct DetailNavStackEntry {
    investigation: InvestigationState,
    selected_row: usize,
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
    table_filter: String,
    table_search_mode: bool,
    session: Option<Session>,
    active_profile: Option<ConnectionProfile>,
    active_connection_target: Option<BookmarkConnectionTarget>,
    active_schema: Option<String>,
    detail: Option<TableDetail>,
    active_investigation: Option<InvestigationState>,
    detail_view: DetailView,
    detail_filter_index: usize,
    detail_filter_prompt: Option<DetailFilterPrompt>,
    detail_export_prompt: Option<DetailExportPrompt>,
    workspace_prompt: Option<WorkspacePrompt>,
    detail_drill_actions: Option<Vec<DrillThroughAction>>,
    detail_drill_index: usize,
    detail_nav_stack: Vec<DetailNavStackEntry>,
    relation_graph: Option<RelationGraph>,
    graph_lane: GraphLane,
    graph_index: usize,
    graph_center_scroll: usize,
    preview: Option<DataPreview>,
    preview_row_index: usize,
    sort_index: usize,
    status: String,
    example_config: String,
    pending_auto_connect: bool,
    startup_target: Option<StartupTarget>,
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
        if args.profile.is_some() && (args.pg_url.is_some() || args.sqlite_path.is_some()) {
            return Err(anyhow!(
                "use either --profile or a direct connection target, not both"
            ));
        }

        let example_config = ConfigStore::example_profiles();
        let bookmark_defaults = resolve_bookmark_defaults(&args, &config, mcp_context.as_ref());
        let startup_target = build_startup_target(&args, mcp_context.as_ref(), &bookmark_defaults);
        let (candidates, selected_index, pending_auto_connect) =
            build_candidates(&args, &config, mcp_context.as_ref(), &bookmark_defaults);

        let status = if let Some(warning) = bookmark_defaults.warning {
            if candidates.is_empty() {
                format!(
                    "{warning} No connection sources found. Use --pg-url/--sqlite-path or add profiles.toml."
                )
            } else {
                format!("{warning} Choose a connection profile and press Enter.")
            }
        } else if candidates.is_empty() {
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
            table_filter: String::new(),
            table_search_mode: false,
            session: None,
            active_profile: None,
            active_connection_target: None,
            active_schema: None,
            detail: None,
            active_investigation: None,
            detail_view: DetailView::Table,
            detail_filter_index: 0,
            detail_filter_prompt: None,
            detail_export_prompt: None,
            workspace_prompt: None,
            detail_drill_actions: None,
            detail_drill_index: 0,
            detail_nav_stack: Vec::new(),
            relation_graph: None,
            graph_lane: GraphLane::Center,
            graph_index: 0,
            graph_center_scroll: 0,
            preview: None,
            preview_row_index: 0,
            sort_index: 0,
            status,
            example_config,
            pending_auto_connect,
            startup_target,
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
        if matches!(key.code, KeyCode::Char('q')) && !self.is_input_mode_active() {
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
        if self.table_search_mode {
            self.handle_search_input(key);
            return Ok(false);
        }

        if self.workspace_prompt.is_some() {
            self.handle_workspace_prompt_key(key).await?;
            return Ok(false);
        }

        match key.code {
            KeyCode::Up | KeyCode::Char('k') => self.move_table(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_table(1),
            KeyCode::Char('/') => {
                self.table_search_mode = true;
                self.status = "Search tables: type to filter, Esc to clear.".into();
            }
            KeyCode::Char('r') => {
                self.reload_tables().await?;
            }
            KeyCode::Enter => {
                self.load_selected_table_detail().await?;
            }
            KeyCode::Char('b') => self.start_workspace_prompt(),
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
        if self.workspace_prompt.is_some() {
            self.handle_workspace_prompt_key(key).await?;
            return Ok(false);
        }

        if self.detail_view == DetailView::Graph {
            return self.handle_graph_key(key).await;
        }

        if self.detail_export_prompt.is_some() {
            self.handle_detail_export_prompt_key(key).await?;
            return Ok(false);
        }

        if self.detail_filter_prompt.is_some() {
            if self.handle_detail_filter_prompt_key(key)? == DetailFilterOutcome::ReloadPreview {
                self.reload_preview().await?;
            }
            return Ok(false);
        }

        if self.detail_drill_actions.is_some() {
            self.handle_detail_drill_key(key).await?;
            return Ok(false);
        }

        match key.code {
            KeyCode::Esc => {
                if !self.pop_detail_nav_stack().await? {
                    self.screen = Screen::Browser;
                    self.status = "Returned to table browser.".into();
                }
            }
            KeyCode::Up | KeyCode::Char('k') => self.move_preview_row(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_preview_row(1),
            KeyCode::Enter => self.start_detail_drill_prompt(),
            KeyCode::Char('g') => {
                self.enter_graph_view().await?;
            }
            KeyCode::Char('b') => self.start_workspace_prompt(),
            KeyCode::Char('e') => self.start_detail_export_prompt(),
            KeyCode::Char('f') => self.start_detail_filter_prompt(),
            KeyCode::Char('h') | KeyCode::Left => self.move_detail_filter_selection(-1),
            KeyCode::Char('l') | KeyCode::Right => self.move_detail_filter_selection(1),
            KeyCode::Char('x') => {
                if self.remove_selected_detail_filter() {
                    self.reload_preview().await?;
                }
            }
            KeyCode::Char('c') => {
                if self.clear_detail_filters() {
                    self.reload_preview().await?;
                }
            }
            KeyCode::Char('[') => {
                if self.sort_index > 0 {
                    self.sort_index -= 1;
                    self.sync_sort_from_index()?;
                    self.reload_preview().await?;
                }
            }
            KeyCode::Char(']') => {
                if let Some(detail) = &self.detail {
                    if self.sort_index + 1 < detail.columns.len() {
                        self.sort_index += 1;
                        self.sync_sort_from_index()?;
                        self.reload_preview().await?;
                    }
                }
            }
            KeyCode::Char('s') => {
                self.toggle_sort_direction()?;
                self.reload_preview().await?;
            }
            KeyCode::Char('n') => {
                if self
                    .preview
                    .as_ref()
                    .map(|preview| preview.has_more)
                    .unwrap_or(false)
                {
                    self.preview_page_forward();
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
                    self.preview_page_back();
                    self.reload_preview().await?;
                }
            }
            KeyCode::Char('r') => {
                self.reload_preview().await?;
                self.status = "Reloaded preview data.".into();
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
            KeyCode::Char('b') => self.start_workspace_prompt(),
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
                self.table_search_mode = false;
                self.table_filter.clear();
                self.table_index = 0;
                self.status = "Cleared table filter.".into();
            }
            KeyCode::Enter => {
                self.table_search_mode = false;
                self.status = format!("Filtering tables by '{}'.", self.table_filter);
            }
            KeyCode::Backspace => {
                self.table_filter.pop();
                self.clamp_table_index();
            }
            KeyCode::Char(ch) => {
                self.table_filter.push(ch);
                self.clamp_table_index();
            }
            _ => {}
        }
    }

    fn start_workspace_prompt(&mut self) {
        let actions = self.available_workspace_actions();
        if actions.is_empty() {
            self.status = "No workspace actions are available here.".into();
            return;
        }

        if actions.len() == 1 {
            self.start_workspace_action(actions[0]);
            return;
        }

        self.workspace_prompt = Some(WorkspacePrompt::ChooseAction { actions, index: 0 });
        self.status = "Choose a workspace action.".into();
    }

    fn available_workspace_actions(&self) -> Vec<WorkspaceAction> {
        match self.screen {
            Screen::Browser => vec![WorkspaceAction::OpenBookmark],
            Screen::Detail if self.detail_view == DetailView::Graph => {
                vec![WorkspaceAction::SaveBookmark, WorkspaceAction::OpenBookmark]
            }
            Screen::Detail => vec![
                WorkspaceAction::SaveBookmark,
                WorkspaceAction::OpenBookmark,
                WorkspaceAction::SavePreset,
                WorkspaceAction::ApplyPreset,
            ],
            _ => Vec::new(),
        }
    }

    fn start_workspace_action(&mut self, action: WorkspaceAction) {
        match action {
            WorkspaceAction::SaveBookmark => self.start_save_bookmark_prompt(),
            WorkspaceAction::OpenBookmark => self.start_open_bookmark_prompt(),
            WorkspaceAction::SavePreset => self.start_save_preset_prompt(),
            WorkspaceAction::ApplyPreset => self.start_apply_preset_prompt(),
        }
    }

    fn start_save_bookmark_prompt(&mut self) {
        let Some(detail) = self.detail.as_ref() else {
            self.status = "Open a table detail first to save a bookmark.".into();
            return;
        };
        if self.active_connection_target.is_none() {
            self.status = "No active connection target is available for bookmark saving.".into();
            return;
        }

        self.workspace_prompt = Some(WorkspacePrompt::EnterName {
            kind: WorkspaceEntryKind::Bookmark,
            value: detail.table.display_name(),
        });
        self.status = "Type a bookmark name and press Enter.".into();
    }

    fn start_open_bookmark_prompt(&mut self) {
        let bookmarks = self.config.sorted_bookmarks();
        if bookmarks.is_empty() {
            self.workspace_prompt = None;
            self.status = "No saved bookmarks were found.".into();
            return;
        }

        self.workspace_prompt = Some(WorkspacePrompt::OpenBookmark {
            bookmarks,
            index: 0,
        });
        self.status = "Choose a bookmark and press Enter.".into();
    }

    fn start_save_preset_prompt(&mut self) {
        if self.detail_view != DetailView::Table {
            self.status = "Filter presets are only available from the table detail view.".into();
            return;
        }
        if self.active_filters().is_empty() {
            self.status = "Add at least one filter before saving a preset.".into();
            return;
        }
        let Some(detail) = self.detail.as_ref() else {
            self.status = "Open a table detail first to save a preset.".into();
            return;
        };
        if self.active_connection_target.is_none() {
            self.status = "No active connection target is available for preset saving.".into();
            return;
        }

        self.workspace_prompt = Some(WorkspacePrompt::EnterName {
            kind: WorkspaceEntryKind::Preset,
            value: format!("{} filters", detail.table.display_name()),
        });
        self.status = "Type a preset name and press Enter.".into();
    }

    fn start_apply_preset_prompt(&mut self) {
        let Ok(connection) = self.active_connection_target() else {
            self.status = "No active connection target is available for preset lookup.".into();
            return;
        };
        let Some(detail) = self.detail.as_ref() else {
            self.status = "Open a table detail first to apply a preset.".into();
            return;
        };

        let presets = self
            .config
            .sorted_presets_for_scope(connection, &detail.table);
        if presets.is_empty() {
            self.workspace_prompt = None;
            self.status = format!(
                "No saved presets match {} on this connection.",
                detail.table.display_name()
            );
            return;
        }

        self.workspace_prompt = Some(WorkspacePrompt::ApplyPreset { presets, index: 0 });
        self.status = "Choose a preset and press Enter.".into();
    }

    async fn handle_workspace_prompt_key(&mut self, key: KeyEvent) -> Result<()> {
        let Some(prompt) = self.workspace_prompt.clone() else {
            return Ok(());
        };

        match prompt {
            WorkspacePrompt::ChooseAction { actions, index } => match key.code {
                KeyCode::Esc => {
                    self.workspace_prompt = None;
                    self.status = "Canceled workspace actions.".into();
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.workspace_prompt = Some(WorkspacePrompt::ChooseAction {
                        index: move_index(index, actions.len(), -1),
                        actions,
                    });
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.workspace_prompt = Some(WorkspacePrompt::ChooseAction {
                        index: move_index(index, actions.len(), 1),
                        actions,
                    });
                }
                KeyCode::Enter => {
                    if let Some(action) = actions.get(index).copied() {
                        self.start_workspace_action(action);
                    }
                }
                _ => {
                    self.workspace_prompt = Some(WorkspacePrompt::ChooseAction { actions, index });
                }
            },
            WorkspacePrompt::EnterName { kind, mut value } => match key.code {
                KeyCode::Esc => {
                    self.workspace_prompt = None;
                    self.status = format!("Canceled {} save.", kind.label());
                }
                KeyCode::Backspace => {
                    value.pop();
                    self.workspace_prompt = Some(WorkspacePrompt::EnterName { kind, value });
                }
                KeyCode::Enter => {
                    let trimmed = value.trim().to_string();
                    if trimmed.is_empty() {
                        self.workspace_prompt = Some(WorkspacePrompt::EnterName { kind, value });
                        self.status = format!(
                            "Enter a non-empty {} name or press Esc to cancel.",
                            kind.label()
                        );
                    } else if self.workspace_entry_exists(kind, &trimmed) {
                        self.workspace_prompt = Some(WorkspacePrompt::ConfirmOverwrite {
                            kind,
                            value: trimmed,
                        });
                        self.status = format!(
                            "A {} named '{}' already exists. Press Enter to overwrite or Esc to edit.",
                            kind.label(),
                            value.trim()
                        );
                    } else {
                        self.save_workspace_entry(kind, trimmed).await?;
                    }
                }
                KeyCode::Char(ch) => {
                    value.push(ch);
                    self.workspace_prompt = Some(WorkspacePrompt::EnterName { kind, value });
                }
                _ => {
                    self.workspace_prompt = Some(WorkspacePrompt::EnterName { kind, value });
                }
            },
            WorkspacePrompt::ConfirmOverwrite { kind, value } => match key.code {
                KeyCode::Esc => {
                    self.workspace_prompt = Some(WorkspacePrompt::EnterName { kind, value });
                    self.status = format!("Returned to {} name entry.", kind.label());
                }
                KeyCode::Enter => self.save_workspace_entry(kind, value).await?,
                _ => {
                    self.workspace_prompt = Some(WorkspacePrompt::ConfirmOverwrite { kind, value });
                }
            },
            WorkspacePrompt::OpenBookmark { bookmarks, index } => match key.code {
                KeyCode::Esc => {
                    self.workspace_prompt = None;
                    self.status = "Canceled bookmark picker.".into();
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.workspace_prompt = Some(WorkspacePrompt::OpenBookmark {
                        index: move_index(index, bookmarks.len(), -1),
                        bookmarks,
                    });
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.workspace_prompt = Some(WorkspacePrompt::OpenBookmark {
                        index: move_index(index, bookmarks.len(), 1),
                        bookmarks,
                    });
                }
                KeyCode::Enter => {
                    if let Some(bookmark) = bookmarks.get(index).cloned() {
                        self.workspace_prompt = None;
                        self.open_bookmark(bookmark).await?;
                    }
                }
                _ => {
                    self.workspace_prompt =
                        Some(WorkspacePrompt::OpenBookmark { bookmarks, index });
                }
            },
            WorkspacePrompt::ApplyPreset { presets, index } => match key.code {
                KeyCode::Esc => {
                    self.workspace_prompt = None;
                    self.status = "Canceled preset picker.".into();
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.workspace_prompt = Some(WorkspacePrompt::ApplyPreset {
                        index: move_index(index, presets.len(), -1),
                        presets,
                    });
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.workspace_prompt = Some(WorkspacePrompt::ApplyPreset {
                        index: move_index(index, presets.len(), 1),
                        presets,
                    });
                }
                KeyCode::Enter => {
                    if let Some(preset) = presets.get(index).cloned() {
                        self.workspace_prompt = None;
                        self.apply_filter_preset(preset).await?;
                    }
                }
                _ => {
                    self.workspace_prompt = Some(WorkspacePrompt::ApplyPreset { presets, index });
                }
            },
        }

        Ok(())
    }

    fn workspace_entry_exists(&self, kind: WorkspaceEntryKind, name: &str) -> bool {
        match kind {
            WorkspaceEntryKind::Bookmark => self.config.find_bookmark(name).is_some(),
            WorkspaceEntryKind::Preset => {
                let Ok(connection) = self.active_connection_target() else {
                    return false;
                };
                let Some(detail) = self.detail.as_ref() else {
                    return false;
                };
                self.config
                    .find_filter_preset(connection, &detail.table, name)
                    .is_some()
            }
        }
    }

    async fn save_workspace_entry(&mut self, kind: WorkspaceEntryKind, name: String) -> Result<()> {
        match kind {
            WorkspaceEntryKind::Bookmark => self.save_bookmark(name)?,
            WorkspaceEntryKind::Preset => self.save_filter_preset(name)?,
        }
        self.workspace_prompt = None;
        Ok(())
    }

    fn save_bookmark(&mut self, name: String) -> Result<()> {
        let table = self
            .detail
            .as_ref()
            .map(|detail| detail.table.clone())
            .ok_or_else(|| anyhow!("bookmark saving requires an active table detail"))?;
        let connection = self.active_connection_target()?.clone();
        let replaced = self.config.upsert_bookmark(SavedBookmark {
            name: name.clone(),
            connection,
            table,
            preferred_view: Some(self.current_startup_view()),
            filters: self.active_filters().to_vec(),
            sort: self.current_sort(),
        })?;
        self.status = if replaced {
            format!("Updated bookmark '{name}'.")
        } else {
            format!("Saved bookmark '{name}'.")
        };
        Ok(())
    }

    fn save_filter_preset(&mut self, name: String) -> Result<()> {
        let table = self
            .detail
            .as_ref()
            .map(|detail| detail.table.clone())
            .ok_or_else(|| anyhow!("preset saving requires an active table detail"))?;
        let connection = self.active_connection_target()?.clone();
        let replaced = self.config.upsert_filter_preset(FilterPreset {
            name: name.clone(),
            connection,
            table,
            filters: self.active_filters().to_vec(),
        })?;
        self.status = if replaced {
            format!("Updated preset '{name}'.")
        } else {
            format!("Saved preset '{name}'.")
        };
        Ok(())
    }

    async fn apply_filter_preset(&mut self, preset: FilterPreset) -> Result<()> {
        let name = preset.name.clone();
        let investigation = self.active_investigation_mut()?;
        investigation.filters = preset.filters;
        investigation.page = 0;
        self.detail_filter_index = self.active_filters().len().saturating_sub(1);
        self.detail_filter_prompt = None;
        self.reload_preview().await?;
        self.status = format!("Applied preset '{name}'.");
        Ok(())
    }

    async fn open_bookmark(&mut self, bookmark: SavedBookmark) -> Result<()> {
        let Some(profile) = self.config.resolve_connection_target(&bookmark.connection) else {
            self.status = format!(
                "Bookmark '{}' could not resolve {}.",
                bookmark.name,
                bookmark.connection.label()
            );
            return Ok(());
        };

        self.startup_target = Some(startup_target_from_bookmark(&bookmark));
        if self.active_connection_target.as_ref() == Some(&bookmark.connection)
            && self.session.is_some()
        {
            if self.session_kind() == Some(DatabaseKind::Postgres) {
                self.schemas = self.session().unwrap().list_schemas().await?;
                self.schema_index = 0;
                self.screen = Screen::Schemas;
                self.continue_startup_after_connect().await?;
            } else {
                self.load_tables(None).await?;
                self.screen = Screen::Browser;
                self.continue_startup_after_table_load().await?;
            }
        } else {
            self.connect_candidate(ConnectionCandidate {
                profile,
                bookmark_target: bookmark.connection.clone(),
                source: "bookmark",
            })
            .await?;
        }

        Ok(())
    }

    fn start_detail_export_prompt(&mut self) {
        if self.preview.is_none() {
            self.status = "No preview data loaded for export.".into();
            return;
        }

        let scope = ExportScope::VisiblePage;
        let default_path = self.default_export_path(scope);
        self.detail_export_prompt = Some(DetailExportPrompt::EnterPath {
            scope,
            value: default_path.display().to_string(),
            edited: false,
        });
        self.status = "Press Enter to export CSV, edit the path, or Tab to change scope.".into();
    }

    fn default_export_path(&self, scope: ExportScope) -> PathBuf {
        let file_name = self
            .detail
            .as_ref()
            .map(|detail| match &detail.table.schema {
                Some(schema) => format!(
                    "{}_{}{}.csv",
                    schema,
                    detail.table.name,
                    if scope == ExportScope::AllMatchingRows {
                        "_all"
                    } else {
                        ""
                    }
                ),
                None => format!(
                    "{}{}.csv",
                    detail.table.name,
                    if scope == ExportScope::AllMatchingRows {
                        "_all"
                    } else {
                        ""
                    }
                ),
            })
            .unwrap_or_else(|| "preview.csv".into());
        PathBuf::from("db_csv").join(file_name)
    }

    async fn handle_detail_export_prompt_key(&mut self, key: KeyEvent) -> Result<()> {
        let Some(prompt) = self.detail_export_prompt.clone() else {
            return Ok(());
        };

        match prompt {
            DetailExportPrompt::EnterPath {
                scope,
                mut value,
                mut edited,
            } => match key.code {
                KeyCode::Esc => {
                    self.detail_export_prompt = None;
                    self.status = "Canceled CSV export.".into();
                }
                KeyCode::Backspace => {
                    value.pop();
                    edited = true;
                    self.detail_export_prompt = Some(DetailExportPrompt::EnterPath {
                        scope,
                        value,
                        edited,
                    });
                }
                KeyCode::Enter => {
                    let trimmed = value.trim();
                    if trimmed.is_empty() {
                        self.detail_export_prompt = Some(DetailExportPrompt::EnterPath {
                            scope,
                            value,
                            edited,
                        });
                        self.status = "Enter a non-empty CSV path or press Esc to cancel.".into();
                    } else {
                        let path = PathBuf::from(trimmed);
                        if path.exists() {
                            self.detail_export_prompt =
                                Some(DetailExportPrompt::ConfirmOverwrite {
                                    scope,
                                    value,
                                    edited,
                                });
                            self.status = format!(
                                "{} already exists. Press Enter to overwrite or Esc to keep editing.",
                                path.display()
                            );
                        } else {
                            self.export_preview_to_path(scope, value, edited).await;
                        }
                    }
                }
                KeyCode::Tab => {
                    let (scope, value, edited) = self.toggle_export_scope(scope, value, edited);
                    self.detail_export_prompt = Some(DetailExportPrompt::EnterPath {
                        scope,
                        value,
                        edited,
                    });
                    self.status = format!(
                        "Export scope: {}. Press Enter to export CSV, edit the path, or Tab to change scope.",
                        scope.label()
                    );
                }
                KeyCode::Char(ch) => {
                    value.push(ch);
                    edited = true;
                    self.detail_export_prompt = Some(DetailExportPrompt::EnterPath {
                        scope,
                        value,
                        edited,
                    });
                }
                _ => {
                    self.detail_export_prompt = Some(DetailExportPrompt::EnterPath {
                        scope,
                        value,
                        edited,
                    });
                }
            },
            DetailExportPrompt::ConfirmOverwrite {
                scope,
                value,
                edited,
            } => match key.code {
                KeyCode::Esc => {
                    self.detail_export_prompt = Some(DetailExportPrompt::EnterPath {
                        scope,
                        value,
                        edited,
                    });
                    self.status = "Returned to CSV path entry.".into();
                }
                KeyCode::Enter => self.export_preview_to_path(scope, value, edited).await,
                _ => {
                    self.detail_export_prompt = Some(DetailExportPrompt::ConfirmOverwrite {
                        scope,
                        value,
                        edited,
                    });
                }
            },
        }

        Ok(())
    }

    fn toggle_export_scope(
        &self,
        scope: ExportScope,
        value: String,
        edited: bool,
    ) -> (ExportScope, String, bool) {
        let next_scope = match scope {
            ExportScope::VisiblePage => ExportScope::AllMatchingRows,
            ExportScope::AllMatchingRows => ExportScope::VisiblePage,
        };
        if edited {
            (next_scope, value, true)
        } else {
            (
                next_scope,
                self.default_export_path(next_scope).display().to_string(),
                false,
            )
        }
    }

    async fn export_preview_to_path(&mut self, scope: ExportScope, value: String, edited: bool) {
        let path = PathBuf::from(value.trim());
        let request = ExportRequest {
            format: ExportFormat::Csv,
            scope,
            path: path.clone(),
        };

        let export_result = match scope {
            ExportScope::VisiblePage => {
                let Some(preview) = self.preview.as_ref() else {
                    self.detail_export_prompt = None;
                    self.status = "No preview data loaded for export.".into();
                    return;
                };
                write_preview_csv(preview, &path).map(|rows_written| ExportSummary {
                    rows_written,
                    scope,
                    path: path.clone(),
                })
            }
            ExportScope::AllMatchingRows => match self.active_investigation() {
                Ok(investigation) => {
                    let investigation = investigation.clone();
                    self.status = format!(
                        "Exporting {} as CSV to {}...",
                        scope.label(),
                        path.display()
                    );
                    self.session()
                        .unwrap()
                        .export_csv(&investigation, &request)
                        .await
                }
                Err(error) => Err(error),
            },
        };

        match export_result {
            Ok(summary) => {
                self.detail_export_prompt = None;
                self.status = format!(
                    "Exported {} row{} ({}) to {}.",
                    summary.rows_written,
                    if summary.rows_written == 1 { "" } else { "s" },
                    summary.scope.label(),
                    summary.path.display()
                );
            }
            Err(error) => {
                self.detail_export_prompt = Some(DetailExportPrompt::EnterPath {
                    scope,
                    value,
                    edited,
                });
                self.status = format!("CSV export failed: {error}");
            }
        }
    }

    async fn handle_detail_drill_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Esc => {
                self.detail_drill_actions = None;
                self.detail_drill_index = 0;
                self.status = "Canceled relation picker.".into();
            }
            KeyCode::Up | KeyCode::Char('k') => self.move_detail_drill_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_detail_drill_selection(1),
            KeyCode::Enter => self.confirm_detail_drill_selection().await?,
            _ => {}
        }
        Ok(())
    }

    fn start_detail_drill_prompt(&mut self) {
        let Some(detail) = &self.detail else {
            self.status = "No table detail loaded for drill-through.".into();
            return;
        };
        let Some(preview) = &self.preview else {
            self.status = "No preview data loaded for drill-through.".into();
            return;
        };
        if preview.rows.is_empty() {
            self.status = "No preview rows available for drill-through.".into();
            return;
        }

        let actions = build_drill_through_actions(detail, preview, self.preview_row_index);
        if actions.is_empty() {
            self.status = "No relationships available for this table.".into();
            return;
        }

        self.detail_drill_index = 0;
        self.detail_drill_actions = Some(actions);
        self.status = "Choose a relationship and press Enter.".into();
    }

    fn move_detail_drill_selection(&mut self, delta: isize) {
        let len = self
            .detail_drill_actions
            .as_ref()
            .map(|actions| actions.len())
            .unwrap_or(0);
        self.detail_drill_index = move_index(self.detail_drill_index, len, delta);
    }

    async fn confirm_detail_drill_selection(&mut self) -> Result<()> {
        let action = self
            .detail_drill_actions
            .as_ref()
            .and_then(|actions| actions.get(self.detail_drill_index))
            .cloned()
            .ok_or_else(|| anyhow!("no drill-through action is selected"))?;

        if !action.is_available() {
            self.status = action
                .unavailable_reason
                .unwrap_or_else(|| "That relationship is unavailable for the selected row.".into());
            return Ok(());
        }

        let snapshot = self
            .current_detail_nav_entry()
            .ok_or_else(|| anyhow!("no detail context available to push"))?;
        let target_table = action.target_table.clone();
        let target_filter = action
            .target_filter
            .clone()
            .ok_or_else(|| anyhow!("selected drill-through action is missing a filter"))?;
        self.detail_nav_stack.push(snapshot);
        self.open_detail_context(
            DetailNavStackEntry {
                investigation: InvestigationState {
                    source: InvestigationSource::Table(target_table.clone()),
                    sort: None,
                    filters: vec![target_filter],
                    page: 0,
                },
                selected_row: 0,
            },
            DetailView::Table,
            false,
        )
        .await?;
        self.status = format!("Opened related rows in {}.", target_table.display_name());
        Ok(())
    }

    fn start_detail_filter_prompt(&mut self) {
        let Some(detail) = &self.detail else {
            self.status = "No table detail loaded for filtering.".into();
            return;
        };
        if detail.columns.is_empty() {
            self.status = "No columns available for filtering.".into();
            return;
        }

        self.detail_filter_prompt = Some(DetailFilterPrompt::SelectColumn { index: 0 });
        self.status = "Choose a column for the new filter.".into();
    }

    fn handle_detail_filter_prompt_key(&mut self, key: KeyEvent) -> Result<DetailFilterOutcome> {
        let Some(prompt) = self.detail_filter_prompt.clone() else {
            return Ok(DetailFilterOutcome::None);
        };
        let column_count = self
            .detail
            .as_ref()
            .map(|detail| detail.columns.len())
            .unwrap_or(0);

        match prompt {
            DetailFilterPrompt::SelectColumn { index } => match key.code {
                KeyCode::Esc => {
                    self.detail_filter_prompt = None;
                    self.status = "Canceled filter builder.".into();
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.detail_filter_prompt = Some(DetailFilterPrompt::SelectColumn {
                        index: move_index(index, column_count, -1),
                    });
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.detail_filter_prompt = Some(DetailFilterPrompt::SelectColumn {
                        index: move_index(index, column_count, 1),
                    });
                }
                KeyCode::Enter => {
                    self.detail_filter_prompt = Some(DetailFilterPrompt::SelectOperator {
                        column_index: index,
                        index: 0,
                    });
                    self.status = "Choose a filter operator.".into();
                }
                _ => {}
            },
            DetailFilterPrompt::SelectOperator {
                column_index,
                index,
            } => match key.code {
                KeyCode::Esc => {
                    self.detail_filter_prompt = Some(DetailFilterPrompt::SelectColumn {
                        index: column_index,
                    });
                    self.status = "Returned to column selection.".into();
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.detail_filter_prompt = Some(DetailFilterPrompt::SelectOperator {
                        column_index,
                        index: move_index(index, FilterOperator::ALL.len(), -1),
                    });
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.detail_filter_prompt = Some(DetailFilterPrompt::SelectOperator {
                        column_index,
                        index: move_index(index, FilterOperator::ALL.len(), 1),
                    });
                }
                KeyCode::Enter => {
                    let operator = FilterOperator::ALL[index];
                    if operator.requires_value() {
                        self.detail_filter_prompt = Some(DetailFilterPrompt::EnterValue {
                            column_index,
                            operator,
                            value: String::new(),
                        });
                        self.status = "Type a filter value and press Enter.".into();
                    } else {
                        self.push_detail_filter(PreviewFilter {
                            column_name: self.detail_column_name(column_index)?.to_string(),
                            operator,
                            value: None,
                        });
                        return Ok(DetailFilterOutcome::ReloadPreview);
                    }
                }
                _ => {}
            },
            DetailFilterPrompt::EnterValue {
                column_index,
                operator,
                mut value,
            } => match key.code {
                KeyCode::Esc => {
                    self.detail_filter_prompt = Some(DetailFilterPrompt::SelectOperator {
                        column_index,
                        index: FilterOperator::ALL
                            .iter()
                            .position(|candidate| *candidate == operator)
                            .unwrap_or(0),
                    });
                    self.status = "Returned to operator selection.".into();
                }
                KeyCode::Backspace => {
                    value.pop();
                    self.detail_filter_prompt = Some(DetailFilterPrompt::EnterValue {
                        column_index,
                        operator,
                        value,
                    });
                }
                KeyCode::Enter => {
                    if value.is_empty() {
                        self.detail_filter_prompt = Some(DetailFilterPrompt::EnterValue {
                            column_index,
                            operator,
                            value,
                        });
                        self.status =
                            "Enter a non-empty filter value or press Esc to cancel.".into();
                    } else {
                        self.push_detail_filter(PreviewFilter {
                            column_name: self.detail_column_name(column_index)?.to_string(),
                            operator,
                            value: Some(value),
                        });
                        return Ok(DetailFilterOutcome::ReloadPreview);
                    }
                }
                KeyCode::Char(ch) => {
                    value.push(ch);
                    self.detail_filter_prompt = Some(DetailFilterPrompt::EnterValue {
                        column_index,
                        operator,
                        value,
                    });
                }
                _ => {
                    self.detail_filter_prompt = Some(DetailFilterPrompt::EnterValue {
                        column_index,
                        operator,
                        value,
                    });
                }
            },
        }

        Ok(DetailFilterOutcome::None)
    }

    fn detail_column_name(&self, column_index: usize) -> Result<&str> {
        self.detail
            .as_ref()
            .and_then(|detail| detail.columns.get(column_index))
            .map(|column| column.name.as_str())
            .ok_or_else(|| anyhow!("filter column index out of range"))
    }

    fn active_investigation(&self) -> Result<&InvestigationState> {
        self.active_investigation
            .as_ref()
            .ok_or_else(|| anyhow!("no investigation state is loaded"))
    }

    fn active_investigation_mut(&mut self) -> Result<&mut InvestigationState> {
        self.active_investigation
            .as_mut()
            .ok_or_else(|| anyhow!("no investigation state is loaded"))
    }

    fn active_filters(&self) -> &[PreviewFilter] {
        self.active_investigation
            .as_ref()
            .map(|investigation| investigation.filters.as_slice())
            .unwrap_or(&[])
    }

    fn ensure_sort_for_detail(
        &self,
        investigation: &mut InvestigationState,
        detail: &TableDetail,
    ) -> usize {
        if detail.columns.is_empty() {
            investigation.sort = None;
            return 0;
        }

        if let Some(sort) = &investigation.sort {
            if let Some(index) = detail
                .columns
                .iter()
                .position(|column| column.name == sort.column_name)
            {
                return index;
            }
        }

        investigation.sort = Some(SortState {
            column_name: detail.columns[0].name.clone(),
            descending: false,
        });
        0
    }

    fn sync_sort_from_index(&mut self) -> Result<()> {
        let column_name = self.detail_column_name(self.sort_index)?.to_string();
        let descending = self
            .active_investigation
            .as_ref()
            .and_then(|investigation| investigation.sort.as_ref())
            .map(|sort| sort.descending)
            .unwrap_or(false);
        self.active_investigation_mut()?.sort = Some(SortState {
            column_name,
            descending,
        });
        Ok(())
    }

    fn toggle_sort_direction(&mut self) -> Result<()> {
        let column_name = self.detail_column_name(self.sort_index)?.to_string();
        let next_descending = self
            .active_investigation
            .as_ref()
            .and_then(|investigation| investigation.sort.as_ref())
            .map(|sort| !sort.descending)
            .unwrap_or(true);
        self.active_investigation_mut()?.sort = Some(SortState {
            column_name,
            descending: next_descending,
        });
        Ok(())
    }

    fn push_detail_filter(&mut self, filter: PreviewFilter) {
        if let Some(investigation) = &mut self.active_investigation {
            investigation.filters.push(filter.clone());
        }
        self.detail_filter_index = self.active_filters().len().saturating_sub(1);
        self.detail_filter_prompt = None;
        self.reset_preview_page();
        self.status = format!("Applied filter: {}.", filter.summary());
    }

    fn move_detail_filter_selection(&mut self, delta: isize) {
        self.detail_filter_index =
            move_index(self.detail_filter_index, self.active_filters().len(), delta);
    }

    fn remove_selected_detail_filter(&mut self) -> bool {
        if self.active_filters().is_empty() {
            self.status = "No active filters to remove.".into();
            return false;
        }

        let removed = self
            .active_investigation
            .as_mut()
            .expect("filter removal requires investigation state")
            .filters
            .remove(self.detail_filter_index);
        if self.detail_filter_index >= self.active_filters().len() {
            self.detail_filter_index = self.active_filters().len().saturating_sub(1);
        }
        self.reset_preview_page();
        self.status = format!("Removed filter: {}.", removed.summary());
        true
    }

    fn clear_detail_filters(&mut self) -> bool {
        if self.active_filters().is_empty() {
            self.status = "No active filters to clear.".into();
            return false;
        }

        if let Some(investigation) = &mut self.active_investigation {
            investigation.filters.clear();
        }
        self.detail_filter_index = 0;
        self.detail_filter_prompt = None;
        self.reset_preview_page();
        self.status = "Cleared all preview filters.".into();
        true
    }

    fn move_preview_row(&mut self, delta: isize) {
        let len = self
            .preview
            .as_ref()
            .map(|preview| preview.rows.len())
            .unwrap_or(0);
        self.preview_row_index = move_index(self.preview_row_index, len, delta);
    }

    fn clamp_preview_row_index(&mut self) {
        let len = self
            .preview
            .as_ref()
            .map(|preview| preview.rows.len())
            .unwrap_or(0);
        if len == 0 {
            self.preview_row_index = 0;
        } else if self.preview_row_index >= len {
            self.preview_row_index = len - 1;
        }
    }

    fn selected_preview_row(&self) -> Option<usize> {
        let preview = self.preview.as_ref()?;
        if preview.rows.is_empty() {
            None
        } else {
            Some(self.preview_row_index.min(preview.rows.len() - 1))
        }
    }

    fn current_detail_nav_entry(&self) -> Option<DetailNavStackEntry> {
        Some(DetailNavStackEntry {
            investigation: self.active_investigation.clone()?,
            selected_row: self.preview_row_index,
        })
    }

    async fn pop_detail_nav_stack(&mut self) -> Result<bool> {
        let Some(entry) = self.detail_nav_stack.pop() else {
            return Ok(false);
        };
        let table_name = entry.investigation.table().display_name();
        self.open_detail_context(entry, DetailView::Table, false)
            .await?;
        self.status = format!("Returned to {table_name}.");
        Ok(true)
    }

    fn preview_page_forward(&mut self) {
        if let Some(investigation) = &mut self.active_investigation {
            investigation.page += 1;
        }
        if let Some(preview) = &mut self.preview {
            preview.page += 1;
        }
    }

    fn preview_page_back(&mut self) {
        if let Some(investigation) = &mut self.active_investigation {
            investigation.page = investigation.page.saturating_sub(1);
        }
        if let Some(preview) = &mut self.preview {
            preview.page = preview.page.saturating_sub(1);
        }
    }

    fn reset_preview_page(&mut self) {
        if let Some(investigation) = &mut self.active_investigation {
            investigation.page = 0;
        }
        if let Some(preview) = &mut self.preview {
            preview.page = 0;
        }
    }

    async fn activate_selected_connection(&mut self) -> Result<()> {
        let candidate = self
            .selected_candidate()
            .cloned()
            .ok_or_else(|| anyhow!("no connection candidate is selected"))?;

        self.connect_candidate(candidate).await
    }

    async fn connect_candidate(&mut self, candidate: ConnectionCandidate) -> Result<()> {
        self.status = format!("Connecting to {}...", candidate.profile.name);
        let session = Session::connect(&candidate.profile).await?;
        self.config.note_recent_profile(&candidate.profile.name)?;
        self.active_profile = Some(candidate.profile.clone());
        self.active_connection_target = Some(candidate.bookmark_target.clone());
        self.session = Some(session);
        self.detail = None;
        self.active_investigation = None;
        self.detail_view = DetailView::Table;
        self.detail_filter_index = 0;
        self.detail_filter_prompt = None;
        self.detail_export_prompt = None;
        self.workspace_prompt = None;
        self.detail_drill_actions = None;
        self.detail_drill_index = 0;
        self.detail_nav_stack.clear();
        self.relation_graph = None;
        self.graph_lane = GraphLane::Center;
        self.graph_index = 0;
        self.graph_center_scroll = 0;
        self.preview = None;
        self.preview_row_index = 0;
        self.sort_index = 0;

        if self.session_kind() == Some(DatabaseKind::Postgres) {
            self.schemas = self.session().unwrap().list_schemas().await?;
            self.schema_index = 0;
            self.screen = Screen::Schemas;
            self.status = "Connected. Choose a schema and press Enter.".into();
        } else {
            self.active_schema = None;
            self.load_tables(None).await?;
            self.screen = Screen::Browser;
            self.status = "Connected. Browse tables and press Enter for details.".into();
        }

        self.continue_startup_after_connect().await?;

        Ok(())
    }

    async fn load_tables_for_selected_schema(&mut self) -> Result<()> {
        let schema = self.selected_schema().cloned();
        self.load_tables(schema).await?;
        self.screen = Screen::Browser;
        self.status = "Schema loaded. Browse tables and press Enter for details.".into();
        self.continue_startup_after_table_load().await?;
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
        self.table_filter.clear();
        self.table_search_mode = false;
        self.detail = None;
        self.active_investigation = None;
        self.detail_view = DetailView::Table;
        self.detail_filter_index = 0;
        self.detail_filter_prompt = None;
        self.detail_export_prompt = None;
        self.workspace_prompt = None;
        self.detail_drill_actions = None;
        self.detail_drill_index = 0;
        self.detail_nav_stack.clear();
        self.relation_graph = None;
        self.graph_lane = GraphLane::Center;
        self.graph_index = 0;
        self.graph_center_scroll = 0;
        self.preview = None;
        self.preview_row_index = 0;
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
        self.open_detail_context(
            DetailNavStackEntry {
                investigation: InvestigationState::for_table(table.clone()),
                selected_row: 0,
            },
            detail_view,
            true,
        )
        .await?;
        self.status = format!("Viewing {}.", table.display_name());
        Ok(())
    }

    async fn open_detail_context(
        &mut self,
        context: DetailNavStackEntry,
        detail_view: DetailView,
        clear_drill_stack: bool,
    ) -> Result<()> {
        let detail = self
            .session()
            .unwrap()
            .load_detail(context.investigation.table())
            .await?;
        let mut investigation = context.investigation;
        self.sort_index = self.ensure_sort_for_detail(&mut investigation, &detail);
        self.detail = Some(detail);
        self.active_investigation = Some(investigation.clone());
        self.detail_view = detail_view;
        self.detail_filter_index = investigation.filters.len().saturating_sub(1);
        self.detail_filter_prompt = None;
        self.detail_export_prompt = None;
        self.workspace_prompt = None;
        self.detail_drill_actions = None;
        self.detail_drill_index = 0;
        if clear_drill_stack {
            self.detail_nav_stack.clear();
        }
        self.graph_lane = GraphLane::Center;
        self.graph_index = 0;
        self.graph_center_scroll = 0;
        self.relation_graph = None;
        self.preview = Some(DataPreview {
            columns: Vec::new(),
            rows: Vec::new(),
            page: investigation.page,
            has_more: false,
        });
        self.preview_row_index = context.selected_row;
        self.reload_preview().await?;
        if detail_view == DetailView::Graph {
            self.reload_relation_graph().await?;
        }
        self.screen = Screen::Detail;
        Ok(())
    }

    async fn reload_preview(&mut self) -> Result<()> {
        let investigation = self
            .active_investigation
            .as_ref()
            .ok_or_else(|| anyhow!("no investigation state is loaded"))?;
        let preview = self.session().unwrap().load_preview(investigation).await?;
        self.preview = Some(preview);
        self.clamp_preview_row_index();
        self.detail_drill_actions = None;
        self.detail_drill_index = 0;
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

    async fn continue_startup_after_connect(&mut self) -> Result<()> {
        let Some(mut target) = self.startup_target.clone() else {
            return Ok(());
        };

        if self.session_kind() == Some(DatabaseKind::Sqlite) {
            target.schema = None;
            self.store_startup_target(target);
            return self.continue_startup_after_table_load().await;
        }

        if self.screen != Screen::Schemas {
            return Ok(());
        }

        let Some(schema_name) = target.schema.clone() else {
            if target.table.is_some() || target.view.is_some() {
                self.status = "Choose a schema to continue the requested startup target.".into();
            }
            self.store_startup_target(target);
            return Ok(());
        };

        if let Some(index) = self
            .schemas
            .iter()
            .position(|schema| schema == &schema_name)
        {
            self.schema_index = index;
            target.schema = None;
            self.store_startup_target(target);
            self.load_tables_for_selected_schema().await?;
        } else {
            self.status =
                format!("Schema '{schema_name}' was not found. Choose a schema to continue.");
            target.schema = None;
            self.store_startup_target(target);
        }

        Ok(())
    }

    async fn continue_startup_after_table_load(&mut self) -> Result<()> {
        let Some(mut target) = self.startup_target.clone() else {
            return Ok(());
        };

        if self.session_kind() == Some(DatabaseKind::Sqlite) {
            target.schema = None;
        }

        if self.screen == Screen::Detail {
            self.startup_target = None;
            return Ok(());
        }
        if self.screen != Screen::Browser {
            self.store_startup_target(target);
            return Ok(());
        }

        if target.table.is_none() {
            if let Some(view) = target.view {
                self.status = format!(
                    "Startup view '{}' requires a target table.",
                    startup_view_label(view)
                );
                target.view = None;
            } else {
                target.filters.clear();
                target.sort = None;
            }
            self.store_startup_target(target);
            return Ok(());
        }

        let table_name = target.table.clone().unwrap();
        if let Some(index) = self
            .tables
            .iter()
            .position(|table| table.name == table_name)
        {
            self.table_index = index;
            if let Some(view) = target.view {
                target.table = None;
                target.view = None;
                let investigation = target.investigation_for_table(
                    self.selected_table()
                        .ok_or_else(|| anyhow!("target table selection is unavailable"))?,
                );
                target.filters.clear();
                target.sort = None;
                self.store_startup_target(target);

                let table = investigation.table().clone();
                self.open_detail_context(
                    DetailNavStackEntry {
                        investigation,
                        selected_row: 0,
                    },
                    startup_view_to_detail_view(view),
                    true,
                )
                .await?;
                self.status = format!(
                    "Opened {} in {} view.",
                    table.display_name(),
                    startup_view_label(view)
                );
            } else {
                self.status = format!("Selected startup table {table_name}.");
                target.table = None;
                self.store_startup_target(target);
            }
        } else {
            self.status = format!(
                "Table '{}' was not found{}.",
                table_name,
                match self.active_schema.as_deref() {
                    Some(schema) => format!(" in schema '{schema}'"),
                    None => String::new(),
                }
            );
            target.table = None;
            target.view = None;
            target.filters.clear();
            target.sort = None;
            self.store_startup_target(target);
        }

        Ok(())
    }

    fn store_startup_target(&mut self, target: StartupTarget) {
        self.startup_target = if target.is_empty() {
            None
        } else {
            Some(target)
        };
    }

    fn current_sort(&self) -> Option<SortState> {
        self.active_investigation
            .as_ref()
            .and_then(|investigation| investigation.sort.clone())
    }

    fn active_connection_target(&self) -> Result<&BookmarkConnectionTarget> {
        self.active_connection_target
            .as_ref()
            .ok_or_else(|| anyhow!("no active connection target is loaded"))
    }

    fn current_startup_view(&self) -> StartupView {
        match self.detail_view {
            DetailView::Table => StartupView::Detail,
            DetailView::Graph => StartupView::Graph,
        }
    }

    fn is_input_mode_active(&self) -> bool {
        self.table_search_mode
            || self.detail_filter_prompt.is_some()
            || self.detail_export_prompt.is_some()
            || self.workspace_prompt.is_some()
            || self.detail_drill_actions.is_some()
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
            Screen::Browser if self.table_search_mode => "Type filter | Enter apply | Esc clear",
            Screen::Browser
                if matches!(
                    self.workspace_prompt,
                    Some(WorkspacePrompt::EnterName { .. })
                ) =>
            {
                "Type name | Enter save | Esc cancel"
            }
            Screen::Browser
                if matches!(
                    self.workspace_prompt,
                    Some(WorkspacePrompt::ConfirmOverwrite { .. })
                ) =>
            {
                "Enter overwrite | Esc edit name"
            }
            Screen::Browser if self.workspace_prompt.is_some() => {
                "Up/Down move | Enter confirm | Esc cancel"
            }
            Screen::Browser => {
                "Enter detail | / filter | b bookmarks | r reload | Esc back | q quit"
            }
            Screen::Detail
                if matches!(
                    self.workspace_prompt,
                    Some(WorkspacePrompt::EnterName { .. })
                ) =>
            {
                "Type name | Enter save | Esc cancel"
            }
            Screen::Detail
                if matches!(
                    self.workspace_prompt,
                    Some(WorkspacePrompt::ConfirmOverwrite { .. })
                ) =>
            {
                "Enter overwrite | Esc edit name"
            }
            Screen::Detail if self.workspace_prompt.is_some() => {
                "Up/Down move | Enter confirm | Esc cancel"
            }
            Screen::Detail if self.detail_view == DetailView::Graph => {
                "Left/Right lane | Up/Down move-or-scroll | Enter center neighbor | b workspace | g/Esc detail | r reload | q quit"
            }
            Screen::Detail
                if matches!(
                    self.detail_export_prompt,
                    Some(DetailExportPrompt::EnterPath { .. })
                ) =>
            {
                "Type path | Tab scope | Enter export | Esc cancel"
            }
            Screen::Detail
                if matches!(
                    self.detail_export_prompt,
                    Some(DetailExportPrompt::ConfirmOverwrite { .. })
                ) =>
            {
                "Enter overwrite | Esc edit path"
            }
            Screen::Detail
                if matches!(
                    self.detail_filter_prompt,
                    Some(DetailFilterPrompt::EnterValue { .. })
                ) =>
            {
                "Type value | Enter apply | Esc cancel"
            }
            Screen::Detail if self.detail_filter_prompt.is_some() => {
                "Up/Down move | Enter confirm | Esc cancel"
            }
            Screen::Detail if self.detail_drill_actions.is_some() => {
                "Up/Down move | Enter open relation | Esc cancel"
            }
            Screen::Detail => {
                "Up/Down row | Enter relations | b workspace | e export | f add filter | h/l pick filter | x remove | c clear | [ ] sort | s order | n/p page | g graph | Esc back | q quit"
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
        let title = if self.table_search_mode {
            format!("Tables (search: {})", self.table_filter)
        } else if self.table_filter.is_empty() {
            "Tables".into()
        } else {
            format!("Tables (filtered: {})", self.table_filter)
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
        if let Some(prompt) = &self.workspace_prompt {
            self.render_workspace_prompt(frame, chunks[0], prompt);
        }
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
            if let Some(prompt) = &self.workspace_prompt {
                self.render_workspace_prompt(frame, chunks[0], prompt);
            }
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

        let metadata_sections = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(65), Constraint::Percentage(35)])
            .split(body[1]);

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
        frame.render_widget(metadata, metadata_sections[0]);

        let filters = Paragraph::new(detail_filter_lines(
            self.active_filters(),
            self.detail_filter_index,
        ))
        .block(
            Block::default()
                .title("Active Filters")
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: false });
        frame.render_widget(filters, metadata_sections[1]);

        let preview = self.preview.as_ref();
        let preview_title = preview_title(
            preview,
            self.current_sort().as_ref(),
            self.active_filters().len(),
        );
        let preview_widget = render_preview(preview, &detail.columns, preview_title);
        let mut preview_state = TableState::default();
        preview_state.select(self.selected_preview_row());
        frame.render_stateful_widget(preview_widget, body[2], &mut preview_state);
        if let Some(prompt) = &self.detail_filter_prompt {
            self.render_detail_filter_prompt(frame, chunks[0], detail, prompt);
        }
        if let Some(prompt) = &self.detail_export_prompt {
            self.render_detail_export_prompt(frame, chunks[0], prompt);
        }
        if let Some(prompt) = &self.workspace_prompt {
            self.render_workspace_prompt(frame, chunks[0], prompt);
        }
        if let Some(actions) = &self.detail_drill_actions {
            self.render_detail_drill_prompt(frame, chunks[0], actions);
        }
        frame.render_widget(status_bar(&self.status, self.controls_hint()), chunks[1]);
    }

    fn render_workspace_prompt(&self, frame: &mut Frame, area: Rect, prompt: &WorkspacePrompt) {
        match prompt {
            WorkspacePrompt::ChooseAction { actions, index } => {
                let popup = centered_rect(48, 10, area);
                frame.render_widget(Clear, popup);
                let items = actions
                    .iter()
                    .map(|action| ListItem::new(action.label()))
                    .collect::<Vec<_>>();
                let mut state = ListState::default();
                state.select(Some(*index));
                let list = List::new(items)
                    .block(Block::default().title("Workspace").borders(Borders::ALL))
                    .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
                frame.render_stateful_widget(list, popup, &mut state);
            }
            WorkspacePrompt::EnterName { kind, value } => {
                let popup = centered_rect(68, 8, area);
                frame.render_widget(Clear, popup);
                let widget = Paragraph::new(vec![
                    Line::from(format!("Save {}.", kind.label())),
                    Line::from("Edit the name and press Enter to save."),
                    Line::from(""),
                    Line::from(format!("Name: {value}")),
                ])
                .block(
                    Block::default()
                        .title(format!("Save {}", kind.label()))
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false });
                frame.render_widget(widget, popup);
            }
            WorkspacePrompt::ConfirmOverwrite { kind, value } => {
                let popup = centered_rect(68, 8, area);
                frame.render_widget(Clear, popup);
                let widget = Paragraph::new(vec![
                    Line::from("This name already exists."),
                    Line::from("Press Enter to overwrite or Esc to return."),
                    Line::from(""),
                    Line::from(format!("Name: {value}")),
                ])
                .block(
                    Block::default()
                        .title(format!("Overwrite {}?", kind.label()))
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false });
                frame.render_widget(widget, popup);
            }
            WorkspacePrompt::OpenBookmark { bookmarks, index } => {
                let popup = centered_rect(72, 14, area);
                frame.render_widget(Clear, popup);
                let items = bookmarks
                    .iter()
                    .map(|bookmark| {
                        let target = match &bookmark.table.schema {
                            Some(schema) => format!("{}.{}", schema, bookmark.table.name),
                            None => bookmark.table.name.clone(),
                        };
                        let view = bookmark
                            .preferred_view
                            .map(startup_view_label)
                            .unwrap_or("detail");
                        ListItem::new(format!("{} [{} | {}]", bookmark.name, target, view))
                    })
                    .collect::<Vec<_>>();
                let mut state = ListState::default();
                state.select(Some(*index));
                let list = List::new(items)
                    .block(Block::default().title("Bookmarks").borders(Borders::ALL))
                    .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
                frame.render_stateful_widget(list, popup, &mut state);
            }
            WorkspacePrompt::ApplyPreset { presets, index } => {
                let popup = centered_rect(72, 14, area);
                frame.render_widget(Clear, popup);
                let items = presets
                    .iter()
                    .map(|preset| {
                        ListItem::new(format!(
                            "{} [{} filters]",
                            preset.name,
                            preset.filters.len()
                        ))
                    })
                    .collect::<Vec<_>>();
                let mut state = ListState::default();
                state.select(Some(*index));
                let list = List::new(items)
                    .block(
                        Block::default()
                            .title("Filter Presets")
                            .borders(Borders::ALL),
                    )
                    .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
                frame.render_stateful_widget(list, popup, &mut state);
            }
        }
    }

    fn render_detail_export_prompt(
        &self,
        frame: &mut Frame,
        area: Rect,
        prompt: &DetailExportPrompt,
    ) {
        let popup = centered_rect(70, 9, area);
        frame.render_widget(Clear, popup);

        let widget = match prompt {
            DetailExportPrompt::EnterPath { scope, value, .. } => Paragraph::new(vec![
                Line::from(format!("Export {} as CSV.", scope.label())),
                Line::from("Edit the path, press Enter to export, or Tab to change scope."),
                Line::from(""),
                Line::from(format!("Scope: {}", scope.label())),
                Line::from(format!("Path: {value}")),
            ])
            .block(Block::default().title("Export CSV").borders(Borders::ALL)),
            DetailExportPrompt::ConfirmOverwrite { scope, value, .. } => Paragraph::new(vec![
                Line::from("This file already exists."),
                Line::from("Press Enter to overwrite or Esc to return."),
                Line::from(""),
                Line::from(format!("Scope: {}", scope.label())),
                Line::from(format!("Path: {}", value.trim())),
            ])
            .block(
                Block::default()
                    .title("Overwrite CSV?")
                    .borders(Borders::ALL),
            ),
        }
        .wrap(Wrap { trim: false });
        frame.render_widget(widget, popup);
    }

    fn render_detail_filter_prompt(
        &self,
        frame: &mut Frame,
        area: Rect,
        detail: &TableDetail,
        prompt: &DetailFilterPrompt,
    ) {
        let popup = centered_rect(60, 10, area);
        frame.render_widget(Clear, popup);

        match prompt {
            DetailFilterPrompt::SelectColumn { index } => {
                let items = detail
                    .columns
                    .iter()
                    .map(|column| ListItem::new(column.name.clone()))
                    .collect::<Vec<_>>();
                let mut state = ListState::default();
                state.select(Some(*index));
                let list = List::new(items)
                    .block(
                        Block::default()
                            .title("Add Filter: Column")
                            .borders(Borders::ALL),
                    )
                    .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
                frame.render_stateful_widget(list, popup, &mut state);
            }
            DetailFilterPrompt::SelectOperator { index, .. } => {
                let items = FilterOperator::ALL
                    .iter()
                    .map(|operator| ListItem::new(operator.label()))
                    .collect::<Vec<_>>();
                let mut state = ListState::default();
                state.select(Some(*index));
                let list = List::new(items)
                    .block(
                        Block::default()
                            .title("Add Filter: Operator")
                            .borders(Borders::ALL),
                    )
                    .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
                frame.render_stateful_widget(list, popup, &mut state);
            }
            DetailFilterPrompt::EnterValue {
                column_index,
                operator,
                value,
            } => {
                let column_name = detail
                    .columns
                    .get(*column_index)
                    .map(|column| column.name.as_str())
                    .unwrap_or("unknown");
                let widget = Paragraph::new(vec![
                    Line::from(format!("Column: {column_name}")),
                    Line::from(format!("Operator: {}", operator.label())),
                    Line::from(String::new()),
                    Line::from(format!("Value: {value}")),
                ])
                .block(
                    Block::default()
                        .title("Add Filter: Value")
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false });
                frame.render_widget(widget, popup);
            }
        }
    }

    fn render_detail_drill_prompt(
        &self,
        frame: &mut Frame,
        area: Rect,
        actions: &[DrillThroughAction],
    ) {
        let popup = centered_rect(70, 12, area);
        frame.render_widget(Clear, popup);

        let items = actions
            .iter()
            .map(|action| {
                let text = if let Some(reason) = &action.unavailable_reason {
                    format!("{} [{}]", action.label(), reason)
                } else {
                    action.label()
                };
                if action.is_available() {
                    ListItem::new(text)
                } else {
                    ListItem::new(Line::styled(
                        text,
                        Style::default().add_modifier(Modifier::DIM),
                    ))
                }
            })
            .collect::<Vec<_>>();
        let mut state = ListState::default();
        state.select(Some(
            self.detail_drill_index.min(actions.len().saturating_sub(1)),
        ));
        let list = List::new(items)
            .block(
                Block::default()
                    .title("Row Relations")
                    .borders(Borders::ALL),
            )
            .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
        frame.render_stateful_widget(list, popup, &mut state);
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
        if self.table_filter.is_empty() {
            return self.tables.clone();
        }

        let needle = self.table_filter.to_lowercase();
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
            let visible_rows = self.graph_center_visible_rows();
            let max_scroll = len.saturating_sub(visible_rows);
            self.graph_center_scroll = move_index(
                self.graph_center_scroll,
                max_scroll.saturating_add(1),
                delta,
            );
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

        let visible_rows = graph_center_visible_rows(area);
        let center_scroll =
            clamp_graph_center_scroll(self.graph_center_scroll, detail.columns.len(), visible_rows);

        if area.width < 100 || area.height < 20 {
            let fallback = Paragraph::new(graph_fallback_lines(
                graph,
                detail,
                center_scroll,
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
            center_scroll,
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
                            center_scroll,
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

    fn graph_center_visible_rows(&self) -> usize {
        terminal::size()
            .ok()
            .map(|(width, height)| graph_center_visible_rows(Rect::new(0, 0, width, height)))
            .unwrap_or(1)
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

fn graph_center_visible_rows(area: Rect) -> usize {
    let detail_area = main_chunks(area)[0];
    if detail_area.width < 100 || detail_area.height < 20 {
        return detail_area.height.saturating_sub(7).max(1) as usize;
    }

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(10), Constraint::Length(7)])
        .split(detail_area);
    sections[0].height.saturating_sub(2).max(1) as usize
}

fn clamp_graph_center_scroll(scroll: usize, total_columns: usize, visible_rows: usize) -> usize {
    if total_columns == 0 {
        return 0;
    }

    scroll.min(total_columns.saturating_sub(visible_rows.max(1)))
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
    mcp_context: Option<&McpContext>,
    bookmark_defaults: &BookmarkDefaults,
) -> (Vec<ConnectionCandidate>, usize, bool) {
    let mut candidates = config
        .ordered_profiles()
        .into_iter()
        .map(|profile| ConnectionCandidate {
            bookmark_target: BookmarkConnectionTarget::SavedProfile {
                name: profile.name.clone(),
            },
            profile,
            source: "saved",
        })
        .collect::<Vec<_>>();

    if let Some(profile) = bookmark_defaults.mcp_profile() {
        let bookmark_target = bookmark_defaults
            .mcp_bookmark()
            .map(|bookmark| bookmark.connection.clone())
            .unwrap_or_else(|| BookmarkConnectionTarget::Direct {
                profile: profile.clone(),
            });
        candidates.insert(
            0,
            ConnectionCandidate {
                profile: profile.clone(),
                bookmark_target,
                source: "bookmark",
            },
        );
    }

    if let Some(context) = mcp_context {
        if let Some(profile) = context.profile.clone() {
            candidates.insert(
                0,
                ConnectionCandidate {
                    bookmark_target: BookmarkConnectionTarget::Direct {
                        profile: profile.clone(),
                    },
                    profile,
                    source: "mcp",
                },
            );
        }
    }

    if let Some(profile) = bookmark_defaults.cli_profile() {
        let bookmark_target = bookmark_defaults
            .cli_bookmark()
            .map(|bookmark| bookmark.connection.clone())
            .unwrap_or_else(|| BookmarkConnectionTarget::Direct {
                profile: profile.clone(),
            });
        candidates.insert(
            0,
            ConnectionCandidate {
                profile: profile.clone(),
                bookmark_target,
                source: "bookmark",
            },
        );
    }

    if let Some(url) = &args.pg_url {
        let profile = ConnectionProfile {
            name: "cli-postgres".into(),
            kind: DatabaseKind::Postgres,
            url: Some(url.clone()),
            path: None,
        };
        candidates.insert(
            0,
            ConnectionCandidate {
                bookmark_target: BookmarkConnectionTarget::Direct {
                    profile: profile.clone(),
                },
                profile,
                source: "cli",
            },
        );
    }

    if let Some(path) = &args.sqlite_path {
        let profile = ConnectionProfile {
            name: "cli-sqlite".into(),
            kind: DatabaseKind::Sqlite,
            url: None,
            path: Some(path.clone()),
        };
        candidates.insert(
            0,
            ConnectionCandidate {
                bookmark_target: BookmarkConnectionTarget::Direct {
                    profile: profile.clone(),
                },
                profile,
                source: "cli",
            },
        );
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
    let pending_auto_connect = args.profile.is_some()
        || args.pg_url.is_some()
        || args.sqlite_path.is_some()
        || bookmark_defaults.cli.is_some()
        || mcp_context
            .and_then(|context| context.profile.as_ref())
            .is_some()
        || bookmark_defaults.mcp.is_some();

    (candidates, selected_index, pending_auto_connect)
}

fn build_startup_target(
    args: &CliArgs,
    mcp_context: Option<&McpContext>,
    bookmark_defaults: &BookmarkDefaults,
) -> Option<StartupTarget> {
    let cli_explicit_connection =
        args.profile.is_some() || args.pg_url.is_some() || args.sqlite_path.is_some();
    let cli_explicit_target = args.schema.is_some() || args.table.is_some();
    let mcp_explicit_connection = mcp_context
        .and_then(|context| context.profile.as_ref())
        .is_some();
    let mcp_explicit_target = mcp_context
        .and_then(|context| context.target_schema.as_ref())
        .is_some()
        || mcp_context
            .and_then(|context| context.target_table.as_ref())
            .is_some();
    let bookmark_query_state = if let Some(bookmark) = bookmark_defaults.cli_bookmark() {
        if !cli_explicit_connection && !cli_explicit_target {
            (bookmark.filters.clone(), bookmark.sort.clone())
        } else {
            (Vec::new(), None)
        }
    } else if let Some(bookmark) = bookmark_defaults.mcp_bookmark() {
        if args.bookmark.is_none()
            && !cli_explicit_connection
            && !cli_explicit_target
            && !mcp_explicit_connection
            && !mcp_explicit_target
        {
            (bookmark.filters.clone(), bookmark.sort.clone())
        } else {
            (Vec::new(), None)
        }
    } else {
        (Vec::new(), None)
    };

    let mut target = StartupTarget {
        schema: args
            .schema
            .clone()
            .or_else(|| {
                bookmark_defaults
                    .cli_bookmark()
                    .and_then(|bookmark| bookmark.table.schema.clone())
            })
            .or_else(|| mcp_context.and_then(|context| context.target_schema.clone()))
            .or_else(|| {
                bookmark_defaults
                    .mcp_bookmark()
                    .and_then(|bookmark| bookmark.table.schema.clone())
            }),
        table: args
            .table
            .clone()
            .or_else(|| {
                bookmark_defaults
                    .cli_bookmark()
                    .map(|bookmark| bookmark.table.name.clone())
            })
            .or_else(|| mcp_context.and_then(|context| context.target_table.clone()))
            .or_else(|| {
                bookmark_defaults
                    .mcp_bookmark()
                    .map(|bookmark| bookmark.table.name.clone())
            }),
        view: args
            .view
            .or_else(|| {
                bookmark_defaults
                    .cli_bookmark()
                    .and_then(|bookmark| bookmark.preferred_view)
            })
            .or_else(|| mcp_context.and_then(|context| context.target_view))
            .or_else(|| {
                bookmark_defaults
                    .mcp_bookmark()
                    .and_then(|bookmark| bookmark.preferred_view)
            }),
        filters: bookmark_query_state.0,
        sort: bookmark_query_state.1,
    };

    if target.view.is_none() && (!target.filters.is_empty() || target.sort.is_some()) {
        target.view = Some(StartupView::Detail);
    }

    if target.is_empty() {
        None
    } else {
        Some(target)
    }
}

fn startup_target_from_bookmark(bookmark: &SavedBookmark) -> StartupTarget {
    let mut target = StartupTarget {
        schema: bookmark.table.schema.clone(),
        table: Some(bookmark.table.name.clone()),
        view: bookmark.preferred_view,
        filters: bookmark.filters.clone(),
        sort: bookmark.sort.clone(),
    };

    if target.view.is_none() && (!target.filters.is_empty() || target.sort.is_some()) {
        target.view = Some(StartupView::Detail);
    }

    target
}

fn resolve_bookmark_defaults(
    args: &CliArgs,
    config: &ConfigStore,
    mcp_context: Option<&McpContext>,
) -> BookmarkDefaults {
    let mut warnings = Vec::new();
    let cli = args
        .bookmark
        .as_deref()
        .and_then(|name| resolve_named_bookmark(config, name, &mut warnings));
    let mcp = mcp_context
        .and_then(|context| context.target_bookmark.as_deref())
        .and_then(|name| resolve_named_bookmark(config, name, &mut warnings));

    BookmarkDefaults {
        cli,
        mcp,
        warning: (!warnings.is_empty()).then(|| warnings.join(" ")),
    }
}

fn resolve_named_bookmark(
    config: &ConfigStore,
    name: &str,
    warnings: &mut Vec<String>,
) -> Option<BookmarkStartupContext> {
    let Some(bookmark) = config.find_bookmark(name) else {
        warnings.push(format!("Bookmark '{name}' was not found."));
        return None;
    };

    let Some(profile) = config.resolve_connection_target(&bookmark.connection) else {
        warnings.push(format!(
            "Bookmark '{}' could not resolve its connection target.",
            bookmark.name
        ));
        return None;
    };

    Some(BookmarkStartupContext { bookmark, profile })
}

fn startup_view_to_detail_view(view: StartupView) -> DetailView {
    match view {
        StartupView::Detail => DetailView::Table,
        StartupView::Graph => DetailView::Graph,
    }
}

fn startup_view_label(view: StartupView) -> &'static str {
    match view {
        StartupView::Detail => "detail",
        StartupView::Graph => "graph",
    }
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

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let popup_width = width.min(area.width.saturating_sub(2).max(1));
    let popup_height = height.min(area.height.saturating_sub(2).max(1));
    Rect::new(
        area.x + area.width.saturating_sub(popup_width) / 2,
        area.y + area.height.saturating_sub(popup_height) / 2,
        popup_width,
        popup_height,
    )
}

fn preview_title(
    preview: Option<&DataPreview>,
    sort: Option<&SortState>,
    filter_count: usize,
) -> String {
    let page = preview.map(|data| data.page + 1).unwrap_or(1);
    let filter_label = match filter_count {
        0 => "no filters".into(),
        1 => "1 filter".into(),
        count => format!("{count} filters"),
    };

    match sort {
        Some(sort) => format!(
            "Preview page {} | {} | sorted by {} {}",
            page,
            filter_label,
            sort.column_name,
            if sort.descending { "desc" } else { "asc" }
        ),
        None => format!("Preview page {page} | {filter_label}"),
    }
}

fn detail_filter_lines(filters: &[PreviewFilter], selected_index: usize) -> Vec<Line<'static>> {
    if filters.is_empty() {
        return vec![
            Line::from("No active filters."),
            Line::from("Press f to add one."),
        ];
    }

    filters
        .iter()
        .enumerate()
        .map(|(index, filter)| {
            let text = format!("{}. {}", index + 1, filter.summary());
            if index == selected_index {
                Line::styled(text, Style::default().add_modifier(Modifier::REVERSED))
            } else {
                Line::from(text)
            }
        })
        .collect()
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
        .map(|row| Row::new(row.display_values()));
    let widths = columns
        .iter()
        .map(|_| Constraint::Length(18))
        .collect::<Vec<_>>();

    Table::new(rows, widths)
        .header(header)
        .block(Block::default().title(title).borders(Borders::ALL))
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED))
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crossterm::event::KeyModifiers;

    fn test_args() -> CliArgs {
        CliArgs {
            profile: None,
            pg_url: None,
            sqlite_path: None,
            schema: None,
            table: None,
            view: None,
            bookmark: None,
            mcp_context_file: None,
        }
    }

    fn test_config() -> ConfigStore {
        ConfigStore {
            profiles_path: PathBuf::from("profiles.toml"),
            state_path: PathBuf::from("state.toml"),
            file: crate::config::ConfigFile::default(),
            state: crate::config::StateFile::default(),
        }
    }

    fn test_app(screen: Screen, search_mode: bool) -> App {
        App {
            config: test_config(),
            screen,
            candidates: vec![ConnectionCandidate {
                profile: ConnectionProfile {
                    name: "sample".into(),
                    kind: DatabaseKind::Sqlite,
                    url: None,
                    path: Some(PathBuf::from("sample.db")),
                },
                bookmark_target: BookmarkConnectionTarget::SavedProfile {
                    name: "sample".into(),
                },
                source: "saved",
            }],
            connection_index: 0,
            schemas: vec!["public".into()],
            schema_index: 0,
            tables: vec![TableRef {
                schema: None,
                name: "widgets".into(),
            }],
            table_index: 0,
            table_filter: String::new(),
            table_search_mode: search_mode,
            session: None,
            active_profile: None,
            active_connection_target: None,
            active_schema: None,
            detail: None,
            active_investigation: None,
            detail_view: DetailView::Table,
            detail_filter_index: 0,
            detail_filter_prompt: None,
            detail_export_prompt: None,
            workspace_prompt: None,
            detail_drill_actions: None,
            detail_drill_index: 0,
            detail_nav_stack: Vec::new(),
            relation_graph: None,
            graph_lane: GraphLane::Center,
            graph_index: 0,
            graph_center_scroll: 0,
            preview: None,
            preview_row_index: 0,
            sort_index: 0,
            status: String::new(),
            example_config: String::new(),
            pending_auto_connect: false,
            startup_target: None,
        }
    }

    fn sample_investigation(page: usize) -> InvestigationState {
        InvestigationState {
            source: InvestigationSource::Table(TableRef {
                schema: None,
                name: "tasks".into(),
            }),
            sort: Some(SortState {
                column_name: "id".into(),
                descending: false,
            }),
            filters: Vec::new(),
            page,
        }
    }

    fn seed_sample_detail_state(app: &mut App, page: usize) {
        app.detail = Some(sample_graph_detail());
        app.active_investigation = Some(sample_investigation(page));
        app.sort_index = 0;
    }

    fn sample_mcp_context() -> McpContext {
        McpContext {
            profile: Some(ConnectionProfile {
                name: "mcp-sample".into(),
                kind: DatabaseKind::Sqlite,
                url: None,
                path: Some(PathBuf::from("sample/readgrid_demo.db")),
            }),
            target_bookmark: None,
            target_schema: Some("mcp_schema".into()),
            target_table: Some("mcp_table".into()),
            target_view: Some(StartupView::Graph),
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

    #[test]
    fn build_startup_target_prefers_cli_fields_over_mcp() {
        let mut args = test_args();
        args.schema = Some("cli_schema".into());
        args.table = Some("cli_table".into());
        args.view = Some(StartupView::Detail);
        let target = build_startup_target(
            &args,
            Some(&sample_mcp_context()),
            &BookmarkDefaults::default(),
        )
        .unwrap();

        assert_eq!(target.schema.as_deref(), Some("cli_schema"));
        assert_eq!(target.table.as_deref(), Some("cli_table"));
        assert_eq!(target.view, Some(StartupView::Detail));
    }

    #[test]
    fn build_startup_target_uses_mcp_fields_when_cli_is_missing() {
        let target = build_startup_target(
            &test_args(),
            Some(&sample_mcp_context()),
            &BookmarkDefaults::default(),
        )
        .unwrap();

        assert_eq!(target.schema.as_deref(), Some("mcp_schema"));
        assert_eq!(target.table.as_deref(), Some("mcp_table"));
        assert_eq!(target.view, Some(StartupView::Graph));
    }

    #[test]
    fn build_candidates_auto_connects_for_mcp_profile() {
        let config = test_config();
        let (_, _, pending_auto_connect) = build_candidates(
            &test_args(),
            &config,
            Some(&sample_mcp_context()),
            &BookmarkDefaults::default(),
        );

        assert!(pending_auto_connect);
    }

    #[test]
    fn build_startup_target_uses_bookmark_query_state_when_bookmark_is_base_context() {
        let defaults = BookmarkDefaults {
            cli: Some(BookmarkStartupContext {
                profile: ConnectionProfile {
                    name: "sample".into(),
                    kind: DatabaseKind::Sqlite,
                    url: None,
                    path: Some(PathBuf::from("sample/readgrid_demo.db")),
                },
                bookmark: sample_saved_bookmark(),
            }),
            ..BookmarkDefaults::default()
        };

        let target = build_startup_target(&test_args(), None, &defaults).unwrap();

        assert_eq!(target.table.as_deref(), Some("tasks"));
        assert_eq!(target.view, Some(StartupView::Graph));
        assert_eq!(target.filters.len(), 1);
        assert_eq!(target.sort.as_ref().unwrap().column_name, "title");
    }

    #[test]
    fn build_startup_target_drops_bookmark_query_state_when_table_is_overridden() {
        let mut args = test_args();
        args.table = Some("projects".into());
        let defaults = BookmarkDefaults {
            cli: Some(BookmarkStartupContext {
                profile: ConnectionProfile {
                    name: "sample".into(),
                    kind: DatabaseKind::Sqlite,
                    url: None,
                    path: Some(PathBuf::from("sample/readgrid_demo.db")),
                },
                bookmark: sample_saved_bookmark(),
            }),
            ..BookmarkDefaults::default()
        };

        let target = build_startup_target(&args, None, &defaults).unwrap();

        assert_eq!(target.table.as_deref(), Some("projects"));
        assert!(target.filters.is_empty());
        assert!(target.sort.is_none());
    }

    #[test]
    fn app_new_rejects_profile_and_direct_connection_mix() {
        let mut args = test_args();
        args.profile = Some("saved".into());
        args.sqlite_path = Some(PathBuf::from("sample/readgrid_demo.db"));

        let error = App::new(args, test_config(), None).err().unwrap();
        assert_eq!(
            error.to_string(),
            "use either --profile or a direct connection target, not both"
        );
    }

    fn line_text(line: &Line<'_>) -> String {
        line.spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>()
    }

    fn preview_cell(raw: Option<&str>) -> crate::db::PreviewCell {
        crate::db::PreviewCell {
            display_value: raw.unwrap_or("NULL").into(),
            raw_value: raw.map(|value| value.into()),
        }
    }

    fn sample_preview() -> DataPreview {
        DataPreview {
            columns: vec![
                "id".into(),
                "project_id".into(),
                "owner_id".into(),
                "title".into(),
            ],
            rows: vec![
                crate::db::PreviewRow {
                    cells: vec![
                        preview_cell(Some("1")),
                        preview_cell(Some("10")),
                        preview_cell(None),
                        preview_cell(Some("Render relationship panel")),
                    ],
                },
                crate::db::PreviewRow {
                    cells: vec![
                        preview_cell(Some("2")),
                        preview_cell(Some("20")),
                        preview_cell(Some("7")),
                        preview_cell(Some("Add paging")),
                    ],
                },
            ],
            page: 0,
            has_more: false,
        }
    }

    fn temp_csv_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "readgrid_app_{name}_{}_{}.csv",
            std::process::id(),
            unique
        ))
    }

    fn temp_state_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "readgrid_app_{name}_{}_{}.toml",
            std::process::id(),
            unique
        ))
    }

    fn sample_saved_bookmark() -> SavedBookmark {
        SavedBookmark {
            name: "tasks-focus".into(),
            connection: BookmarkConnectionTarget::SavedProfile {
                name: "sample".into(),
            },
            table: TableRef {
                schema: None,
                name: "tasks".into(),
            },
            preferred_view: Some(StartupView::Graph),
            filters: vec![PreviewFilter {
                column_name: "title".into(),
                operator: FilterOperator::Contains,
                value: Some("Build".into()),
            }],
            sort: Some(SortState {
                column_name: "title".into(),
                descending: true,
            }),
        }
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

    #[test]
    fn clamp_graph_center_scroll_resets_when_columns_fit() {
        assert_eq!(clamp_graph_center_scroll(3, 4, 10), 0);
    }

    #[test]
    fn clamp_graph_center_scroll_caps_to_last_full_window() {
        assert_eq!(clamp_graph_center_scroll(9, 10, 4), 6);
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
        app.table_filter = "wi".into();

        let should_quit = app
            .handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(!should_quit);
        assert_eq!(app.screen, Screen::Browser);
        assert!(!app.table_search_mode);
        assert!(app.table_filter.is_empty());
    }

    #[tokio::test]
    async fn q_is_search_input_while_filtering() {
        let mut app = test_app(Screen::Browser, true);

        let should_quit = app
            .handle_key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(!should_quit);
        assert_eq!(app.table_filter, "q");
    }

    #[test]
    fn start_detail_filter_prompt_begins_with_column_selection() {
        let mut app = test_app(Screen::Detail, false);
        app.detail = Some(sample_graph_detail());

        app.start_detail_filter_prompt();

        assert!(matches!(
            app.detail_filter_prompt,
            Some(DetailFilterPrompt::SelectColumn { index: 0 })
        ));
    }

    #[test]
    fn value_filter_application_resets_preview_page() {
        let mut app = test_app(Screen::Detail, false);
        seed_sample_detail_state(&mut app, 3);
        app.preview = Some(DataPreview {
            columns: vec![],
            rows: vec![],
            page: 3,
            has_more: false,
        });
        app.detail_filter_prompt = Some(DetailFilterPrompt::EnterValue {
            column_index: 3,
            operator: FilterOperator::Contains,
            value: "page".into(),
        });

        let outcome = app
            .handle_detail_filter_prompt_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .unwrap();

        assert_eq!(outcome, DetailFilterOutcome::ReloadPreview);
        assert_eq!(app.preview.as_ref().unwrap().page, 0);
        assert_eq!(app.active_filters().len(), 1);
        assert_eq!(app.active_filters()[0].column_name, "title");
        assert_eq!(app.active_filters()[0].value.as_deref(), Some("page"));
        assert!(app.detail_filter_prompt.is_none());
    }

    #[test]
    fn null_filter_application_skips_value_entry() {
        let mut app = test_app(Screen::Detail, false);
        seed_sample_detail_state(&mut app, 2);
        app.preview = Some(DataPreview {
            columns: vec![],
            rows: vec![],
            page: 2,
            has_more: false,
        });
        app.detail_filter_prompt = Some(DetailFilterPrompt::SelectOperator {
            column_index: 2,
            index: FilterOperator::ALL
                .iter()
                .position(|operator| *operator == FilterOperator::IsNull)
                .unwrap(),
        });

        let outcome = app
            .handle_detail_filter_prompt_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .unwrap();

        assert_eq!(outcome, DetailFilterOutcome::ReloadPreview);
        assert_eq!(app.preview.as_ref().unwrap().page, 0);
        assert_eq!(app.active_filters().len(), 1);
        assert_eq!(app.active_filters()[0].column_name, "owner_id");
        assert_eq!(app.active_filters()[0].operator, FilterOperator::IsNull);
        assert!(app.active_filters()[0].value.is_none());
    }

    #[test]
    fn remove_selected_filter_clamps_index_and_resets_page() {
        let mut app = test_app(Screen::Detail, false);
        seed_sample_detail_state(&mut app, 4);
        app.preview = Some(DataPreview {
            columns: vec![],
            rows: vec![],
            page: 4,
            has_more: false,
        });
        app.active_investigation.as_mut().unwrap().filters = vec![
            PreviewFilter {
                column_name: "status".into(),
                operator: FilterOperator::Equals,
                value: Some("todo".into()),
            },
            PreviewFilter {
                column_name: "title".into(),
                operator: FilterOperator::Contains,
                value: Some("page".into()),
            },
        ];
        app.detail_filter_index = 1;

        assert!(app.remove_selected_detail_filter());
        assert_eq!(app.preview.as_ref().unwrap().page, 0);
        assert_eq!(app.detail_filter_index, 0);
        assert_eq!(app.active_filters().len(), 1);
        assert_eq!(app.active_filters()[0].column_name, "status");
    }

    #[tokio::test]
    async fn q_is_filter_input_while_entering_filter_value() {
        let mut app = test_app(Screen::Detail, false);
        app.detail = Some(sample_graph_detail());
        app.detail_filter_prompt = Some(DetailFilterPrompt::EnterValue {
            column_index: 3,
            operator: FilterOperator::Contains,
            value: String::new(),
        });

        let should_quit = app
            .handle_key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(!should_quit);
        assert!(matches!(
            app.detail_filter_prompt,
            Some(DetailFilterPrompt::EnterValue { ref value, .. }) if value == "q"
        ));
    }

    #[tokio::test]
    async fn e_opens_export_prompt_from_detail_view() {
        let mut app = test_app(Screen::Detail, false);
        seed_sample_detail_state(&mut app, 0);
        app.preview = Some(sample_preview());

        app.handle_key(KeyEvent::new(KeyCode::Char('e'), KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(matches!(
            app.detail_export_prompt,
            Some(DetailExportPrompt::EnterPath { scope: ExportScope::VisiblePage, ref value, edited: false }) if value == "db_csv/tasks.csv"
        ));
        assert_eq!(
            app.status,
            "Press Enter to export CSV, edit the path, or Tab to change scope."
        );
    }

    #[tokio::test]
    async fn q_is_export_input_while_entering_csv_path() {
        let mut app = test_app(Screen::Detail, false);
        seed_sample_detail_state(&mut app, 0);
        app.preview = Some(sample_preview());
        app.detail_export_prompt = Some(DetailExportPrompt::EnterPath {
            scope: ExportScope::VisiblePage,
            value: String::new(),
            edited: false,
        });

        let should_quit = app
            .handle_key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(!should_quit);
        assert!(matches!(
            app.detail_export_prompt,
            Some(DetailExportPrompt::EnterPath { ref value, .. }) if value == "q"
        ));
    }

    #[tokio::test]
    async fn tab_toggles_export_scope_without_overwriting_default_path_state() {
        let mut app = test_app(Screen::Detail, false);
        seed_sample_detail_state(&mut app, 0);
        app.preview = Some(sample_preview());

        app.handle_key(KeyEvent::new(KeyCode::Char('e'), KeyModifiers::NONE))
            .await
            .unwrap();
        app.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(matches!(
            app.detail_export_prompt,
            Some(DetailExportPrompt::EnterPath {
                scope: ExportScope::AllMatchingRows,
                ref value,
                edited: false
            }) if value == "db_csv/tasks_all.csv"
        ));
        assert_eq!(
            app.status,
            "Export scope: all matching rows. Press Enter to export CSV, edit the path, or Tab to change scope."
        );
    }

    #[tokio::test]
    async fn blank_export_path_is_rejected() {
        let mut app = test_app(Screen::Detail, false);
        seed_sample_detail_state(&mut app, 0);
        app.preview = Some(sample_preview());
        let default_value = app
            .default_export_path(ExportScope::VisiblePage)
            .display()
            .to_string();

        app.handle_key(KeyEvent::new(KeyCode::Char('e'), KeyModifiers::NONE))
            .await
            .unwrap();
        for _ in 0..default_value.chars().count() {
            app.handle_key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE))
                .await
                .unwrap();
        }

        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(matches!(
            app.detail_export_prompt,
            Some(DetailExportPrompt::EnterPath { ref value, .. }) if value.is_empty()
        ));
        assert_eq!(
            app.status,
            "Enter a non-empty CSV path or press Esc to cancel."
        );
    }

    #[tokio::test]
    async fn export_existing_file_requires_confirmation_before_overwrite() {
        let mut app = test_app(Screen::Detail, false);
        seed_sample_detail_state(&mut app, 0);
        app.preview = Some(sample_preview());
        let path = temp_csv_path("confirm");
        fs::write(&path, "old").unwrap();
        let value = path.display().to_string();
        app.detail_export_prompt = Some(DetailExportPrompt::EnterPath {
            scope: ExportScope::VisiblePage,
            value: value.clone(),
            edited: true,
        });

        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(matches!(
            app.detail_export_prompt,
            Some(DetailExportPrompt::ConfirmOverwrite { value: ref prompt_value, scope: ExportScope::VisiblePage, .. }) if prompt_value == &value
        ));

        app.handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(matches!(
            app.detail_export_prompt,
            Some(DetailExportPrompt::EnterPath { value: ref prompt_value, scope: ExportScope::VisiblePage, .. }) if prompt_value == &value
        ));
        fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn export_prompt_writes_csv_and_updates_status() {
        let mut app = test_app(Screen::Detail, false);
        seed_sample_detail_state(&mut app, 0);
        app.preview = Some(sample_preview());
        let path = temp_csv_path("success");
        let value = path.display().to_string();
        app.detail_export_prompt = Some(DetailExportPrompt::EnterPath {
            scope: ExportScope::VisiblePage,
            value: value.clone(),
            edited: true,
        });

        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .await
            .unwrap();

        let mut reader = csv::Reader::from_path(&path).unwrap();
        let headers = reader.headers().unwrap().clone();
        let rows = reader
            .records()
            .map(|row| row.unwrap().iter().map(str::to_string).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        fs::remove_file(&path).ok();

        assert!(app.detail_export_prompt.is_none());
        assert_eq!(
            app.status,
            format!("Exported 2 rows (visible page) to {value}.")
        );
        assert_eq!(
            headers.iter().collect::<Vec<_>>(),
            vec!["id", "project_id", "owner_id", "title"]
        );
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][2], "NULL");
    }

    #[tokio::test]
    async fn export_failure_keeps_prompt_open_and_reports_status() {
        let mut app = test_app(Screen::Detail, false);
        seed_sample_detail_state(&mut app, 0);
        app.preview = Some(sample_preview());
        let parent = temp_csv_path("failure_parent");
        fs::write(&parent, "not a directory").unwrap();
        let path = parent.join("export.csv");
        let value = path.display().to_string();
        app.detail_export_prompt = Some(DetailExportPrompt::EnterPath {
            scope: ExportScope::VisiblePage,
            value: value.clone(),
            edited: true,
        });

        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .await
            .unwrap();

        assert!(matches!(
            app.detail_export_prompt,
            Some(DetailExportPrompt::EnterPath { value: ref prompt_value, scope: ExportScope::VisiblePage, .. }) if prompt_value == &value
        ));
        assert!(app.status.starts_with("CSV export failed: "));
        fs::remove_file(&parent).ok();
    }

    #[tokio::test]
    async fn save_bookmark_persists_current_detail_context() {
        let mut app = test_app(Screen::Detail, false);
        app.config.state_path = temp_state_path("bookmark_save");
        seed_sample_detail_state(&mut app, 0);
        app.active_connection_target = Some(BookmarkConnectionTarget::SavedProfile {
            name: "sample".into(),
        });

        app.start_save_bookmark_prompt();
        app.handle_workspace_prompt_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .await
            .unwrap();

        let bookmark = app.config.find_bookmark("tasks").unwrap();
        assert_eq!(bookmark.table.name, "tasks");
        assert_eq!(bookmark.preferred_view, Some(StartupView::Detail));
        assert_eq!(
            bookmark.connection,
            app.active_connection_target.clone().unwrap()
        );
        fs::remove_file(&app.config.state_path).ok();
    }

    #[tokio::test]
    async fn applying_preset_replaces_filters_and_resets_page() {
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
        app.active_connection_target = Some(BookmarkConnectionTarget::SavedProfile {
            name: "sample".into(),
        });
        app.detail = Some(sample_graph_detail());
        app.active_investigation = Some(InvestigationState {
            source: InvestigationSource::Table(TableRef {
                schema: None,
                name: "tasks".into(),
            }),
            sort: Some(SortState {
                column_name: "id".into(),
                descending: false,
            }),
            filters: vec![PreviewFilter {
                column_name: "id".into(),
                operator: FilterOperator::Equals,
                value: Some("1".into()),
            }],
            page: 2,
        });
        app.config.state.filter_presets.push(FilterPreset {
            name: "build-only".into(),
            connection: BookmarkConnectionTarget::SavedProfile {
                name: "sample".into(),
            },
            table: TableRef {
                schema: None,
                name: "tasks".into(),
            },
            filters: vec![PreviewFilter {
                column_name: "title".into(),
                operator: FilterOperator::Contains,
                value: Some("Build".into()),
            }],
        });

        app.start_apply_preset_prompt();
        app.handle_workspace_prompt_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .await
            .unwrap();

        let investigation = app.active_investigation.as_ref().unwrap();
        assert_eq!(investigation.page, 0);
        assert_eq!(investigation.filters.len(), 1);
        assert_eq!(investigation.filters[0].column_name, "title");
        assert_eq!(app.status, "Applied preset 'build-only'.");
    }

    #[tokio::test]
    async fn up_down_moves_preview_row_selection() {
        let mut app = test_app(Screen::Detail, false);
        app.detail = Some(sample_graph_detail());
        app.preview = Some(sample_preview());

        app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE))
            .await
            .unwrap();
        assert_eq!(app.preview_row_index, 1);

        app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE))
            .await
            .unwrap();
        assert_eq!(app.preview_row_index, 1);

        app.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE))
            .await
            .unwrap();
        assert_eq!(app.preview_row_index, 0);
    }

    #[tokio::test]
    async fn enter_opens_relation_picker_from_selected_row() {
        let mut app = test_app(Screen::Detail, false);
        app.detail = Some(sample_graph_detail());
        app.preview = Some(sample_preview());

        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .await
            .unwrap();

        assert_eq!(
            app.detail_drill_actions
                .as_ref()
                .map(|actions| actions.len()),
            Some(2)
        );
        assert_eq!(app.detail_drill_index, 0);
    }

    #[tokio::test]
    async fn disabled_relation_action_updates_status_without_navigation() {
        let mut app = test_app(Screen::Detail, false);
        app.detail = Some(TableDetail {
            table: TableRef {
                schema: None,
                name: "tasks".into(),
            },
            columns: sample_graph_detail().columns,
            foreign_keys: vec![ForeignKeyMeta {
                from_column: "owner_id".into(),
                to_table: TableRef {
                    schema: None,
                    name: "users".into(),
                },
                to_column: "id".into(),
                direction: crate::db::RelationshipDirection::Outgoing,
            }],
            indexes: vec![],
        });
        app.preview = Some(sample_preview());

        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .await
            .unwrap();
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .await
            .unwrap();

        assert_eq!(app.detail.as_ref().unwrap().table.name, "tasks");
        assert!(app.detail_drill_actions.is_some());
        assert_eq!(app.detail_nav_stack.len(), 0);
        assert_eq!(app.status, "Selected row has NULL in owner_id.");
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

        let visible_rows = app.graph_center_visible_rows();
        let expected_scroll = 2usize.min(
            app.detail
                .as_ref()
                .unwrap()
                .columns
                .len()
                .saturating_sub(visible_rows),
        );

        app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE))
            .await
            .unwrap();
        app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE))
            .await
            .unwrap();

        assert_eq!(app.graph_lane, GraphLane::Center);
        assert_eq!(app.graph_index, 0);
        assert_eq!(app.graph_center_scroll, expected_scroll);

        app.handle_key(KeyEvent::new(KeyCode::Left, KeyModifiers::NONE))
            .await
            .unwrap();
        app.handle_key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE))
            .await
            .unwrap();
        assert_eq!(app.graph_center_scroll, expected_scroll);
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
    async fn export_sample_preview_uses_current_filtered_rows() {
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
        app.load_table_detail(
            TableRef {
                schema: None,
                name: "tasks".into(),
            },
            DetailView::Table,
        )
        .await
        .unwrap();
        app.active_investigation.as_mut().unwrap().filters = vec![PreviewFilter {
            column_name: "status".into(),
            operator: FilterOperator::Equals,
            value: Some("todo".into()),
        }];
        app.sort_index = 0;
        app.sync_sort_from_index().unwrap();
        app.reload_preview().await.unwrap();

        let expected_preview = app.preview.clone().unwrap();
        let export_path = temp_csv_path("sample_preview");
        let export_value = export_path.display().to_string();
        app.detail_export_prompt = Some(DetailExportPrompt::EnterPath {
            scope: ExportScope::VisiblePage,
            value: export_value.clone(),
            edited: true,
        });

        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .await
            .unwrap();

        let mut reader = csv::Reader::from_path(&export_path).unwrap();
        let headers = reader.headers().unwrap().clone();
        let rows = reader
            .records()
            .map(|row| row.unwrap().iter().map(str::to_string).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        fs::remove_file(&export_path).ok();

        assert!(app.detail_export_prompt.is_none());
        assert_eq!(
            app.status,
            format!("Exported 2 rows (visible page) to {export_value}.")
        );
        assert_eq!(
            headers.iter().map(str::to_string).collect::<Vec<_>>(),
            expected_preview.columns
        );
        assert_eq!(
            rows,
            expected_preview
                .rows
                .iter()
                .map(|row| row.display_values())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn drill_through_and_escape_restore_parent_context_using_sample_db() {
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
        app.load_table_detail(
            TableRef {
                schema: None,
                name: "tasks".into(),
            },
            DetailView::Table,
        )
        .await
        .unwrap();
        app.sort_index = 0;
        app.sync_sort_from_index().unwrap();
        app.reload_preview().await.unwrap();

        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .await
            .unwrap();
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .await
            .unwrap();

        assert_eq!(app.detail.as_ref().unwrap().table.name, "projects");
        assert_eq!(app.detail_nav_stack.len(), 1);
        assert_eq!(app.active_filters().len(), 1);
        assert_eq!(app.active_filters()[0].column_name, "id");
        assert_eq!(app.active_filters()[0].value.as_deref(), Some("1"));

        app.handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE))
            .await
            .unwrap();

        assert_eq!(app.detail.as_ref().unwrap().table.name, "tasks");
        assert_eq!(app.detail_nav_stack.len(), 0);
        assert_eq!(app.preview_row_index, 0);
        assert!(app.active_filters().is_empty());
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

    #[tokio::test]
    async fn startup_target_selects_sqlite_table_in_browser() {
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

        let mut app = test_app(Screen::Browser, false);
        app.session = Some(session);
        app.startup_target = Some(StartupTarget {
            schema: Some("ignored".into()),
            table: Some("tasks".into()),
            view: None,
            filters: Vec::new(),
            sort: None,
        });

        app.load_tables(None).await.unwrap();
        app.screen = Screen::Browser;
        app.continue_startup_after_table_load().await.unwrap();

        assert_eq!(app.selected_table().unwrap().name, "tasks");
        assert!(app.detail.is_none());
        assert!(app.startup_target.is_none());
        assert_eq!(app.status, "Selected startup table tasks.");
    }

    #[tokio::test]
    async fn startup_target_opens_sqlite_detail_view() {
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

        let mut app = test_app(Screen::Browser, false);
        app.session = Some(session);
        app.startup_target = Some(StartupTarget {
            schema: None,
            table: Some("tasks".into()),
            view: Some(StartupView::Detail),
            filters: Vec::new(),
            sort: None,
        });

        app.load_tables(None).await.unwrap();
        app.screen = Screen::Browser;
        app.continue_startup_after_table_load().await.unwrap();

        assert_eq!(app.screen, Screen::Detail);
        assert_eq!(app.detail.as_ref().unwrap().table.name, "tasks");
        assert_eq!(app.detail_view, DetailView::Table);
        assert!(app.startup_target.is_none());
        assert_eq!(app.status, "Opened tasks in detail view.");
    }

    #[tokio::test]
    async fn startup_target_opens_sqlite_graph_view() {
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

        let mut app = test_app(Screen::Browser, false);
        app.session = Some(session);
        app.startup_target = Some(StartupTarget {
            schema: None,
            table: Some("tasks".into()),
            view: Some(StartupView::Graph),
            filters: Vec::new(),
            sort: None,
        });

        app.load_tables(None).await.unwrap();
        app.screen = Screen::Browser;
        app.continue_startup_after_table_load().await.unwrap();

        assert_eq!(app.screen, Screen::Detail);
        assert_eq!(app.detail.as_ref().unwrap().table.name, "tasks");
        assert_eq!(app.detail_view, DetailView::Graph);
        assert!(app.relation_graph.is_some());
        assert!(app.startup_target.is_none());
        assert_eq!(app.status, "Opened tasks in graph view.");
    }

    #[tokio::test]
    async fn startup_target_invalid_schema_keeps_remaining_target_pending() {
        let mut app = test_app(Screen::Schemas, false);
        app.schemas = vec!["public".into()];
        app.startup_target = Some(StartupTarget {
            schema: Some("missing".into()),
            table: Some("tasks".into()),
            view: Some(StartupView::Detail),
            filters: Vec::new(),
            sort: None,
        });

        app.continue_startup_after_connect().await.unwrap();

        assert_eq!(
            app.status,
            "Schema 'missing' was not found. Choose a schema to continue."
        );
        assert_eq!(app.startup_target.as_ref().unwrap().schema, None);
        assert_eq!(
            app.startup_target.as_ref().unwrap().table.as_deref(),
            Some("tasks")
        );
        assert_eq!(
            app.startup_target.as_ref().unwrap().view,
            Some(StartupView::Detail)
        );
    }

    #[tokio::test]
    async fn startup_view_without_table_falls_back_in_browser() {
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

        let mut app = test_app(Screen::Browser, false);
        app.session = Some(session);
        app.startup_target = Some(StartupTarget {
            schema: None,
            table: None,
            view: Some(StartupView::Graph),
            filters: Vec::new(),
            sort: None,
        });

        app.load_tables(None).await.unwrap();
        app.screen = Screen::Browser;
        app.continue_startup_after_table_load().await.unwrap();

        assert_eq!(app.screen, Screen::Browser);
        assert!(app.detail.is_none());
        assert!(app.startup_target.is_none());
        assert_eq!(app.status, "Startup view 'graph' requires a target table.");
    }
}
