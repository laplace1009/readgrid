use std::{fmt, path::PathBuf};

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

pub const PAGE_SIZE: usize = 50;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseKind {
    Postgres,
    Sqlite,
}

impl fmt::Display for DatabaseKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Postgres => write!(f, "postgres"),
            Self::Sqlite => write!(f, "sqlite"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectionProfile {
    pub name: String,
    pub kind: DatabaseKind,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub path: Option<PathBuf>,
}

impl ConnectionProfile {
    pub fn summary(&self) -> String {
        match self.kind {
            DatabaseKind::Postgres => self.url.clone().unwrap_or_else(|| "missing url".into()),
            DatabaseKind::Sqlite => self
                .path
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "missing path".into()),
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self.kind {
            DatabaseKind::Postgres if self.url.is_none() => {
                Err(anyhow!("profile '{}' is missing a postgres url", self.name))
            }
            DatabaseKind::Sqlite if self.path.is_none() => {
                Err(anyhow!("profile '{}' is missing a sqlite path", self.name))
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TableRef {
    pub schema: Option<String>,
    pub name: String,
}

impl TableRef {
    pub fn display_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", schema, self.name),
            None => self.name.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub is_primary_key: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationshipDirection {
    Outgoing,
    Incoming,
}

impl RelationshipDirection {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Outgoing => "out",
            Self::Incoming => "in",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyMeta {
    pub from_column: String,
    pub to_table: TableRef,
    pub to_column: String,
    pub direction: RelationshipDirection,
}

impl ForeignKeyMeta {
    pub fn local_column(&self) -> &str {
        match self.direction {
            RelationshipDirection::Outgoing => &self.from_column,
            RelationshipDirection::Incoming => &self.to_column,
        }
    }

    pub fn remote_column(&self) -> &str {
        match self.direction {
            RelationshipDirection::Outgoing => &self.to_column,
            RelationshipDirection::Incoming => &self.from_column,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexMeta {
    pub name: String,
    pub is_unique: bool,
    pub details: String,
}

#[derive(Debug, Clone)]
pub struct TableDetail {
    pub table: TableRef,
    pub columns: Vec<ColumnMeta>,
    pub foreign_keys: Vec<ForeignKeyMeta>,
    pub indexes: Vec<IndexMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SortState {
    pub column_name: String,
    pub descending: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FilterOperator {
    Equals,
    NotEquals,
    Contains,
    StartsWith,
    IsNull,
    IsNotNull,
}

impl FilterOperator {
    pub const ALL: [Self; 6] = [
        Self::Equals,
        Self::NotEquals,
        Self::Contains,
        Self::StartsWith,
        Self::IsNull,
        Self::IsNotNull,
    ];

    pub fn label(&self) -> &'static str {
        match self {
            Self::Equals => "equals",
            Self::NotEquals => "not equals",
            Self::Contains => "contains",
            Self::StartsWith => "starts with",
            Self::IsNull => "is null",
            Self::IsNotNull => "is not null",
        }
    }

    pub fn requires_value(&self) -> bool {
        !matches!(self, Self::IsNull | Self::IsNotNull)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PreviewFilter {
    pub column_name: String,
    pub operator: FilterOperator,
    pub value: Option<String>,
}

impl PreviewFilter {
    pub fn summary(&self) -> String {
        match self.operator {
            FilterOperator::IsNull | FilterOperator::IsNotNull => {
                format!("{} {}", self.column_name, self.operator.label())
            }
            _ => format!(
                "{} {} {}",
                self.column_name,
                self.operator.label(),
                self.value.as_deref().unwrap_or("")
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvestigationSource {
    Table(TableRef),
}

impl InvestigationSource {
    pub fn table(&self) -> &TableRef {
        match self {
            Self::Table(table) => table,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvestigationState {
    pub source: InvestigationSource,
    pub sort: Option<SortState>,
    pub filters: Vec<PreviewFilter>,
    pub page: usize,
}

impl InvestigationState {
    pub fn for_table(table: TableRef) -> Self {
        Self {
            source: InvestigationSource::Table(table),
            sort: None,
            filters: Vec::new(),
            page: 0,
        }
    }

    pub fn table(&self) -> &TableRef {
        self.source.table()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportScope {
    VisiblePage,
    AllMatchingRows,
}

impl ExportScope {
    pub fn label(&self) -> &'static str {
        match self {
            Self::VisiblePage => "visible page",
            Self::AllMatchingRows => "all matching rows",
        }
    }

    pub fn file_suffix(&self) -> &'static str {
        match self {
            Self::VisiblePage => "",
            Self::AllMatchingRows => "_all",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    Csv,
    Json,
}

impl ExportFormat {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Csv => "CSV",
            Self::Json => "JSON",
        }
    }

    pub fn extension(&self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Json => "json",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportRequest {
    pub format: ExportFormat,
    pub scope: ExportScope,
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportSummary {
    pub rows_written: usize,
    pub format: ExportFormat,
    pub scope: ExportScope,
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewCell {
    pub display_value: String,
    pub raw_value: Option<String>,
}

impl PreviewCell {
    pub(crate) fn from_raw(raw_value: Option<String>) -> Self {
        Self {
            display_value: raw_value.clone().unwrap_or_else(|| "NULL".into()),
            raw_value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewRow {
    pub cells: Vec<PreviewCell>,
}

impl PreviewRow {
    pub fn display_values(&self) -> Vec<String> {
        self.cells
            .iter()
            .map(|cell| cell.display_value.clone())
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct DataPreview {
    pub columns: Vec<String>,
    pub rows: Vec<PreviewRow>,
    pub page: usize,
    pub has_more: bool,
}

impl DataPreview {
    pub fn cell(&self, row_index: usize, column_name: &str) -> Option<&PreviewCell> {
        let column_index = self
            .columns
            .iter()
            .position(|column| column == column_name)?;
        self.rows.get(row_index)?.cells.get(column_index)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrillThroughAction {
    pub relation: ForeignKeyMeta,
    pub target_table: TableRef,
    pub target_filter: Option<PreviewFilter>,
    pub unavailable_reason: Option<String>,
}

impl DrillThroughAction {
    pub fn label(&self) -> String {
        format!(
            "[{}] {}.{} <- {}",
            self.relation.direction.label(),
            self.target_table.display_name(),
            self.relation.remote_column(),
            self.relation.local_column(),
        )
    }

    pub fn is_available(&self) -> bool {
        self.target_filter.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RelationNodeRole {
    Incoming,
    Center,
    Outgoing,
}

#[derive(Debug, Clone)]
pub struct RelationNode {
    pub table: TableRef,
    pub key_columns: Vec<String>,
    pub role: RelationNodeRole,
}

#[derive(Debug, Clone)]
pub struct RelationEdge {
    pub source_table: TableRef,
    pub source_column: String,
    pub target_table: TableRef,
    pub target_column: String,
}

#[derive(Debug, Clone)]
pub struct RelationGraph {
    pub center: TableRef,
    pub nodes: Vec<RelationNode>,
    pub edges: Vec<RelationEdge>,
}
