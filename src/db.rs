use std::{
    collections::{HashMap, HashSet},
    fmt,
    path::PathBuf,
};

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row, SqlitePool, postgres::PgRow, sqlite::SqliteRow};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct SortState {
    pub column_name: String,
    pub descending: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone)]
pub struct PreviewRequest {
    pub sort: Option<SortState>,
    pub filters: Vec<PreviewFilter>,
    pub page: usize,
}

#[derive(Debug, Clone)]
pub struct DataPreview {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub page: usize,
    pub has_more: bool,
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

#[derive(Debug, Clone)]
struct RelationNodeSpec {
    table: TableRef,
    related_columns: Vec<String>,
    role: RelationNodeRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreviewDialect {
    Postgres,
    Sqlite,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct FilterClause {
    sql: String,
    bindings: Vec<String>,
}

pub enum Session {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

impl Session {
    pub async fn connect(profile: &ConnectionProfile) -> Result<Self> {
        profile.validate()?;

        match profile.kind {
            DatabaseKind::Postgres => {
                let url = profile.url.as_ref().context("missing postgres url")?;
                let pool = PgPool::connect(url).await?;
                Ok(Self::Postgres(pool))
            }
            DatabaseKind::Sqlite => {
                let path = profile.path.as_ref().context("missing sqlite path")?;
                let url = format!("sqlite:{}", path.display());
                let pool = SqlitePool::connect(&url).await?;
                Ok(Self::Sqlite(pool))
            }
        }
    }

    pub fn kind(&self) -> DatabaseKind {
        match self {
            Self::Postgres(_) => DatabaseKind::Postgres,
            Self::Sqlite(_) => DatabaseKind::Sqlite,
        }
    }

    pub async fn list_schemas(&self) -> Result<Vec<String>> {
        match self {
            Self::Postgres(pool) => {
                let rows = sqlx::query(
                    "SELECT schema_name FROM information_schema.schemata
                     WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
                     ORDER BY schema_name",
                )
                .fetch_all(pool)
                .await?;
                Ok(rows
                    .into_iter()
                    .map(|row| row.get::<String, _>("schema_name"))
                    .collect())
            }
            Self::Sqlite(_) => Ok(vec!["main".into()]),
        }
    }

    pub async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableRef>> {
        match self {
            Self::Postgres(pool) => {
                let schema = schema.context("postgres table listing requires a schema")?;
                let rows = sqlx::query(
                    "SELECT table_name FROM information_schema.tables
                     WHERE table_schema = $1 AND table_type = 'BASE TABLE'
                     ORDER BY table_name",
                )
                .bind(schema)
                .fetch_all(pool)
                .await?;
                Ok(rows
                    .into_iter()
                    .map(|row| TableRef {
                        schema: Some(schema.to_string()),
                        name: row.get("table_name"),
                    })
                    .collect())
            }
            Self::Sqlite(pool) => {
                let rows = sqlx::query(
                    "SELECT name FROM sqlite_master
                     WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                     ORDER BY name",
                )
                .fetch_all(pool)
                .await?;
                Ok(rows
                    .into_iter()
                    .map(|row| TableRef {
                        schema: None,
                        name: row.get("name"),
                    })
                    .collect())
            }
        }
    }

    pub async fn load_detail(&self, table: &TableRef) -> Result<TableDetail> {
        match self {
            Self::Postgres(pool) => load_postgres_detail(pool, table).await,
            Self::Sqlite(pool) => load_sqlite_detail(pool, table).await,
        }
    }

    pub async fn load_preview(
        &self,
        table: &TableRef,
        request: &PreviewRequest,
    ) -> Result<DataPreview> {
        match self {
            Self::Postgres(pool) => load_postgres_preview(pool, table, request).await,
            Self::Sqlite(pool) => load_sqlite_preview(pool, table, request).await,
        }
    }

    pub async fn load_relation_graph(&self, table: &TableRef) -> Result<RelationGraph> {
        match self {
            Self::Postgres(pool) => load_postgres_relation_graph(pool, table).await,
            Self::Sqlite(pool) => load_sqlite_relation_graph(pool, table).await,
        }
    }
}

async fn load_postgres_columns(pool: &PgPool, table: &TableRef) -> Result<Vec<ColumnMeta>> {
    let schema = table
        .schema
        .as_deref()
        .context("postgres detail requires schema")?;
    Ok(sqlx::query(
        "SELECT c.column_name, c.data_type, c.is_nullable, c.column_default,
                EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                      AND tc.table_schema = c.table_schema
                      AND tc.table_name = c.table_name
                      AND kcu.column_name = c.column_name
                ) AS is_primary_key
         FROM information_schema.columns c
         WHERE c.table_schema = $1 AND c.table_name = $2
         ORDER BY c.ordinal_position",
    )
    .bind(schema)
    .bind(&table.name)
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| ColumnMeta {
        name: row.get("column_name"),
        data_type: row.get("data_type"),
        nullable: row.get::<String, _>("is_nullable") == "YES",
        default_value: row.try_get("column_default").ok(),
        is_primary_key: row.get("is_primary_key"),
    })
    .collect())
}

async fn load_postgres_foreign_keys(
    pool: &PgPool,
    table: &TableRef,
) -> Result<Vec<ForeignKeyMeta>> {
    let schema = table
        .schema
        .as_deref()
        .context("postgres detail requires schema")?;

    let outgoing = sqlx::query(
        "SELECT kcu.column_name AS from_column,
                ccu.table_schema AS target_schema,
                ccu.table_name AS target_table,
                ccu.column_name AS target_column
         FROM information_schema.table_constraints tc
         JOIN information_schema.key_column_usage kcu
           ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
         JOIN information_schema.constraint_column_usage ccu
           ON ccu.constraint_name = tc.constraint_name
          AND ccu.constraint_schema = tc.table_schema
         WHERE tc.constraint_type = 'FOREIGN KEY'
           AND tc.table_schema = $1
           AND tc.table_name = $2",
    )
    .bind(schema)
    .bind(&table.name)
    .fetch_all(pool)
    .await?;

    let incoming = sqlx::query(
        "SELECT kcu.table_schema AS source_schema,
                kcu.table_name AS source_table,
                kcu.column_name AS source_column,
                ccu.column_name AS target_column
         FROM information_schema.table_constraints tc
         JOIN information_schema.key_column_usage kcu
           ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
         JOIN information_schema.constraint_column_usage ccu
           ON ccu.constraint_name = tc.constraint_name
          AND ccu.constraint_schema = tc.table_schema
         WHERE tc.constraint_type = 'FOREIGN KEY'
           AND ccu.table_schema = $1
           AND ccu.table_name = $2",
    )
    .bind(schema)
    .bind(&table.name)
    .fetch_all(pool)
    .await?;

    let mut foreign_keys = Vec::new();
    foreign_keys.extend(outgoing.into_iter().map(|row| ForeignKeyMeta {
        from_column: row.get("from_column"),
        to_table: TableRef {
            schema: Some(row.get("target_schema")),
            name: row.get("target_table"),
        },
        to_column: row.get("target_column"),
        direction: RelationshipDirection::Outgoing,
    }));
    foreign_keys.extend(incoming.into_iter().map(|row| ForeignKeyMeta {
        from_column: row.get("source_column"),
        to_table: TableRef {
            schema: Some(row.get("source_schema")),
            name: row.get("source_table"),
        },
        to_column: row.get("target_column"),
        direction: RelationshipDirection::Incoming,
    }));
    Ok(foreign_keys)
}

async fn load_postgres_detail(pool: &PgPool, table: &TableRef) -> Result<TableDetail> {
    let schema = table
        .schema
        .as_deref()
        .context("postgres detail requires schema")?;
    let columns = load_postgres_columns(pool, table).await?;
    let foreign_keys = load_postgres_foreign_keys(pool, table).await?;

    let indexes = sqlx::query(
        "SELECT indexname, indexdef FROM pg_indexes
         WHERE schemaname = $1 AND tablename = $2
         ORDER BY indexname",
    )
    .bind(schema)
    .bind(&table.name)
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| {
        let details: String = row.get("indexdef");
        IndexMeta {
            name: row.get("indexname"),
            is_unique: details.contains("UNIQUE INDEX"),
            details,
        }
    })
    .collect();

    Ok(TableDetail {
        table: table.clone(),
        columns,
        foreign_keys,
        indexes,
    })
}

async fn load_sqlite_columns(pool: &SqlitePool, table: &TableRef) -> Result<Vec<ColumnMeta>> {
    Ok(
        sqlx::query(&format!("PRAGMA table_info({})", quote_ident(&table.name)))
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| ColumnMeta {
                name: row.get("name"),
                data_type: row.get("type"),
                nullable: row.get::<i64, _>("notnull") == 0,
                default_value: row.try_get("dflt_value").ok(),
                is_primary_key: row.get::<i64, _>("pk") > 0,
            })
            .collect::<Vec<_>>(),
    )
}

async fn load_sqlite_foreign_keys(
    pool: &SqlitePool,
    table: &TableRef,
) -> Result<Vec<ForeignKeyMeta>> {
    let outgoing = sqlx::query(&format!(
        "PRAGMA foreign_key_list({})",
        quote_ident(&table.name)
    ))
    .fetch_all(pool)
    .await?;

    let table_names = sqlx::query(
        "SELECT name FROM sqlite_master
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
    )
    .fetch_all(pool)
    .await?;

    let mut foreign_keys = Vec::new();
    foreign_keys.extend(outgoing.into_iter().map(|row| ForeignKeyMeta {
        from_column: row.get("from"),
        to_table: TableRef {
            schema: None,
            name: row.get("table"),
        },
        to_column: row.get("to"),
        direction: RelationshipDirection::Outgoing,
    }));

    for name_row in table_names {
        let other_name: String = name_row.get("name");
        let edges = sqlx::query(&format!(
            "PRAGMA foreign_key_list({})",
            quote_ident(&other_name)
        ))
        .fetch_all(pool)
        .await?;

        for edge in edges {
            let target_table: String = edge.get("table");
            if target_table == table.name {
                foreign_keys.push(ForeignKeyMeta {
                    from_column: edge.get("from"),
                    to_table: TableRef {
                        schema: None,
                        name: other_name.clone(),
                    },
                    to_column: edge.get("to"),
                    direction: RelationshipDirection::Incoming,
                });
            }
        }
    }

    Ok(foreign_keys)
}

async fn load_sqlite_detail(pool: &SqlitePool, table: &TableRef) -> Result<TableDetail> {
    let columns = load_sqlite_columns(pool, table).await?;
    let foreign_keys = load_sqlite_foreign_keys(pool, table).await?;

    let index_list = sqlx::query(&format!("PRAGMA index_list({})", quote_ident(&table.name)))
        .fetch_all(pool)
        .await?;

    let mut indexes = Vec::new();
    for row in index_list {
        let index_name: String = row.get("name");
        let unique = row.get::<i64, _>("unique") == 1;
        let columns = sqlx::query(&format!("PRAGMA index_info({})", quote_ident(&index_name)))
            .fetch_all(pool)
            .await?;
        let details = columns
            .into_iter()
            .map(|col| col.get::<String, _>("name"))
            .collect::<Vec<_>>()
            .join(", ");
        indexes.push(IndexMeta {
            name: index_name,
            is_unique: unique,
            details,
        });
    }

    Ok(TableDetail {
        table: table.clone(),
        columns,
        foreign_keys,
        indexes,
    })
}

async fn load_postgres_relation_graph(pool: &PgPool, table: &TableRef) -> Result<RelationGraph> {
    let detail = load_postgres_detail(pool, table).await?;
    let specs = relation_node_specs(&detail);
    let mut nodes = Vec::new();

    for spec in specs {
        let columns = if spec.role == RelationNodeRole::Center {
            detail.columns.clone()
        } else {
            load_postgres_columns(pool, &spec.table).await?
        };
        nodes.push(RelationNode {
            table: spec.table,
            key_columns: collect_key_columns(&columns, &spec.related_columns),
            role: spec.role,
        });
    }

    sort_relation_nodes(&mut nodes);
    Ok(RelationGraph {
        center: detail.table.clone(),
        nodes,
        edges: relation_edges(&detail),
    })
}

async fn load_sqlite_relation_graph(pool: &SqlitePool, table: &TableRef) -> Result<RelationGraph> {
    let detail = load_sqlite_detail(pool, table).await?;
    let specs = relation_node_specs(&detail);
    let mut nodes = Vec::new();

    for spec in specs {
        let columns = if spec.role == RelationNodeRole::Center {
            detail.columns.clone()
        } else {
            load_sqlite_columns(pool, &spec.table).await?
        };
        nodes.push(RelationNode {
            table: spec.table,
            key_columns: collect_key_columns(&columns, &spec.related_columns),
            role: spec.role,
        });
    }

    sort_relation_nodes(&mut nodes);
    Ok(RelationGraph {
        center: detail.table.clone(),
        nodes,
        edges: relation_edges(&detail),
    })
}

fn relation_node_specs(detail: &TableDetail) -> Vec<RelationNodeSpec> {
    let mut center_columns = Vec::new();
    let mut grouped = HashMap::<(RelationNodeRole, TableRef), Vec<String>>::new();

    for edge in &detail.foreign_keys {
        center_columns.push(edge.local_column().to_string());
        let role = match edge.direction {
            RelationshipDirection::Incoming => RelationNodeRole::Incoming,
            RelationshipDirection::Outgoing => RelationNodeRole::Outgoing,
        };
        grouped
            .entry((role, edge.to_table.clone()))
            .or_default()
            .push(edge.remote_column().to_string());
    }

    let mut specs = vec![RelationNodeSpec {
        table: detail.table.clone(),
        related_columns: center_columns,
        role: RelationNodeRole::Center,
    }];

    for ((role, table), related_columns) in grouped {
        specs.push(RelationNodeSpec {
            table,
            related_columns,
            role,
        });
    }

    specs
}

fn relation_edges(detail: &TableDetail) -> Vec<RelationEdge> {
    detail
        .foreign_keys
        .iter()
        .map(|edge| match edge.direction {
            RelationshipDirection::Outgoing => RelationEdge {
                source_table: detail.table.clone(),
                source_column: edge.from_column.clone(),
                target_table: edge.to_table.clone(),
                target_column: edge.to_column.clone(),
            },
            RelationshipDirection::Incoming => RelationEdge {
                source_table: edge.to_table.clone(),
                source_column: edge.from_column.clone(),
                target_table: detail.table.clone(),
                target_column: edge.to_column.clone(),
            },
        })
        .collect()
}

fn collect_key_columns(columns: &[ColumnMeta], related_columns: &[String]) -> Vec<String> {
    let related = related_columns.iter().cloned().collect::<HashSet<_>>();
    let mut rendered = Vec::new();

    for column in columns {
        if column.is_primary_key || related.contains(&column.name) {
            rendered.push(column.name.clone());
        }
    }

    rendered
}

fn sort_relation_nodes(nodes: &mut [RelationNode]) {
    nodes.sort_by_key(|node| (relation_role_rank(node.role), node.table.display_name()));
}

fn relation_role_rank(role: RelationNodeRole) -> u8 {
    match role {
        RelationNodeRole::Incoming => 0,
        RelationNodeRole::Center => 1,
        RelationNodeRole::Outgoing => 2,
    }
}

async fn load_postgres_preview(
    pool: &PgPool,
    table: &TableRef,
    request: &PreviewRequest,
) -> Result<DataPreview> {
    let schema = table
        .schema
        .as_deref()
        .context("postgres preview requires schema")?;
    let columns = postgres_column_names(pool, table).await?;
    let select_list = casted_select_list(&columns);
    let filters = build_filter_clause(&request.filters, &columns, PreviewDialect::Postgres)?;
    let order_clause = build_order_clause(request.sort.as_ref());
    let query = format!(
        "SELECT {} FROM {}.{}{}{} LIMIT {} OFFSET {}",
        select_list,
        quote_ident(schema),
        quote_ident(&table.name),
        filters.sql,
        order_clause,
        PAGE_SIZE + 1,
        request.page * PAGE_SIZE,
    );
    let mut query = sqlx::query(&query);
    for binding in filters.bindings {
        query = query.bind(binding);
    }
    let rows = query.fetch_all(pool).await?;
    preview_from_pg_rows(columns, rows, request.page)
}

async fn load_sqlite_preview(
    pool: &SqlitePool,
    table: &TableRef,
    request: &PreviewRequest,
) -> Result<DataPreview> {
    let columns = sqlite_column_names(pool, table).await?;
    let select_list = casted_select_list(&columns);
    let filters = build_filter_clause(&request.filters, &columns, PreviewDialect::Sqlite)?;
    let order_clause = build_order_clause(request.sort.as_ref());
    let query = format!(
        "SELECT {} FROM {}{}{} LIMIT {} OFFSET {}",
        select_list,
        quote_ident(&table.name),
        filters.sql,
        order_clause,
        PAGE_SIZE + 1,
        request.page * PAGE_SIZE,
    );
    let mut query = sqlx::query(&query);
    for binding in filters.bindings {
        query = query.bind(binding);
    }
    let rows = query.fetch_all(pool).await?;
    preview_from_sqlite_rows(columns, rows, request.page)
}

async fn postgres_column_names(pool: &PgPool, table: &TableRef) -> Result<Vec<String>> {
    let schema = table
        .schema
        .as_deref()
        .context("postgres preview requires schema")?;
    let rows = sqlx::query(
        "SELECT column_name FROM information_schema.columns
         WHERE table_schema = $1 AND table_name = $2
         ORDER BY ordinal_position",
    )
    .bind(schema)
    .bind(&table.name)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|row| row.get("column_name")).collect())
}

async fn sqlite_column_names(pool: &SqlitePool, table: &TableRef) -> Result<Vec<String>> {
    let rows = sqlx::query(&format!("PRAGMA table_info({})", quote_ident(&table.name)))
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|row| row.get("name")).collect())
}

fn casted_select_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|name| {
            let ident = quote_ident(name);
            format!("CAST({ident} AS TEXT) AS {ident}")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn build_order_clause(sort: Option<&SortState>) -> String {
    match sort {
        Some(sort) => format!(
            " ORDER BY {} {}",
            quote_ident(&sort.column_name),
            if sort.descending { "DESC" } else { "ASC" }
        ),
        None => String::new(),
    }
}

fn build_filter_clause(
    filters: &[PreviewFilter],
    columns: &[String],
    dialect: PreviewDialect,
) -> Result<FilterClause> {
    let allowed_columns = columns.iter().cloned().collect::<HashSet<_>>();
    let mut clauses = Vec::new();
    let mut bindings = Vec::new();

    for filter in filters {
        clauses.push(build_filter_condition(
            filter,
            &allowed_columns,
            dialect,
            &mut bindings,
        )?);
    }

    if clauses.is_empty() {
        return Ok(FilterClause::default());
    }

    Ok(FilterClause {
        sql: format!(" WHERE {}", clauses.join(" AND ")),
        bindings,
    })
}

fn build_filter_condition(
    filter: &PreviewFilter,
    allowed_columns: &HashSet<String>,
    dialect: PreviewDialect,
    bindings: &mut Vec<String>,
) -> Result<String> {
    if !allowed_columns.contains(&filter.column_name) {
        return Err(anyhow!("unknown filter column '{}'", filter.column_name));
    }

    let ident = quote_ident(&filter.column_name);
    Ok(match filter.operator {
        FilterOperator::Equals => {
            let placeholder = push_binding(dialect, bindings, required_filter_value(filter)?);
            format!("CAST({ident} AS TEXT) = {placeholder}")
        }
        FilterOperator::NotEquals => {
            let placeholder = push_binding(dialect, bindings, required_filter_value(filter)?);
            format!("CAST({ident} AS TEXT) <> {placeholder}")
        }
        FilterOperator::Contains => {
            let placeholder = push_binding(
                dialect,
                bindings,
                format!("%{}%", required_filter_value(filter)?),
            );
            format!("LOWER(CAST({ident} AS TEXT)) LIKE LOWER({placeholder})")
        }
        FilterOperator::StartsWith => {
            let placeholder = push_binding(
                dialect,
                bindings,
                format!("{}%", required_filter_value(filter)?),
            );
            format!("LOWER(CAST({ident} AS TEXT)) LIKE LOWER({placeholder})")
        }
        FilterOperator::IsNull => format!("{ident} IS NULL"),
        FilterOperator::IsNotNull => format!("{ident} IS NOT NULL"),
    })
}

fn required_filter_value(filter: &PreviewFilter) -> Result<String> {
    filter
        .value
        .clone()
        .context("filter operator requires a value")
}

fn push_binding(dialect: PreviewDialect, bindings: &mut Vec<String>, value: String) -> String {
    bindings.push(value);
    match dialect {
        PreviewDialect::Postgres => format!("${}", bindings.len()),
        PreviewDialect::Sqlite => "?".into(),
    }
}

fn preview_from_pg_rows(
    columns: Vec<String>,
    rows: Vec<PgRow>,
    page: usize,
) -> Result<DataPreview> {
    let has_more = rows.len() > PAGE_SIZE;
    let mut rendered_rows = Vec::new();

    for row in rows.into_iter().take(PAGE_SIZE) {
        let mut values = Vec::new();
        for index in 0..columns.len() {
            let cell = row
                .try_get::<Option<String>, _>(index)
                .ok()
                .flatten()
                .unwrap_or_else(|| "NULL".into());
            values.push(cell);
        }
        rendered_rows.push(values);
    }

    Ok(DataPreview {
        columns,
        rows: rendered_rows,
        page,
        has_more,
    })
}

fn preview_from_sqlite_rows(
    columns: Vec<String>,
    rows: Vec<SqliteRow>,
    page: usize,
) -> Result<DataPreview> {
    let has_more = rows.len() > PAGE_SIZE;
    let mut rendered_rows = Vec::new();

    for row in rows.into_iter().take(PAGE_SIZE) {
        let mut values = Vec::new();
        for index in 0..columns.len() {
            let cell = row
                .try_get::<Option<String>, _>(index)
                .ok()
                .flatten()
                .unwrap_or_else(|| "NULL".into());
            values.push(cell);
        }
        rendered_rows.push(values);
    }

    Ok(DataPreview {
        columns,
        rows: rendered_rows,
        page,
        has_more,
    })
}

fn quote_ident(input: &str) -> String {
    format!("\"{}\"", input.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_table(name: &str) -> TableRef {
        TableRef {
            schema: None,
            name: name.into(),
        }
    }

    #[test]
    fn collect_key_columns_keeps_source_order_and_dedupes() {
        let columns = vec![
            ColumnMeta {
                name: "id".into(),
                data_type: "INTEGER".into(),
                nullable: false,
                default_value: None,
                is_primary_key: true,
            },
            ColumnMeta {
                name: "owner_id".into(),
                data_type: "INTEGER".into(),
                nullable: false,
                default_value: None,
                is_primary_key: false,
            },
            ColumnMeta {
                name: "name".into(),
                data_type: "TEXT".into(),
                nullable: false,
                default_value: None,
                is_primary_key: false,
            },
        ];

        let key_columns = collect_key_columns(
            &columns,
            &["owner_id".into(), "owner_id".into(), "missing".into()],
        );

        assert_eq!(key_columns, vec!["id", "owner_id"]);
    }

    #[test]
    fn relation_node_specs_group_neighbors_by_role() {
        let detail = TableDetail {
            table: sample_table("tasks"),
            columns: vec![],
            foreign_keys: vec![
                ForeignKeyMeta {
                    from_column: "project_id".into(),
                    to_table: sample_table("projects"),
                    to_column: "id".into(),
                    direction: RelationshipDirection::Outgoing,
                },
                ForeignKeyMeta {
                    from_column: "project_id_backup".into(),
                    to_table: sample_table("projects"),
                    to_column: "id".into(),
                    direction: RelationshipDirection::Outgoing,
                },
                ForeignKeyMeta {
                    from_column: "task_id".into(),
                    to_table: sample_table("comments"),
                    to_column: "id".into(),
                    direction: RelationshipDirection::Incoming,
                },
            ],
            indexes: vec![],
        };

        let specs = relation_node_specs(&detail);
        let outgoing = specs
            .iter()
            .find(|spec| spec.role == RelationNodeRole::Outgoing)
            .unwrap();
        let incoming = specs
            .iter()
            .find(|spec| spec.role == RelationNodeRole::Incoming)
            .unwrap();
        let center = specs
            .iter()
            .find(|spec| spec.role == RelationNodeRole::Center)
            .unwrap();

        assert_eq!(outgoing.table.name, "projects");
        assert_eq!(outgoing.related_columns, vec!["id", "id"]);
        assert_eq!(incoming.table.name, "comments");
        assert_eq!(incoming.related_columns, vec!["task_id"]);
        assert_eq!(
            center.related_columns,
            vec!["project_id", "project_id_backup", "id"]
        );
    }

    #[test]
    fn postgres_filter_clause_uses_numbered_placeholders() {
        let filters = vec![
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
            PreviewFilter {
                column_name: "assignee_id".into(),
                operator: FilterOperator::IsNull,
                value: None,
            },
        ];
        let clause = build_filter_clause(
            &filters,
            &["status".into(), "title".into(), "assignee_id".into()],
            PreviewDialect::Postgres,
        )
        .unwrap();

        assert_eq!(
            clause.sql,
            " WHERE CAST(\"status\" AS TEXT) = $1 AND LOWER(CAST(\"title\" AS TEXT)) LIKE LOWER($2) AND \"assignee_id\" IS NULL"
        );
        assert_eq!(clause.bindings, vec!["todo", "%page%"]);
    }

    #[test]
    fn sqlite_filter_clause_uses_qmark_placeholders_and_bind_order() {
        let filters = vec![
            PreviewFilter {
                column_name: "title".into(),
                operator: FilterOperator::StartsWith,
                value: Some("Render".into()),
            },
            PreviewFilter {
                column_name: "status".into(),
                operator: FilterOperator::NotEquals,
                value: Some("done".into()),
            },
        ];
        let clause = build_filter_clause(
            &filters,
            &["title".into(), "status".into()],
            PreviewDialect::Sqlite,
        )
        .unwrap();

        assert_eq!(
            clause.sql,
            " WHERE LOWER(CAST(\"title\" AS TEXT)) LIKE LOWER(?) AND CAST(\"status\" AS TEXT) <> ?"
        );
        assert_eq!(clause.bindings, vec!["Render%", "done"]);
    }

    #[test]
    fn build_filter_clause_rejects_unknown_columns() {
        let err = build_filter_clause(
            &[PreviewFilter {
                column_name: "missing".into(),
                operator: FilterOperator::Equals,
                value: Some("todo".into()),
            }],
            &["status".into()],
            PreviewDialect::Sqlite,
        )
        .unwrap_err();

        assert!(err.to_string().contains("unknown filter column 'missing'"));
    }

    #[tokio::test]
    async fn sqlite_filtered_preview_uses_sample_schema() {
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

        let todo_preview = session
            .load_preview(
                &sample_table("tasks"),
                &PreviewRequest {
                    sort: Some(SortState {
                        column_name: "title".into(),
                        descending: false,
                    }),
                    filters: vec![PreviewFilter {
                        column_name: "status".into(),
                        operator: FilterOperator::Equals,
                        value: Some("todo".into()),
                    }],
                    page: 0,
                },
            )
            .await
            .unwrap();
        let title_index = todo_preview
            .columns
            .iter()
            .position(|name| name == "title")
            .unwrap();
        let status_index = todo_preview
            .columns
            .iter()
            .position(|name| name == "status")
            .unwrap();

        assert_eq!(todo_preview.rows.len(), 2);
        assert_eq!(
            todo_preview.rows[0][title_index],
            "Add sample data preview paging"
        );
        assert_eq!(
            todo_preview.rows[1][title_index],
            "Render relationship panel"
        );
        assert!(
            todo_preview
                .rows
                .iter()
                .all(|row| row[status_index] == "todo")
        );

        let null_preview = session
            .load_preview(
                &sample_table("tasks"),
                &PreviewRequest {
                    sort: None,
                    filters: vec![PreviewFilter {
                        column_name: "assignee_id".into(),
                        operator: FilterOperator::IsNull,
                        value: None,
                    }],
                    page: 0,
                },
            )
            .await
            .unwrap();
        let assignee_index = null_preview
            .columns
            .iter()
            .position(|name| name == "assignee_id")
            .unwrap();

        assert_eq!(null_preview.rows.len(), 1);
        assert_eq!(null_preview.rows[0][assignee_index], "NULL");
    }

    #[tokio::test]
    async fn sqlite_relation_graph_uses_sample_schema() {
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

        let graph = session
            .load_relation_graph(&sample_table("tasks"))
            .await
            .unwrap();

        assert_eq!(graph.center.name, "tasks");
        assert!(
            graph
                .nodes
                .iter()
                .any(|node| node.role == RelationNodeRole::Center
                    && node.key_columns == vec!["id", "project_id", "assignee_id"])
        );
        assert!(
            graph.nodes.iter().any(
                |node| node.role == RelationNodeRole::Incoming && node.table.name == "comments"
            )
        );
        assert!(
            graph.nodes.iter().any(
                |node| node.role == RelationNodeRole::Outgoing && node.table.name == "projects"
            )
        );
        assert!(
            graph
                .nodes
                .iter()
                .any(|node| node.role == RelationNodeRole::Outgoing && node.table.name == "users")
        );
        assert!(
            graph
                .edges
                .iter()
                .any(|edge| edge.source_table.name == "tasks"
                    && edge.target_table.name == "projects")
        );
        assert!(
            graph
                .edges
                .iter()
                .any(|edge| edge.source_table.name == "comments"
                    && edge.target_table.name == "tasks")
        );
    }
}
