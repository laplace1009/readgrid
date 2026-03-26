use std::{fmt, path::PathBuf};

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

#[derive(Debug, Clone)]
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
    pub to_table: String,
    pub to_column: String,
    pub direction: RelationshipDirection,
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

#[derive(Debug, Clone)]
pub struct DataPreview {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub page: usize,
    pub has_more: bool,
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
        sort: Option<&SortState>,
        page: usize,
    ) -> Result<DataPreview> {
        match self {
            Self::Postgres(pool) => load_postgres_preview(pool, table, sort, page).await,
            Self::Sqlite(pool) => load_sqlite_preview(pool, table, sort, page).await,
        }
    }
}

async fn load_postgres_detail(pool: &PgPool, table: &TableRef) -> Result<TableDetail> {
    let schema = table
        .schema
        .as_deref()
        .context("postgres detail requires schema")?;
    let columns = sqlx::query(
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
    .collect();

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
        to_table: format!(
            "{}.{}",
            row.get::<String, _>("target_schema"),
            row.get::<String, _>("target_table")
        ),
        to_column: row.get("target_column"),
        direction: RelationshipDirection::Outgoing,
    }));
    foreign_keys.extend(incoming.into_iter().map(|row| ForeignKeyMeta {
        from_column: row.get("source_column"),
        to_table: format!(
            "{}.{}",
            row.get::<String, _>("source_schema"),
            row.get::<String, _>("source_table")
        ),
        to_column: row.get("target_column"),
        direction: RelationshipDirection::Incoming,
    }));

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

async fn load_sqlite_detail(pool: &SqlitePool, table: &TableRef) -> Result<TableDetail> {
    let columns = sqlx::query(&format!("PRAGMA table_info({})", quote_ident(&table.name)))
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
        .collect::<Vec<_>>();

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
        to_table: row.get("table"),
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
                    to_table: other_name.clone(),
                    to_column: edge.get("to"),
                    direction: RelationshipDirection::Incoming,
                });
            }
        }
    }

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

async fn load_postgres_preview(
    pool: &PgPool,
    table: &TableRef,
    sort: Option<&SortState>,
    page: usize,
) -> Result<DataPreview> {
    let schema = table
        .schema
        .as_deref()
        .context("postgres preview requires schema")?;
    let columns = postgres_column_names(pool, table).await?;
    let select_list = casted_select_list(&columns, true);
    let order_clause = build_order_clause(sort, true);
    let query = format!(
        "SELECT {} FROM {}.{}{} LIMIT {} OFFSET {}",
        select_list,
        quote_ident(schema),
        quote_ident(&table.name),
        order_clause,
        PAGE_SIZE + 1,
        page * PAGE_SIZE,
    );
    let rows = sqlx::query(&query).fetch_all(pool).await?;
    preview_from_pg_rows(columns, rows, page)
}

async fn load_sqlite_preview(
    pool: &SqlitePool,
    table: &TableRef,
    sort: Option<&SortState>,
    page: usize,
) -> Result<DataPreview> {
    let columns = sqlite_column_names(pool, table).await?;
    let select_list = casted_select_list(&columns, false);
    let order_clause = build_order_clause(sort, false);
    let query = format!(
        "SELECT {} FROM {}{} LIMIT {} OFFSET {}",
        select_list,
        quote_ident(&table.name),
        order_clause,
        PAGE_SIZE + 1,
        page * PAGE_SIZE,
    );
    let rows = sqlx::query(&query).fetch_all(pool).await?;
    preview_from_sqlite_rows(columns, rows, page)
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

fn casted_select_list(columns: &[String], postgres: bool) -> String {
    columns
        .iter()
        .map(|name| {
            let ident = quote_ident(name);
            if postgres {
                format!("CAST({ident} AS TEXT) AS {ident}")
            } else {
                format!("CAST({ident} AS TEXT) AS {ident}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn build_order_clause(sort: Option<&SortState>, _postgres: bool) -> String {
    match sort {
        Some(sort) => format!(
            " ORDER BY {} {}",
            quote_ident(&sort.column_name),
            if sort.descending { "DESC" } else { "ASC" }
        ),
        None => String::new(),
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
