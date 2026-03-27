use anyhow::{Context, Result};
use sqlx::{PgPool, Row, SqlitePool};

use crate::db::{
    ConnectionProfile, DataPreview, DatabaseKind, ExportRequest, ExportSummary, InvestigationState,
    RelationGraph, TableDetail, TableRef,
    postgres::{
        export_postgres_data, load_postgres_detail, load_postgres_preview,
        load_postgres_relation_graph,
    },
    sqlite::{
        export_sqlite_data, load_sqlite_detail, load_sqlite_preview, load_sqlite_relation_graph,
    },
};

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

    pub async fn load_preview(&self, state: &InvestigationState) -> Result<DataPreview> {
        match self {
            Self::Postgres(pool) => load_postgres_preview(pool, state).await,
            Self::Sqlite(pool) => load_sqlite_preview(pool, state).await,
        }
    }

    pub async fn export(
        &self,
        state: &InvestigationState,
        request: &ExportRequest,
    ) -> Result<ExportSummary> {
        match self {
            Self::Postgres(pool) => export_postgres_data(pool, state, request).await,
            Self::Sqlite(pool) => export_sqlite_data(pool, state, request).await,
        }
    }

    pub async fn load_relation_graph(&self, table: &TableRef) -> Result<RelationGraph> {
        match self {
            Self::Postgres(pool) => load_postgres_relation_graph(pool, table).await,
            Self::Sqlite(pool) => load_sqlite_relation_graph(pool, table).await,
        }
    }
}
