use anyhow::Result;
use sqlx::{Row, SqlitePool};

use crate::db::{
    ColumnMeta, ExportFormat, ExportRequest, ExportScope, ExportSummary, ForeignKeyMeta, IndexMeta,
    InvestigationState, PAGE_SIZE, RelationGraph, RelationNode, RelationNodeRole,
    RelationshipDirection, TableDetail, TableRef,
    export::{
        CsvExportWriter, EXPORT_BATCH_SIZE, ExportRowWriter, create_json_writer, write_sqlite_row,
    },
    query::{
        PreviewDialect, QueryPlan, build_filter_clause, build_order_clause, casted_select_list,
        preview_from_sqlite_rows, quote_ident,
    },
    relations::{
        collect_key_columns, relation_edges, relation_graph, relation_node_specs,
        sort_relation_nodes,
    },
};

pub(crate) async fn load_sqlite_columns(
    pool: &SqlitePool,
    table: &TableRef,
) -> Result<Vec<ColumnMeta>> {
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

pub(crate) async fn load_sqlite_detail(pool: &SqlitePool, table: &TableRef) -> Result<TableDetail> {
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

pub(crate) async fn load_sqlite_relation_graph(
    pool: &SqlitePool,
    table: &TableRef,
) -> Result<RelationGraph> {
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
    Ok(relation_graph(
        detail.table.clone(),
        nodes,
        relation_edges(&detail),
    ))
}

pub(crate) async fn load_sqlite_preview(
    pool: &SqlitePool,
    state: &InvestigationState,
) -> Result<crate::db::DataPreview> {
    let plan = build_sqlite_query_plan(pool, state).await?;
    let query = plan.paged_sql(PAGE_SIZE + 1, state.page * PAGE_SIZE);
    let mut query = sqlx::query(&query);
    for binding in plan.bindings {
        query = query.bind(binding);
    }
    let rows = query.fetch_all(pool).await?;
    preview_from_sqlite_rows(plan.columns, rows, state.page)
}

pub(crate) async fn export_sqlite_data(
    pool: &SqlitePool,
    state: &InvestigationState,
    request: &ExportRequest,
) -> Result<ExportSummary> {
    let plan = build_sqlite_query_plan(pool, state).await?;
    let rows_written = match request.format {
        ExportFormat::Csv => {
            let mut writer = CsvExportWriter::new(&request.path, &plan.columns)?;
            match request.scope {
                ExportScope::VisiblePage => {
                    export_sqlite_page(pool, &plan, state.page, PAGE_SIZE, &mut writer).await?;
                }
                ExportScope::AllMatchingRows => {
                    export_sqlite_all_rows(pool, &plan, &mut writer).await?;
                }
            }
            writer.finish()?
        }
        ExportFormat::Json => {
            let mut writer = create_json_writer(&request.path)?;
            match request.scope {
                ExportScope::VisiblePage => {
                    export_sqlite_page(pool, &plan, state.page, PAGE_SIZE, &mut writer).await?;
                }
                ExportScope::AllMatchingRows => {
                    export_sqlite_all_rows(pool, &plan, &mut writer).await?;
                }
            }
            writer.finish()?
        }
    };

    Ok(ExportSummary {
        rows_written,
        format: request.format,
        scope: request.scope,
        path: request.path.clone(),
    })
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

async fn build_sqlite_query_plan(
    pool: &SqlitePool,
    state: &InvestigationState,
) -> Result<QueryPlan> {
    let table = state.table();
    let columns = sqlite_column_names(pool, table).await?;
    let select_list = casted_select_list(&columns);
    let filters = build_filter_clause(&state.filters, &columns, PreviewDialect::Sqlite)?;
    let order_clause = build_order_clause(state.sort.as_ref());
    Ok(QueryPlan {
        columns,
        sql: format!(
            "SELECT {} FROM {}{}{}",
            select_list,
            quote_ident(&table.name),
            filters.sql,
            order_clause,
        ),
        bindings: filters.bindings,
    })
}

async fn export_sqlite_page(
    pool: &SqlitePool,
    plan: &QueryPlan,
    page: usize,
    page_size: usize,
    writer: &mut impl ExportRowWriter,
) -> Result<usize> {
    let query = plan.paged_sql(page_size, page * page_size);
    let mut query = sqlx::query(&query);
    for binding in &plan.bindings {
        query = query.bind(binding);
    }

    let rows = query.fetch_all(pool).await?;
    let mut rows_written = 0;
    for row in rows {
        rows_written += write_sqlite_row(writer, &row, &plan.columns)?;
    }

    Ok(rows_written)
}

async fn export_sqlite_all_rows(
    pool: &SqlitePool,
    plan: &QueryPlan,
    writer: &mut impl ExportRowWriter,
) -> Result<usize> {
    let mut page = 0;
    let mut rows_written = 0;

    loop {
        let written = export_sqlite_page(pool, plan, page, EXPORT_BATCH_SIZE, writer).await?;
        rows_written += written;
        if written < EXPORT_BATCH_SIZE {
            break;
        }
        page += 1;
    }

    Ok(rows_written)
}

async fn sqlite_column_names(pool: &SqlitePool, table: &TableRef) -> Result<Vec<String>> {
    let rows = sqlx::query(&format!("PRAGMA table_info({})", quote_ident(&table.name)))
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|row| row.get("name")).collect())
}
