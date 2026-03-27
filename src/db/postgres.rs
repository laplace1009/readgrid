use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

use crate::db::{
    ColumnMeta, ExportFormat, ExportRequest, ExportScope, ExportSummary, ForeignKeyMeta, IndexMeta,
    InvestigationState, PAGE_SIZE, RelationGraph, RelationNode, RelationNodeRole,
    RelationshipDirection, TableDetail, TableRef,
    export::{
        CsvExportWriter, EXPORT_BATCH_SIZE, ExportRowWriter, create_json_writer, write_pg_row,
    },
    query::{
        PreviewDialect, QueryPlan, build_filter_clause, build_order_clause, casted_select_list,
        preview_from_pg_rows, quote_ident,
    },
    relations::{
        collect_key_columns, relation_edges, relation_graph, relation_node_specs,
        sort_relation_nodes,
    },
};

pub(crate) async fn load_postgres_columns(
    pool: &PgPool,
    table: &TableRef,
) -> Result<Vec<ColumnMeta>> {
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

pub(crate) async fn load_postgres_detail(pool: &PgPool, table: &TableRef) -> Result<TableDetail> {
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

pub(crate) async fn load_postgres_relation_graph(
    pool: &PgPool,
    table: &TableRef,
) -> Result<RelationGraph> {
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
    Ok(relation_graph(
        detail.table.clone(),
        nodes,
        relation_edges(&detail),
    ))
}

pub(crate) async fn load_postgres_preview(
    pool: &PgPool,
    state: &InvestigationState,
) -> Result<crate::db::DataPreview> {
    let plan = build_postgres_query_plan(pool, state).await?;
    let query = plan.paged_sql(PAGE_SIZE + 1, state.page * PAGE_SIZE);
    let mut query = sqlx::query(&query);
    for binding in plan.bindings {
        query = query.bind(binding);
    }
    let rows = query.fetch_all(pool).await?;
    preview_from_pg_rows(plan.columns, rows, state.page)
}

pub(crate) async fn export_postgres_data(
    pool: &PgPool,
    state: &InvestigationState,
    request: &ExportRequest,
) -> Result<ExportSummary> {
    let plan = build_postgres_query_plan(pool, state).await?;
    let rows_written = match request.format {
        ExportFormat::Csv => {
            let mut writer = CsvExportWriter::new(&request.path, &plan.columns)?;
            match request.scope {
                ExportScope::VisiblePage => {
                    export_postgres_page(pool, &plan, state.page, PAGE_SIZE, &mut writer).await?;
                }
                ExportScope::AllMatchingRows => {
                    export_postgres_all_rows(pool, &plan, &mut writer).await?;
                }
            }
            writer.finish()?
        }
        ExportFormat::Json => {
            let mut writer = create_json_writer(&request.path)?;
            match request.scope {
                ExportScope::VisiblePage => {
                    export_postgres_page(pool, &plan, state.page, PAGE_SIZE, &mut writer).await?;
                }
                ExportScope::AllMatchingRows => {
                    export_postgres_all_rows(pool, &plan, &mut writer).await?;
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

async fn build_postgres_query_plan(pool: &PgPool, state: &InvestigationState) -> Result<QueryPlan> {
    let table = state.table();
    let schema = table
        .schema
        .as_deref()
        .context("postgres preview requires schema")?;
    let columns = postgres_column_names(pool, table).await?;
    let select_list = casted_select_list(&columns);
    let filters = build_filter_clause(&state.filters, &columns, PreviewDialect::Postgres)?;
    let order_clause = build_order_clause(state.sort.as_ref());
    Ok(QueryPlan {
        columns,
        sql: format!(
            "SELECT {} FROM {}.{}{}{}",
            select_list,
            quote_ident(schema),
            quote_ident(&table.name),
            filters.sql,
            order_clause,
        ),
        bindings: filters.bindings,
    })
}

async fn export_postgres_page(
    pool: &PgPool,
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
        rows_written += write_pg_row(writer, &row, &plan.columns)?;
    }

    Ok(rows_written)
}

async fn export_postgres_all_rows(
    pool: &PgPool,
    plan: &QueryPlan,
    writer: &mut impl ExportRowWriter,
) -> Result<usize> {
    let mut page = 0;
    let mut rows_written = 0;

    loop {
        let written = export_postgres_page(pool, plan, page, EXPORT_BATCH_SIZE, writer).await?;
        rows_written += written;
        if written < EXPORT_BATCH_SIZE {
            break;
        }
        page += 1;
    }

    Ok(rows_written)
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
