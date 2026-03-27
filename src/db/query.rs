use std::collections::HashSet;

use anyhow::{Context, Result, anyhow};
use sqlx::{Row, postgres::PgRow, sqlite::SqliteRow};

use crate::db::{
    DataPreview, FilterOperator, PAGE_SIZE, PreviewCell, PreviewFilter, PreviewRow, SortState,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreviewDialect {
    Postgres,
    Sqlite,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FilterClause {
    pub(crate) sql: String,
    pub(crate) bindings: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct QueryPlan {
    pub(crate) columns: Vec<String>,
    pub(crate) sql: String,
    pub(crate) bindings: Vec<String>,
}

impl QueryPlan {
    pub(crate) fn paged_sql(&self, limit: usize, offset: usize) -> String {
        format!("{} LIMIT {} OFFSET {}", self.sql, limit, offset)
    }
}

pub(crate) fn casted_select_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|name| {
            let ident = quote_ident(name);
            format!("CAST({ident} AS TEXT) AS {ident}")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn build_order_clause(sort: Option<&SortState>) -> String {
    match sort {
        Some(sort) => format!(
            " ORDER BY {} {}",
            quote_ident(&sort.column_name),
            if sort.descending { "DESC" } else { "ASC" }
        ),
        None => String::new(),
    }
}

pub(crate) fn build_filter_clause(
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

pub(crate) fn preview_from_pg_rows(
    columns: Vec<String>,
    rows: Vec<PgRow>,
    page: usize,
) -> Result<DataPreview> {
    let has_more = rows.len() > PAGE_SIZE;
    let mut rendered_rows = Vec::new();

    for row in rows.into_iter().take(PAGE_SIZE) {
        let mut cells = Vec::new();
        for index in 0..columns.len() {
            let raw_value = row.try_get::<Option<String>, _>(index).ok().flatten();
            cells.push(PreviewCell::from_raw(raw_value));
        }
        rendered_rows.push(PreviewRow { cells });
    }

    Ok(DataPreview {
        columns,
        rows: rendered_rows,
        page,
        has_more,
    })
}

pub(crate) fn preview_from_sqlite_rows(
    columns: Vec<String>,
    rows: Vec<SqliteRow>,
    page: usize,
) -> Result<DataPreview> {
    let has_more = rows.len() > PAGE_SIZE;
    let mut rendered_rows = Vec::new();

    for row in rows.into_iter().take(PAGE_SIZE) {
        let mut cells = Vec::new();
        for index in 0..columns.len() {
            let raw_value = row.try_get::<Option<String>, _>(index).ok().flatten();
            cells.push(PreviewCell::from_raw(raw_value));
        }
        rendered_rows.push(PreviewRow { cells });
    }

    Ok(DataPreview {
        columns,
        rows: rendered_rows,
        page,
        has_more,
    })
}

pub(crate) fn raw_pg_row(row: &PgRow, column_count: usize) -> Vec<Option<String>> {
    (0..column_count)
        .map(|index| row.try_get::<Option<String>, _>(index).ok().flatten())
        .collect()
}

pub(crate) fn raw_sqlite_row(row: &SqliteRow, column_count: usize) -> Vec<Option<String>> {
    (0..column_count)
        .map(|index| row.try_get::<Option<String>, _>(index).ok().flatten())
        .collect()
}

pub(crate) fn quote_ident(input: &str) -> String {
    format!("\"{}\"", input.replace('"', "\"\""))
}
