use std::{
    fs,
    path::PathBuf,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use sqlx::{Executor, SqlitePool, sqlite::SqliteConnectOptions};

use super::{
    ColumnMeta, ConnectionProfile, DataPreview, DatabaseKind, ExportFormat, ExportRequest,
    ExportScope, FilterOperator, ForeignKeyMeta, InvestigationSource, InvestigationState,
    PAGE_SIZE, PreviewCell, PreviewFilter, PreviewRow, RelationNodeRole, RelationshipDirection,
    Session, SortState, TableDetail, TableRef, build_drill_through_actions,
    query::{PreviewDialect, build_filter_clause},
    relations::{collect_key_columns, relation_node_specs},
    write_preview_csv, write_preview_json,
};

fn sample_table(name: &str) -> TableRef {
    TableRef {
        schema: None,
        name: name.into(),
    }
}

fn sample_investigation(
    table_name: &str,
    sort: Option<SortState>,
    filters: Vec<PreviewFilter>,
    page: usize,
) -> InvestigationState {
    InvestigationState {
        source: InvestigationSource::Table(sample_table(table_name)),
        sort,
        filters,
        page,
    }
}

fn temp_csv_path(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "readgrid_{name}_{}_{}.csv",
        std::process::id(),
        unique
    ))
}

fn temp_export_path(name: &str, extension: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "readgrid_{name}_{}_{}.{}",
        std::process::id(),
        unique,
        extension
    ))
}

fn temp_sqlite_path(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "readgrid_{name}_{}_{}.db",
        std::process::id(),
        unique
    ))
}

async fn large_sqlite_session(row_count: usize) -> (Session, PathBuf) {
    let path = temp_sqlite_path("large_export");
    let options = SqliteConnectOptions::from_str(path.to_str().unwrap())
        .unwrap()
        .create_if_missing(true);
    let pool = SqlitePool::connect_with(options).await.unwrap();
    pool.execute(
        "CREATE TABLE tasks (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            status TEXT NOT NULL
        )",
    )
    .await
    .unwrap();
    for id in 1..=row_count {
        sqlx::query("INSERT INTO tasks (id, title, status) VALUES (?, ?, ?)")
            .bind(id as i64)
            .bind(format!("Task {id:03}"))
            .bind(if id % 3 == 0 { "done" } else { "todo" })
            .execute(&pool)
            .await
            .unwrap();
    }
    (Session::Sqlite(pool), path)
}

fn sample_csv_preview(rows: Vec<Vec<Option<&str>>>) -> DataPreview {
    DataPreview {
        columns: vec!["id".into(), "title".into(), "notes".into()],
        rows: rows
            .into_iter()
            .map(|cells| PreviewRow {
                cells: cells
                    .into_iter()
                    .map(|raw| PreviewCell::from_raw(raw.map(str::to_string)))
                    .collect(),
            })
            .collect(),
        page: 0,
        has_more: false,
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

#[test]
fn preview_cell_from_raw_preserves_null_state() {
    let missing = PreviewCell::from_raw(None);
    let present = PreviewCell::from_raw(Some("literal".into()));

    assert_eq!(missing.display_value, "NULL");
    assert_eq!(missing.raw_value, None);
    assert_eq!(present.display_value, "literal");
    assert_eq!(present.raw_value.as_deref(), Some("literal"));
}

#[test]
fn write_preview_csv_writes_headers_and_rendered_nulls() {
    let path = temp_csv_path("rendered_nulls");
    let preview = sample_csv_preview(vec![vec![Some("1"), Some("alpha"), None]]);

    write_preview_csv(&preview, &path).unwrap();

    let mut reader = csv::Reader::from_path(&path).unwrap();
    let headers = reader.headers().unwrap().clone();
    let rows = reader
        .records()
        .map(|row| row.unwrap().iter().map(str::to_string).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    fs::remove_file(&path).ok();

    assert_eq!(
        headers.iter().collect::<Vec<_>>(),
        vec!["id", "title", "notes"]
    );
    assert_eq!(
        rows,
        vec![vec![
            String::from("1"),
            String::from("alpha"),
            String::from("NULL"),
        ]]
    );
}

#[test]
fn write_preview_csv_escapes_commas_quotes_and_newlines() {
    let path = temp_csv_path("escaping");
    let preview = sample_csv_preview(vec![vec![
        Some("1"),
        Some("comma,quote"),
        Some("needs \"quotes\", commas,\nand newlines"),
    ]]);

    write_preview_csv(&preview, &path).unwrap();

    let raw = fs::read_to_string(&path).unwrap();
    let mut reader = csv::Reader::from_path(&path).unwrap();
    let rows = reader
        .records()
        .map(|row| row.unwrap().iter().map(str::to_string).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    fs::remove_file(&path).ok();

    assert!(raw.contains("\"comma,quote\""));
    assert!(raw.contains("\"needs \"\"quotes\"\", commas,"));
    assert_eq!(
        rows,
        vec![vec![
            String::from("1"),
            String::from("comma,quote"),
            String::from("needs \"quotes\", commas,\nand newlines"),
        ]]
    );
}

#[test]
fn write_preview_csv_writes_header_only_for_empty_preview() {
    let path = temp_csv_path("header_only");
    let preview = sample_csv_preview(Vec::new());

    write_preview_csv(&preview, &path).unwrap();

    let mut reader = csv::Reader::from_path(&path).unwrap();
    let headers = reader.headers().unwrap().clone();
    let rows = reader
        .records()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    fs::remove_file(&path).ok();

    assert_eq!(
        headers.iter().collect::<Vec<_>>(),
        vec!["id", "title", "notes"]
    );
    assert!(rows.is_empty());
}

#[test]
fn write_preview_csv_creates_missing_parent_directories() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "readgrid_db_nested_{}_{}",
        std::process::id(),
        unique
    ));
    let path = dir.join("exports").join("preview.csv");
    let preview = sample_csv_preview(vec![vec![Some("1"), Some("alpha"), None]]);

    write_preview_csv(&preview, &path).unwrap();

    assert!(path.exists());
    let mut reader = csv::Reader::from_path(&path).unwrap();
    let rows = reader
        .records()
        .map(|row| row.unwrap().iter().map(str::to_string).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    std::fs::remove_dir_all(&dir).ok();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][2], "NULL");
}

#[test]
fn write_preview_json_preserves_column_order_and_nulls() {
    let path = temp_export_path("preview_json", "json");
    let preview = sample_csv_preview(vec![vec![Some("1"), Some("alpha"), None]]);

    write_preview_json(&preview, &path).unwrap();

    let raw = fs::read_to_string(&path).unwrap();
    let rows = serde_json::from_str::<Vec<serde_json::Value>>(&raw).unwrap();
    fs::remove_file(&path).ok();

    assert!(raw.contains(r#"[{"id":"1","title":"alpha","notes":null}]"#));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], serde_json::Value::String("1".into()));
    assert_eq!(rows[0]["notes"], serde_json::Value::Null);
}

#[test]
fn csv_and_json_exports_handle_nulls_differently() {
    let csv_path = temp_csv_path("null_compare");
    let json_path = temp_export_path("null_compare", "json");
    let preview = sample_csv_preview(vec![vec![Some("1"), Some("alpha"), None]]);

    write_preview_csv(&preview, &csv_path).unwrap();
    write_preview_json(&preview, &json_path).unwrap();

    let csv_raw = fs::read_to_string(&csv_path).unwrap();
    let json_rows =
        serde_json::from_str::<Vec<serde_json::Value>>(&fs::read_to_string(&json_path).unwrap())
            .unwrap();
    fs::remove_file(&csv_path).ok();
    fs::remove_file(&json_path).ok();

    assert!(csv_raw.contains("NULL"));
    assert_eq!(json_rows[0]["notes"], serde_json::Value::Null);
}

#[test]
fn drill_through_actions_cover_outgoing_incoming_and_null_values() {
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
                from_column: "assignee_id".into(),
                to_table: sample_table("users"),
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
    let preview = DataPreview {
        columns: vec!["id".into(), "project_id".into(), "assignee_id".into()],
        rows: vec![PreviewRow {
            cells: vec![
                PreviewCell::from_raw(Some("7".into())),
                PreviewCell::from_raw(Some("3".into())),
                PreviewCell::from_raw(None),
            ],
        }],
        page: 0,
        has_more: false,
    };

    let actions = build_drill_through_actions(&detail, &preview, 0);
    let projects = actions
        .iter()
        .find(|action| action.target_table.name == "projects")
        .unwrap();
    let comments = actions
        .iter()
        .find(|action| action.target_table.name == "comments")
        .unwrap();
    let users = actions
        .iter()
        .find(|action| action.target_table.name == "users")
        .unwrap();

    assert_eq!(projects.target_filter.as_ref().unwrap().column_name, "id");
    assert_eq!(
        projects.target_filter.as_ref().unwrap().value.as_deref(),
        Some("3")
    );
    assert_eq!(
        comments.target_filter.as_ref().unwrap().column_name,
        "task_id"
    );
    assert_eq!(
        comments.target_filter.as_ref().unwrap().value.as_deref(),
        Some("7")
    );
    assert!(!users.is_available());
    assert_eq!(
        users.unavailable_reason.as_deref(),
        Some("Selected row has NULL in assignee_id.")
    );
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
        .load_preview(&sample_investigation(
            "tasks",
            Some(SortState {
                column_name: "title".into(),
                descending: false,
            }),
            vec![PreviewFilter {
                column_name: "status".into(),
                operator: FilterOperator::Equals,
                value: Some("todo".into()),
            }],
            0,
        ))
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
        todo_preview.rows[0].cells[title_index].display_value,
        "Add sample data preview paging"
    );
    assert_eq!(
        todo_preview.rows[1].cells[title_index].display_value,
        "Render relationship panel"
    );
    assert!(
        todo_preview
            .rows
            .iter()
            .all(|row| row.cells[status_index].display_value == "todo")
    );

    let null_preview = session
        .load_preview(&sample_investigation(
            "tasks",
            None,
            vec![PreviewFilter {
                column_name: "assignee_id".into(),
                operator: FilterOperator::IsNull,
                value: None,
            }],
            0,
        ))
        .await
        .unwrap();
    let assignee_index = null_preview
        .columns
        .iter()
        .position(|name| name == "assignee_id")
        .unwrap();

    assert_eq!(null_preview.rows.len(), 1);
    assert_eq!(
        null_preview.rows[0].cells[assignee_index].display_value,
        "NULL"
    );
    assert_eq!(null_preview.rows[0].cells[assignee_index].raw_value, None);
}

#[tokio::test]
async fn sqlite_full_export_writes_all_matching_rows_across_pages() {
    let (session, db_path) = large_sqlite_session(PAGE_SIZE + 17).await;
    let export_path = temp_csv_path("all_matching_rows");
    let state = sample_investigation(
        "tasks",
        Some(SortState {
            column_name: "title".into(),
            descending: false,
        }),
        vec![PreviewFilter {
            column_name: "status".into(),
            operator: FilterOperator::Equals,
            value: Some("todo".into()),
        }],
        1,
    );

    let summary = session
        .export(
            &state,
            &ExportRequest {
                format: ExportFormat::Csv,
                scope: ExportScope::AllMatchingRows,
                path: export_path.clone(),
            },
        )
        .await
        .unwrap();

    let mut reader = csv::Reader::from_path(&export_path).unwrap();
    let headers = reader.headers().unwrap().clone();
    let rows = reader
        .records()
        .map(|row| row.unwrap().iter().map(str::to_string).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    fs::remove_file(&export_path).ok();
    fs::remove_file(&db_path).ok();

    assert_eq!(summary.format, ExportFormat::Csv);
    assert_eq!(summary.scope, ExportScope::AllMatchingRows);
    assert_eq!(summary.rows_written, 45);
    assert_eq!(
        headers.iter().collect::<Vec<_>>(),
        vec!["id", "title", "status"]
    );
    assert_eq!(rows.len(), 45);
    assert_eq!(rows.first().unwrap()[0], "1");
    assert_eq!(rows.last().unwrap()[0], "67");
    assert!(rows.iter().all(|row| row[2] == "todo"));
}

#[tokio::test]
async fn sqlite_full_json_export_writes_all_matching_rows_across_pages() {
    let (session, db_path) = large_sqlite_session(PAGE_SIZE + 17).await;
    let export_path = temp_export_path("all_matching_rows", "json");
    let state = sample_investigation(
        "tasks",
        Some(SortState {
            column_name: "title".into(),
            descending: false,
        }),
        vec![PreviewFilter {
            column_name: "status".into(),
            operator: FilterOperator::Equals,
            value: Some("todo".into()),
        }],
        1,
    );

    let summary = session
        .export(
            &state,
            &ExportRequest {
                format: ExportFormat::Json,
                scope: ExportScope::AllMatchingRows,
                path: export_path.clone(),
            },
        )
        .await
        .unwrap();

    let raw = fs::read_to_string(&export_path).unwrap();
    let rows = serde_json::from_str::<Vec<serde_json::Value>>(&raw).unwrap();
    fs::remove_file(&export_path).ok();
    fs::remove_file(&db_path).ok();

    assert_eq!(summary.format, ExportFormat::Json);
    assert_eq!(summary.scope, ExportScope::AllMatchingRows);
    assert_eq!(summary.rows_written, 45);
    assert_eq!(rows.len(), 45);
    let first_object = raw
        .strip_prefix('[')
        .and_then(|text| text.split("},{").next())
        .unwrap();
    let id_pos = first_object.find(r#""id":"#).unwrap();
    let title_pos = first_object.find(r#""title":"#).unwrap();
    let status_pos = first_object.find(r#""status":"#).unwrap();
    assert!(id_pos < title_pos);
    assert!(title_pos < status_pos);
    assert_eq!(
        rows.first().unwrap()["id"],
        serde_json::Value::String("1".into())
    );
    assert_eq!(
        rows.last().unwrap()["id"],
        serde_json::Value::String("67".into())
    );
    assert!(
        rows.iter()
            .all(|row| row["status"] == serde_json::Value::String("todo".into()))
    );
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
        graph
            .nodes
            .iter()
            .any(|node| node.role == RelationNodeRole::Incoming && node.table.name == "comments")
    );
    assert!(
        graph
            .nodes
            .iter()
            .any(|node| node.role == RelationNodeRole::Outgoing && node.table.name == "projects")
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
            .any(|edge| edge.source_table.name == "tasks" && edge.target_table.name == "projects")
    );
    assert!(
        graph
            .edges
            .iter()
            .any(|edge| edge.source_table.name == "comments" && edge.target_table.name == "tasks")
    );
}
