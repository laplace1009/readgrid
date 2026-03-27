use std::collections::{HashMap, HashSet};

use crate::db::{
    ColumnMeta, DataPreview, DrillThroughAction, FilterOperator, ForeignKeyMeta, PreviewFilter,
    RelationEdge, RelationGraph, RelationNode, RelationNodeRole, RelationshipDirection,
    TableDetail, TableRef,
};

#[derive(Debug, Clone)]
pub(crate) struct RelationNodeSpec {
    pub(crate) table: TableRef,
    pub(crate) related_columns: Vec<String>,
    pub(crate) role: RelationNodeRole,
}

pub fn build_drill_through_actions(
    detail: &TableDetail,
    preview: &DataPreview,
    row_index: usize,
) -> Vec<DrillThroughAction> {
    let row_available = preview.rows.get(row_index).is_some();
    let mut actions = detail
        .foreign_keys
        .iter()
        .cloned()
        .map(|relation| {
            let (target_filter, unavailable_reason) = if !row_available {
                (None, Some("No preview row is selected.".into()))
            } else {
                drill_through_filter_for_relation(preview, row_index, &relation)
            };

            DrillThroughAction {
                target_table: relation.to_table.clone(),
                relation,
                target_filter,
                unavailable_reason,
            }
        })
        .collect::<Vec<_>>();
    actions.sort_by_key(|action| {
        (
            drill_through_direction_rank(action.relation.direction),
            action.target_table.display_name(),
            action.relation.remote_column().to_string(),
            action.relation.local_column().to_string(),
        )
    });
    actions
}

fn drill_through_filter_for_relation(
    preview: &DataPreview,
    row_index: usize,
    relation: &ForeignKeyMeta,
) -> (Option<PreviewFilter>, Option<String>) {
    match preview.cell(row_index, relation.local_column()) {
        Some(cell) => match &cell.raw_value {
            Some(value) => (
                Some(PreviewFilter {
                    column_name: relation.remote_column().to_string(),
                    operator: FilterOperator::Equals,
                    value: Some(value.clone()),
                }),
                None,
            ),
            None => (
                None,
                Some(format!(
                    "Selected row has NULL in {}.",
                    relation.local_column()
                )),
            ),
        },
        None => (
            None,
            Some(format!(
                "Preview is missing column {}.",
                relation.local_column()
            )),
        ),
    }
}

fn drill_through_direction_rank(direction: RelationshipDirection) -> u8 {
    match direction {
        RelationshipDirection::Outgoing => 0,
        RelationshipDirection::Incoming => 1,
    }
}

pub(crate) fn relation_node_specs(detail: &TableDetail) -> Vec<RelationNodeSpec> {
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

pub(crate) fn relation_edges(detail: &TableDetail) -> Vec<RelationEdge> {
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

pub(crate) fn collect_key_columns(
    columns: &[ColumnMeta],
    related_columns: &[String],
) -> Vec<String> {
    let related = related_columns.iter().cloned().collect::<HashSet<_>>();
    let mut rendered = Vec::new();

    for column in columns {
        if column.is_primary_key || related.contains(&column.name) {
            rendered.push(column.name.clone());
        }
    }

    rendered
}

pub(crate) fn sort_relation_nodes(nodes: &mut [RelationNode]) {
    nodes.sort_by_key(|node| (relation_role_rank(node.role), node.table.display_name()));
}

fn relation_role_rank(role: RelationNodeRole) -> u8 {
    match role {
        RelationNodeRole::Incoming => 0,
        RelationNodeRole::Center => 1,
        RelationNodeRole::Outgoing => 2,
    }
}

pub(crate) fn relation_graph(
    center: TableRef,
    nodes: Vec<RelationNode>,
    edges: Vec<RelationEdge>,
) -> RelationGraph {
    RelationGraph {
        center,
        nodes,
        edges,
    }
}
