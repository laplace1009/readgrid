use std::{env, fs, path::PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::{
    app::{CliArgs, StartupView},
    db::ConnectionProfile,
};

#[derive(Debug, Clone, Deserialize)]
pub struct McpContext {
    #[serde(default)]
    pub profile: Option<ConnectionProfile>,
    #[serde(default)]
    pub target_bookmark: Option<String>,
    #[serde(default, alias = "preferred_schema")]
    pub target_schema: Option<String>,
    #[serde(default)]
    pub target_table: Option<String>,
    #[serde(default)]
    pub target_view: Option<StartupView>,
}

impl McpContext {
    pub fn load(args: &CliArgs) -> Result<Option<Self>> {
        if let Some(path) = &args.mcp_context_file {
            return read_context_file(path);
        }

        if let Ok(path) = env::var("READGRID_MCP_CONTEXT_FILE") {
            return read_context_file(&PathBuf::from(path));
        }

        if let Ok(raw) = env::var("READGRID_MCP_CONTEXT") {
            let context =
                serde_json::from_str(&raw).context("failed to parse READGRID_MCP_CONTEXT")?;
            return Ok(Some(context));
        }

        Ok(None)
    }
}

fn read_context_file(path: &PathBuf) -> Result<Option<McpContext>> {
    if !path.exists() {
        return Ok(None);
    }

    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let context = serde_json::from_str::<McpContext>(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(Some(context))
}

#[cfg(test)]
mod tests {
    use super::McpContext;

    #[test]
    fn preferred_schema_alias_still_populates_target_schema() {
        let context: McpContext =
            serde_json::from_str(r#"{"preferred_schema":"public","target_table":"tasks"}"#)
                .unwrap();

        assert_eq!(context.target_schema.as_deref(), Some("public"));
        assert_eq!(context.target_table.as_deref(), Some("tasks"));
    }

    #[test]
    fn target_bookmark_parses_when_present() {
        let context: McpContext =
            serde_json::from_str(r#"{"target_bookmark":"incident-root"}"#).unwrap();

        assert_eq!(context.target_bookmark.as_deref(), Some("incident-root"));
    }
}
