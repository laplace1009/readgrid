use std::{env, fs, path::PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::{app::CliArgs, db::ConnectionProfile};

#[derive(Debug, Clone, Deserialize)]
pub struct McpContext {
    #[serde(default)]
    pub profile: Option<ConnectionProfile>,
    #[serde(default)]
    pub preferred_schema: Option<String>,
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

    pub fn into_profile(self) -> Option<ConnectionProfile> {
        self.profile
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
