use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::db::{ConnectionProfile, DatabaseKind};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ConfigFile {
    #[serde(default)]
    pub profiles: Vec<ConnectionProfile>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StateFile {
    #[serde(default)]
    pub recent_profiles: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ConfigStore {
    pub profiles_path: PathBuf,
    pub state_path: PathBuf,
    pub file: ConfigFile,
    pub state: StateFile,
}

impl ConfigStore {
    pub fn load() -> Result<Self> {
        let config_root = dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("readgrid");
        let profiles_path = config_root.join("profiles.toml");
        let state_path = config_root.join("state.toml");
        let file = read_toml::<ConfigFile>(&profiles_path)?.unwrap_or_default();
        let state = read_toml::<StateFile>(&state_path)?.unwrap_or_default();

        Ok(Self {
            profiles_path,
            state_path,
            file,
            state,
        })
    }

    pub fn ordered_profiles(&self) -> Vec<ConnectionProfile> {
        let mut profiles = self.file.profiles.clone();
        profiles.sort_by_key(|profile| {
            self.state
                .recent_profiles
                .iter()
                .position(|recent| recent == &profile.name)
                .unwrap_or(usize::MAX)
        });
        profiles
    }

    pub fn note_recent_profile(&mut self, profile_name: &str) -> Result<()> {
        self.state
            .recent_profiles
            .retain(|name| name != profile_name);
        self.state
            .recent_profiles
            .insert(0, profile_name.to_string());
        self.state.recent_profiles.truncate(10);
        self.save_state()
    }

    pub fn save_state(&self) -> Result<()> {
        let parent = self
            .state_path
            .parent()
            .context("missing config directory for state file")?;
        fs::create_dir_all(parent)?;
        fs::write(&self.state_path, toml::to_string_pretty(&self.state)?)?;
        Ok(())
    }

    pub fn example_profiles() -> String {
        toml::to_string_pretty(&ConfigFile {
            profiles: vec![
                ConnectionProfile {
                    name: "local-postgres".into(),
                    kind: DatabaseKind::Postgres,
                    url: Some("postgres://localhost/app".into()),
                    path: None,
                },
                ConnectionProfile {
                    name: "sample-sqlite".into(),
                    kind: DatabaseKind::Sqlite,
                    url: None,
                    path: Some(PathBuf::from("./sample.db")),
                },
            ],
        })
        .unwrap_or_default()
    }
}

fn read_toml<T>(path: &PathBuf) -> Result<Option<T>>
where
    T: for<'de> Deserialize<'de>,
{
    if !path.exists() {
        return Ok(None);
    }

    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let parsed =
        toml::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(Some(parsed))
}
