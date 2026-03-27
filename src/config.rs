use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::{
    app::StartupView,
    db::{ConnectionProfile, DatabaseKind, PreviewFilter, SortState, TableRef},
};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ConfigFile {
    #[serde(default)]
    pub profiles: Vec<ConnectionProfile>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StateFile {
    #[serde(default)]
    pub recent_profiles: Vec<String>,
    #[serde(default)]
    pub bookmarks: Vec<SavedBookmark>,
    #[serde(default)]
    pub filter_presets: Vec<FilterPreset>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BookmarkConnectionTarget {
    SavedProfile { name: String },
    Direct { profile: ConnectionProfile },
}

impl BookmarkConnectionTarget {
    pub fn label(&self) -> String {
        match self {
            Self::SavedProfile { name } => format!("profile:{name}"),
            Self::Direct { profile } => format!("direct:{}", profile.summary()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SavedBookmark {
    pub name: String,
    pub connection: BookmarkConnectionTarget,
    pub table: TableRef,
    #[serde(default)]
    pub preferred_view: Option<StartupView>,
    #[serde(default)]
    pub filters: Vec<PreviewFilter>,
    #[serde(default)]
    pub sort: Option<SortState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FilterPreset {
    pub name: String,
    pub connection: BookmarkConnectionTarget,
    pub table: TableRef,
    #[serde(default)]
    pub filters: Vec<PreviewFilter>,
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

    pub fn find_profile(&self, profile_name: &str) -> Option<ConnectionProfile> {
        self.file
            .profiles
            .iter()
            .find(|profile| profile.name == profile_name)
            .cloned()
    }

    pub fn resolve_connection_target(
        &self,
        target: &BookmarkConnectionTarget,
    ) -> Option<ConnectionProfile> {
        match target {
            BookmarkConnectionTarget::SavedProfile { name } => self.find_profile(name),
            BookmarkConnectionTarget::Direct { profile } => Some(profile.clone()),
        }
    }

    pub fn find_bookmark(&self, name: &str) -> Option<SavedBookmark> {
        self.state
            .bookmarks
            .iter()
            .find(|bookmark| bookmark.name == name)
            .cloned()
    }

    pub fn sorted_bookmarks(&self) -> Vec<SavedBookmark> {
        let mut bookmarks = self.state.bookmarks.clone();
        bookmarks.sort_by(|left, right| left.name.cmp(&right.name));
        bookmarks
    }

    pub fn upsert_bookmark(&mut self, bookmark: SavedBookmark) -> Result<bool> {
        let replaced = upsert_named_entry(&mut self.state.bookmarks, bookmark);
        self.save_state()?;
        Ok(replaced)
    }

    pub fn sorted_presets_for_scope(
        &self,
        connection: &BookmarkConnectionTarget,
        table: &TableRef,
    ) -> Vec<FilterPreset> {
        let mut presets = self
            .state
            .filter_presets
            .iter()
            .filter(|preset| &preset.connection == connection && &preset.table == table)
            .cloned()
            .collect::<Vec<_>>();
        presets.sort_by(|left, right| left.name.cmp(&right.name));
        presets
    }

    pub fn find_filter_preset(
        &self,
        connection: &BookmarkConnectionTarget,
        table: &TableRef,
        name: &str,
    ) -> Option<FilterPreset> {
        self.state
            .filter_presets
            .iter()
            .find(|preset| {
                preset.name == name && &preset.connection == connection && &preset.table == table
            })
            .cloned()
    }

    pub fn upsert_filter_preset(&mut self, preset: FilterPreset) -> Result<bool> {
        let replaced = upsert_named_entry(&mut self.state.filter_presets, preset);
        self.save_state()?;
        Ok(replaced)
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

fn upsert_named_entry<T>(entries: &mut Vec<T>, entry: T) -> bool
where
    T: NamedStateEntry,
{
    if let Some(index) = entries
        .iter()
        .position(|existing| existing.entry_name() == entry.entry_name())
    {
        entries[index] = entry;
        true
    } else {
        entries.push(entry);
        false
    }
}

trait NamedStateEntry {
    fn entry_name(&self) -> &str;
}

impl NamedStateEntry for SavedBookmark {
    fn entry_name(&self) -> &str {
        &self.name
    }
}

impl NamedStateEntry for FilterPreset {
    fn entry_name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn old_state_files_default_new_workspace_fields() {
        let state: StateFile = toml::from_str("recent_profiles = [\"sample\"]").unwrap();

        assert_eq!(state.recent_profiles, vec!["sample"]);
        assert!(state.bookmarks.is_empty());
        assert!(state.filter_presets.is_empty());
    }

    #[test]
    fn resolve_connection_target_uses_saved_profiles() {
        let store = ConfigStore {
            profiles_path: PathBuf::from("profiles.toml"),
            state_path: PathBuf::from("state.toml"),
            file: ConfigFile {
                profiles: vec![ConnectionProfile {
                    name: "sample".into(),
                    kind: DatabaseKind::Sqlite,
                    url: None,
                    path: Some(PathBuf::from("sample.db")),
                }],
            },
            state: StateFile::default(),
        };

        let profile = store
            .resolve_connection_target(&BookmarkConnectionTarget::SavedProfile {
                name: "sample".into(),
            })
            .unwrap();

        assert_eq!(profile.name, "sample");
        assert_eq!(profile.kind, DatabaseKind::Sqlite);
    }
}
