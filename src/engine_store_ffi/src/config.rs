use std::{
    path::{Path, PathBuf},
};

use serde_derive::{Deserialize, Serialize};
use online_config::OnlineConfig;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ProxyConfig {
    // TODO read from config
    pub snap_handle_pool_size: usize,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        ProxyConfig {
            snap_handle_pool_size: 2,
        }
    }
}

impl ProxyConfig {
    pub fn from_file(
        path: &Path,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let s = std::fs::read_to_string(path)?;
        let mut deserializer = toml::Deserializer::new(&s);
        let cfg: ProxyConfig = serde_ignored::deserialize(&mut deserializer, |_| {} )?;
        deserializer.end()?;
        Ok(cfg)
    }
}