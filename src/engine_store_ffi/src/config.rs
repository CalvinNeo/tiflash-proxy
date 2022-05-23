use std::{
    path::{Path, PathBuf},
};

use serde_derive::{Deserialize, Serialize};
use online_config::OnlineConfig;
use std::collections::HashSet;
use std::iter::FromIterator;
use itertools::Itertools;
use std::collections::hash_map::RandomState;

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
        unrecognized_keys: Option<&mut Vec<String>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let s = std::fs::read_to_string(path)?;
        let mut deserializer = toml::Deserializer::new(&s);
        let mut cfg: ProxyConfig = if let Some(keys) = unrecognized_keys {
            serde_ignored::deserialize(&mut deserializer, |key| keys.push(key.to_string()))
        } else {
            <ProxyConfig as serde::Deserialize>::deserialize(&mut deserializer)
        }?;
        deserializer.end()?;
        Ok(cfg)
    }
}

pub fn ensure_no_common_unrecognized_keys(proxy_unrecognized_keys: &Vec<String>, unrecognized_keys: &Vec<String>) -> Result<(), String> {
    // We can't just compute intersection, since `rocksdb.z` equals not `rocksdb`.

    let proxy_part = HashSet::<_>::from_iter(proxy_unrecognized_keys.iter());
    let inter = unrecognized_keys.iter().filter(|s| {
        let mut pref: String = String::from("");
        for p in s.split(".") {
            if pref != "" {
                pref += "."
            }
            pref += p;
            if proxy_part.contains(&pref) {
                // common unrecognized by both config.
                return true;
            }
        }
        return false;
    }).collect::<Vec<_>>();
    if inter.len() != 0 {
        return Err(inter.iter().join(", "))
    }
    Ok(())
}