// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

//! Configuration for the entire server.
//!
//! TiKV is configured through the `TiKvConfig` type, which is in turn
//! made up of many other configuration types.

use std::cmp;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::i32;
use std::io::Write;
use std::io::{Error as IoError, ErrorKind};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::usize;

use api_version::{match_template_api_version, APIVersion};
use causal_ts::Config as CausalTsConfig;
use encryption_export::DataKeyManager;
use engine_rocks::config::{self as rocks_config, BlobRunMode, CompressionType, LogLevel};
use engine_rocks::get_env;
use engine_rocks::properties::MvccPropertiesCollectorFactory;
use engine_rocks::raw::{
    BlockBasedOptions, Cache, ColumnFamilyOptions, CompactionPriority, DBCompactionStyle,
    DBCompressionType, DBOptions, DBRateLimiterMode, DBRecoveryMode, Env, LRUCacheOptions,
    TitanDBOptions,
};
use engine_rocks::raw_util::CFOptions;
use engine_rocks::util::{
    FixedPrefixSliceTransform, FixedSuffixSliceTransform, NoopSliceTransform,
};
use engine_rocks::{
    RaftDBLogger, RangePropertiesCollectorFactory, RocksEngine, RocksEventListener,
    RocksSstPartitionerFactory, RocksdbLogger, TtlPropertiesCollectorFactory,
    DEFAULT_PROP_KEYS_INDEX_DISTANCE, DEFAULT_PROP_SIZE_INDEX_DISTANCE,
};
use engine_traits::{CFOptionsExt, ColumnFamilyOptions as ColumnFamilyOptionsTrait, DBOptionsExt};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use file_system::{IOPriority, IORateLimiter};
use keys::region_raft_prefix_len;
use kvproto::kvrpcpb::ApiVersion;
use online_config::{ConfigChange, ConfigManager, ConfigValue, OnlineConfig, Result as CfgResult};
use pd_client::Config as PdConfig;
use raft_log_engine::RaftEngineConfig as RawRaftEngineConfig;
use raftstore::coprocessor::{Config as CopConfig, RegionInfoAccessor};
use raftstore::store::Config as RaftstoreConfig;
use raftstore::store::{CompactionGuardGeneratorFactory, SplitConfig};
use resource_metering::Config as ResourceMeteringConfig;
use security::SecurityConfig;
use tikv_util::config::{
    self, LogFormat, RaftDataStateMachine, ReadableDuration, ReadableSize, TomlWriter, GIB, MIB,
};
use tikv_util::sys::SysQuota;
use tikv_util::time::duration_to_sec;
use tikv_util::yatp_pool;

use server::fatal;
use tikv_util::{crit, info, warn, error, error_unknown, thd_name};
use tikv::config::DBType;
use tikv::server::CONFIG_ROCKSDB_GAUGE;
use tikv::config::config_to_slice;
use tikv::config::config_value_to_string;

pub const DEFAULT_ROCKSDB_SUB_DIR: &str = "db";

/// By default, block cache size will be set to 45% of system memory.
pub const BLOCK_CACHE_RATE: f64 = 0.45;
/// By default, TiKV will try to limit memory usage to 75% of system memory.
pub const MEMORY_USAGE_LIMIT_RATE: f64 = 0.75;

const LOCKCF_MIN_MEM: usize = 256 * MIB as usize;
const LOCKCF_MAX_MEM: usize = GIB as usize;
const RAFT_MIN_MEM: usize = 256 * MIB as usize;
const RAFT_MAX_MEM: usize = 2 * GIB as usize;
const LAST_CONFIG_FILE: &str = "last_tikv.toml";
const TMP_CONFIG_FILE: &str = "tmp_tikv.toml";
const MAX_BLOCK_SIZE: usize = 32 * MIB as usize;

pub struct DBConfigManger {
    db: engine_tiflash::RocksEngine,
    db_type: DBType,
    shared_block_cache: bool,
}

impl DBConfigManger {
    pub fn new(db: engine_tiflash::RocksEngine, db_type: DBType, shared_block_cache: bool) -> Self {
        DBConfigManger {
            db,
            db_type,
            shared_block_cache,
        }
    }
}

impl DBConfigManger {
    fn set_db_config(&self, opts: &[(&str, &str)]) -> Result<(), Box<dyn Error>> {
        self.db.set_db_options(opts)?;
        Ok(())
    }

    fn set_cf_config(&self, cf: &str, opts: &[(&str, &str)]) -> Result<(), Box<dyn Error>> {
        self.validate_cf(cf)?;
        self.db.set_options_cf(cf, opts)?;
        // Write config to metric
        for (cfg_name, cfg_value) in opts {
            let cfg_value = match cfg_value {
                v if *v == "true" => Ok(1f64),
                v if *v == "false" => Ok(0f64),
                v => v.parse::<f64>(),
            };
            if let Ok(v) = cfg_value {
                CONFIG_ROCKSDB_GAUGE
                    .with_label_values(&[cf, cfg_name])
                    .set(v);
            }
        }
        Ok(())
    }

    fn set_block_cache_size(&self, cf: &str, size: ReadableSize) -> Result<(), Box<dyn Error>> {
        self.validate_cf(cf)?;
        if self.shared_block_cache {
            return Err("shared block cache is enabled, change cache size through \
                 block-cache.capacity in storage module instead"
                .into());
        }
        let opt = self.db.get_options_cf(cf)?;
        opt.set_block_cache_capacity(size.0)?;
        // Write config to metric
        CONFIG_ROCKSDB_GAUGE
            .with_label_values(&[cf, "block_cache_size"])
            .set(size.0 as f64);
        Ok(())
    }

    fn set_rate_bytes_per_sec(&self, rate_bytes_per_sec: i64) -> Result<(), Box<dyn Error>> {
        let mut opt = self.db.as_inner().get_db_options();
        opt.set_rate_bytes_per_sec(rate_bytes_per_sec)?;
        Ok(())
    }

    fn set_rate_limiter_auto_tuned(
        &self,
        rate_limiter_auto_tuned: bool,
    ) -> Result<(), Box<dyn Error>> {
        let mut opt = self.db.as_inner().get_db_options();
        opt.set_auto_tuned(rate_limiter_auto_tuned)?;
        // double check the new state
        let new_auto_tuned = opt.get_auto_tuned();
        if new_auto_tuned.is_none() || new_auto_tuned.unwrap() != rate_limiter_auto_tuned {
            return Err("fail to set rate_limiter_auto_tuned".into());
        }
        Ok(())
    }

    fn set_max_background_jobs(&self, max_background_jobs: i32) -> Result<(), Box<dyn Error>> {
        self.set_db_config(&[("max_background_jobs", &max_background_jobs.to_string())])?;
        Ok(())
    }

    fn set_max_background_flushes(
        &self,
        max_background_flushes: i32,
    ) -> Result<(), Box<dyn Error>> {
        self.set_db_config(&[(
            "max_background_flushes",
            &max_background_flushes.to_string(),
        )])?;
        Ok(())
    }

    fn validate_cf(&self, cf: &str) -> Result<(), Box<dyn Error>> {
        match (self.db_type, cf) {
            (DBType::Kv, CF_DEFAULT)
            | (DBType::Kv, CF_WRITE)
            | (DBType::Kv, CF_LOCK)
            | (DBType::Kv, CF_RAFT)
            | (DBType::Raft, CF_DEFAULT) => Ok(()),
            _ => Err(format!("invalid cf {:?} for db {:?}", cf, self.db_type).into()),
        }
    }
}

impl ConfigManager for DBConfigManger {
    fn dispatch(&mut self, change: ConfigChange) -> Result<(), Box<dyn Error>> {
        let change_str = format!("{:?}", change);
        let mut change: Vec<(String, ConfigValue)> = change.into_iter().collect();
        let cf_config = change.drain_filter(|(name, _)| name.ends_with("cf"));
        for (cf_name, cf_change) in cf_config {
            if let ConfigValue::Module(mut cf_change) = cf_change {
                // defaultcf -> default
                let cf_name = &cf_name[..(cf_name.len() - 2)];
                if let Some(v) = cf_change.remove("block_cache_size") {
                    // currently we can't modify block_cache_size via set_options_cf
                    self.set_block_cache_size(cf_name, v.into())?;
                }
                if let Some(ConfigValue::Module(titan_change)) = cf_change.remove("titan") {
                    for (name, value) in titan_change {
                        cf_change.insert(name, value);
                    }
                }
                if !cf_change.is_empty() {
                    let cf_change = config_value_to_string(cf_change.into_iter().collect());
                    let cf_change_slice = config_to_slice(&cf_change);
                    self.set_cf_config(cf_name, &cf_change_slice)?;
                }
            }
        }

        if let Some(rate_bytes_config) = change
            .drain_filter(|(name, _)| name == "rate_bytes_per_sec")
            .next()
        {
            let rate_bytes_per_sec: ReadableSize = rate_bytes_config.1.into();
            self.set_rate_bytes_per_sec(rate_bytes_per_sec.0 as i64)?;
        }

        if let Some(rate_bytes_config) = change
            .drain_filter(|(name, _)| name == "rate_limiter_auto_tuned")
            .next()
        {
            let rate_limiter_auto_tuned: bool = rate_bytes_config.1.into();
            self.set_rate_limiter_auto_tuned(rate_limiter_auto_tuned)?;
        }

        if let Some(background_jobs_config) = change
            .drain_filter(|(name, _)| name == "max_background_jobs")
            .next()
        {
            let max_background_jobs = background_jobs_config.1.into();
            self.set_max_background_jobs(max_background_jobs)?;
        }

        if let Some(background_flushes_config) = change
            .drain_filter(|(name, _)| name == "max_background_flushes")
            .next()
        {
            let max_background_flushes = background_flushes_config.1.into();
            self.set_max_background_flushes(max_background_flushes)?;
        }

        if !change.is_empty() {
            let change = config_value_to_string(change);
            let change_slice = config_to_slice(&change);
            self.set_db_config(&change_slice)?;
        }
        info!(
            "rocksdb config changed";
            "db" => ?self.db_type,
            "change" => change_str
        );
        Ok(())
    }
}