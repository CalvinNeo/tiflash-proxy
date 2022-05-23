// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use api_version::{dispatch_api_version, APIVersion};
use raft_log_engine::RaftLogEngine;
use tikv::{config::TiKvConfig, server::CPU_CORES_QUOTA_GAUGE};
use tikv_util::{
    sys::{register_memory_usage_high_water, SysQuota},
    time::{Instant, Monitor},
};
use engine_traits::{
    ColumnFamilyOptions, Engines,
    RaftEngine
};
use std::sync::{Arc, Mutex};
use file_system::get_io_rate_limiter;
use std::path::Path;
use raftstore::store::LocalReader;
use tikv::server::RaftKv;
use crate::rawserver;
use rawserver::ConfiguredRaftEngine;
use rawserver::EnginesResourceInfo;
use raftstore::store::fsm::StoreMeta;
use raftstore::store::fsm::store::PENDING_MSG_CAP;

use server::fatal;
use tikv_util::{crit, info, warn, error, error_unknown, thd_name};
use engine_store_ffi::*;
use std::time::Duration;
use std::sync::atomic::AtomicU8;
use engine_store_ffi::config::ProxyConfig;

pub fn init_tiflash_engines<CER: ConfiguredRaftEngine>(
    tikv: &mut rawserver::TiKVServer<CER>,
    flow_listener: engine_rocks::FlowListener,
    engine_store_server_helper: isize,
) -> (Engines<engine_tiflash::RocksEngine, CER>, Arc<EnginesResourceInfo>) {
    let block_cache = tikv.config.storage.block_cache.build_shared_cache();
    let env = tikv
        .config
        .build_shared_rocks_env(tikv.encryption_key_manager.clone(), get_io_rate_limiter())
        .unwrap();

    // Create raft engine
    let raft_engine = CER::build(tikv, &env, &block_cache);

    // old-fashioned kv_engine creation
    let mut kv_db_opts = tikv.config.rocksdb.build_opt();
    kv_db_opts.set_env(env);
    // kv_db_opts.add_event_listener(tikv.create_raftstore_compaction_listener());
    let kv_cfs_opts = tikv.config.rocksdb.build_cf_opts(
        &block_cache,
        Some(&tikv.region_info_accessor),
        tikv.config.storage.api_version(),
    );
    let db_path = tikv.store_path.join(Path::new(tikv::config::DEFAULT_ROCKSDB_SUB_DIR));
    let kv_engine = engine_tiflash::raw_util::new_engine_opt(
        db_path.to_str().unwrap(),
        kv_db_opts,
        kv_cfs_opts,
    ).unwrap_or_else(|s| fatal!("failed to create kv engine: {}", s));

    // engine_tiflash::RocksEngine has engine_rocks::RocksEngine inside
    let mut kv_engine = engine_tiflash::RocksEngine::from_db(Arc::new(kv_engine));
    kv_engine.engine_store_server_helper = engine_store_server_helper;
    let engines = Engines::new(kv_engine, raft_engine);

    let cfg_controller = tikv.cfg_controller.as_mut().unwrap();
    cfg_controller.register(
        tikv::config::Module::Rocksdb,
        Box::new(tikv::config::DBConfigManger::new(
            engines.kv.rocks.clone(),
            tikv::config::DBType::Kv,
            tikv.config.storage.block_cache.shared,
        )),
    );
    engines
        .raft
        .register_config(cfg_controller, tikv.config.storage.block_cache.shared);

    let engines_info = Arc::new(EnginesResourceInfo::new(
        &engines, 180, /*max_samples_to_preserve*/
    ));

    (engines, engines_info)
}

#[inline]
fn run_impl<CER: ConfiguredRaftEngine, Api: APIVersion>(config: TiKvConfig, proxy_config: ProxyConfig, engine_store_server_helper: isize) {
    let mut tikv = rawserver::TiKVServer::<CER>::init(config, proxy_config);

    // Must be called after `TiKVServer::init`.
    let memory_limit = tikv.config.memory_usage_limit.unwrap().0;
    let high_water = (tikv.config.memory_usage_high_water * memory_limit as f64) as u64;
    register_memory_usage_high_water(high_water);

    tikv.check_conflict_addr();
    tikv.init_fs();
    tikv.init_yatp();
    tikv.init_encryption();

    let mut proxy = RaftStoreProxy::new(
        AtomicU8::new(RaftProxyStatus::Idle as u8),
        tikv.encryption_key_manager.clone(),
        Box::new(ReadIndexClient::new(
            tikv.router.clone(),
            SysQuota::cpu_cores_quota() as usize * 2,
        )),
        std::sync::RwLock::new(None),
    );

    let proxy_helper = RaftStoreProxyFFIHelper::new(&proxy);

    info!("set raft-store proxy helper");

    get_engine_store_server_helper().handle_set_proxy(&proxy_helper);

    info!("wait for engine-store server to start");
    while get_engine_store_server_helper().handle_get_engine_store_server_status()
        == EngineStoreServerStatus::Idle
    {
        std::thread::sleep(Duration::from_millis(200));
    }

    if get_engine_store_server_helper().handle_get_engine_store_server_status()
        != EngineStoreServerStatus::Running
    {
        info!("engine-store server is not running, make proxy exit");
        return;
    }

    info!("engine-store server is started");

    let fetcher = tikv.init_io_utility();
    let listener = tikv.init_flow_receiver();
    // We use engine_tiflash
    let (engines, engines_info) = init_tiflash_engines(&mut tikv, listener, engine_store_server_helper);
    tikv.init_engines(engines.clone());

    {
        // Should be engine_rocks::RocksEngine
        proxy.set_kv_engine(Some(engines.kv.clone()));
    }

    proxy.set_status(RaftProxyStatus::Running);

    let server_config = tikv.init_servers::<Api>(engine_store_server_helper);
    tikv.register_services();
    tikv.init_metrics_flusher(fetcher, engines_info);
    tikv.init_storage_stats_task(engines);
    tikv.run_server(server_config);
    tikv.run_status_server();

    proxy.set_status(RaftProxyStatus::Running);

    {
        debug_assert!(
            get_engine_store_server_helper().handle_get_engine_store_server_status()
                == EngineStoreServerStatus::Running
        );
        let _ = tikv.engines.take().unwrap().engines;
        loop {
            if get_engine_store_server_helper().handle_get_engine_store_server_status()
                != EngineStoreServerStatus::Running
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    info!("found engine-store server status is {:?}, start to stop all services",
            get_engine_store_server_helper().handle_get_engine_store_server_status()
        );


    server::signal_handler::wait_for_signal(Some(tikv.engines.take().unwrap().engines));
    tikv.stop();

    proxy.set_status(RaftProxyStatus::Stopped);

    info!("all services in raft-store proxy are stopped");

    info!("wait for engine-store server to stop");
    while get_engine_store_server_helper().handle_get_engine_store_server_status()
        != EngineStoreServerStatus::Terminated
    {
        std::thread::sleep(Duration::from_millis(200));
    }
    info!("engine-store server is stopped");
}

/// Run a TiKV server. Returns when the server is shutdown by the user, in which
/// case the server will be properly stopped.
pub fn run_tikv(config: TiKvConfig, proxy_config: ProxyConfig, engine_store_server_helper: isize) {
    // Sets the global logger ASAP.
    // It is okay to use the config w/o `validate()`,
    // because `initial_logger()` handles various conditions.
    server::setup::initial_logger(&config);

    // Print version information.
    let build_timestamp = option_env!("TIKV_BUILD_TIME");
    tikv::log_tikv_info(build_timestamp);

    // Print resource quota.
    SysQuota::log_quota();
    CPU_CORES_QUOTA_GAUGE.set(SysQuota::cpu_cores_quota());

    // Do some prepare works before start.
    rawserver::pre_start();

    let _m = Monitor::default();

    dispatch_api_version!(config.storage.api_version(), {
        if !config.raft_engine.enable {
            run_impl::<engine_rocks::RocksEngine, API>(config, proxy_config, engine_store_server_helper)
        } else {
            run_impl::<RaftLogEngine, API>(config, proxy_config, engine_store_server_helper)
        }
    })
}
