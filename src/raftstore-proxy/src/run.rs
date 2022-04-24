// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use api_version::{dispatch_api_version, APIVersion};
use engine_rocks::RocksEngine;
use engine_traits::RaftEngine;
use raft_log_engine::RaftLogEngine;
use tikv::{config::TiKvConfig, server::CPU_CORES_QUOTA_GAUGE};
use tikv_util::{
    sys::{register_memory_usage_high_water, SysQuota},
    time::{Instant, Monitor},
};

#[inline]
fn run_impl<CER: server::server::ConfiguredRaftEngine, Api: APIVersion>(config: TiKvConfig) {
    let mut tikv = server::server::TiKVServer::<CER>::init(config);

    // Must be called after `TiKVServer::init`.
    let memory_limit = tikv.config.memory_usage_limit.unwrap().0;
    let high_water = (tikv.config.memory_usage_high_water * memory_limit as f64) as u64;
    register_memory_usage_high_water(high_water);

    tikv.check_conflict_addr();
    tikv.init_fs();
    tikv.init_yatp();
    tikv.init_encryption();
    let fetcher = tikv.init_io_utility();
    let listener = tikv.init_flow_receiver();
    let (engines, engines_info) = tikv.init_raw_engines(listener);
    tikv.init_engines(engines.clone());
    let server_config = tikv.init_servers::<Api>();
    tikv.register_services();
    tikv.init_metrics_flusher(fetcher, engines_info);
    tikv.init_storage_stats_task(engines);
    tikv.run_server(server_config);
    tikv.run_status_server();

    server::signal_handler::wait_for_signal(Some(tikv.engines.take().unwrap().engines));
    tikv.stop();
}

/// Run a TiKV server. Returns when the server is shutdown by the user, in which
/// case the server will be properly stopped.
pub fn run_tikv(config: TiKvConfig) {
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
    server::server::pre_start();

    let _m = Monitor::default();

    dispatch_api_version!(config.storage.api_version(), {
        if !config.raft_engine.enable {
            run_impl::<RocksEngine, API>(config)
        } else {
            run_impl::<RaftLogEngine, API>(config)
        }
    })
}
