#![feature(slice_take)]

use std::{
    borrow::BorrowMut,
    cell::RefCell,
    collections::{BTreeMap, HashMap, HashSet},
    path::Path,
    pin::Pin,
    sync::{atomic::AtomicU8, Arc, Mutex, RwLock},
    time::Duration,
};

use encryption::DataKeyManager;
use engine_rocks::raw::DB;
use engine_store_ffi::{
    config::ProxyConfig, interfaces::root::DB as ffi_interfaces, EngineStoreServerHelper,
    RaftStoreProxyFFIHelper, RawCppPtr, UnwrapExternCFunc,
};
use engine_tiflash::RocksEngine;
use engine_traits::{Engines, SyncMutable, CF_DEFAULT, CF_LOCK, CF_WRITE};
use file_system::IORateLimiter;
use kvproto::{
    metapb,
    raft_cmdpb::{RaftCmdRequest, RaftCmdResponse},
};
use protobuf::Message;
use raftstore::{
    store::{
        fsm::{
            create_raft_batch_system,
            store::{StoreMeta, PENDING_MSG_CAP},
        },
        RaftRouter,
    },
    Error, Result,
};
use server::fatal;
use tempfile::TempDir;
use test_raftstore::{Config, Simulator, TestPdClient};
use tikv::server::{Node, Result as ServerResult};
use tikv_util::{
    crit, debug, info, sys::SysQuota, thread_group::GroupProperties, warn, HandyRwLock,
};

use crate::{gen_engine_store_server_helper, EngineStoreServer, EngineStoreServerWrap, TiKvConfig};
// mock cluster

pub struct FFIHelperSet {
    pub proxy: Box<engine_store_ffi::RaftStoreProxy>,
    pub proxy_helper: Box<engine_store_ffi::RaftStoreProxyFFIHelper>,
    pub engine_store_server: Box<EngineStoreServer>,
    // Make interface happy, don't own proxy and server.
    pub engine_store_server_wrap: Box<EngineStoreServerWrap>,
    pub engine_store_server_helper: Box<engine_store_ffi::EngineStoreServerHelper>,
    pub engine_store_server_helper_ptr: isize,
}

pub struct EngineHelperSet {
    pub engine_store_server: Box<EngineStoreServer>,
    pub engine_store_server_wrap: Box<EngineStoreServerWrap>,
    pub engine_store_server_helper: Box<engine_store_ffi::EngineStoreServerHelper>,
}

pub struct Cluster<T: Simulator<engine_tiflash::RocksEngine>> {
    pub raw: test_raftstore::Cluster<T, engine_tiflash::RocksEngine>,
    pub ffi_helper_lst: Vec<FFIHelperSet>,
    pub ffi_helper_set: Arc<Mutex<HashMap<u64, FFIHelperSet>>>,
    pub proxy_cfg: ProxyConfig,
    pub proxy_compat: bool,
}

impl<T: Simulator<engine_tiflash::RocksEngine>> Cluster<T> {
    pub fn new(
        id: u64,
        count: usize,
        sim: Arc<RwLock<T>>,
        pd_client: Arc<TestPdClient>,
        proxy_cfg: ProxyConfig,
    ) -> Cluster<T> {
        // Force sync to enable Leader run as a Leader, rather than proxy
        test_util::init_log_for_test();
        fail::cfg("apply_on_handle_snapshot_sync", "return").unwrap();

        let cls = test_raftstore::Cluster::new(
            id,
            count,
            sim,
            pd_client,
            create_tiflash_test_engine,
        );
        Cluster {
            raw: cls,
            ffi_helper_lst: Vec::default(),
            ffi_helper_set: Arc::new(Mutex::new(HashMap::default())),
            proxy_cfg,
            proxy_compat: false,
        }
    }

    pub fn make_ffi_helper_set_no_bind(
        id: u64,
        engines: Engines<engine_tiflash::RocksEngine, engine_rocks::RocksEngine>,
        key_mgr: &Option<Arc<DataKeyManager>>,
        router: &Option<RaftRouter<engine_tiflash::RocksEngine, engine_rocks::RocksEngine>>,
        node_cfg: TiKvConfig,
        cluster_id: isize,
        proxy_compat: bool,
    ) -> (FFIHelperSet, TiKvConfig) {
        // We must allocate on heap to avoid move.
        let proxy = Box::new(engine_store_ffi::RaftStoreProxy {
            status: AtomicU8::new(engine_store_ffi::RaftProxyStatus::Idle as u8),
            key_manager: key_mgr.clone(),
            read_index_client: match router {
                Some(r) => Some(Box::new(engine_store_ffi::ReadIndexClient::new(
                    r.clone(),
                    SysQuota::cpu_cores_quota() as usize * 2,
                ))),
                None => None,
            },
            kv_engine: std::sync::RwLock::new(Some(engines.kv.clone())),
        });

        let mut proxy_helper = Box::new(engine_store_ffi::RaftStoreProxyFFIHelper::new(&proxy));
        let mut engine_store_server = Box::new(EngineStoreServer::new(id, Some(engines)));
        engine_store_server.proxy_compat = proxy_compat;
        let engine_store_server_wrap = Box::new(EngineStoreServerWrap::new(
            &mut *engine_store_server,
            Some(&mut *proxy_helper),
            cluster_id,
        ));
        let engine_store_server_helper = Box::new(gen_engine_store_server_helper(
            std::pin::Pin::new(&*engine_store_server_wrap),
        ));

        let engine_store_server_helper_ptr = &*engine_store_server_helper as *const _ as isize;
        proxy
            .kv_engine
            .write()
            .unwrap()
            .as_mut()
            .unwrap()
            .engine_store_server_helper = engine_store_server_helper_ptr;
        let ffi_helper_set = FFIHelperSet {
            proxy,
            proxy_helper,
            engine_store_server,
            engine_store_server_wrap,
            engine_store_server_helper,
            engine_store_server_helper_ptr,
        };
        (ffi_helper_set, node_cfg)
    }

    pub fn make_ffi_helper_set(
        &mut self,
        id: u64,
        engines: Engines<engine_tiflash::RocksEngine, engine_rocks::RocksEngine>,
        key_mgr: &Option<Arc<DataKeyManager>>,
        router: &Option<RaftRouter<engine_tiflash::RocksEngine, engine_rocks::RocksEngine>>,
    ) -> (FFIHelperSet, TiKvConfig) {
        Cluster::<T>::make_ffi_helper_set_no_bind(
            id,
            engines,
            key_mgr,
            router,
            self.raw.cfg.tikv.clone(),
            self as *const Cluster<T> as isize,
            self.proxy_compat,
        )
    }

    pub fn create_engines(&mut self) {
        self.raw.io_rate_limiter = Some(Arc::new(
            self.raw
                .cfg
                .storage
                .io_rate_limit
                .build(true /*enable_statistics*/),
        ));
        for _ in 0..self.raw.count {
            self.create_engine(None);
        }
    }

    pub fn run(&mut self) {
        self.create_engines();
        self.raw.bootstrap_region().unwrap();
        self.start().unwrap();
    }

    pub fn run_conf_change(&mut self) -> u64 {
        self.create_engines();
        let region_id = self.raw.bootstrap_conf_change();
        // Will not start new nodes in `start`
        self.start().unwrap();
        region_id
    }

    pub fn create_ffi_helper_set(
        &mut self,
        engines: Engines<engine_tiflash::RocksEngine, engine_rocks::RocksEngine>,
        key_manager: &Option<Arc<DataKeyManager>>,
        router: &Option<RaftRouter<engine_tiflash::RocksEngine, engine_rocks::RocksEngine>>,
    ) {
        debug!("!!!!!! create_ffi_helper_set");
        let (mut ffi_helper_set, mut node_cfg) =
            self.make_ffi_helper_set(0, engines, key_manager, router);

        // We can not use moved or cloned engines any more.
        let (helper_ptr, ffi_hub) = {
            let helper_ptr = ffi_helper_set
                .proxy
                .kv_engine
                .write()
                .unwrap()
                .as_mut()
                .unwrap()
                .engine_store_server_helper;

            let helper = engine_store_ffi::gen_engine_store_server_helper(helper_ptr);
            let ffi_hub = Arc::new(engine_store_ffi::observer::TiFlashFFIHub {
                engine_store_server_helper: helper,
            });
            (helper_ptr, ffi_hub)
        };
        let engines = ffi_helper_set.engine_store_server.engines.as_mut().unwrap();

        engines.kv.init(
            helper_ptr,
            self.proxy_cfg.snap_handle_pool_size,
            Some(ffi_hub),
        );

        assert_ne!(engines.kv.engine_store_server_helper, 0);
        self.ffi_helper_lst.push(ffi_helper_set);
    }

    pub fn associate_ffi_helper_set(&mut self, index: Option<usize>, node_id: u64) {
        let mut ffi_helper_set = if let Some(i) = index {
            self.ffi_helper_lst.remove(i)
        } else {
            self.ffi_helper_lst.pop().unwrap()
        };
        ffi_helper_set.engine_store_server.id = node_id;
        self.ffi_helper_set
            .lock()
            .unwrap()
            .insert(node_id, ffi_helper_set);
    }

    pub fn create_engine(
        &mut self,
        router: Option<RaftRouter<engine_tiflash::RocksEngine, engine_rocks::RocksEngine>>,
    ) {
        let (mut engines, key_manager, dir) = create_tiflash_test_engine(
            router.clone(),
            self.raw.io_rate_limiter.clone(),
            &self.raw.cfg,
        );

        self.create_ffi_helper_set(engines, &key_manager, &router);
        let ffi_helper_set = self.ffi_helper_lst.last_mut().unwrap();
        let engines = ffi_helper_set.engine_store_server.engines.as_mut().unwrap();

        // replace self.raw.create_engine
        self.raw.dbs.push(engines.clone());
        self.raw.key_managers.push(key_manager.clone());
        self.raw.paths.push(dir);
    }

    pub fn start(&mut self) -> ServerResult<()> {
        init_global_ffi_helper_set();

        // Try recover from last shutdown.
        let node_ids: Vec<u64> = self.raw.engines.iter().map(|(&id, _)| id).collect();
        for node_id in node_ids {
            let mut engines = self.raw.engines.get_mut(&node_id).unwrap().clone();
            let key_mgr = self.raw.key_managers_map[&node_id].clone();
            // // We must set up ffi_helper_set again
            // self.create_ffi_helper_set(engines, &key_mgr, &None);
            self.associate_ffi_helper_set(Some(0), node_id);
            // Like TiKVServer::init
            self.raw.run_node(node_id)?;
            // Since we use None to create_ffi_helper_set, we must init again.
            let router = self.raw.sim.rl().get_router(node_id).unwrap();
            let mut lock = self.ffi_helper_set.lock().unwrap();
            let ffi_helper_set = lock.get_mut(&node_id).unwrap();
            ffi_helper_set.proxy.read_index_client =
                Some(Box::new(engine_store_ffi::ReadIndexClient::new(
                    router.clone(),
                    SysQuota::cpu_cores_quota() as usize * 2,
                )));
        }

        // Try start new nodes.
        for _ in 0..self.raw.count - self.raw.engines.len() {
            let (router, system) = create_raft_batch_system(&self.raw.cfg.raft_store);
            self.create_engine(Some(router.clone()));

            let store_meta = Arc::new(Mutex::new(StoreMeta::new(PENDING_MSG_CAP)));
            let props = GroupProperties::default();
            tikv_util::thread_group::set_properties(Some(props.clone()));

            let engines = self.raw.dbs.last().unwrap().clone();
            let key_manager = self.raw.key_managers.last().unwrap().clone();
            let node_id = {
                let mut sim = self.raw.sim.wl();
                // Like TiKVServer::init
                sim.run_node(
                    0,
                    self.raw.cfg.clone(),
                    engines.clone(),
                    store_meta.clone(),
                    key_manager.clone(),
                    router,
                    system,
                )?
            };
            debug!("start new node {}", node_id);
            self.raw.group_props.insert(node_id, props);
            self.raw.engines.insert(node_id, engines.clone());
            self.raw.store_metas.insert(node_id, store_meta);
            self.raw
                .key_managers_map
                .insert(node_id, key_manager.clone());
            self.associate_ffi_helper_set(None, node_id);
        }
        Ok(())
    }

    pub fn shutdown(&mut self) {
        self.raw.shutdown();
    }

    pub fn must_put(&mut self, key: &[u8], value: &[u8]) {
        self.raw.must_put(key, value);
    }

    pub fn get_region(&self, key: &[u8]) -> metapb::Region {
        self.raw.get_region(key)
    }

    pub fn get_tiflash_engine(&self, node_id: u64) -> &engine_tiflash::RocksEngine {
        &self.raw.engines[&node_id].kv
    }

    pub fn get_engine(&self, node_id: u64) -> Arc<DB> {
        self.raw.get_engine(node_id)
    }

    pub fn must_transfer_leader(&mut self, region_id: u64, leader: metapb::Peer) {
        self.raw.must_transfer_leader(region_id, leader)
    }

    pub fn call_command_on_leader(
        &mut self,
        mut request: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        self.raw.call_command_on_leader(request, timeout)
    }

    pub fn must_split(&mut self, region: &metapb::Region, split_key: &[u8]) {
        self.raw.must_split(region, split_key)
    }
}

static mut GLOBAL_ENGINE_HELPER_SET: Option<EngineHelperSet> = None;
static START: std::sync::Once = std::sync::Once::new();

pub unsafe fn get_global_engine_helper_set() -> &'static Option<EngineHelperSet> {
    &GLOBAL_ENGINE_HELPER_SET
}

fn make_global_ffi_helper_set_no_bind() -> (EngineHelperSet, *const u8) {
    let mut engine_store_server = Box::new(EngineStoreServer::new(99999, None));
    let engine_store_server_wrap = Box::new(EngineStoreServerWrap::new(
        &mut *engine_store_server,
        None,
        0,
    ));
    let engine_store_server_helper = Box::new(gen_engine_store_server_helper(std::pin::Pin::new(
        &*engine_store_server_wrap,
    )));
    let ptr =
        &*engine_store_server_helper as *const engine_store_ffi::EngineStoreServerHelper as *mut u8;
    // Will mutate ENGINE_STORE_SERVER_HELPER_PTR
    (
        EngineHelperSet {
            engine_store_server,
            engine_store_server_wrap,
            engine_store_server_helper,
        },
        ptr,
    )
}

pub fn init_global_ffi_helper_set() {
    unsafe {
        START.call_once(|| {
            assert_eq!(engine_store_ffi::get_engine_store_server_helper_ptr(), 0);
            let (set, ptr) = make_global_ffi_helper_set_no_bind();
            engine_store_ffi::init_engine_store_server_helper(ptr);
            GLOBAL_ENGINE_HELPER_SET = Some(set);
        });
    }
}

pub fn create_tiflash_test_engine(
    // ref init_tiflash_engines and create_test_engine
    // TODO: pass it in for all cases.
    router: Option<RaftRouter<engine_tiflash::RocksEngine, engine_rocks::RocksEngine>>,
    limiter: Option<Arc<IORateLimiter>>,
    cfg: &Config,
) -> (
    Engines<engine_tiflash::RocksEngine, engine_rocks::RocksEngine>,
    Option<Arc<DataKeyManager>>,
    TempDir,
) {
    let dir = test_util::temp_dir("test_cluster", cfg.prefer_mem);
    let key_manager = encryption_export::data_key_manager_from_config(
        &cfg.security.encryption,
        dir.path().to_str().unwrap(),
    )
    .unwrap()
    .map(Arc::new);

    let env = engine_rocks::get_env(key_manager.clone(), limiter).unwrap();
    let cache = cfg.storage.block_cache.build_shared_cache();

    let kv_path = dir.path().join(tikv::config::DEFAULT_ROCKSDB_SUB_DIR);
    let kv_path_str = kv_path.to_str().unwrap();

    let mut kv_db_opt = cfg.rocksdb.build_opt();
    kv_db_opt.set_env(env.clone());

    let kv_cfs_opt = cfg
        .rocksdb
        .build_cf_opts(&cache, None, cfg.storage.api_version());

    let engine = Arc::new(
        engine_rocks::raw_util::new_engine_opt(kv_path_str, kv_db_opt, kv_cfs_opt).unwrap(),
    );

    let raft_path = dir.path().join("raft");
    let raft_path_str = raft_path.to_str().unwrap();

    let mut raft_db_opt = cfg.raftdb.build_opt();
    raft_db_opt.set_env(env);

    let raft_cfs_opt = cfg.raftdb.build_cf_opts(&cache);
    let raft_engine = Arc::new(
        engine_rocks::raw_util::new_engine_opt(raft_path_str, raft_db_opt, raft_cfs_opt).unwrap(),
    );

    let mut engine = engine_tiflash::RocksEngine::from_db(engine);
    // FFI is not usable, until create_engine.
    let mut raft_engine = engine_rocks::RocksEngine::from_db(raft_engine);
    let shared_block_cache = cache.is_some();
    engine.set_shared_block_cache(shared_block_cache);
    raft_engine.set_shared_block_cache(shared_block_cache);
    let engines = Engines::new(engine, raft_engine);
    (engines, key_manager, dir)
}
