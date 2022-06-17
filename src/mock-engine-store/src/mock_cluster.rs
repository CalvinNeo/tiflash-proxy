#![feature(slice_take)]

use crate::TiKvConfig;
use crate::{gen_engine_store_server_helper, EngineStoreServer, EngineStoreServerWrap};
use encryption::DataKeyManager;
use engine_store_ffi::interfaces::root::DB as ffi_interfaces;
use engine_store_ffi::{
    EngineStoreServerHelper, RaftStoreProxyFFIHelper, RawCppPtr, UnwrapExternCFunc,
};
use engine_traits::{Engines, SyncMutable};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use protobuf::Message;
use raftstore::store::fsm::create_raft_batch_system;
use raftstore::store::fsm::store::{StoreMeta, PENDING_MSG_CAP};
use raftstore::store::RaftRouter;
use server::fatal;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::AtomicU8;
use std::sync::Mutex;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use test_raftstore::Config;
use test_raftstore::{Simulator, TestPdClient};
use tikv::server::{Node, Result as ServerResult};
use tikv_util::crit;
use tikv_util::sys::SysQuota;
use tikv_util::thread_group::GroupProperties;
use tikv_util::HandyRwLock;
use tikv_util::{debug, info, warn};

use file_system::IORateLimiter;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use tempfile::TempDir;
use engine_store_ffi::config::ProxyConfig;

// mock cluster

pub struct FFIHelperSet {
    pub proxy: Box<engine_store_ffi::RaftStoreProxy>,
    pub proxy_helper: Box<engine_store_ffi::RaftStoreProxyFFIHelper>,
    pub engine_store_server: Box<EngineStoreServer>,
    pub engine_store_server_wrap: Box<EngineStoreServerWrap>,
    pub engine_store_server_helper: Box<engine_store_ffi::EngineStoreServerHelper>,
}

pub struct EngineHelperSet {
    pub engine_store_server: Box<EngineStoreServer>,
    pub engine_store_server_wrap: Box<EngineStoreServerWrap>,
    pub engine_store_server_helper: Box<engine_store_ffi::EngineStoreServerHelper>,
}

pub struct Cluster<T: Simulator<engine_tiflash::RocksEngine>> {
    pub raw: test_raftstore::Cluster<T, engine_tiflash::RocksEngine>,
    pub ffi_helper_set: Arc<Mutex<RefCell<HashMap<u64, FFIHelperSet>>>>,
    pub proxy_cfg: ProxyConfig,
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

        let mut cls = test_raftstore::Cluster::new(
            id,
            count,
            sim,
            pd_client,
            create_tiflash_test_engine,
            |r: &engine_tiflash::RocksEngine| Arc::clone(r.rocks.as_inner()),
        );
        Cluster {
            raw: cls,
            ffi_helper_set: Arc::new(Mutex::new(RefCell::new(HashMap::default()))),
            proxy_cfg,
        }
    }

    pub fn make_ffi_helper_set_no_bind(
        id: u64,
        engines: Engines<engine_tiflash::RocksEngine, engine_rocks::RocksEngine>,
        key_mgr: &Option<Arc<DataKeyManager>>,
        router: &Option<RaftRouter<engine_tiflash::RocksEngine, engine_rocks::RocksEngine>>,
        mut node_cfg: TiKvConfig,
        cluster_id: isize,
    ) -> (FFIHelperSet, TiKvConfig) {
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
        let engine_store_server_wrap = Box::new(EngineStoreServerWrap::new(
            &mut *engine_store_server,
            Some(&mut *proxy_helper),
            cluster_id,
        ));
        let engine_store_server_helper = Box::new(gen_engine_store_server_helper(
            std::pin::Pin::new(&*engine_store_server_wrap),
        ));

        let helper_sz = &*engine_store_server_helper as *const _ as isize;
        proxy
            .kv_engine
            .write()
            .unwrap()
            .as_mut()
            .unwrap()
            .engine_store_server_helper = helper_sz;
        let ffi_helper_set = FFIHelperSet {
            proxy,
            proxy_helper,
            engine_store_server,
            engine_store_server_wrap,
            engine_store_server_helper,
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
        )
    }

    pub fn create_engines(&mut self) {
        self.raw.io_rate_limiter = Some(Arc::new(
            self.raw.cfg
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

    pub fn create_engine(
        &mut self,
        router: Option<RaftRouter<engine_tiflash::RocksEngine, engine_rocks::RocksEngine>>,
    ) -> FFIHelperSet {
        let (mut engines, key_manager, dir) = create_tiflash_test_engine(
            router.clone(),
            self.raw.io_rate_limiter.clone(),
            &self.raw.cfg,
        );

        let (mut ffi_helper_set, mut node_cfg) =
            self.make_ffi_helper_set(0, engines.clone(), &key_manager, &router);

        let helper_sz = ffi_helper_set
            .proxy
            .kv_engine
            .write()
            .unwrap()
            .as_mut()
            .unwrap()
            .engine_store_server_helper;

        let helper = engine_store_ffi::gen_engine_store_server_helper(helper_sz);
        let ffi_hub = Arc::new(engine_store_ffi::observer::TiFlashFFIHub {
            engine_store_server_helper: helper,
        });

        engines.kv.init(helper_sz, self.proxy_cfg.snap_handle_pool_size, Some(ffi_hub));

        assert_ne!(engines.kv.engine_store_server_helper, 0);

        // replace self.raw.create_engine
        self.raw.dbs.push(engines.clone());
        self.raw.key_managers.push(key_manager.clone());
        self.raw.paths.push(dir);

        ffi_helper_set
    }

    pub fn start(&mut self) -> ServerResult<()> {
        init_global_ffi_helper_set();

        // Try recover from last shutdown.
        let node_ids: Vec<u64> = self.raw.engines.iter().map(|(&id, _)| id).collect();
        for node_id in node_ids {
            let mut engines = self.raw.engines.get_mut(&node_id).unwrap().clone();
            let key_mgr = self.raw.key_managers_map[&node_id].clone();
            let (mut ffi_helper_set, mut node_cfg) =
                self.make_ffi_helper_set(node_id, engines, &key_mgr, &None);
            self.raw
                .engines
                .get_mut(&node_id)
                .unwrap()
                .kv
                .engine_store_server_helper = ffi_helper_set
                .proxy
                .kv_engine
                .write()
                .unwrap()
                .as_mut()
                .unwrap()
                .engine_store_server_helper;
            ffi_helper_set.engine_store_server.id = node_id;
            self.ffi_helper_set
                .lock()
                .unwrap()
                .get_mut()
                .insert(node_id, ffi_helper_set);
            self.raw.run_node(node_id)?;
            let router = self.raw.sim.rl().get_router(node_id).unwrap();
            self.ffi_helper_set
                .lock()
                .unwrap()
                .get_mut()
                .get_mut(&node_id)
                .unwrap()
                .proxy
                .read_index_client = Some(Box::new(engine_store_ffi::ReadIndexClient::new(
                router.clone(),
                SysQuota::cpu_cores_quota() as usize * 2,
            )));
        }

        // Try start new nodes.
        for _ in 0..self.raw.count - self.raw.engines.len() {
            let (router, system) = create_raft_batch_system(&self.raw.cfg.raft_store);
            let mut ffi_helper_set = self.create_engine(Some(router.clone()));

            let store_meta = Arc::new(Mutex::new(StoreMeta::new(PENDING_MSG_CAP)));
            let props = GroupProperties::default();
            tikv_util::thread_group::set_properties(Some(props.clone()));

            let engines = self.raw.dbs.last().unwrap().clone();
            let key_manager = self.raw.key_managers.last().unwrap().clone();

            let mut sim = self.raw.sim.wl();
            let node_id = sim.run_node(
                0,
                self.raw.cfg.clone(),
                engines.clone(),
                store_meta.clone(),
                key_manager.clone(),
                router,
                system,
            )?;
            debug!("start new node {}", node_id);
            self.raw.group_props.insert(node_id, props);
            self.raw.engines.insert(node_id, engines.clone());
            self.raw.store_metas.insert(node_id, store_meta);
            self.raw
                .key_managers_map
                .insert(node_id, key_manager.clone());
            ffi_helper_set.engine_store_server.id = node_id;
            self.ffi_helper_set
                .lock()
                .unwrap()
                .get_mut()
                .insert(node_id, ffi_helper_set);
        }
        Ok(())
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
