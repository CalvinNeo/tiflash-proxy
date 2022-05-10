#![feature(slice_take)]

use engine_store_ffi::interfaces::root::DB as ffi_interfaces;
use engine_store_ffi::{EngineStoreServerHelper, RaftStoreProxyFFIHelper, UnwrapExternCFunc, RawCppPtr};
use engine_traits::{Engines, SyncMutable};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use protobuf::Message;
use std::collections::{HashMap, HashSet, BTreeMap};
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;
use tikv_util::{debug, info, warn};
use test_raftstore::{Simulator, TestPdClient};
use std::sync::{Arc, RwLock};
use raftstore::store::RaftRouter;
use encryption::DataKeyManager;
use tikv::server::{Node, Result as ServerResult};
use crate::{EngineStoreServer, EngineStoreServerWrap, gen_engine_store_server_helper};
use std::sync::atomic::AtomicU8;
use tikv_util::sys::SysQuota;
use tikv_util::HandyRwLock;
use test_raftstore::Config;
use crate::TiKvConfig;
use raftstore::store::fsm::{create_raft_batch_system};
use raftstore::store::fsm::store::{StoreMeta, PENDING_MSG_CAP};
use tikv_util::thread_group::GroupProperties;
use std::path::Path;
use server::fatal;
use tikv_util::crit;

use file_system::IORateLimiter;
use tempfile::TempDir;

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
    pub ffi_helper_set: HashMap<u64, FFIHelperSet>,
}

impl<T: Simulator<engine_tiflash::RocksEngine>> Cluster<T> {
    pub fn new(
        id: u64,
        count: usize,
        sim: Arc<RwLock<T>>,
        pd_client: Arc<TestPdClient>,
    ) -> Cluster<T> {
        let mut cls = test_raftstore::Cluster::new(id, count, sim, pd_client,
create_tiflash_test_engine, |r: &engine_tiflash::RocksEngine| Arc::clone(r.rocks.as_inner()));
        Cluster {
            raw: cls,
            ffi_helper_set: HashMap::default(),
        }
    }

    pub fn make_ffi_helper_set_no_bind(
        id: u64,
        engines: Engines<engine_tiflash::RocksEngine, engine_rocks::RocksEngine>,
        key_mgr: &Option<Arc<DataKeyManager>>,
        router: &RaftRouter<engine_tiflash::RocksEngine, engine_rocks::RocksEngine>,
        mut node_cfg: TiKvConfig,
        cluster_id: isize,
    ) -> (FFIHelperSet, TiKvConfig) {
        let proxy = Box::new(engine_store_ffi::RaftStoreProxy {
            status: AtomicU8::new(engine_store_ffi::RaftProxyStatus::Idle as u8),
            key_manager: key_mgr.clone(),
            read_index_client: Box::new(engine_store_ffi::ReadIndexClient::new(
                router.clone(),
                SysQuota::cpu_cores_quota() as usize * 2,
            )),
            kv_engine: std::sync::RwLock::new(Some(engines.kv.clone())),
        });

        let mut proxy_helper = Box::new(engine_store_ffi::RaftStoreProxyFFIHelper::new(
            &proxy,
        ));
        let mut engine_store_server =
            Box::new(EngineStoreServer::new(id, Some(engines)));
        let engine_store_server_wrap = Box::new(EngineStoreServerWrap::new(
            &mut *engine_store_server,
            Some(&mut *proxy_helper),
            cluster_id,
        ));
        let engine_store_server_helper =
            Box::new(gen_engine_store_server_helper(
                std::pin::Pin::new(&*engine_store_server_wrap),
            ));

        let helper_sz = &*engine_store_server_helper as *const _ as isize;
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
        router: &RaftRouter<engine_tiflash::RocksEngine, engine_rocks::RocksEngine>,
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

    pub fn start(&mut self) -> ServerResult<()> {
        init_global_ffi_helper_set();

        // Try recover from last shutdown.
        let node_ids: Vec<u64> = self.raw.engines.iter().map(|(&id, _)| id).collect();
        for node_id in node_ids {
            self.raw.run_node(node_id)?;
        }

        // Try start new nodes.
        for _ in 0..self.raw.count - self.raw.engines.len() {
            let (router, system) = create_raft_batch_system(&self.raw.cfg.raft_store);

            // replace self.raw.create_engine
            let (engines, key_manager, dir) =
                create_tiflash_test_engine(Some(router.clone()), self.raw.io_rate_limiter.clone(), &self.raw.cfg);

            self.raw.dbs.push(engines.clone());
            self.raw.key_managers.push(key_manager.clone());
            self.raw.paths.push(dir);

            let store_meta = Arc::new(Mutex::new(StoreMeta::new(PENDING_MSG_CAP)));

            let props = GroupProperties::default();
            tikv_util::thread_group::set_properties(Some(props.clone()));

            let (mut ffi_helper_set, mut node_cfg) =
                self.make_ffi_helper_set(0, engines.clone(), &key_manager, &router);

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
            self.raw.engines.insert(node_id, engines);
            self.raw.store_metas.insert(node_id, store_meta);
            self.raw.key_managers_map.insert(node_id, key_manager.clone());
            ffi_helper_set.engine_store_server.id = node_id;
            self.ffi_helper_set.insert(node_id, ffi_helper_set);
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
    let engine_store_server_helper = Box::new(gen_engine_store_server_helper(
        std::pin::Pin::new(&*engine_store_server_wrap),
    ));
    let ptr = &*engine_store_server_helper
        as *const engine_store_ffi::EngineStoreServerHelper as *mut u8;
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
            assert_eq!(
                engine_store_ffi::get_engine_store_server_helper_ptr(),
                0
            );
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
    let key_manager =
        encryption_export::data_key_manager_from_config(&cfg.security.encryption, dir.path().to_str().unwrap())
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
    let mut raft_engine = engine_rocks::RocksEngine::from_db(raft_engine);
    let shared_block_cache = cache.is_some();
    engine.set_shared_block_cache(shared_block_cache);
    raft_engine.set_shared_block_cache(shared_block_cache);
    let engines = Engines::new(engine, raft_engine);
    (engines, key_manager, dir)
}