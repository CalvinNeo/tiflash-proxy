

pub struct FFIHelperSet {
    pub proxy: Box<raftstore::engine_store_ffi::RaftStoreProxy>,
    pub proxy_helper: Box<raftstore::engine_store_ffi::RaftStoreProxyFFIHelper>,
    pub engine_store_server: Box<mock_engine_store::EngineStoreServer>,
    pub engine_store_server_wrap: Box<mock_engine_store::EngineStoreServerWrap>,
    pub engine_store_server_helper: Box<raftstore::engine_store_ffi::EngineStoreServerHelper>,
}

pub struct EngineHelperSet {
    pub engine_store_server: Box<mock_engine_store::EngineStoreServer>,
    pub engine_store_server_wrap: Box<mock_engine_store::EngineStoreServerWrap>,
    pub engine_store_server_helper: Box<raftstore::engine_store_ffi::EngineStoreServerHelper>,
}

pub struct Cluster<T: Simulator> {
    pub raw: test_raftstore::Cluster<T>,
    pub ffi_helper_set: HashMap<u64, FFIHelperSet>,
}

impl<T: Simulator> Cluster<T> {
    pub fn new(
        id: u64,
        count: usize,
        sim: Arc<RwLock<T>>,
        pd_client: Arc<TestPdClient>,
    ) -> Cluster<T> {
        let mut cls = test_raftstore::Cluster::new(id, count, sim, pd_client);
        Cluster {
            raw: cls,
            ffi_helper_set: HashMap::default(),
        }
    }

    pub fn make_ffi_helper_set(
        &mut self,
        id: u64,
        engines: Engines<RocksEngine, RocksEngine>,
        key_mgr: &Option<Arc<DataKeyManager>>,
        router: &RaftRouter<RocksEngine, RocksEngine>,
    ) -> (FFIHelperSet, TiKvConfig) {
        Cluster::<T>::make_ffi_helper_set_no_bind(
            id,
            engines,
            key_mgr,
            router,
            self.cfg.clone(),
            self as *const Cluster<T> as isize,
        )
    }

    pub fn start(&mut self) -> ServerResult<()> {
        init_global_ffi_helper_set();
        let node_ids: Vec<u64> = self.engines.iter().map(|(&id, _)| id).collect();
        let new_node_cnt = self.count - self.engines.len();
        self.raw.start();
        for node_id in node_ids {
            for _ in 0..new_node_cnt {

            }
        }
        let (mut ffi_helper_set, node_cfg) =
            self.make_ffi_helper_set(0, self.dbs.last().unwrap().clone(), &key_mgr, &router);
        ffi_helper_set.engine_store_server.id = node_id;
        self.ffi_helper_set.insert(node_id, ffi_helper_set);
    }
}

static mut GLOBAL_ENGINE_HELPER_SET: Option<EngineHelperSet> = None;
static START: std::sync::Once = std::sync::Once::new();

pub unsafe fn get_global_engine_helper_set() -> &'static Option<EngineHelperSet> {
    &GLOBAL_ENGINE_HELPER_SET
}

fn make_global_ffi_helper_set_no_bind() -> (EngineHelperSet, *const u8) {
    let mut engine_store_server = Box::new(mock_engine_store::EngineStoreServer::new(99999, None));
    let engine_store_server_wrap = Box::new(mock_engine_store::EngineStoreServerWrap::new(
        &mut *engine_store_server,
        None,
        0,
    ));
    let engine_store_server_helper = Box::new(mock_engine_store::gen_engine_store_server_helper(
        std::pin::Pin::new(&*engine_store_server_wrap),
    ));
    let ptr = &*engine_store_server_helper
        as *const raftstore::engine_store_ffi::EngineStoreServerHelper as *mut u8;
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
                raftstore::engine_store_ffi::get_engine_store_server_helper_ptr(),
                0
            );
            let (set, ptr) = make_global_ffi_helper_set_no_bind();
            raftstore::engine_store_ffi::init_engine_store_server_helper(ptr);
            GLOBAL_ENGINE_HELPER_SET = Some(set);
        });
    }
}