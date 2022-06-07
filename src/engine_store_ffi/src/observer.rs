use crate::interfaces::root::DB::EngineStoreApplyRes;
use crate::{ColumnFamilyType, RaftCmdHeader, WriteCmdType};
use engine_traits::{Peekable, SyncMutable};
use kvproto::metapb::Region;
use kvproto::raft_cmdpb::{
    AdminCmdType, AdminRequest, AdminResponse, ChangePeerRequest, CmdType, CommitMergeRequest,
    RaftCmdRequest, RaftCmdResponse, Request,
};
use kvproto::raft_serverpb::RaftApplyState;
use raft::{eraftpb, StateRole};
use raftstore::coprocessor::{
    AdminObserver, BoxAdminObserver, BoxQueryObserver, BoxRegionChangeObserver, Coprocessor,
    CoprocessorHost, ObserverContext, QueryObserver, RegionChangeEvent, RegionChangeObserver,
    RegionState,
};
use raftstore::coprocessor::{ApplySnapshotObserver, BoxApplySnapshotObserver, Cmd};
use raftstore::store::SnapKey;
use sst_importer::SSTImporter;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use tikv_util::{error, info, warn, debug};
use yatp::pool::{Builder, ThreadPool};
use yatp::task::future::TaskCell;
use std::ops::{Deref, DerefMut};

pub struct PtrWrapper(crate::RawCppPtr);

unsafe impl Send for PtrWrapper {}
unsafe impl Sync for PtrWrapper {}

#[derive(Default, Debug)]
pub struct PrehandleContext {
    // tracer holds ptr of snapshot prehandled by TiFlash side.
    pub tracer: HashMap<SnapKey, Arc<Mutex<PrehandleTask>>>,
}

#[derive(Debug)]
pub struct PrehandleTask {
    pub recv: mpsc::Receiver<Option<PtrWrapper>>,
}

impl PrehandleTask {
    fn new(recv: mpsc::Receiver<Option<PtrWrapper>>) -> Self {
        PrehandleTask { recv }
    }
}

pub struct TiFlashObserver {
    pub peer_id: u64,
    pub engine_store_server_helper: &'static crate::EngineStoreServerHelper,
    pub engine: engine_tiflash::RocksEngine,
    pub sst_importer: Arc<SSTImporter>,
    pub pre_handle_snapshot_ctx: Arc<Mutex<RefCell<PrehandleContext>>>,
}

impl Clone for TiFlashObserver {
    fn clone(&self) -> Self {
        TiFlashObserver {
            peer_id: self.peer_id,
            engine_store_server_helper: self.engine_store_server_helper,
            engine: self.engine.clone(),
            sst_importer: self.sst_importer.clone(),
            pre_handle_snapshot_ctx: self.pre_handle_snapshot_ctx.clone(),
        }
    }
}

// TiFlash observer's priority should be higher than all other observers, to avoid being bypassed.
const TIFLASH_OBSERVER_PRIORITY: u32 = 0;

impl TiFlashObserver {
    pub fn new(
        peer_id: u64,
        engine: engine_tiflash::RocksEngine,
        sst_importer: Arc<SSTImporter>,
    ) -> Self {
        let engine_store_server_helper =
            crate::gen_engine_store_server_helper(engine.engine_store_server_helper);
        TiFlashObserver {
            peer_id,
            engine_store_server_helper,
            engine,
            sst_importer,
            pre_handle_snapshot_ctx: Arc::new(Mutex::new(
                RefCell::new(PrehandleContext::default()),
            )),
        }
    }

    pub fn register_to<E: engine_traits::KvEngine>(
        &self,
        coprocessor_host: &mut CoprocessorHost<E>,
    ) {
        coprocessor_host.registry.register_query_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxQueryObserver::new(self.clone()),
        );
        coprocessor_host.registry.register_admin_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxAdminObserver::new(self.clone()),
        );
        coprocessor_host.registry.register_apply_snapshot_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxApplySnapshotObserver::new(self.clone()),
        );
    }

    fn write_apply_state(&self, region_id: u64, state: &RaftApplyState) {
        self.engine
            .put_msg_cf(
                engine_traits::CF_RAFT,
                &keys::apply_state_key(region_id),
                state,
            )
            .unwrap_or_else(|e| {
                panic!("failed to save apply state to write batch, error: {:?}", e);
            });
    }

    fn handle_ingest_sst_for_engine_store(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        ssts: &Vec<engine_traits::SSTMetaInfo>,
        index: u64,
        term: u64,
    ) -> EngineStoreApplyRes {
        let mut ssts_wrap = vec![];
        let mut sst_views = vec![];

        for sst in ssts {
            let sst = &sst.meta;
            if sst.get_cf_name() == engine_traits::CF_LOCK {
                panic!("should not ingest sst of lock cf");
            }

            // We have already `check_sst_for_ingestion` in handle_ingest_sst

            ssts_wrap.push((
                self.sst_importer.get_path(sst),
                crate::name_to_cf(sst.get_cf_name()),
            ));
        }

        for (path, cf) in &ssts_wrap {
            sst_views.push((path.to_str().unwrap().as_bytes(), *cf));
        }

        self.engine_store_server_helper.handle_ingest_sst(
            sst_views,
            RaftCmdHeader::new(ob_ctx.region().get_id(), index, term),
        )
    }
}

impl Coprocessor for TiFlashObserver {}

impl QueryObserver for TiFlashObserver {
    fn address_apply_result(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        cmd: &Cmd,
        apply_state: &RaftApplyState,
        region_state: &RegionState,
    ) {
        fail::fail_point!("on_address_apply_result_normal", |_| {});
        const NONE_STR: &str = "";
        let requests = cmd.request.get_requests();

        let mut ssts = vec![];
        let mut cmds = crate::WriteCmds::with_capacity(requests.len());
        for req in requests {
            let cmd_type = req.get_cmd_type();
            match cmd_type {
                CmdType::Put => {
                    let put = req.get_put();
                    let cf = crate::name_to_cf(put.get_cf());
                    let (key, value) = (put.get_key(), put.get_value());
                    cmds.push(key, value, WriteCmdType::Put, cf);
                }
                CmdType::Delete => {
                    let del = req.get_delete();
                    let cf = crate::name_to_cf(del.get_cf());
                    let key = del.get_key();
                    cmds.push(key, NONE_STR.as_ref(), WriteCmdType::Del, cf);
                }
                CmdType::IngestSst => {
                    ssts.push(engine_traits::SSTMetaInfo {
                        total_bytes: 0,
                        total_kvs: 0,
                        meta: req.get_ingest_sst().get_sst().clone(),
                    });
                }
                CmdType::Snap | CmdType::Get | CmdType::DeleteRange => {
                    // engine-store will drop table, no need DeleteRange
                    // We will filter delete range in engine_tiflash
                    continue;
                }
                CmdType::Prewrite | CmdType::Invalid | CmdType::ReadIndex => {
                    panic!("invalid cmd type, message maybe corrupted");
                }
            }
        }

        if !ssts.is_empty() {
            assert_eq!(cmds.len(), 0);
            let persist =
                match self.handle_ingest_sst_for_engine_store(ob_ctx, &ssts, cmd.index, cmd.term) {
                    EngineStoreApplyRes::None => {
                        // Before, BR/Lightning may let ingest sst cmd contain only one cf,
                        // which may cause that tiflash can not flush all region cache into column.
                        // so we have a optimization proxy@cee1f003. However, this is fixed in tiflash#1811.
                        error!(
                            "should not skip persist for ingest sst";
                            "region_id" => ob_ctx.region().get_id(),
                            "peer_id" => region_state.peer_id,
                            "term" => cmd.term,
                            "index" => cmd.index,
                            "ssts_to_clean" => ?ssts
                        );
                        true
                    }
                    EngineStoreApplyRes::NotFound | EngineStoreApplyRes::Persist => {
                        info!(
                            "ingest sst success";
                            "region_id" => ob_ctx.region().get_id(),
                            "peer_id" => region_state.peer_id,
                            "term" => cmd.term,
                            "index" => cmd.index,
                            "ssts_to_clean" => ?ssts
                        );
                        true
                    }
                };
        } else {
            let flash_res = {
                info!("!!!! write haha {} {} {:?}", cmd.index, cmd.term, cmds);
                self.engine_store_server_helper.handle_write_raft_cmd(
                    &cmds,
                    RaftCmdHeader::new(ob_ctx.region().get_id(), cmd.index, cmd.term),
                )
            };
            let persisted = match flash_res {
                EngineStoreApplyRes::None => {
                    info!("!!!! write None {} {}", cmd.index, cmd.term);
                    false
                }
                EngineStoreApplyRes::Persist => {
                    if !region_state.pending_remove {
                        info!("persist apply state for write"; "region_id" => ob_ctx.region().get_id(),
                            "peer_id" => region_state.peer_id, "state" => ?apply_state);
                        self.write_apply_state(ob_ctx.region().get_id(), apply_state);
                        true
                    } else {
                        false
                    }
                }
                EngineStoreApplyRes::NotFound => false,
            };
        }
    }
}

impl AdminObserver for TiFlashObserver {
    fn address_apply_result(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        cmd: &Cmd,
        apply_state: &RaftApplyState,
        region_state: &RegionState,
    ) {
        fail::fail_point!("on_address_apply_result_admin", |_| {});
        let request = cmd.request.get_admin_request();
        let response = cmd.response.get_admin_response();
        let cmd_type = request.get_cmd_type();

        match cmd_type {
            AdminCmdType::CompactLog | AdminCmdType::CommitMerge => {}
            AdminCmdType::ComputeHash | AdminCmdType::VerifyHash => {
                info!(
                    "useless admin command";
                    "region_id" => ob_ctx.region().get_id(),
                    "peer_id" => region_state.peer_id,
                    "term" => cmd.term,
                    "index" => cmd.index,
                    "type" => ?cmd_type,
                );
            }
            _ => {
                info!(
                    "execute admin command";
                    "region_id" => ob_ctx.region().get_id(),
                    "peer_id" => region_state.peer_id,
                    "term" => cmd.term,
                    "index" => cmd.index,
                    "command" => ?request
                );
            }
        }

        // TODO revert ApplyState for CompactLog

        // TODO set region for all split/merge ops

        let flash_res = {
            self.engine_store_server_helper.handle_admin_raft_cmd(
                &request,
                &response,
                RaftCmdHeader::new(ob_ctx.region().get_id(), cmd.index, cmd.term),
            )
        };
        let persisted = match flash_res {
            EngineStoreApplyRes::None => {
                if cmd_type == AdminCmdType::CompactLog {
                    // TODO need to revert
                }
                false
            }
            EngineStoreApplyRes::Persist => {
                if !region_state.pending_remove {
                    info!("persist apply state for admin"; "region_id" => ob_ctx.region().get_id(),
                            "peer_id" => region_state.peer_id, "state" => ?apply_state);
                    self.write_apply_state(ob_ctx.region().get_id(), apply_state);
                    true
                } else {
                    false
                }
            }
            EngineStoreApplyRes::NotFound => {
                error!(
                    "region not found in engine-store, maybe have exec `RemoveNode` first";
                    "region_id" => ob_ctx.region().get_id(),
                    "peer_id" => region_state.peer_id,
                    "term" => cmd.term,
                    "index" => cmd.index,
                );
                false
            }
        };
    }
}

impl TiFlashObserver {
    fn pre_handle_snapshot_impl(
        &self,
        region: &Region,
        peer_id: u64,
        snap_key: &SnapKey,
        sst_views: Vec<(PathBuf, ColumnFamilyType)>,
    ) {
        let (sender, receiver) = mpsc::channel();
        let idx = snap_key.idx;
        let term = snap_key.term;
        let engine_store_server_helper = self.engine_store_server_helper;
        let r = region.clone();
        let v = sst_views.clone();
        self.engine.apply_snap_pool.as_ref().unwrap().spawn(async move {
            let views = sst_views
                .iter()
                .map(|(b, c)| (b.to_str().unwrap().as_bytes(), c.clone()))
                .collect();

            fail::fail_point!("before_actually_pre_handle", |_| {});
            let s = engine_store_server_helper.pre_handle_snapshot(&r, peer_id, views, idx, term);
            sender.send(Some(PtrWrapper(s)));
        });

        self.engine.pending_applies_count.fetch_add(1, Ordering::Relaxed);

        let e = Arc::new(Mutex::new(PrehandleTask::new(receiver)));

        let locked = self.pre_handle_snapshot_ctx
            .lock()
            .unwrap();

        let mut b = locked
            .borrow_mut();

        b.tracer.insert(snap_key.clone(), e.clone());

        info!("!!!!! tracer len push {} {} {}", b.tracer.len(), self.peer_id, b.deref() as *const PrehandleContext as usize);
    }
}

impl ApplySnapshotObserver for TiFlashObserver {
    fn pre_handle_snapshot(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        peer_id: u64,
        snap_key: &SnapKey,
        ssts: &[raftstore::store::snap::CfFile],
    ) {
        fail::fail_point!("on_ob_pre_handle_snapshot", |_| {});
        let mut sst_views = vec![];
        for cf_file in ssts {
            // cf_file will be changed by dynamic region.
            if cf_file.size == 0 {
                // Skip empty cf file.
                continue;
            }

            // if plain_file_used(cf_file.cf) {
            //     assert!(cf_file.cf == CF_LOCK);
            // }

            sst_views.push((cf_file.path.clone(), crate::name_to_cf(cf_file.cf)));
        }

        self.pre_handle_snapshot_impl(ob_ctx.region(), peer_id, snap_key, sst_views);
    }

    fn post_apply_snapshot(
        &self,
        _: &mut ObserverContext<'_>,
        snap_key: &raftstore::store::SnapKey,
    ) {
        fail::fail_point!("on_ob_post_apply_snapshot", |_| {});
        let ctx = self.pre_handle_snapshot_ctx.lock().unwrap();
        let mut b = ctx.borrow_mut();
        match b.tracer.get(snap_key) {
            Some(t) => {
                let need_retry = match t.lock().unwrap().recv.recv() {
                    Ok(snap_ptr) => match snap_ptr {
                        Some(s) => {
                            debug!("get prehandled snapshot success");
                            self.engine_store_server_helper
                                .apply_pre_handled_snapshot(s.0);
                            false
                        }
                        None => true,
                    },
                    Err(_) => true,
                };
                if need_retry {
                    panic!("get prehandled snapshot failed, need prehandle again here")
                }
            }
            None => {
                panic!("Can not get snapshot of {:?}", snap_key);
            }
        }

        self.engine.pending_applies_count.fetch_sub(1, Ordering::Relaxed);
        b.tracer.remove(snap_key);
        info!("!!!!! tracer len remove {} {} {}", b.tracer.len(), self.peer_id, b.deref() as *const PrehandleContext as usize);
    }
}
