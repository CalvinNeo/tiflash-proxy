use raftstore::coprocessor::Cmd;
use raftstore::coprocessor::{Coprocessor, AdminObserver, QueryObserver, ObserverContext, RegionChangeObserver,
                             BoxQueryObserver, BoxAdminObserver, CoprocessorHost, BoxRegionChangeObserver,
                             RegionChangeEvent, RegionState};
use crate::{WriteCmdType, RaftCmdHeader};
use crate::interfaces::root::DB::EngineStoreApplyRes;
use kvproto::raft_serverpb::RaftApplyState;
use kvproto::raft_cmdpb::{
    AdminCmdType, AdminRequest, AdminResponse, ChangePeerRequest, CmdType, CommitMergeRequest,
    RaftCmdRequest, RaftCmdResponse, Request,
};
use engine_traits::{Peekable, SyncMutable};
use raft::{eraftpb, StateRole};
use tikv_util::{info, error, warn};
use sst_importer::SSTImporter;
use std::sync::Arc;
use yatp::pool::{Builder, ThreadPool};
use yatp::task::future::TaskCell;

pub struct TiFlashObserver {
    pub engine_store_server_helper: &'static crate::EngineStoreServerHelper,
    pub engine: engine_tiflash::RocksEngine,
    pub sst_importer: Arc<SSTImporter>,
    pub apply_snap_pool: Arc<ThreadPool<TaskCell>>,
}

impl Clone for TiFlashObserver {
    fn clone(&self) -> Self {
        TiFlashObserver {
            engine_store_server_helper: self.engine_store_server_helper,
            engine: self.engine.clone(),
            sst_importer: self.sst_importer.clone(),
            apply_snap_pool: self.apply_snap_pool.clone(),
        }
    }
}

// TiFlash observer's priority should be higher than all other observers, to avoid being bypassed.
const TIFLASH_OBSERVER_PRIORITY: u32 = 0;

impl TiFlashObserver {
    pub fn new(engine_store_server_helper: &'static crate::EngineStoreServerHelper,
               engine: engine_tiflash::RocksEngine,
               sst_importer: Arc<SSTImporter>,
               snap_handle_pool_size: usize,
    ) -> Self {
        let snap_pool = Builder::new(tikv_util::thd_name!("region-task"))
            .max_thread_count(snap_handle_pool_size)
            .build_future_pool();
        TiFlashObserver {
            engine_store_server_helper,
            engine,
            sst_importer,
            apply_snap_pool: Arc::new(snap_pool),
        }
    }

    pub fn register_to<E: engine_traits::KvEngine>(&self, coprocessor_host: &mut CoprocessorHost<E>) {
        coprocessor_host.registry.register_query_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxQueryObserver::new(self.clone()),
        );
        coprocessor_host.registry.register_admin_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxAdminObserver::new(self.clone()),
        );
    }

    fn write_apply_state(&self, region_id: u64, state: &RaftApplyState) {
        self.engine.put_msg_cf(
            engine_traits::CF_RAFT,
            &keys::apply_state_key(region_id),
            state,
        )
        .unwrap_or_else(|e| {
            panic!(
                "failed to save apply state to write batch, error: {:?}",
                e
            );
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
            let persist = match self.handle_ingest_sst_for_engine_store(ob_ctx, &ssts, cmd.index, cmd.term) {
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
                self.engine_store_server_helper.handle_write_raft_cmd(
                    &cmds,
                    RaftCmdHeader::new(ob_ctx.region().get_id(), cmd.index, cmd.term),
                )
            };
            let persisted = match flash_res {
                EngineStoreApplyRes::None => {
                    false
                },
                EngineStoreApplyRes::Persist => {
                    if !region_state.pending_remove {
                        info!("persist apply state for write"; "region_id" => ob_ctx.region().get_id(),
                            "peer_id" => region_state.peer_id, "state" => ?apply_state);
                        self.write_apply_state(ob_ctx.region().get_id(), apply_state);
                        true
                    } else {
                        false
                    }
                },
                EngineStoreApplyRes::NotFound => {
                    false
                },
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

        // TODO set region for merge ops

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
            },
            EngineStoreApplyRes::Persist => {
                if !region_state.pending_remove {
                    info!("persist apply state for admin"; "region_id" => ob_ctx.region().get_id(),
                            "peer_id" => region_state.peer_id, "state" => ?apply_state);
                    self.write_apply_state(ob_ctx.region().get_id(), apply_state);
                    true
                } else {
                    false
                }
            },
            EngineStoreApplyRes::NotFound => {
                error!(
                        "region not found in engine-store, maybe have exec `RemoveNode` first";
                        "region_id" => ob_ctx.region().get_id(),
                        "peer_id" => region_state.peer_id,
                        "term" => cmd.term,
                        "index" => cmd.index,
                    );
                false
            },
        };
    }
}

