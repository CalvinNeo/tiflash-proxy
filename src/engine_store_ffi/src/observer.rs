use raftstore::coprocessor::Cmd;
use raftstore::coprocessor::{Coprocessor, AdminObserver, QueryObserver, ObserverContext, RegionChangeObserver,
                             BoxQueryObserver, BoxAdminObserver, CoprocessorHost, BoxRegionChangeObserver,
                             RegionChangeEvent};
use crate::{WriteCmdType, RaftCmdHeader};
use crate::interfaces::root::DB::EngineStoreApplyRes;
use kvproto::raft_cmdpb::CmdType;
use kvproto::raft_serverpb::RaftApplyState;
use engine_traits::{Peekable, SyncMutable};
use raft::{eraftpb, StateRole};

pub struct TiFlashObserver {
    pub engine_store_server_helper: &'static crate::EngineStoreServerHelper,
    pub engine: engine_tiflash::RocksEngine,
}

impl Clone for TiFlashObserver {
    fn clone(&self) -> Self {
        TiFlashObserver {
            engine_store_server_helper: self.engine_store_server_helper,
            engine: self.engine.clone(),
        }
    }
}

// TiFlash observer's priority should be higher than all other observers, to avoid being bypassed.
const TIFLASH_OBSERVER_PRIORITY: u32 = 0;

impl TiFlashObserver {
    pub fn new(engine_store_server_helper: &'static crate::EngineStoreServerHelper, engine: engine_tiflash::RocksEngine) -> Self {
        TiFlashObserver {
            engine_store_server_helper,
            engine,
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
}

impl Coprocessor for TiFlashObserver {}

impl QueryObserver for TiFlashObserver {
    fn address_apply_result(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        cmd: &Cmd,
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
                    continue;
                }
                CmdType::Prewrite | CmdType::Invalid | CmdType::ReadIndex => {
                    panic!("invalid cmd type, message maybe currupted");
                }
            }
        }

        if !ssts.is_empty() {
            unimplemented!();
        } else {
            let flash_res = {
                self.engine_store_server_helper.handle_write_raft_cmd(
                    &cmds,
                    RaftCmdHeader::new(ob_ctx.region().get_id(), cmd.index, cmd.term),
                )
            };
            match flash_res {
                EngineStoreApplyRes::None => {},
                EngineStoreApplyRes::Persist => {},
                EngineStoreApplyRes::NotFound => {},

            }
        }
    }
}

impl AdminObserver for TiFlashObserver {
    fn address_apply_result(
        &self,
        _: &mut ObserverContext<'_>,
        cmd: &Cmd,
    ) {

    }
}

