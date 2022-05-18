// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_store_ffi::{KVGetStatus, RaftStoreProxyFFI};
use std::sync::{Arc, RwLock};
use test_raftstore::{TestPdClient, must_get_equal, must_get_none};
use mock_engine_store::node::NodeCluster;
extern crate rocksdb;
use ::rocksdb::{DB};
use engine_traits::Iterator;
use engine_traits::SeekKey;
use engine_traits::{Error, Result};
use engine_traits::{ExternalSstFileInfo, SstExt, SstReader, SstWriter, SstWriterBuilder};
use engine_tiflash::*;
use engine_traits::Iterable;
use engine_traits::Peekable;
use engine_traits::{CF_RAFT,CF_LOCK,CF_WRITE,CF_DEFAULT};
use crate::normal::rocksdb::Writable;

#[test]
fn test_normal() {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = mock_engine_store::mock_cluster::Cluster::new(0, 3, sim, pd_client);

    // Try to start this node, return after persisted some keys.
    let _ = cluster.start();

    let total = 1;
    for i in 0..total {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.raw.must_put(k.as_bytes(), v.as_bytes());
        for id in cluster.raw.engines.keys() {
            let engine = &cluster.raw.get_engine(*id);
            // We can get nothing, since engine_tiflash filters all data.
            must_get_none(engine, k.as_bytes());
        }
    }

    for id in cluster.raw.engines.keys() {
        let r1 = cluster.raw.get_region(b"k1").get_id();
        let db = cluster.raw.get_engine(*id);
        let engine = engine_rocks::RocksEngine::from_db(db);
        // We can still get RegionLocalState
        let region_state_key = keys::region_state_key(r1);
        match engine.get_msg_cf::<kvproto::raft_serverpb::RegionLocalState>(CF_RAFT, &region_state_key) {
            Ok(Some(_)) => (),
            _ => unreachable!(),
        };

        let region_state_key = keys::apply_state_key(r1);
        match engine.get_msg_cf::<kvproto::raft_serverpb::RaftApplyState>(CF_RAFT, &region_state_key) {
            Ok(Some(_)) => (),
            _ => unreachable!(),
        };

        match engine.get_msg::<kvproto::raft_serverpb::StoreIdent>(keys::STORE_IDENT_KEY){
            Ok(Some(_)) => (),
            _ => unreachable!(),
        };
    }


    // get RegionLocalState through ffi
    let k = "k1".as_bytes();
    let region_id = cluster.raw.get_region(k).get_id();
    unsafe {
        for (_, ffi_set) in cluster.ffi_helper_set.iter_mut() {
            let f = ffi_set.proxy_helper.fn_get_region_local_state.unwrap();
            let mut state = kvproto::raft_serverpb::RegionLocalState::default();
            let mut error_msg = mock_engine_store::RawCppStringPtrGuard::default();

            assert_eq!(
                f(
                    ffi_set.proxy_helper.proxy_ptr,
                    region_id,
                    &mut state as *mut _ as _,
                    error_msg.as_mut(),
                ),
                KVGetStatus::Ok
            );
            assert!(state.has_region());
            assert_eq!(state.get_state(), kvproto::raft_serverpb::PeerState::Normal);
            assert!(error_msg.as_ref().is_null());

            let mut state = kvproto::raft_serverpb::RegionLocalState::default();
            assert_eq!(
                f(
                    ffi_set.proxy_helper.proxy_ptr,
                    0, // not exist
                    &mut state as *mut _ as _,
                    error_msg.as_mut(),
                ),
                KVGetStatus::NotFound
            );
            assert!(!state.has_region());
            assert!(error_msg.as_ref().is_null());

            ffi_set
                .proxy
                .get_value_cf("none_cf", "123".as_bytes(), |value| {
                    let msg = value.unwrap_err();
                    assert_eq!(msg, "Storage Engine cf none_cf not found");
                });
            ffi_set
                .proxy
                .get_value_cf("raft", "123".as_bytes(), |value| {
                    let res = value.unwrap();
                    assert!(res.is_none());
                });

            // If we have no kv engine.
            ffi_set.proxy.set_kv_engine(None);
            let res = ffi_set.proxy_helper.fn_get_region_local_state.unwrap()(
                ffi_set.proxy_helper.proxy_ptr,
                region_id,
                &mut state as *mut _ as _,
                error_msg.as_mut(),
            );
            assert_eq!(res, KVGetStatus::Error);
            assert!(!error_msg.as_ref().is_null());
            assert_eq!(
                error_msg.as_str(),
                "KV engine is not initialized".as_bytes()
            );
        }
    }

    cluster.raw.shutdown();
    panic!();
}
