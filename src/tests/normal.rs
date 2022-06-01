// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_store_ffi::{KVGetStatus, RaftStoreProxyFFI};
use mock_engine_store::node::NodeCluster;
use mock_engine_store::server::ServerCluster;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use test_raftstore::{must_get_equal, must_get_none, new_peer, TestPdClient};

extern crate rocksdb;
use crate::normal::rocksdb::Writable;
use ::rocksdb::DB;
use engine_store_ffi::config::ensure_no_common_unrecognized_keys;
use engine_tiflash::*;
use engine_traits::Iterable;
use engine_traits::Iterator;
use engine_traits::MiscExt;
use engine_traits::Peekable;
use engine_traits::SeekKey;
use engine_traits::{Error, Result};
use engine_traits::{ExternalSstFileInfo, SstExt, SstReader, SstWriter, SstWriterBuilder};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use kvproto::raft_serverpb::{RaftApplyState, RegionLocalState, StoreIdent};
use pd_client::PdClient;
use sst_importer::SSTImporter;
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::Once;
use tikv::config::TiKvConfig;
use tikv_util::config::{LogFormat, ReadableDuration, ReadableSize};
use tikv_util::HandyRwLock;

#[test]
fn test_config() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    let text = "memory-usage-high-water=0.65\nsnap-handle-pool-size=4\n[nosense]\nfoo=2\n[rocksdb]\nmax-open-files = 111\nz=1";
    write!(file, "{}", text);
    let path = file.path();

    let mut unrecognized_keys = Vec::new();
    let mut config = TiKvConfig::from_file(path, Some(&mut unrecognized_keys)).unwrap();
    assert_eq!(config.memory_usage_high_water, 0.65);
    assert_eq!(config.rocksdb.max_open_files, 111);
    assert_eq!(unrecognized_keys.len(), 3);

    let mut proxy_unrecognized_keys = Vec::new();
    let proxy_config =
        engine_store_ffi::config::ProxyConfig::from_file(path, Some(&mut proxy_unrecognized_keys))
            .unwrap();
    assert_eq!(proxy_config.snap_handle_pool_size, 4);
    let unknown = ensure_no_common_unrecognized_keys(
        &vec!["a.b", "b"].iter().map(|e| String::from(*e)).collect(),
        &vec!["a.b", "b.b", "c"]
            .iter()
            .map(|e| String::from(*e))
            .collect(),
    );
    assert_eq!(unknown.is_err(), true);
    assert_eq!(unknown.unwrap_err(), "a.b, b.b");
    let unknown = ensure_no_common_unrecognized_keys(&proxy_unrecognized_keys, &unrecognized_keys);
    assert_eq!(unknown.is_err(), true);
    assert_eq!(unknown.unwrap_err(), "nosense, rocksdb.z");

    // Need ENGINE_LABEL_VALUE=tiflash, otherwise will fatal exit.
    server::setup::validate_and_persist_config(&mut config, true);

    // Will not override ProxyConfig
    let proxy_config_new = engine_store_ffi::config::ProxyConfig::from_file(path, None).unwrap();
    assert_eq!(proxy_config_new.snap_handle_pool_size, 4);
}

#[test]
fn test_store_setup() {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = mock_engine_store::mock_cluster::Cluster::new(0, 3, sim, pd_client.clone());

    // Add label to cluster
    engine_store_ffi::config::address_proxy_config(&mut cluster.raw.cfg.tikv);

    // Try to start this node, return after persisted some keys.
    let _ = cluster.start();
    let store_id = cluster.raw.engines.keys().last().unwrap();
    let store = pd_client.get_store(*store_id).unwrap();
    println!("store {:?}", store);
    assert!(store
        .get_labels()
        .iter()
        .find(|&x| x.key == "engine" && x.value == "tiflash")
        .is_some());

    cluster.raw.shutdown();
}

#[test]
fn test_normal() {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = mock_engine_store::mock_cluster::Cluster::new(0, 3, sim, pd_client);

    fail::cfg("on_address_apply_result_normal", "return").unwrap();
    fail::cfg("on_address_apply_result_admin", "return").unwrap();

    // Try to start this node, return after persisted some keys.
    let _ = cluster.start();

    for i in 0..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.raw.must_put(k.as_bytes(), v.as_bytes());
        for id in cluster.raw.engines.keys() {
            let engine = &cluster.raw.get_engine(*id);
            // We can get nothing, since engine_tiflash filters all data.
            must_get_none(engine, k.as_bytes());
        }
    }

    // We can read initial raft state
    let mut prev_apply_state: HashMap<u64, RaftApplyState> = HashMap::default();
    for id in cluster.raw.engines.keys() {
        let r1 = cluster.raw.get_region(b"k1").get_id();
        let db = cluster.raw.get_engine(*id);
        let engine = engine_rocks::RocksEngine::from_db(db);
        // We can still get RegionLocalState
        let region_state_key = keys::region_state_key(r1);
        match engine.get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key) {
            Ok(Some(_)) => (),
            _ => unreachable!(),
        };

        let region_state_key = keys::apply_state_key(r1);
        let apply_state = match engine.get_msg_cf::<RaftApplyState>(CF_RAFT, &region_state_key) {
            Ok(Some(s)) => s,
            _ => unreachable!(),
        };
        prev_apply_state.insert(*id, apply_state);

        match engine.get_msg::<StoreIdent>(keys::STORE_IDENT_KEY) {
            Ok(Some(_)) => (),
            _ => unreachable!(),
        };
    }

    fail::remove("on_address_apply_result_normal");
    fail::remove("on_address_apply_result_admin");
    for i in 10..20 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.raw.must_put(k.as_bytes(), v.as_bytes());
        let region_id = cluster.raw.get_region(k.as_bytes()).get_id();
        for id in cluster.raw.engines.keys() {
            let engine = &cluster.raw.get_engine(*id);
            // We can get nothing, since engine_tiflash filters all data.
            must_get_equal(engine, k.as_bytes(), v.as_bytes());
            let mut locked = cluster.ffi_helper_set.lock().unwrap();
            let mocker = locked.get_mut().get(&id).unwrap();
            let region = mocker.engine_store_server.kvstore.get(&region_id).unwrap();
            let bmap = &region.data
                [engine_store_ffi::interfaces::root::DB::ColumnFamilyType::Default as usize];
            let ks = k.as_bytes().to_vec();
            println!("!!!! open H {} ks {:?}", id, ks);
            assert!(bmap.get(&ks).is_some());
        }
    }

    for id in cluster.raw.engines.keys() {
        let r1 = cluster.raw.get_region(b"k1").get_id();
        let db = cluster.raw.get_engine(*id);
        let engine = engine_rocks::RocksEngine::from_db(db);

        let region_state_key = keys::apply_state_key(r1);
        let apply_state = match engine.get_msg_cf::<RaftApplyState>(CF_RAFT, &region_state_key) {
            Ok(Some(s)) => s,
            _ => unreachable!(),
        };

        // assert_eq!(&apply_state, prev_apply_state.get(&id).unwrap());
    }

    // get RegionLocalState through ffi
    let k = "k1".as_bytes();
    let region_id = cluster.raw.get_region(k).get_id();
    unsafe {
        for (_, ffi_set) in cluster.ffi_helper_set.lock().unwrap().get_mut().iter_mut() {
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
}

#[test]
fn test_huge_snapshot() {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = mock_engine_store::mock_cluster::Cluster::new(0, 3, sim, pd_client.clone());

    cluster.raw.cfg.raft_store.raft_log_gc_count_limit = 1000;
    cluster.raw.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(10);
    cluster.raw.cfg.raft_store.snap_apply_batch_size = ReadableSize(500);

    // Disable default max peer count check.
    pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();

    let first_value = vec![0; 10240];
    // at least 4m data
    for i in 0..400 {
        let key = format!("{:03}", i);
        cluster.raw.must_put(key.as_bytes(), &first_value);
    }
    let first_key: &[u8] = b"000";

    let eng_ids = cluster.raw.engines.iter().map(|e| e.0).collect::<Vec<_>>();
    tikv_util::info!("engine_2 is {}", *eng_ids[1]);
    let engine_2 = cluster.raw.get_engine(*eng_ids[1]);
    must_get_none(&engine_2, first_key);
    // add peer (engine_2,engine_2) to region 1.
    pd_client.must_add_peer(r1, new_peer(*eng_ids[1], *eng_ids[1]));

    let (key, value) = (b"k2", b"v2");
    cluster.raw.must_put(key, value);
    assert_eq!(cluster.raw.get(key), Some(value.to_vec()));
    must_get_equal(&engine_2, key, value);
    // now snapshot must be applied on peer engine_2;
    must_get_equal(&engine_2, first_key, first_value.as_slice());

    cluster.raw.shutdown();
}
