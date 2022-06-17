// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_store_ffi::{KVGetStatus, RaftStoreProxyFFI};
use mock_engine_store::node::NodeCluster;
use mock_engine_store::server::ServerCluster;
use std::collections::HashMap;
use std::sync::{Arc, RwLock, mpsc};
use test_raftstore::{must_get_equal, must_get_none, new_peer, TestPdClient};

extern crate rocksdb;
use crate::normal::rocksdb::Writable;
use ::rocksdb::DB;
use engine_store_ffi::config::ensure_no_common_unrecognized_keys;
use engine_tiflash::*;
use engine_traits::{Iterable, WriteBatchExt, Mutable, WriteBatch};
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
use test_raftstore::Simulator;
use mock_engine_store::transport_simulate::{CloneFilterFactory, RegionPacketFilter, CollectSnapshotFilter};
use tikv_util::time::Duration;
use raft::eraftpb::MessageType;
use mock_engine_store::transport_simulate::Direction;
use std::sync::atomic::{AtomicUsize, Ordering};
use kvproto::raft_cmdpb::{AdminRequest, AdminCmdType};
use raftstore::coprocessor::{ConsistencyCheckMethod, Coprocessor};
use raftstore::store::util::find_peer;
use std::ops::Deref;

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
fn test_store_stats() {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = mock_engine_store::mock_cluster::Cluster::new(0, 1, sim, pd_client.clone());

    let _ = cluster.run();

    for id in cluster.raw.engines.keys() {
        let store_stats = pd_client.get_store_stats(*id);
        assert!(store_stats.is_some());
    }

    cluster.raw.shutdown();
}

#[test]
fn test_write_batch() {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = mock_engine_store::mock_cluster::Cluster::new(0, 1, sim, pd_client);

    let _ = cluster.run();

    for id in cluster.raw.engines.keys() {
        let db = cluster.raw.get_engine(*id);
        let engine = engine_rocks::RocksEngine::from_db(db.clone());
        let mut wb = engine.write_batch();
        wb.put(b"k2", b"v2");
        let res = db.get(b"k2");
        assert!(res.is_ok());
        assert!(res.unwrap().is_none());
        wb.write();
        let res = db.get(b"k2");
        let v = res.unwrap().unwrap();
        assert_eq!(v.deref(), b"v2");
        break;
    }

    cluster.raw.shutdown();
}

pub fn must_get_mem(engine_store_server: &Box<mock_engine_store::EngineStoreServer>, region_id: u64, key: &[u8], value: Option<&[u8]>) {
    let mut last_res: Option<&Vec<u8>> = None;
    for _ in 1..300 {
        let res = engine_store_server
            .get_mem(region_id, mock_engine_store::ffi_interfaces::ColumnFamilyType::Default, &key.to_vec());

        if let (Some(value), Some(last_res)) = (value, res) {
            assert_eq!(value, &last_res[..]);
            return;
        }
        if value.is_none() && last_res.is_none() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    panic!(
        "can't get mem value {:?} for key {} in {}, actual {:?}",
        value.map(tikv_util::escape),
        log_wrappers::hex_encode_upper(key),
        engine_store_server.id,
        last_res,
    )
}

pub fn check_key(cluster: &mock_engine_store::mock_cluster::Cluster<NodeCluster>,
             k: &[u8], v: &[u8], in_mem: Option<bool>, in_disk: Option<bool>, engines: Option<Vec<u64>>) {
    let region_id = cluster.raw.get_region(k).get_id();
    let engine_keys = {
        match engines {
            Some(e) => e.to_vec(),
            None => {
                cluster.raw.engines.keys().map(|k| *k).collect::<Vec<u64>>()
            },
        }
    };
    for id in engine_keys {
        let engine = &cluster.raw.get_engine(id);

        match in_disk {
            Some(b) => {
                if b {
                    must_get_equal(engine, k, v);
                } else {
                    must_get_none(engine, k);
                }
            },
            None => ()
        };

        match in_mem {
            Some(b) => {
                let mut lock = cluster
                    .ffi_helper_set.lock().unwrap();
                let server = &lock.get_mut().get(&id).unwrap()
                    .engine_store_server;
                if b {
                    must_get_mem(server, region_id, k, Some(v));
                } else {
                    must_get_mem(server, region_id, k, None);
                }
            },
            None => ()
        };
    }
}

#[test]
fn test_leadership_change() {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = mock_engine_store::mock_cluster::Cluster::new(0, 3, sim, pd_client);

    let _ = cluster.run();

    cluster.raw.must_put(b"k1", b"v1");
    let region = cluster.raw.get_region(b"k1");
    let region_id = region.get_id();

    let eng_ids = cluster.raw.engines.iter().map(|e| e.0.to_owned()).collect::<Vec<_>>();
    let peer_1 = find_peer(&region, eng_ids[0]).cloned().unwrap();
    let peer_2 = find_peer(&region, eng_ids[1]).cloned().unwrap();
    cluster.raw.must_transfer_leader(region.get_id(), peer_1.clone());

    cluster.raw.must_put(b"k2", b"v2");
    fail::cfg("on_empty_cmd_normal", "return");

    let prev_states = collect_all_states(&cluster, region_id);
    cluster.raw.must_transfer_leader(region.get_id(), peer_2.clone());

    let new_states = collect_all_states(&cluster, region_id);
    for i in prev_states.keys() {
        let old = prev_states.get(i).unwrap();
        let new = new_states.get(i).unwrap();
        assert_eq!(old.in_memory_apply_state, new.in_memory_apply_state);
        assert_eq!(old.in_memory_applied_term, new.in_memory_applied_term);
    }

    fail::remove("on_empty_cmd_normal");
    // We need forward empty cmd generated by leadership changing to TiFlash.
    cluster.raw.must_transfer_leader(region.get_id(), peer_1.clone());
    let new_states = collect_all_states(&cluster, region_id);
    for i in prev_states.keys() {
        let old = prev_states.get(i).unwrap();
        let new = new_states.get(i).unwrap();
        assert_ne!(old.in_memory_apply_state, new.in_memory_apply_state);
        assert_ne!(old.in_memory_applied_term, new.in_memory_applied_term);
    }

    cluster.raw.shutdown();
}

fn get_region_local_state(engine: &engine_rocks::RocksEngine, region_id: u64) -> RegionLocalState {
    let region_state_key = keys::region_state_key(region_id);
    let region_state = match engine.get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key) {
        Ok(Some(s)) => s,
        _ => unreachable!(),
    };
    region_state
}

struct States {
    in_memory_apply_state: RaftApplyState,
    in_memory_applied_term: u64,
    in_disk_apply_state: RaftApplyState,
    in_disk_region_state: RegionLocalState,
    ident: StoreIdent,
}

fn collect_all_states(cluster: &mock_engine_store::mock_cluster::Cluster<NodeCluster>,
                      region_id: u64) -> HashMap<u64, States> {
    let mut prev_state: HashMap<u64, States> = HashMap::default();
    for id in cluster.raw.engines.keys() {
        let db = cluster.raw.get_engine(*id);
        let engine = engine_rocks::RocksEngine::from_db(db);


        let mut lock = cluster
            .ffi_helper_set.lock().unwrap();
        let server = &lock.get_mut().get(id).unwrap()
            .engine_store_server;

        let region = server.kvstore.get(&region_id).unwrap();

        let ident = match engine.get_msg::<StoreIdent>(keys::STORE_IDENT_KEY) {
            Ok(Some(i)) => (i),
            _ => unreachable!(),
        };

        prev_state.insert(*id, States {
            in_memory_apply_state: region.apply_state.clone(),
            in_memory_applied_term: region.applied_term,
            in_disk_apply_state: get_apply_state(&engine, region_id),
            in_disk_region_state: get_region_local_state(&engine, region_id),
            ident: ident
        });
    }
    prev_state
}

#[test]
fn test_kv_write() {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = mock_engine_store::mock_cluster::Cluster::new(0, 3, sim, pd_client);

    fail::cfg("on_address_apply_result_normal", "return(false)").unwrap();
    fail::cfg("on_address_apply_result_admin", "return(false)").unwrap();
    fail::cfg("on_handle_admin_raft_cmd_no_persist", "return").unwrap();

    // Try to start this node, return after persisted some keys.
    let _ = cluster.run();

    for i in 0..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.raw.must_put(k.as_bytes(), v.as_bytes());
    }

    // Since we disable all observers, we can get nothing in either memory and disk.
    for i in 0..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(false), Some(false), None);
    }

    // We can read initial raft state, since we don't persist meta either.
    let r1 = cluster.raw.get_region(b"k1").get_id();
    let prev_states = collect_all_states(&cluster, r1);

    fail::remove("on_address_apply_result_normal");
    fail::remove("on_address_apply_result_admin");
    for i in 10..20 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.raw.must_put(k.as_bytes(), v.as_bytes());
    }

    // Since we enable all observers, we can get in memory, but nothing in disk since we don't persist.
    for i in 10..20 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(true), Some(false), None);
    }

    let new_states = collect_all_states(&cluster, r1);
    for id in cluster.raw.engines.keys() {
        assert_ne!(&prev_states.get(id).unwrap().in_memory_apply_state,
                   &new_states.get(id).unwrap().in_memory_apply_state);
        assert_eq!(&prev_states.get(id).unwrap().in_disk_apply_state,
                   &new_states.get(id).unwrap().in_disk_apply_state);
    }

    fail::remove("on_handle_admin_raft_cmd_no_persist");


    let prev_states = collect_all_states(&cluster, r1);
    // Write more after we force persist when compact log.
    for i in 20..30 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.raw.must_put(k.as_bytes(), v.as_bytes());
    }

    // We can read from mock-store's memory, we are not sure if we can read from disk,
    // since there may be or may not be a compact log.
    for i in 11..30 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(true), None, None);
    }

    // Force a compact log to persist.
    let region_r = cluster.raw.get_region("k1".as_bytes());
    let region_id = region_r.get_id();
    let compact_log = test_raftstore::new_compact_log_request(100, 10);
    let req = test_raftstore::new_admin_request(region_id, region_r.get_region_epoch(), compact_log);
    let res = cluster
        .raw
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(res.get_header().has_error(), "{:?}", res);

    for i in 11..30 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(true), Some(true), None);
    }

    let new_states = collect_all_states(&cluster, r1);

    // apply_state is changed in memory, and persisted.
    for id in cluster.raw.engines.keys() {
        assert_ne!(&prev_states.get(id).unwrap().in_memory_apply_state,
                   &new_states.get(id).unwrap().in_memory_apply_state);
        assert_ne!(&prev_states.get(id).unwrap().in_disk_apply_state,
                   &new_states.get(id).unwrap().in_disk_apply_state);
    }

    fail::remove("on_handle_admin_raft_cmd_no_persist");
    cluster.raw.shutdown();
}

fn get_apply_state(engine: &engine_rocks::RocksEngine, region_id: u64) -> RaftApplyState {
    let apply_state_key = keys::apply_state_key(region_id);
    let apply_state = match engine.get_msg_cf::<RaftApplyState>(CF_RAFT, &apply_state_key) {
        Ok(Some(s)) => s,
        _ => unreachable!(),
    };
    apply_state
}

pub fn new_compute_hash_request() -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::ComputeHash);
    req.mut_compute_hash().set_context(vec![ConsistencyCheckMethod::Raw as u8]);
    req
}

pub fn new_verify_hash_request(hash: Vec<u8>, index: u64) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::VerifyHash);
    req.mut_verify_hash().set_hash(hash);
    req.mut_verify_hash().set_index(index);
    req
}

#[test]
fn test_consistency_check() {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = mock_engine_store::mock_cluster::Cluster::new(0, 2, sim, pd_client.clone());

    cluster.run();

    cluster.raw.must_put(b"k", b"v");
    let region = cluster.raw.get_region("k".as_bytes());
    let region_id = region.get_id();

    let r = new_verify_hash_request(vec![1,2,3,4,5,6], 1000);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), r);
    let res = cluster
        .raw
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();

    let r = new_verify_hash_request(vec![7,8,9,0], 1000);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), r);
    let res = cluster
        .raw
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();

    cluster.raw.must_put(b"k2", b"v2");
    cluster.raw.shutdown();
}

#[test]
fn test_compact_log() {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = mock_engine_store::mock_cluster::Cluster::new(0, 3, sim, pd_client.clone());

    cluster.run();

    cluster.raw.must_put(b"k", b"v");
    let region = cluster.raw.get_region("k".as_bytes());
    let region_id = region.get_id();

    fail::cfg("can_flush_data", "return(0)");
    for i in 0..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.raw.must_put(k.as_bytes(), v.as_bytes());
    }

    let mut hmap: HashMap<u64, RaftApplyState> = Default::default();
    for id in cluster.raw.engines.keys() {
        let mut lock = cluster
            .ffi_helper_set.lock().unwrap();
        let server = &lock.get_mut().get(id).unwrap()
            .engine_store_server;
        let apply_state = get_apply_state(&server.engines.as_ref().unwrap().kv.rocks, region_id);
        hmap.insert(*id, apply_state);
    }

    let compact_log = test_raftstore::new_compact_log_request(100, 10);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
    let res = cluster
        .raw
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();

    // compact log is filtered
    for id in cluster.raw.engines.keys() {
        let mut lock = cluster
            .ffi_helper_set.lock().unwrap();
        let server = &lock.get_mut().get(id).unwrap()
            .engine_store_server;
        let apply_state = get_apply_state(&server.engines.as_ref().unwrap().kv.rocks, region_id);
        assert_eq!(apply_state.get_truncated_state(), hmap.get(id).unwrap().get_truncated_state());
    }

    fail::remove("can_flush_data");
    let compact_log = test_raftstore::new_compact_log_request(100, 10);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
    let res = cluster
        .raw
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(res.get_header().has_error(), "{:?}", res);

    cluster.raw.must_put(b"kz", b"vz");
    check_key(&cluster, b"kz", b"vz", Some(true), None, None);

    // compact log is not filtered
    for id in cluster.raw.engines.keys() {
        let mut lock = cluster
            .ffi_helper_set.lock().unwrap();
        let server = &lock.get_mut().get(id).unwrap()
            .engine_store_server;
        let apply_state = get_apply_state(&server.engines.as_ref().unwrap().kv.rocks, region_id);
        assert_ne!(apply_state.get_truncated_state(), hmap.get(id).unwrap().get_truncated_state());
    }

    cluster.raw.shutdown();
}

#[test]
fn test_split_merge() {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = mock_engine_store::mock_cluster::Cluster::new(0, 3, sim, pd_client.clone());

    // can always apply snapshot immediately
    fail::cfg("on_can_apply_snapshot", "return(true)");
    cluster.raw.cfg.raft_store.right_derive_when_split = true;

    // May fail if cluster.start, since node 2 is not in region1.peers(),
    // and node 2 has not bootstrap region1,
    // because region1 is not bootstrap if we only call cluster.start()
    cluster.run();

    cluster.raw.must_put(b"k1", b"v1");
    cluster.raw.must_put(b"k3", b"v3");

    check_key(&cluster, b"k1", b"v1", Some(true), None, None);
    check_key(&cluster, b"k3", b"v3", Some(true), None, None);

    let r1 = cluster.raw.get_region(b"k1");
    let r3 = cluster.raw.get_region(b"k3");
    assert_eq!(r1.get_id(), r3.get_id());

    cluster.raw.must_split(&r1, b"k2");
    let r1_new = cluster.raw.get_region(b"k1");
    let r3_new = cluster.raw.get_region(b"k3");

    assert_eq!(r1.get_id(), r3_new.get_id());

    for id in cluster.raw.engines.keys() {
        let mut lock = cluster
            .ffi_helper_set.lock().unwrap();
        let server = &lock.get_mut().get(id).unwrap()
            .engine_store_server;
        if !server.kvstore.contains_key(&r1_new.get_id()) {
            panic!("node {} has no region {}", id, r1_new.get_id())
        }
        if !server.kvstore.contains_key(&r3_new.get_id()) {
            panic!("node {} has no region {}", id, r3_new.get_id())
        }
        // Region meta must equal
        assert_eq!(server.kvstore.get(&r1_new.get_id()).unwrap().region, r1_new);
        assert_eq!(server.kvstore.get(&r3_new.get_id()).unwrap().region, r3_new);

        // Can get from disk
        check_key(&cluster, b"k1", b"v1", None, Some(true), None);
        check_key(&cluster, b"k3", b"v3", None, Some(true), None);
        // TODO Region in memory data must not contradict, but now we do not delete data
    }

    pd_client.must_merge(r1_new.get_id(), r3_new.get_id());
    let r1_new2 = cluster.raw.get_region(b"k1");
    let r3_new2 = cluster.raw.get_region(b"k3");

    for id in cluster.raw.engines.keys() {
        let mut lock = cluster
            .ffi_helper_set.lock().unwrap();
        let server = &lock.get_mut().get(id).unwrap()
            .engine_store_server;

        // The left region is removed
        if server.kvstore.contains_key(&r1_new.get_id()) {
            panic!("node {} should has no region {}", id, r1_new.get_id())
        }
        if !server.kvstore.contains_key(&r3_new.get_id()) {
            panic!("node {} has no region {}", id, r3_new.get_id())
        }
        // Region meta must equal
        assert_eq!(server.kvstore.get(&r3_new2.get_id()).unwrap().region, r3_new2);

        // Can get from disk
        check_key(&cluster, b"k1", b"v1", None, Some(true), None);
        check_key(&cluster, b"k3", b"v3", None, Some(true), None);
        // TODO Region in memory data must not contradict, but now we do not delete data

        let origin_epoch = r3_new.get_region_epoch();
        let new_epoch = r3_new2.get_region_epoch();
        tikv_util::debug!("!!!! origin {:?} new {:?}", r3_new, r3_new2);
        // PrepareMerge + CommitMerge, so it should be 2.
        assert_eq!(new_epoch.get_version(), origin_epoch.get_version() + 2);
        assert_eq!(new_epoch.get_conf_ver(), origin_epoch.get_conf_ver());
    }

    fail::remove("on_can_apply_snapshot");
    cluster.raw.shutdown();
}

#[test]
fn test_get_region_local_state() {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = mock_engine_store::mock_cluster::Cluster::new(0, 3, sim, pd_client);

    cluster.start();

    let k = b"k1";
    let v = b"v1";
    cluster.raw.must_put(k, v);
    for id in cluster.raw.engines.keys() {
        must_get_equal(&cluster.raw.get_engine(*id), k, v);
    }
    let region_id = cluster.raw.get_region(k).get_id();

    // Get RegionLocalState through ffi
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

    fail::cfg("on_can_apply_snapshot", "return(true)");
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

    let eng_ids = cluster.raw.engines.iter().map(|e| e.0.to_owned()).collect::<Vec<_>>();
    tikv_util::info!("engine_2 is {}", eng_ids[1]);
    let engine_2 = cluster.raw.get_engine(eng_ids[1]);
    must_get_none(&engine_2, first_key);
    // add peer (engine_2,engine_2) to region 1.
    pd_client.must_add_peer(r1, new_peer(eng_ids[1], eng_ids[1]));

    let (key, value) = (b"k2", b"v2");
    cluster.raw.must_put(key, value);
    // we can get in memory, since snapshot is pre handled, though it is not persisted
    check_key(&cluster, key, value, Some(true), None, Some(vec![eng_ids[1]]));
    // now snapshot must be applied on peer engine_2
    must_get_equal(&engine_2, first_key, first_value.as_slice());

    fail::cfg("on_ob_post_apply_snapshot", "return").unwrap();

    tikv_util::info!("engine_3 is {}", eng_ids[2]);
    let engine_3 = cluster.raw.get_engine(eng_ids[2]);
    must_get_none(&engine_3, first_key);
    pd_client.must_add_peer(r1, new_peer(eng_ids[2], eng_ids[2]));

    let (key, value) = (b"k3", b"v3");
    cluster.raw.must_put(key, value);
    check_key(&cluster, key, value, Some(true), None, None);
    // We have not persist pre handled snapshot,
    // we can't be sure if it exists in only get from memory too, since pre handle snapshot is async.
    must_get_none(&engine_3, first_key);

    fail::remove("on_ob_post_apply_snapshot");
    fail::remove("on_can_apply_snapshot");

    cluster.raw.shutdown();
}

#[test]
fn test_concurrent_snapshot() {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = mock_engine_store::mock_cluster::Cluster::new(0, 3, sim, pd_client.clone());

    // Disable raft log gc in this test case.
    cluster.raw.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::secs(60);

    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    cluster.raw.must_put(b"k1", b"v1");
    pd_client.must_add_peer(r1, new_peer(2, 2));
    // Force peer 2 to be followers all the way.
    cluster.raw.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(r1, 2)
            .msg_type(MessageType::MsgRequestVote)
            .direction(Direction::Send),
    ));
    cluster.raw.must_transfer_leader(r1, new_peer(1, 1));
    cluster.raw.must_put(b"k3", b"v3");
    // Pile up snapshots of overlapped region ranges and deliver them all at once.
    let (tx, rx) = mpsc::channel();
    cluster
        .raw
        .sim
        .wl()
        .add_recv_filter(3, Box::new(CollectSnapshotFilter::new(tx)));
    pd_client.must_add_peer(r1, new_peer(3, 3));
    let region = cluster.raw.get_region(b"k1");
    // Ensure the snapshot of range ("", "") is sent and piled in filter.
    if let Err(e) = rx.recv_timeout(Duration::from_secs(1)) {
        panic!("the snapshot is not sent before split, e: {:?}", e);
    }

    // Split the region range and then there should be another snapshot for the split ranges.
    cluster.raw.must_split(&region, b"k2");
    must_get_equal(&cluster.raw.get_engine(3), b"k3", b"v3");

    // // Ensure the regions work after split.
    // cluster.raw.must_put(b"k11", b"v11");
    // must_get_equal(&cluster.raw.get_engine(3), b"k11", b"v11");
    // cluster.raw.must_put(b"k4", b"v4");
    // must_get_equal(&cluster.raw.get_engine(3), b"k4", b"v4");

    cluster.raw.shutdown();
}

#[test]
fn test_concurrent_snapshot2() {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = mock_engine_store::mock_cluster::Cluster::new(0, 3, sim, pd_client.clone());

    // Disable raft log gc in this test case.
    cluster.raw.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::secs(60);

    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    cluster.raw.must_put(b"k1", b"v1");
    cluster.raw.must_put(b"k3", b"v3");

    let region1 = cluster.raw.get_region(b"k1");
    cluster.raw.must_split(&region1, b"k2");
    let r1 = cluster.raw.get_region(b"k1").get_id();
    let r3 = cluster.raw.get_region(b"k3").get_id();

    fail::cfg("before_actually_pre_handle", "sleep(1000)");
    tikv_util::info!("region k1 {} k3 {}", r1, r3);
    pd_client.add_peer(r1, new_peer(2, 2));
    pd_client.add_peer(r3, new_peer(2, 2));
    std::thread::sleep(std::time::Duration::from_millis(500));
    // Now, k1 and k3 are not handled, since pre-handle process is not finished.

    let pending_count = cluster.raw.engines.get(&2).unwrap().kv.pending_applies_count.clone();
    assert_eq!(pending_count.load(Ordering::Relaxed), 2);
    std::thread::sleep(std::time::Duration::from_millis(1000));
    // Now, k1 and k3 are handled.
    assert_eq!(pending_count.load(Ordering::Relaxed), 0);

    cluster.raw.shutdown();
}

#[test]
fn test_prehandle_fail() {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = mock_engine_store::mock_cluster::Cluster::new(0, 3, sim, pd_client.clone());

    // Disable raft log gc in this test case.
    cluster.raw.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::secs(60);

    // Disable default max peer count check.
    pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();
    cluster.raw.must_put(b"k1", b"v1");

    let eng_ids = cluster.raw.engines.iter().map(|e| e.0.to_owned()).collect::<Vec<_>>();
    fail::cfg("before_actually_pre_handle", "return");
    pd_client.add_peer(r1, new_peer(eng_ids[1], eng_ids[1]));
    check_key(&cluster, b"k1", b"v1", Some(true), Some(true), Some(vec![eng_ids[1]]));
    fail::remove("before_actually_pre_handle");

    cluster.raw.shutdown();
}


#[test]
fn test_handle_destroy() {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = mock_engine_store::mock_cluster::Cluster::new(0, 3, sim, pd_client.clone());

    // Disable raft log gc in this test case.
    cluster.raw.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::secs(60);

    // Disable default max peer count check.
    pd_client.disable_default_operator();

    cluster.run();
    cluster.raw.must_put(b"k1", b"v1");
    let eng_ids = cluster.raw.engines.iter().map(|e| e.0.to_owned()).collect::<Vec<_>>();


    let region = cluster.raw.get_region(b"k1");
    let region_id = region.get_id();
    let peer_1 = find_peer(&region, eng_ids[0]).cloned().unwrap();
    let peer_2 = find_peer(&region, eng_ids[1]).cloned().unwrap();
    cluster.raw.must_transfer_leader(region_id, peer_1);

    {
        let mut lock = cluster
            .ffi_helper_set.lock().unwrap();
        let server = &lock.get_mut().get(&eng_ids[1]).unwrap()
            .engine_store_server;

        assert!(server.kvstore.contains_key(&region_id));
    }

    pd_client.must_remove_peer(region_id, peer_2);

    check_key(&cluster, b"k1", b"k2", Some(false), None, Some(vec![eng_ids[1]]));

    {
        let mut lock = cluster
            .ffi_helper_set.lock().unwrap();
        let server = &lock.get_mut().get(&eng_ids[1]).unwrap()
            .engine_store_server;

        assert!(!server.kvstore.contains_key(&region_id));
    }

    cluster.raw.shutdown();
}