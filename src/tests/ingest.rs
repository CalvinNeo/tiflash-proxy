use engine_store_ffi::{KVGetStatus, RaftStoreProxyFFI};
use mock_engine_store::node::NodeCluster;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use test_raftstore::{must_get_equal, must_get_none, TestPdClient};

extern crate rocksdb;
use ::rocksdb::DB;
use engine_tiflash::*;
use engine_traits::{Iterable, MiscExt};
use engine_traits::Iterator;
use engine_traits::Peekable;
use engine_traits::SeekKey;
use engine_traits::{Error, Result};
use engine_traits::{ExternalSstFileInfo, SstExt, SstReader, SstWriter, SstWriterBuilder};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use kvproto::raft_serverpb::{RaftApplyState, RegionLocalState, StoreIdent};

use std::{fs::File, time::Duration};

use external_storage_export::{create_storage, make_local_backend};
use futures::{executor::block_on, AsyncReadExt, StreamExt};
use kvproto::import_sstpb::*;
use kvproto::kvrpcpb::*;
use tempfile::Builder;
use tikv_util::HandyRwLock;

use txn_types::TimeStamp;
use std::path::{Path, PathBuf};
use test_sst_importer::gen_sst_file_with_kvs;
use kvproto::raft_cmdpb::{
    AdminCmdType, AdminRequest, CmdType, RaftCmdRequest,
    RaftCmdResponse, Request, StatusCmdType, StatusRequest,
};
use kvproto::import_sstpb::SstMeta;
use sst_importer::SSTImporter;
use tempfile::TempDir;
use sst_importer::Config as ImportConfig;
use test_raftstore::Config;

pub fn new_ingest_sst_cmd(meta: SstMeta) -> Request {
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::IngestSst);
    cmd.mut_ingest_sst().set_sst(meta);
    cmd
}

pub fn create_tmp_importer(cfg: &Config, kv_path: &str) -> (PathBuf, Arc<SSTImporter>) {
    let dir = Path::new(kv_path).join("import-sst");
    let importer = {
        Arc::new(SSTImporter::new(&cfg.import, dir.clone(), None, cfg.storage.api_version()).unwrap())
    };
    (dir, importer)
}

#[test]
fn test_handle_ingest_sst() {
    let (mut cluster, pd_client) = crate::normal::new_mock_cluster(0, 1);

    cluster.run();

    let key = "k";
    cluster.raw.must_put(key.as_bytes(), b"v");
    let region = cluster.raw.get_region(key.as_bytes());

    let path = cluster.raw.engines.iter().last().unwrap().1.kv.rocks.path();

    let (import_dir, importer) = create_tmp_importer(&cluster.raw.cfg, path);

    let keys_count = 100;
    let mut kvs: Vec<(&[u8], &[u8])> = Vec::new();
    let mut keys = Vec::new();
    for i in 0..keys_count {
        keys.push(format!("k{}", i))
    }
    keys.sort();
    for i in 0..keys_count {
        kvs.push((keys[i].as_bytes(), b"2"));
    }

    let sst_path = import_dir.join("test.sst");
    let (mut meta, data) = gen_sst_file_with_kvs(&sst_path, &kvs);
    meta.set_region_id(region.get_id());
    meta.set_region_epoch(region.get_region_epoch().clone());
    // meta.mut_region_epoch().set_conf_ver(1);
    // meta.mut_region_epoch().set_version(3);
    let mut file = importer.create(&meta).unwrap();
    file.append(&data).unwrap();
    file.finish().unwrap();
    let src = sst_path.clone();
    let dst = file.path.save.to_str().unwrap();
    std::fs::copy(src.clone(), dst);

    let req = new_ingest_sst_cmd(meta);
    let resp = cluster.raw.request(
        key.as_bytes(),
        vec![req],
        false,
        Duration::from_secs(5),
    );

    crate::normal::check_key(&cluster, b"k66", b"2", None, Some(true), None);

    cluster.raw.shutdown();
}