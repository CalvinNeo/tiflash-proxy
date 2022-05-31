// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use crate::util;
use engine_traits::ImportExt;
use engine_traits::IngestExternalFileOptions;
use engine_traits::Result;
use rocksdb::set_external_sst_file_global_seq_no;
use rocksdb::IngestExternalFileOptions as RawIngestExternalFileOptions;
use std::fs::File;

impl ImportExt for RocksEngine {
    type IngestExternalFileOptions = RocksIngestExternalFileOptions;

    fn ingest_external_file_cf(&self, cf: &str, files: &[&str]) -> Result<()> {
        // do nothing
        return Ok(());
    }
}

pub struct RocksIngestExternalFileOptions(RawIngestExternalFileOptions);

impl IngestExternalFileOptions for RocksIngestExternalFileOptions {
    fn new() -> RocksIngestExternalFileOptions {
        RocksIngestExternalFileOptions(RawIngestExternalFileOptions::new())
    }

    fn move_files(&mut self, f: bool) {
        self.0.move_files(f);
    }

    fn get_write_global_seqno(&self) -> bool {
        self.0.get_write_global_seqno()
    }

    fn set_write_global_seqno(&mut self, f: bool) {
        self.0.set_write_global_seqno(f);
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use crate::engine::RocksEngine;
    use crate::raw::{ColumnFamilyOptions, DBOptions};
    use crate::raw_util::{new_engine_opt, CFOptions};
    use std::sync::Arc;

    use super::*;
    use crate::RocksSstWriterBuilder;
    use engine_traits::{
        FlowControlFactorsExt, Mutable, SstWriter, SstWriterBuilder, WriteBatchExt,
    };
    use engine_traits::{MiscExt, WriteBatch, ALL_CFS, CF_DEFAULT};

    #[test]
    fn test_ingest_multiple_file() {
        let path_dir = Builder::new()
            .prefix("test_ingest_multiple_file")
            .tempdir()
            .unwrap();
        let root_path = path_dir.path();
        let db_path = root_path.join("db");
        let path_str = db_path.to_str().unwrap();

        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| {
                let mut opt = ColumnFamilyOptions::new();
                opt.set_force_consistency_checks(true);
                CFOptions::new(cf, opt)
            })
            .collect();
        let db = new_engine_opt(path_str, DBOptions::new(), cfs_opts).unwrap();
        let db = Arc::new(db);
        let db = RocksEngine::from_db(db);
        let mut wb = db.write_batch();
        for i in 1000..5000 {
            let v = i.to_string();
            wb.put(v.as_bytes(), v.as_bytes()).unwrap();
            if i % 1000 == 100 {
                wb.write().unwrap();
                wb.clear();
            }
        }
        // Flush one memtable to L0 to make sure that the next sst files to be ingested
        //  must locate in L0.
        db.flush_cf(CF_DEFAULT, true).unwrap();
        assert_eq!(
            1,
            db.get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap()
        );

        let p1 = root_path.join("sst1");
        let p2 = root_path.join("sst2");
        let mut sst1 = RocksSstWriterBuilder::new()
            .set_db(&db)
            .set_cf(CF_DEFAULT)
            .build(p1.to_str().unwrap())
            .unwrap();
        let mut sst2 = RocksSstWriterBuilder::new()
            .set_db(&db)
            .set_cf(CF_DEFAULT)
            .build(p2.to_str().unwrap())
            .unwrap();
        for i in 1001..2000 {
            let v = i.to_string();
            sst1.put(v.as_bytes(), v.as_bytes()).unwrap();
        }
        sst1.finish().unwrap();
        for i in 2001..3000 {
            let v = i.to_string();
            sst2.put(v.as_bytes(), v.as_bytes()).unwrap();
        }
        sst2.finish().unwrap();
        db.ingest_external_file_cf(CF_DEFAULT, &[p1.to_str().unwrap(), p2.to_str().unwrap()])
            .unwrap();
    }
}
