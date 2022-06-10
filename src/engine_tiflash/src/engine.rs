// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex, mpsc};
use std::cell::RefCell;
use std::collections::HashMap;

use engine_traits::{
    Error, IterOptions, Iterable, KvEngine, Peekable, ReadOptions, Result, SyncMutable,
};
use rocksdb::{DBIterator, Writable, DB};

use engine_rocks::db_vector::RocksDBVector;
use engine_rocks::options::RocksReadOptions;
use engine_rocks::rocks_metrics::{
    flush_engine_histogram_metrics, flush_engine_iostall_properties, flush_engine_properties,
    flush_engine_ticker_metrics,
};
use engine_rocks::rocks_metrics_defs::{
    ENGINE_HIST_TYPES, ENGINE_TICKER_TYPES, TITAN_ENGINE_HIST_TYPES, TITAN_ENGINE_TICKER_TYPES,
};
use engine_rocks::util::get_cf_handle;
use engine_rocks::{RocksEngineIterator, RocksSnapshot};
use yatp::pool::{Builder, ThreadPool};
use yatp::task::future::TaskCell;
use std::collections::hash_map::RandomState;
use raftstore::store::SnapKey;
use std::fmt::Formatter;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
pub struct RocksEngine {
    // Must ensure rocks is the first field, for RocksEngine::from_ref
    pub rocks: engine_rocks::RocksEngine,
    pub engine_store_server_helper: isize,
    pub apply_snap_pool: Option<Arc<ThreadPool<TaskCell>>>,
    pub pool_capacity: usize,
    pub pending_applies_count: Arc<AtomicUsize>,
}

impl std::fmt::Debug for RocksEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TiFlashEngine")
            .field("rocks", &self.rocks)
            .field("engine_store_server_helper", &self.engine_store_server_helper)
            .finish()
    }
}

impl RocksEngine {
    pub fn init(&mut self, engine_store_server_helper: isize, snap_handle_pool_size: usize) {
        self.engine_store_server_helper = engine_store_server_helper;
        let snap_pool = Builder::new(tikv_util::thd_name!("region-task"))
            .max_thread_count(snap_handle_pool_size)
            .build_future_pool();
        self.apply_snap_pool = Some(Arc::new(snap_pool));
        self.pool_capacity = snap_handle_pool_size;
        self.pending_applies_count.store(0, Ordering::Relaxed);
    }

    pub fn from_db(db: Arc<DB>) -> Self {
        RocksEngine {
            rocks: engine_rocks::RocksEngine {
                db,
                shared_block_cache: false,
            },
            engine_store_server_helper: 0,
            apply_snap_pool: None,
            pool_capacity: 0,
            pending_applies_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn from_ref(db: &Arc<DB>) -> &Self {
        unsafe { &*(db as *const Arc<DB> as *const RocksEngine) }
    }

    pub fn as_inner(&self) -> &Arc<DB> {
        &self.rocks.db
    }

    pub fn get_sync_db(&self) -> Arc<DB> {
        self.rocks.db.clone()
    }

    pub fn exists(path: &str) -> bool {
        let path = Path::new(path);
        if !path.exists() || !path.is_dir() {
            return false;
        }

        // If path is not an empty directory, we say db exists. If path is not an empty directory
        // but db has not been created, `DB::list_column_families` fails and we can clean up
        // the directory by this indication.
        fs::read_dir(&path).unwrap().next().is_some()
    }

    pub fn set_shared_block_cache(&mut self, enable: bool) {
        self.rocks.shared_block_cache = enable;
    }
}

impl KvEngine for RocksEngine {
    type Snapshot = RocksSnapshot;

    fn snapshot(&self) -> RocksSnapshot {
        RocksSnapshot::new(self.rocks.db.clone())
    }

    fn sync(&self) -> Result<()> {
        self.rocks.db.sync_wal().map_err(Error::Engine)
    }

    fn flush_metrics(&self, instance: &str) {
        for t in ENGINE_TICKER_TYPES {
            let v = self.rocks.db.get_and_reset_statistics_ticker_count(*t);
            flush_engine_ticker_metrics(*t, v, instance);
        }
        for t in ENGINE_HIST_TYPES {
            if let Some(v) = self.rocks.db.get_statistics_histogram(*t) {
                flush_engine_histogram_metrics(*t, v, instance);
            }
        }
        if self.rocks.db.is_titan() {
            for t in TITAN_ENGINE_TICKER_TYPES {
                let v = self.rocks.db.get_and_reset_statistics_ticker_count(*t);
                flush_engine_ticker_metrics(*t, v, instance);
            }
            for t in TITAN_ENGINE_HIST_TYPES {
                if let Some(v) = self.rocks.db.get_statistics_histogram(*t) {
                    flush_engine_histogram_metrics(*t, v, instance);
                }
            }
        }
        flush_engine_properties(&self.rocks.db, instance, self.rocks.shared_block_cache);
        flush_engine_iostall_properties(&self.rocks.db, instance);
    }

    fn reset_statistics(&self) {
        self.rocks.db.reset_statistics();
    }

    fn bad_downcast<T: 'static>(&self) -> &T {
        let e: &dyn Any = &self.rocks.db;
        e.downcast_ref().expect("bad engine downcast")
    }

    fn can_apply_snapshot(&self) -> bool {
        // is called after calling observer's pre_handle_snapshot
        let in_queue = self.pending_applies_count.load(Ordering::Relaxed);
        // if queue is full, we should begin to handle
        let can = in_queue > self.pool_capacity;
        fail::fail_point!("on_can_apply_snapshot", |e| e.unwrap().parse::<bool>().unwrap());
        can
    }
}

impl Iterable for RocksEngine {
    type Iterator = RocksEngineIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        let opt: RocksReadOptions = opts.into();
        Ok(RocksEngineIterator::from_raw(DBIterator::new(
            self.rocks.db.clone(),
            opt.into_raw(),
        )))
    }

    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let handle = get_cf_handle(&self.rocks.db, cf)?;
        let opt: RocksReadOptions = opts.into();
        Ok(RocksEngineIterator::from_raw(DBIterator::new_cf(
            self.rocks.db.clone(),
            handle,
            opt.into_raw(),
        )))
    }
}

impl Peekable for RocksEngine {
    type DBVector = RocksDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<RocksDBVector>> {
        let opt: RocksReadOptions = opts.into();
        let v = self.rocks.db.get_opt(key, &opt.into_raw())?;
        Ok(v.map(RocksDBVector::from_raw))
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<RocksDBVector>> {
        let opt: RocksReadOptions = opts.into();
        let handle = get_cf_handle(&self.rocks.db, cf)?;
        let v = self.rocks.db.get_cf_opt(handle, key, &opt.into_raw())?;
        Ok(v.map(RocksDBVector::from_raw))
    }
}

impl RocksEngine {
    fn do_write(&self, cf: &str, key: &[u8]) -> bool {
        crate::do_write(cf, key)
    }
}

impl SyncMutable for RocksEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.do_write(engine_traits::CF_DEFAULT, key) {
            return self.rocks.db.put(key, value).map_err(Error::Engine);
        }
        Ok(())
    }

    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        if self.do_write(cf, key) {
            let handle = get_cf_handle(&self.rocks.db, cf)?;
            return self
                .rocks
                .db
                .put_cf(handle, key, value)
                .map_err(Error::Engine);
        }
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        if self.do_write(engine_traits::CF_DEFAULT, key) {
            return self.rocks.db.delete(key).map_err(Error::Engine);
        }
        Ok(())
    }

    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        if self.do_write(cf, key) {
            let handle = get_cf_handle(&self.rocks.db, cf)?;
            return self.rocks.db.delete_cf(handle, key).map_err(Error::Engine);
        }
        Ok(())
    }

    fn delete_range(&self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        // do nothing
        Ok(())
    }

    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        // do nothing
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::raw_util;
    use engine_traits::{Iterable, KvEngine, Peekable, SyncMutable};
    use kvproto::metapb::Region;
    use std::sync::Arc;
    use tempfile::Builder;

    use crate::{RocksEngine, RocksSnapshot};

    #[test]
    fn test_base() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = RocksEngine::from_db(Arc::new(
            raw_util::new_engine(path.path().to_str().unwrap(), None, &[cf], None).unwrap(),
        ));

        let mut r = Region::default();
        r.set_id(10);

        let key = b"key";
        engine.put_msg(key, &r).unwrap();
        engine.put_msg_cf(cf, key, &r).unwrap();

        let snap = engine.snapshot();

        let mut r1: Region = engine.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r1);
        let r1_cf: Region = engine.get_msg_cf(cf, key).unwrap().unwrap();
        assert_eq!(r, r1_cf);

        let mut r2: Region = snap.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r2);
        let r2_cf: Region = snap.get_msg_cf(cf, key).unwrap().unwrap();
        assert_eq!(r, r2_cf);

        r.set_id(11);
        engine.put_msg(key, &r).unwrap();
        r1 = engine.get_msg(key).unwrap().unwrap();
        r2 = snap.get_msg(key).unwrap().unwrap();
        assert_ne!(r1, r2);

        let b: Option<Region> = engine.get_msg(b"missing_key").unwrap();
        assert!(b.is_none());
    }

    #[test]
    fn test_peekable() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = RocksEngine::from_db(Arc::new(
            raw_util::new_engine(path.path().to_str().unwrap(), None, &[cf], None).unwrap(),
        ));

        engine.put(b"k1", b"v1").unwrap();
        engine.put_cf(cf, b"k1", b"v2").unwrap();

        assert_eq!(&*engine.get_value(b"k1").unwrap().unwrap(), b"v1");
        assert!(engine.get_value_cf("foo", b"k1").is_err());
        assert_eq!(&*engine.get_value_cf(cf, b"k1").unwrap().unwrap(), b"v2");
    }

    #[test]
    fn test_scan() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = RocksEngine::from_db(Arc::new(
            raw_util::new_engine(path.path().to_str().unwrap(), None, &[cf], None).unwrap(),
        ));

        engine.put(b"a1", b"v1").unwrap();
        engine.put(b"a2", b"v2").unwrap();
        engine.put_cf(cf, b"a1", b"v1").unwrap();
        engine.put_cf(cf, b"a2", b"v22").unwrap();

        let mut data = vec![];
        engine
            .scan(b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                Ok(true)
            })
            .unwrap();
        assert_eq!(
            data,
            vec![
                (b"a1".to_vec(), b"v1".to_vec()),
                (b"a2".to_vec(), b"v2".to_vec()),
            ]
        );
        data.clear();

        engine
            .scan_cf(cf, b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                Ok(true)
            })
            .unwrap();
        assert_eq!(
            data,
            vec![
                (b"a1".to_vec(), b"v1".to_vec()),
                (b"a2".to_vec(), b"v22".to_vec()),
            ]
        );
        data.clear();

        let pair = engine.seek(b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(engine.seek(b"a3").unwrap().is_none());
        let pair_cf = engine.seek_cf(cf, b"a1").unwrap().unwrap();
        assert_eq!(pair_cf, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(engine.seek_cf(cf, b"a3").unwrap().is_none());

        let mut index = 0;
        engine
            .scan(b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                index += 1;
                Ok(index != 1)
            })
            .unwrap();

        assert_eq!(data.len(), 1);

        let snap = RocksSnapshot::new(engine.get_sync_db());

        engine.put(b"a3", b"v3").unwrap();
        assert!(engine.seek(b"a3").unwrap().is_some());

        let pair = snap.seek(b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(snap.seek(b"a3").unwrap().is_none());

        data.clear();

        snap.scan(b"", &[0xFF, 0xFF], false, |key, value| {
            data.push((key.to_vec(), value.to_vec()));
            Ok(true)
        })
        .unwrap();

        assert_eq!(data.len(), 2);
    }
}
