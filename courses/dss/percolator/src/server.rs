use crate::msg::*;
use crate::service::*;
use crate::*;
use std::alloc::System;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::{Debug, Display};
use std::ops::Bound::Included;
use std::sync::MutexGuard;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};
use std::u64;

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    // You definitions here if needed.
    local_time: Arc<Mutex<u64>>,
}

impl TimestampOracle {
    pub fn new() -> TimestampOracle {
        TimestampOracle {
            local_time: Arc::new(Mutex::new(0)),
        }
    }
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        // Your code here.
        loop {
            if let Ok(now_time) = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                let mut local_time = self.local_time.lock().unwrap();
                let now_time_ns = now_time.as_nanos() as u64;
                if now_time_ns < *local_time {
                    continue;
                }
                *local_time = now_time_ns;
                let resp = TimestampResponse {
                    time_stamp: *local_time,
                };
                return labrpc::Result::Ok(resp);
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}
impl Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Format Timestamp as "ts(...)"
            Value::Timestamp(ts) => write!(f, "ts({})", ts),
            // Format Vector by attempting to convert it to a UTF-8 string.
            // Lossy conversion means invalid UTF-8 sequences are replaced with a placeholder.
            Value::Vector(vec) => write!(f, "vec({})", String::from_utf8_lossy(vec)),
        }
    }
}

pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        // Your code here.

        let start_key = (key.clone(), ts_start_inclusive.unwrap_or(0));
        let end_key = (key.clone(), ts_end_inclusive.unwrap_or(u64::max_value()));
        match column {
            Column::Write => {
                return self
                    .write
                    .range((Included(start_key), Included(end_key)))
                    .rev()
                    .next()
            }
            Column::Data => {
                return self
                    .data
                    .range((Included(start_key), Included(end_key)))
                    .rev()
                    .next()
            }
            Column::Lock => {
                return self
                    .lock
                    .range((Included(start_key), Included(end_key)))
                    .rev()
                    .next()
            }
        };
        None
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        // Your code here.
        let write_key = (key, ts);
        match column {
            Column::Write => self.write.insert(write_key, value),
            Column::Data => self.data.insert(write_key, value),
            Column::Lock => self.lock.insert(write_key, value),
        };
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        // Your code here.
        let erase_key = (key, commit_ts);
        match column {
            Column::Write => self.write.remove(&erase_key),
            Column::Data => self.data.remove(&erase_key),
            Column::Lock => self.lock.remove(&erase_key),
        };
    }
    #[inline]
    fn get_committed_timestamp(&self, key: Vec<u8>, start_ts: u64) -> Option<u64> {
        let start_key = (key.clone(), start_ts);
        let end_key = (key.clone(), u64::MAX);
        for (key, value) in self.write.range((Included(start_key), Included(end_key))) {
            if let Value::Timestamp(x) = value {
                if x == &start_ts {
                    return Some(key.1);
                }
            }
        }
        None
    }
    #[inline]
    pub fn print_merged(&self) {
        // Step 1: Collect all unique keys from the three maps.
        // A BTreeSet is used because it automatically keeps the keys sorted and unique.
        // 步骤 1: 从三个映射中收集所有唯一的键。
        // 使用 BTreeSet 是因为它会自动保持键的唯一性和有序性。
        let mut all_keys = BTreeSet::new();
        for key in self
            .data
            .keys()
            .chain(self.lock.keys())
            .chain(self.write.keys())
        {
            all_keys.insert(key.clone());
        }

        // Helper function to get a value from a map as a String.
        // If the key doesn't exist, it returns an empty string.
        // 辅助函数，用于从映射中获取值的字符串表示。
        // 如果键不存在，则返回一个空字符串。
        fn get_value_display<K, V>(map: &BTreeMap<K, V>, key: &K) -> String
        where
            K: Ord,
            V: Display,
        {
            map.get(key).map_or(String::new(), |v| v.to_string())
        }

        // Print a header for clarity.
        // 打印表头，方便阅读。
        print_message!("<Key,       TimeStamp       > < Data,    Lock,    Write >");
        print_message!("----------------------------------------------------------");

        // Step 2: Iterate through each unique, sorted key.
        // 步骤 2: 遍历每一个唯一的、排好序的键。
        for key in all_keys {
            // For each key, get the corresponding value from each map.
            // 对每个键，分别从 data, lock, write 映射中获取对应的值。
            let data_val = get_value_display(&self.data, &key);
            let lock_val = get_value_display(&self.lock, &key);
            let write_val = get_value_display(&self.write, &key);

            // Print the merged row using the requested format.
            // 使用您指定的格式打印合并后的行。
            print_message!(
                "<{:?}, {}> < {:?}, {:?}, {:?} >",
                String::from_utf8_lossy(&key.0),
                key.1,
                data_val,
                lock_val,
                write_val
            );
        }
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {
        // Your code here.
        loop {
            let mut kv_data = self.data.lock().unwrap();
            if let Some(_) = kv_data.read(req.key.clone(), Column::Lock, None, Some(req.start_ts)) {
                self.back_off_maybe_clean_up_lock(req.start_ts, req.key.clone(), kv_data);
                continue;
            }
            if let Some((write_key, write_value)) =
                kv_data.read(req.key.clone(), Column::Write, None, Some(req.start_ts))
            {
                match write_value {
                    // committed data, contains start_ts
                    Value::Timestamp(write_start_ts) => {
                        if let Some((data_key, data_value)) = kv_data.read(
                            req.key.clone(),
                            Column::Data,
                            Some(*write_start_ts),
                            Some(*write_start_ts),
                        ) {
                            if let Value::Vector(data_actual_value) = data_value {
                                return labrpc::Result::Ok(GetResponse {
                                    value: data_actual_value.clone(),
                                });
                            }
                        }
                    }
                    Value::Vector(_) => panic!("Write column value store vector"),
                }
            }
            return labrpc::Result::Ok(GetResponse { value: Vec::new() });
        }
    }

    // example prewrite RPC handler.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        // Your code here.
        let mut kv_data = self.data.lock().unwrap();

        // write-write conflict check [start_ts, max]
        if let Some(_) = kv_data.read(req.key.clone(), Column::Write, Some(req.start_ts), None) {
            return labrpc::Result::Ok(PrewriteResponse { is_success: false });
        }
        // check whether is locked by other txn
        if let Some(_) = kv_data.read(req.key.clone(), Column::Lock, None, None) {
            return labrpc::Result::Ok(PrewriteResponse { is_success: false });
        }

        kv_data.write(
            req.key.clone(),
            Column::Data,
            req.start_ts,
            Value::Vector(req.value.clone()),
        );

        // primary add lock
        if req.is_primary {
            kv_data.write(
                req.key.clone(),
                Column::Lock,
                req.start_ts,
                Value::Timestamp(req.primary_ttl),
            );
        } else {
            // no primary add pointer to primary
            kv_data.write(
                req.key.clone(),
                Column::Lock,
                req.start_ts,
                Value::Vector(req.primary_key),
            );
        }
        kv_data.print_merged();

        return labrpc::Result::Ok(PrewriteResponse { is_success: true });
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        // Your code here.
        print_message!("commit message:{:?}", req);
        let mut kv_data = self.data.lock().unwrap();
        kv_data.print_merged();
        // no lock
        if let None = kv_data.read(
            req.key.clone(),
            Column::Lock,
            Some(req.start_ts),
            Some(req.start_ts),
        ) {
            if let Some(ts) = kv_data.get_committed_timestamp(req.key.clone(), req.start_ts) {
                return labrpc::Result::Ok(CommitResponse { is_success: true });
            }
            return labrpc::Result::Ok(CommitResponse { is_success: false });
        }
        // clean lock
        kv_data.erase(req.key.clone(), Column::Lock, req.start_ts);
        // write commit_ts and start_ts to end the txn
        kv_data.write(
            req.key.clone(),
            Column::Write,
            req.commit_ts,
            Value::Timestamp(req.start_ts),
        );

        return labrpc::Result::Ok(CommitResponse { is_success: true });
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(
        &self,
        start_ts: u64,
        key: Vec<u8>,
        mut kv_data: MutexGuard<server::KvTable>,
    ) {
        // Your code here.
        if let Some((lock_key, lock_value)) =
            kv_data.read(key.clone(), Column::Lock, None, Some(start_ts))
        {
            let lock_ts = lock_key.1;
            match lock_value {
                // primary key
                Value::Timestamp(ttl) => {
                    let now_ts = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .expect("Time went backwards")
                        .as_nanos() as u64;
                    if (start_ts + ttl > now_ts) {
                        thread::sleep(Duration::from_nanos(start_ts + ttl - now_ts));
                        return;
                    } else {
                        kv_data.erase(key.clone(), Column::Lock, lock_ts);
                        kv_data.erase(key.clone(), Column::Data, lock_ts);
                    }
                }
                // not primary
                Value::Vector(primary_key) => {
                    match kv_data.read(
                        primary_key.clone(),
                        Column::Lock,
                        Some(lock_ts),
                        Some(lock_ts),
                    ) {
                        Some((_, _)) => {
                            return self.back_off_maybe_clean_up_lock(
                                lock_ts,
                                primary_key.clone(),
                                kv_data,
                            );
                        }
                        None => {
                            match kv_data.get_committed_timestamp(primary_key.clone(), lock_ts) {
                                Some(commit_ts) => {
                                    kv_data.erase(key.clone(), Column::Lock, lock_ts);
                                    kv_data.write(
                                        key.clone(),
                                        Column::Write,
                                        commit_ts,
                                        Value::Timestamp(lock_ts),
                                    );
                                }
                                None => {
                                    kv_data.erase(key.clone(), Column::Lock, lock_ts);
                                    kv_data.erase(key.clone(), Column::Data, lock_ts);
                                }
                            }
                        }
                    }
                }
            };
        };
    }
}
