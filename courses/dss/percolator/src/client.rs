use std::{collections::BTreeMap, thread, time::Duration};

use crate::{msg::*, server::Value, service::*};
use futures::executor::block_on;
use labrpc::*;

#[macro_export]
macro_rules! print_message {
   ($($arg: tt)*) => (
    //    eprintln!("Debug[{}:{}]: {}", file!(), line!(),format_args!($($arg)*));
   )
}

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

const TRANSACTION_TTL_PER_OPERATION :u64 = 100;



/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    // Your definitions here.
    tso_client: TSOClient,
    txn_client: TransactionClient,
    start_ts: u64,
    mutations: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client {
            tso_client: tso_client,
            txn_client: txn_client,
            start_ts: 0,
            mutations: BTreeMap::new(),
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        let req = TimestampRequest {};
        for i in 0..RETRY_TIMES {
            if let Ok(resp) = block_on(async { self.tso_client.get_timestamp(&req).await }) {
                return Ok(resp.time_stamp);
            }
            thread::sleep(Duration::from_millis(BACKOFF_TIME_MS * (1 << i)));
        }
        Err(Error::Timeout)
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        // Your code here.
        self.mutations.clear();
        loop {
            if let Ok(ts) = self.get_timestamp() {
                self.start_ts = ts;
                return;
            }
        }
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        print_message!("client get {:?}", String::from_utf8_lossy(&key));
        let get_message = GetRequest {
            start_ts: self.start_ts,
            key: key,
        };
        if let Ok(get_response) = block_on(async { self.txn_client.get(&get_message).await }) {
            print_message!("the get value is {:?}", get_response.value);
            return Ok(get_response.value);
        }
        Err(Error::Timeout)
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        print_message!("client set <{:?}, {}>", String::from_utf8_lossy(&key), Value::Vector(value.clone()));
        self.mutations.insert(key, value);
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        // Your code here.
        print_message!("client commit");
        //if the mutations are empty ,so the it is done.
        if self.mutations.is_empty() {
            return Ok(true);
        }

        //Phase 1: prewrite
        //Phase 1.1: prewrite the primary key
        //choose the primary key
        let mut primary_key: Vec<u8> = self.mutations.iter().next().unwrap().0.clone();
        //calculate the transaction ttl
        let transcation_ttl = TRANSACTION_TTL_PER_OPERATION * self.mutations.len() as u64;
        for (key, value) in self.mutations.iter() {
            let mut prewrite_req = PrewriteRequest {
                start_ts: self.start_ts,
                is_primary: false,
                primary_ttl: transcation_ttl,
                primary_key: primary_key.to_vec(),
                key: key.to_vec(),
                value: value.to_vec(),
            };
            if (key == &primary_key) {
                prewrite_req.is_primary = true;
            }
            if let Ok(prewrite_resp) =
                block_on(async { self.txn_client.prewrite(&prewrite_req).await })
            {
                if !prewrite_resp.is_success {
                    return Ok(false);
                }
            } else {
                panic!("wrong at prewrite");
            }
        }

        //Phase 2: commit
        let mut commit_ts: u64;
        loop {
            if let Ok(ts) = self.get_timestamp() {
                commit_ts = ts;
                break;
            }
        }
        for (key, value) in self.mutations.iter() {
            let mut commit_req = CommitRequest {
                is_primary: false,
                start_ts: self.start_ts,
                commit_ts: commit_ts,
                key: key.to_vec(),
            };
            //Phase 2.1: commit the primary key
            if (key == &primary_key) {
                commit_req.is_primary = true;
                match block_on(async { self.txn_client.commit(&commit_req).await }) {
                    Ok(commit_resp) => {
                        if !commit_resp.is_success {
                            return Ok(false);
                        }
                    }
                    Err(e) => {
                        if e == Error::Other("resphook".to_string()) {
                            return Err(Error::Other("resphook".to_string()));
                        } else {
                            return Ok(false);
                        }
                    }
                }
            } else {
                //Phase 2.2: commit the write out write records for secondary cells
                block_on(async { self.txn_client.commit(&commit_req).await });
            }
        }
        Ok(true)
    }
}
