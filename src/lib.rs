//! # RocksCache
//! The first Redis cache library to ensure eventual consistency and strong consistency with DB.
//!
//! ## Features
//! * Eventual Consistency: ensures eventual consistency of cache even in extreme cases
//! * Strong consistency: provides strong consistent access to applications
//! * Anti-breakdown: a better solution for cache breakdown
//! * Anti-penetration
//! * Anti-avalanche
//! * Batch Query
//! ## Usage
//! This cache repository uses the most common update DB and then delete cache cache management policy
//!
//! ### Read cache
//!
//!
//! ```ignore
//! use std::thread::sleep;
//! use std::time::Duration;
//! use rockscache;
//! use redis;
//!
//!
//! // new a client for rockscache using the default options
//! let rc = rockscache::Client::new("redis://127.0.0.1:6379/", rockscache::Options::default()).unwrap();
//!
//! // use fetch to fetch data
//! // 1. the first parameter is the key of the data
//! // 2. the second parameter is the data expiration time
//! // 3. the third parameter is the data fetch function which is called when the cache does not exist
//! let res = rc.fetch("key1".to_string(), Duration::new(60, 0), || {
//!     // fetch data from database or other sources
//!     sleep(Duration::new(0, 200_000));
//!     Ok("value1".to_string())
//! });
//! ```
//!
//! ### Delete the cache
//!
//! ```ignore
//! rc.tag_as_deleted(key)
//! ```
//!
//! ## Batch usage
//!
//! ### Batch read cache
//!
//! ```ignore
//! use std::collections::HashMap;
//! use std::thread::sleep;
//! use std::time::Duration;
//! use rockscache;
//! use redis;
//!
//! // new a client for rockscache using the default options
//! let rc = rockscache::Client::new("redis://127.0.0.1:6379/", rockscache::Options::default()).unwrap();
//!
//! // use fetch_batch to fetch data
//! // 1. the first parameter is the keys list of the data
//! // 2. the second parameter is the data expiration time
//! // 3. the third parameter is the batch data fetch function which is called when the cache does not exist
//! // the parameter of the batch data fetch function is the index list of those keys
//! // missing in cache, which can be used to form a batch query for missing data.
//! // the return value of the batch data fetch function is a map, with key of the
//! // index and value of the corresponding data in form of string
//! let keys = vec!["key1", "key2", "key3"];
//! let result = rc.fetch_batch(keys, Duration::from_secs(300), |idxs| {
//!     // fetch data from database or other sources
//!     let mut values = HashMap::new();
//!     for i in idxs {
//!         values.insert(*i, format!("value{}", i));
//!     }
//!     return Ok(values)
//! });
//! ```
//!
//! ### Batch delete cache
//!
//! ```ignore
//! rc.tag_as_deleted_batch(keys)
//! ```
#[cfg(feature = "async")]
mod async_client;
#[cfg(feature = "async")]
pub use async_client::AsyncClient;
#[cfg(feature = "thread")]
mod client;
#[cfg(feature = "thread")]
pub use client::Client;
pub mod types;
pub use types::Options;
pub mod utils;
