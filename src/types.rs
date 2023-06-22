use redis::{from_redis_value, Client, ErrorKind, FromRedisValue, RedisError, RedisResult, Value};
use std::{collections::HashMap, time::Duration};

/// Options represents the options for rockscache client
pub struct Options {
    /// Delay is the delay delete time for keys that are tag deleted. default is 10s
    pub delay: Duration,
    /// EmptyExpire is the expire time for empty result. default is 60s
    pub empty_expire: Duration,
    /// LockExpire is the expire time for the lock which is allocated when updating cache. default is 3s
    /// should be set to the max of the underling data calculating time.
    pub lock_expire: Duration,
    /// LockSleep is the sleep interval time if try lock failed. default is 100ms
    pub lock_sleep: Duration,
    /// WaitReplicas is the number of replicas to wait for. default is 0
    /// if WaitReplicas is > 0, it will use redis WAIT command to wait for TagAsDeleted synchronized.
    pub wait_replicas: i32,
    /// WaitReplicasTimeout is the number of replicas to wait for. default is 3000ms
    /// if WaitReplicas is > 0, WaitReplicasTimeout is the timeout for WAIT command.
    pub wait_replicas_timeout: Duration,
    /// RandomExpireAdjustment is the random adjustment for the expire time. default 0.1
    /// if the expire time is set to 600s, and this value is set to 0.1, then the actual expire time will be 540s - 600s
    /// solve the problem of cache avalanche.
    pub random_expire_adjustment: f64,
    /// CacheReadDisabled is the flag to disable read cache. default is false
    /// when redis is down, set this flat to downgrade.
    pub disable_cache_read: bool,
    /// CacheDeleteDisabled is the flag to disable delete cache. default is false
    /// when redis is down, set this flat to downgrade.
    pub disable_cache_delete: bool,
    /// StrongConsistency is the flag to enable strong consistency. default is false
    /// if enabled, the Fetch result will be consistent with the db result, but performance is bad.
    pub strong_consistency: bool,
    // Context for redis command
    // Context: context.Context,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            delay: Duration::new(10, 0),
            empty_expire: Duration::new(60, 0),
            lock_expire: Duration::new(3, 0),
            lock_sleep: Duration::new(0, 100_000_000),
            wait_replicas: 0,
            wait_replicas_timeout: Duration::new(3, 0),
            random_expire_adjustment: 0.1f64,
            disable_cache_read: false,
            disable_cache_delete: false,
            strong_consistency: false,
        }
    }
}

impl Clone for Options {
    fn clone(&self) -> Self {
        Self {
            delay: self.delay.clone(),
            empty_expire: self.empty_expire.clone(),
            lock_expire: self.lock_expire.clone(),
            lock_sleep: self.lock_sleep.clone(),
            wait_replicas: self.wait_replicas.clone(),
            wait_replicas_timeout: self.wait_replicas_timeout.clone(),
            random_expire_adjustment: self.random_expire_adjustment.clone(),
            disable_cache_read: self.disable_cache_read.clone(),
            disable_cache_delete: self.disable_cache_delete.clone(),
            strong_consistency: self.strong_consistency.clone(),
        }
    }
}

pub struct Pair {
    pub idx: i32,
    pub data: String,
    pub err: RedisError,
}

pub trait RocksCacheClient {
    /// new return a new rockscache client
    /// for each key, rockscache client store a hash set,
    /// the hash set contains the following fields:
    /// value: the value of the key
    /// lockUntil: the time when the lock is released.
    /// lockOwner: the owner of the lock.
    /// if a thread query the cache for data, and no cache exists, it will lock the key before querying data in DB
    fn new(rdb: r2d2::Pool<Client>, options: Options) -> Self;
    /// tag_as_deleted a key, the key will expire after delay time.
    fn tag_as_deleted(&self, key: String) -> RedisResult<()>;
    /// Fetch returns the value store in cache indexed by the key.
    /// If the key doest not exists, call fn to get result, store it in cache, then return.
    fn fetch<F>(&self, key: String, expire: Duration, func: F) -> RedisResult<String>
    where
        F: 'static + Send + Fn() -> Result<String, RedisError>;
    fn _strong_fetch<F>(&self, key: String, ex: Duration, func: F) -> RedisResult<String>
    where
        F: Fn() -> Result<String, RedisError>;
    fn _weak_fetch<F>(&self, key: String, ex: Duration, func: F) -> RedisResult<String>
    where
        F: 'static + Send + Fn() -> Result<String, RedisError>;
    fn _lua_get<T>(&self, key: String, owner: String) -> RedisResult<T>
    where
        T: FromRedisValue;
    fn _lua_set(&self, key: String, value: String, expire: i32, owner: String) -> RedisResult<()>;
    fn _fetch_new<F>(
        &self,
        key: String,
        ex: Duration,
        owner: String,
        func: F,
    ) -> RedisResult<String>
    where
        F: Fn() -> Result<String, RedisError>;
    /// raw_get returns the value store in cache indexed by the key, no matter if the key locked or not
    fn raw_get(&self, key: String) -> RedisResult<String>;
    /// raw_set sets the value store in cache indexed by the key, no matter if the key locked or not
    fn raw_set(&self, key: String, value: String, expire: Duration) -> RedisResult<()>;
    /// lock_for_update locks the key, used in very strict strong consistency mode
    fn lock_for_update(&self, key: String, owner: String) -> RedisResult<()>;
    /// unlock_for_update unlocks the key, used in very strict strong consistency mode
    fn unlock_for_update(&self, key: String, owner: String) -> RedisResult<()>;
}

pub trait RocksCacheBatch {
    fn _lua_get_batch<T>(&self, keys: Vec<String>, owner: String) -> RedisResult<Vec<T>>;
    fn _lua_set_batch(
        &self,
        keys: Vec<String>,
        values: Vec<String>,
        expires: Vec<i32>,
        owner: String,
    ) -> RedisResult<()>;
    fn _fetch_batch<F>(
        &self,
        keys: Vec<String>,
        idxs: Vec<i32>,
        expire: Duration,
        owner: String,
        func: F,
    ) -> RedisResult<HashMap<i32, String>>
    where
        F: Fn(Vec<i32>) -> RedisResult<HashMap<i32, String>>;
    fn _keys_idx(&self, keys: Vec<String>) -> Vec<i32>;
    fn _weak_fetch_batch<F>(
        &self,
        keys: Vec<String>,
        expire: Duration,
        func: F,
    ) -> RedisResult<HashMap<i32, String>>
    where
        F: Fn(Vec<i32>) -> RedisResult<HashMap<i32, String>>;
    fn _strong_fetch_batch<F>(
        &self,
        keys: Vec<String>,
        expire: Duration,
        func: F,
    ) -> RedisResult<HashMap<i32, String>>
    where
        F: Fn(Vec<i32>) -> RedisResult<HashMap<i32, String>>;
    /// FetchBatch returns a map with values indexed by index of keys list.
    /// 1. the first parameter is the keys list of the data
    /// 2. the second parameter is the data expiration time
    /// 3. the third parameter is the batch data fetch fntion which is called when the cache does not exist
    /// the parameter of the batch data fetch fntion is the index list of those keys
    /// missing in cache, which can be used to form a batch query for missing data.
    /// the return value of the batch data fetch fntion is a map, with key of the
    /// index and value of the corresponding data in form of String
    fn fetch_batch<F>(
        &self,
        keys: Vec<String>,
        expire: Duration,
        func: F,
    ) -> RedisResult<HashMap<i32, String>>
    where
        F: Fn(Vec<i32>) -> RedisResult<HashMap<i32, String>>;
    /// TagAsDeletedBatch a key list, the keys in list will expire after delay time.
    fn tag_as_deleted_batch(&self, keys: Vec<String>) -> RedisResult<()>;
}

pub enum LockableValue<T> {
    /// `lockUntil == nil` lock not set
    Nil(Option<T>),
    /// `lockUntil == "LOCKED"` lock aquired
    Value(Option<T>),
    /// `lockUntil == lock expire timestamp` locked by other
    Locked(Option<T>),
}

impl<T: FromRedisValue> FromRedisValue for LockableValue<T> {
    fn from_redis_value(v: &Value) -> RedisResult<LockableValue<T>> {
        match *v {
            Value::Bulk(ref items) => {
                if items.len() != 2 {
                    Err(RedisError::from((
                        ErrorKind::TypeError,
                        "Response was of incompatible type",
                        "Expect response of two element tuple [T, String]".to_string(),
                    )))
                } else {
                    match items[1] {
                        // lockUntil not set
                        Value::Nil => {
                            println!("lockUntil: nil");
                            Ok(LockableValue::Nil(from_redis_value::<Option<T>>(
                                &items[0],
                            )?))
                        }
                        _ => {
                            match from_redis_value::<u64>(&items[1]) {
                                Ok(i) => {
                                    println!("lockUntil: {i}");
                                    //locked by other
                                    Ok(LockableValue::Locked(from_redis_value::<Option<T>>(
                                        &items[0],
                                    )?))
                                }
                                Err(_) => {
                                    // not integer lockUntil
                                    match from_redis_value::<String>(&items[1]) {
                                        Ok(s) => {
                                            println!("lockUntil: {s}");
                                            if s == "LOCKED" {
                                                Ok(LockableValue::Value(from_redis_value::<
                                                    Option<T>,
                                                >(
                                                    &items[0]
                                                )?))
                                            } else {
                                                Err(RedisError::from((
                                                    ErrorKind::TypeError,
                                                    "Response was of incompatible type",
                                                    "Expect second element of respond tuple is `LOCKED` or lock expire timestamp".to_string(),
                                                )))
                                            }
                                        }
                                        Err(e) => Err(e),
                                    }
                                }
                            }
                        }
                    }
                }
            }
            _ => Err(RedisError::from((
                ErrorKind::TypeError,
                "Response was of incompatible type",
                format!(
                    "{:?} (response was {:?})",
                    "Response type not vector compatible.", v
                ),
            ))),
        }
    }
}
