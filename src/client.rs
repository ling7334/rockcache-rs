use anyhow::{anyhow, Result};
use r2d2;
use redis::{Client as RedisClient, Commands, FromRedisValue, RedisResult, ToRedisArgs};
use singleflight::Group;
use std::collections::HashMap;
use std::sync::mpsc;
use std::thread::sleep;
use std::time::Duration;
use threadpool::ThreadPool;
use tracing::{error, instrument, trace};
use uuid::Uuid;

use crate::types::{LockableValue, Options, Pair};
use crate::utils::{call_lua, now};

#[derive(Debug)]
/// Client delay client
pub struct Client {
    pool: r2d2::Pool<RedisClient>,
    options: Options,
    group: Group,
    threads: ThreadPool,
}

impl Drop for Client {
    fn drop(&mut self) {
        self.threads.join();
    }
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            options: self.options.clone(),
            group: self.group.clone(),
            threads: ThreadPool::new(16),
        }
    }
}

impl Client {
    /// new return a new rockscache client
    /// for each key, rockscache client store a hash set,
    /// the hash set contains the following fields:
    ///
    /// value: the value of the key
    ///
    /// lockUntil: the time when the lock is released.
    ///
    /// lockOwner: the owner of the lock.
    ///
    /// if a thread query the cache for data, and no cache exists, it will lock the key before querying data in DB
    pub fn new(uri: &str, options: Options) -> Result<Self> {
        if options.delay == Duration::ZERO || options.lock_expire == Duration::ZERO {
            panic!("cache options error: Delay and LockExpire should not be 0, you should call NewDefaultOptions() to get default options");
        }

        Ok(Client {
            pool: r2d2::Pool::builder()
                .max_size(15)
                .min_idle(Some(3))
                .build(RedisClient::open(uri)?)?,
            options: options,
            group: Group::new(),
            threads: ThreadPool::new(16),
        })
    }

    /// tag_as_deleted a key, the key will expire after delay time.
    #[instrument]
    pub fn tag_as_deleted(&self, key: &str) -> Result<bool> {
        if self.options.disable_cache_delete {
            return Ok(false);
        }
        let script = r#"
            -- delete
            redis.call('HSET', KEYS[1], 'lockUntil', 0)
            redis.call('HDEL', KEYS[1], 'lockOwner')
            redis.call('EXPIRE', KEYS[1], ARGV[1])
            return true
        "#;
        let delay_sec = self.options.delay.as_secs();
        let ref keys = vec![key];
        let ref args = vec![delay_sec];
        let pool = self.pool.clone();
        let ref mut con = pool.get()?;
        if self.options.wait_replicas > 0 {
            let _: bool = call_lua(con, script, keys, args)?;

            let replicas: i32 = redis::cmd("WAIT")
                .arg(self.options.wait_replicas)
                .arg(self.options.wait_replicas_timeout.as_secs())
                .query(con)?;
            if replicas < self.options.wait_replicas {
                return Err(anyhow!(format!(
                    "wait replicas {} failed. result replicas: {}",
                    self.options.wait_replicas, replicas,
                ),));
            }
        }
        Ok(call_lua(con, script, keys, args)?)
    }

    /// fetch returns the value store in cache indexed by the key.
    /// If the key doest not exists, call fn to get result, store it in cache, then return.
    #[instrument(skip(func))]
    pub fn fetch<T, F, E>(&self, key: &str, expire: Duration, func: F) -> Result<Option<T>>
    where
        T: FromRedisValue + ToRedisArgs + Clone + Send + Sync + std::fmt::Debug + 'static,
        F: 'static + Send + Fn() -> core::result::Result<Option<T>, E>,
        E: Into<anyhow::Error> + Send + Sync + std::fmt::Debug,
    {
        let ex = expire
            - self.options.delay
            - Duration::from_secs_f64(
                rand::random::<f64>()
                    * self.options.random_expire_adjustment
                    * expire.as_secs_f64(),
            );
        let v = self.group.work(key, || {
            if self.options.disable_cache_read {
                return match func() {
                    Ok(r) => Ok(r),
                    Err(e) => Err(e.into()),
                };
            } else if self.options.strong_consistency {
                return self._strong_fetch(key, ex, func);
            } else {
                return self._weak_fetch(key, ex, func);
            }
        })?;
        Ok(v)
    }

    #[instrument(skip(func))]
    fn _strong_fetch<T, F, E>(&self, key: &str, ex: Duration, func: F) -> Result<Option<T>>
    where
        T: FromRedisValue + ToRedisArgs + Clone + std::fmt::Debug,
        E: Into<anyhow::Error>,
        F: Fn() -> core::result::Result<Option<T>, E>,
    {
        let owner = Uuid::new_v4().to_string();

        loop {
            match self._lua_get::<LockableValue<T>>(key.clone(), owner.as_str()) {
                Ok(LockableValue::Nil(r)) => return Ok(r),
                Ok(LockableValue::Value(_)) => {
                    return self._fetch_new(key, ex, owner.as_str(), func)
                }
                Ok(LockableValue::Locked(_)) => {
                    // locked by other
                    sleep(self.options.lock_sleep);
                    continue;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    #[instrument(skip(func))]
    fn _weak_fetch<T, F, E>(&self, key: &str, ex: Duration, func: F) -> Result<Option<T>>
    where
        T: FromRedisValue + ToRedisArgs + Clone + Send + Sync + std::fmt::Debug,
        E: Into<anyhow::Error> + Send + Sync + std::fmt::Debug,
        F: 'static + Send + Fn() -> core::result::Result<Option<T>, E>,
    {
        let owner = Uuid::new_v4().to_string();

        loop {
            match self._lua_get::<LockableValue<T>>(key, owner.as_str()) {
                Ok(LockableValue::Nil(r)) => match r {
                    Some(v) => {
                        trace!("LockableValue::Nil: {:?}", v);
                        return Ok(Some(v));
                    }
                    None => {
                        trace!("LockableValue::Nil: None sleep");
                        sleep(self.options.lock_sleep);
                        continue;
                    }
                },
                Ok(LockableValue::Value(r)) => match r {
                    Some(v) => {
                        trace!("LockableValue::Value: {:?} _fetch_new return old", v);
                        let key = key.to_owned();
                        let this = self.clone();
                        self.threads.execute(move || {
                            let _ = this._fetch_new(&key, ex, owner.as_str(), func);
                            trace!("async fetch new complete");
                        });
                        return Ok(Some(v));
                    }
                    None => {
                        trace!("LockableValue::Value: None _fetch_new");
                        return self._fetch_new(key, ex, owner.as_str(), func);
                    }
                },
                Ok(LockableValue::Locked(r)) => match r {
                    Some(v) => {
                        trace!("LockableValue::Locked: {:?} return", v);
                        return Ok(Some(v));
                    }
                    None => {
                        trace!("LockableValue::Locked: None sleep");
                        sleep(self.options.lock_sleep);
                        continue;
                    }
                },
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    #[instrument]
    fn _lua_get<T>(&self, key: &str, owner: &str) -> Result<T>
    where
        T: FromRedisValue,
    {
        let pool = self.pool.clone();
        let ref mut con = pool.get()?;
        Ok(call_lua(
            con,
            r#" -- luaGet
            local v = redis.call('HGET', KEYS[1], 'value')
            local lu = redis.call('HGET', KEYS[1], 'lockUntil')
            if lu ~= false and tonumber(lu) < tonumber(ARGV[1]) or lu == false and v == false then
                redis.call('HSET', KEYS[1], 'lockUntil', ARGV[2])
                redis.call('HSET', KEYS[1], 'lockOwner', ARGV[3])
                return { v, 'LOCKED' }
            end
            return { v, lu }
            "#,
            &[key],
            &[
                now().to_redis_args(),
                (now() + (self.options.lock_expire.as_secs())).to_redis_args(),
                owner.to_redis_args(),
            ],
        )?)
    }

    #[instrument]
    fn _lua_set<T>(&self, key: &str, value: T, expire: u64, owner: &str) -> Result<bool>
    where
        T: ToRedisArgs + std::fmt::Debug,
    {
        let pool = self.pool.clone();
        let ref mut con = pool.get()?;
        let res: bool = call_lua(
            con,
            r#"-- luaSet
            local o = redis.call('HGET', KEYS[1], 'lockOwner')
            if o ~= ARGV[2] then
                    return false
            end
            redis.call('HSET', KEYS[1], 'value', ARGV[1])
            redis.call('HDEL', KEYS[1], 'lockUntil')
            redis.call('HDEL', KEYS[1], 'lockOwner')
            redis.call('EXPIRE', KEYS[1], ARGV[3])
            return true
            "#,
            &[key],
            &[
                value.to_redis_args(),
                owner.to_redis_args(),
                expire.to_redis_args(),
            ],
        )?;
        trace!("lua set {}", res);
        Ok(res)
    }

    #[instrument(skip(func))]
    fn _fetch_new<T, F, E>(
        &self,
        key: &str,
        ex: Duration,
        owner: &str,
        func: F,
    ) -> Result<Option<T>>
    where
        T: FromRedisValue + ToRedisArgs + std::fmt::Debug,
        E: Into<anyhow::Error>,
        F: Fn() -> core::result::Result<Option<T>, E>,
    {
        match func() {
            Err(e) => {
                // error! {"func execute error {}", e};
                let _ = self.unlock_for_update(key, owner)?;
                return Err(e.into());
            }
            Ok(r) => {
                // trace! {"func execute result {}", r};
                let mut expire = ex;
                if r.is_none() {
                    if self.options.empty_expire == Duration::ZERO {
                        let pool = self.pool.clone();
                        let ref mut con = pool.get()?;
                        return match con.del::<&str, T>(key) {
                            Ok(_) => Ok(None),
                            Err(e) => Err(e.into()),
                        };
                    }
                    expire = self.options.empty_expire;
                }
                match self._lua_set(key, &r, expire.as_secs() as u64, owner) {
                    Err(e) => {
                        error!("_lua_set error {}", e);
                        return Err(e);
                    }
                    Ok(_) => return Ok(r),
                }
            }
        }
    }

    /// raw_get returns the value store in cache indexed by the key, no matter if the key locked or not
    #[instrument]
    pub fn raw_get<T>(&self, key: &str) -> Result<T>
    where
        T: FromRedisValue,
    {
        let pool = self.pool.clone();
        let ref mut con = pool.get()?;
        Ok(con.hget(key, "value")?)
    }

    /// raw_set sets the value store in cache indexed by the key, no matter if the key locked or not
    #[instrument]
    pub fn raw_set<T>(&self, key: &str, value: T, expire: Duration) -> Result<()>
    where
        T: ToRedisArgs + std::fmt::Debug,
    {
        let pool = self.pool.clone();
        let ref mut con = pool.get()?;
        match con.hset(key.clone(), "value", value) {
            Err(e) => Err(e.into()),
            Ok(()) => {
                let _ = con.expire::<_, ()>(key, expire.as_secs() as usize);
                Ok(())
            }
        }
    }

    /// lock_for_update locks the key, used in very strict strong consistency mode
    #[instrument]
    pub fn lock_for_update(&self, key: &str, owner: &str) -> Result<()> {
        let pool = self.pool.clone();
        let ref mut con = pool.get()?;
        let lock_until = 10u64.pow(10);
        let res: RedisResult<String> = call_lua(
            con,
            r#" -- luaLock
        local lu = redis.call('HGET', KEYS[1], 'lockUntil')
        local lo = redis.call('HGET', KEYS[1], 'lockOwner')
        if lu == false or tonumber(lu) < tonumber(ARGV[2]) or lo == ARGV[1] then
            redis.call('HSET', KEYS[1], 'lockUntil', ARGV[2])
            redis.call('HSET', KEYS[1], 'lockOwner', ARGV[1])
            return 'LOCKED'
        end
        return lo"#,
            &[key],
            &[owner.to_redis_args(), lock_until.to_redis_args()],
        );
        match res {
            Ok(r) => {
                if r != "LOCKED" {
                    return Err(anyhow!(format!("{} has been locked by {}", key, r),));
                }
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// unlock_for_update unlocks the key, used in very strict strong consistency mode
    #[instrument]
    pub fn unlock_for_update(&self, key: &str, owner: &str) -> Result<()> {
        let pool = self.pool.clone();
        let ref mut con = pool.get()?;
        Ok(call_lua(
            con,
            r#" -- luaUnlock
            local lo = redis.call('HGET', KEYS[1], 'lockOwner')
            if lo == ARGV[1] then
                redis.call('HSET', KEYS[1], 'lockUntil', 0)
                redis.call('HDEL', KEYS[1], 'lockOwner')
                redis.call('EXPIRE', KEYS[1], ARGV[2])
            end"#,
            &[key],
            &[
                owner.to_redis_args(),
                self.options.lock_expire.as_secs().to_redis_args(),
            ],
        )?)
    }
}

impl Client {
    #[instrument]
    fn _lua_get_batch<T: FromRedisValue>(&self, keys: &[&str], owner: &str) -> Result<Vec<T>> {
        let pool = self.pool.clone();
        let ref mut con = pool.get()?;
        let script = r#"-- luaGetBatch
        local rets = {}
        for i, key in ipairs(KEYS)
        do
            local v = redis.call('HGET', key, 'value')
            local lu = redis.call('HGET', key, 'lockUntil')
            if lu ~= false and tonumber(lu) < tonumber(ARGV[1]) or lu == false and v == false then
                redis.call('HSET', key, 'lockUntil', ARGV[2])
                redis.call('HSET', key, 'lockOwner', ARGV[3])
                table.insert(rets, { v, 'LOCKED' })
            else
                table.insert(rets, {v, lu})
            end
        end
        return rets"#;
        Ok(call_lua(
            con,
            script,
            keys,
            &[
                now().to_redis_args(),
                (now() + self.options.lock_expire.as_secs() as u64).to_redis_args(),
                owner.to_redis_args(),
            ],
        )?)
    }

    #[instrument]
    fn _lua_set_batch<T>(
        &self,
        keys: &[String],
        values: &[Option<T>],
        expires: &[u64],
        owner: &str,
    ) -> Result<bool>
    where
        T: ToRedisArgs + std::fmt::Debug,
    {
        let mut vals = vec![owner.to_redis_args()];
        vals.extend(
            values
                .into_iter()
                .map(|x| x.to_redis_args())
                .collect::<Vec<_>>(),
        );
        vals.extend(
            expires
                .into_iter()
                .map(|x| x.to_redis_args())
                .collect::<Vec<_>>(),
        );
        let pool = self.pool.clone();
        let ref mut con = pool.get()?;
        Ok(call_lua(
            con,
            r#"-- luaSetBatch
            local n = #KEYS
            for i, key in ipairs(KEYS) do
                local o = redis.call('HGET', key, 'lockOwner')
                if o ~= ARGV[1] then
                    return false
                end
                redis.call('HSET', key, 'value', ARGV[i + 1])
                redis.call('HDEL', key, 'lockUntil')
                redis.call('HDEL', key, 'lockOwner')
                redis.call('EXPIRE', key, ARGV[i + 1 + n])
            end
            return true
            "#,
            keys,
            vals,
        )?)
    }

    #[instrument(skip(func))]
    fn _fetch_batch<T, E, F>(
        &self,
        keys: &[String],
        idxs: &[usize],
        expire: Duration,
        owner: &str,
        func: F,
    ) -> Result<HashMap<usize, T>>
    where
        T: FromRedisValue + ToRedisArgs + std::fmt::Debug,
        E: Into<anyhow::Error>,
        F: Fn(&[usize]) -> core::result::Result<HashMap<usize, T>, E>,
    {
        let data = match func(idxs) {
            Ok(r) => r,
            Err(e) => {
                for id in idxs {
                    if let Some(key) = keys.get(*id) {
                        let _ = self.unlock_for_update(key, owner);
                    }
                }
                return Err(e.into());
            }
        };

        let mut batch_keys = Vec::new();
        let mut batch_values = Vec::new();
        let mut batch_expires = Vec::new();

        let pool = self.pool.clone();
        let ref mut con = pool.get()?;

        for i in idxs {
            let v = data.get(&i);
            let mut ex = expire
                - self.options.delay
                - Duration::from_secs_f64(
                    rand::random::<f64>()
                        * self.options.random_expire_adjustment
                        * expire.as_secs_f64(),
                );
            if v.is_none() {
                if self.options.empty_expire == Duration::ZERO {
                    // if empty expire is 0, then delete the key
                    if let Some(r) = keys.get(*i) {
                        let _ = con.del::<_, ()>(r);
                    }
                    continue;
                }
                ex = self.options.empty_expire;

                // data.insert(i, "".to_owned()); // in case idx not in data
            }
            if let Some(r) = keys.get(*i) {
                batch_keys.push(r.clone());
            }
            batch_values.push(v);
            batch_expires.push(ex.as_secs() as u64);
        }

        let _ = self._lua_set_batch(&batch_keys, &batch_values, &batch_expires, owner);
        Ok(data)
    }

    #[instrument]
    fn _keys_idx(&self, keys: &[&str]) -> Vec<usize> {
        keys.iter().enumerate().map(|(idx, _)| idx).collect()
    }

    #[instrument(skip(func))]
    fn _weak_fetch_batch<T, E, F>(
        &self,
        keys: &[&str],
        expire: Duration,
        func: F,
    ) -> Result<HashMap<usize, T>>
    where
        T: FromRedisValue + ToRedisArgs + Send + Sync + Clone + std::fmt::Debug + 'static,
        E: Into<anyhow::Error>,
        F: 'static + Send + Copy + Fn(&[usize]) -> core::result::Result<HashMap<usize, T>, E>,
    {
        let mut result: HashMap<usize, T> = HashMap::new();
        let owner = uuid::Uuid::new_v4().to_string();

        let mut to_get = Vec::new();
        let mut to_fetch = Vec::new();
        let mut to_fetch_async = Vec::new();

        // read from redis without sleep
        let res: Vec<LockableValue<T>> = self._lua_get_batch(keys, owner.as_str())?;
        for (i, v) in res.iter().enumerate() {
            match v {
                LockableValue::Nil(r) => match r {
                    Some(s) => {
                        result.insert(i, s.clone());
                    }
                    None => {
                        to_get.push(i);
                        continue;
                    }
                },
                LockableValue::Value(r) => match r {
                    Some(s) => {
                        to_fetch_async.push(i);
                        result.insert(i, s.clone());
                    }
                    None => {
                        to_fetch.push(i);
                        continue;
                    }
                },
                LockableValue::Locked(r) => match r {
                    Some(s) => {
                        result.insert(i, s.clone());
                    }
                    None => {
                        to_get.push(i);
                        continue;
                    }
                },
            };
        }

        if !to_fetch_async.is_empty() {
            let this = self.clone();
            let owner = owner.clone();
            let func = func.clone();
            let keys: Vec<String> = keys.iter().map(|x| x.to_string()).collect();
            let to_fetch_async = to_fetch_async.clone();
            self.threads.execute(move || {
                let _ = this._fetch_batch(&keys, &to_fetch_async, expire, owner.as_str(), func);
                trace!("to_fetch_async async run finished");
            });
            // to_fetch_async.clear(); // reset to_fetch
        }

        if !to_fetch.is_empty() {
            // batch fetch
            let key1: Vec<String> = keys.iter().map(|x| x.to_string()).collect();
            let to_fetch = to_fetch.clone();
            let fetched = self._fetch_batch(&key1, &to_fetch, expire, owner.as_str(), func)?;
            for k in to_fetch {
                if let Some(r) = fetched.get(&k) {
                    result.insert(k, r.clone());
                }
                // result.insert(k, fetched.get(&k).unwrap_or(&"".to_owned()).clone());
            }
            // to_fetch = Vec::new(); // reset to_fetch
        }

        if !to_get.is_empty() {
            // read from redis and sleep to wait
            let (tx, rx) = mpsc::channel::<Pair<T>>();
            let ths = ThreadPool::new(to_get.len());
            for idx in to_get {
                let tx = tx.clone();
                let owner = owner.clone();
                let key = match keys.get(idx) {
                    Some(r) => r.to_string(),
                    None => continue,
                };
                let this = self.clone();
                ths.execute(move || loop {
                    match this._lua_get::<LockableValue<T>>(&key, owner.as_str()) {
                        Ok(LockableValue::Nil(r)) => {
                            let _ = tx.send(Pair {
                                idx,
                                data: r,
                                err: None,
                            });
                            return;
                        }
                        Ok(LockableValue::Value(r)) => match r {
                            Some(_) => {
                                let _ = tx.send(Pair {
                                    idx,
                                    data: None,
                                    err: Some(crate::types::Errors::NeedAsyncFetch),
                                });
                                return;
                            }
                            None => {
                                let _ = tx.send(Pair {
                                    idx,
                                    data: None,
                                    err: Some(crate::types::Errors::NeedFetch),
                                });
                                return;
                            }
                        },
                        Ok(LockableValue::Locked(r)) => match r {
                            Some(r) => {
                                let _ = tx.send(Pair {
                                    idx,
                                    data: Some(r),
                                    err: None,
                                });
                                return;
                            }
                            None => {
                                sleep(this.options.lock_sleep);
                                continue;
                            }
                        },
                        Err(e) => {
                            let _ = tx.send(Pair {
                                idx,
                                data: None,
                                err: Some(crate::types::Errors::Custom(e)),
                            });
                            return;
                        }
                    }
                });
            }
            ths.join();
            drop(tx);

            while let Ok(p) = rx.recv() {
                if let Some(err) = p.err {
                    match err {
                        crate::types::Errors::NeedFetch => {
                            to_fetch.push(p.idx);
                            continue;
                        }
                        crate::types::Errors::NeedAsyncFetch => {
                            to_fetch_async.push(p.idx);
                            continue;
                        }
                        crate::types::Errors::Custom(e) => return Err(e),
                    }
                }
                if let Some(r) = p.data {
                    result.insert(p.idx, r);
                }
            }
        }

        if !to_fetch_async.is_empty() {
            let this = self.clone();
            let keys: Vec<String> = keys.iter().map(|x| x.to_string()).collect();
            let func = func.clone();
            let owner = owner.clone();
            self.threads.execute(move || {
                let _ = this._fetch_batch(&keys, &to_fetch_async, expire, owner.as_str(), func);
                trace!("to_fetch_async async run finished");
            });
        }

        if !to_fetch.is_empty() {
            // batch fetch
            let keys: Vec<String> = keys.iter().map(|x| x.to_string()).collect();
            let fetched = self._fetch_batch(&keys, &to_fetch, expire, owner.as_str(), func)?;
            for k in to_fetch {
                if let Some(r) = fetched.get(&k) {
                    result.insert(k, r.clone());
                }
            }
        }

        Ok(result)
    }

    #[instrument(skip(func))]
    fn _strong_fetch_batch<T, E, F>(
        &self,
        keys: &[&str],
        expire: Duration,
        func: F,
    ) -> Result<HashMap<usize, T>>
    where
        T: FromRedisValue + ToRedisArgs + Send + Sync + Clone + std::fmt::Debug + 'static,
        E: Into<anyhow::Error>,
        F: Copy + Fn(&[usize]) -> core::result::Result<HashMap<usize, T>, E>,
    {
        let mut result = HashMap::new();
        let owner = Uuid::new_v4().to_string();

        let mut to_get = Vec::new();
        let mut to_fetch = Vec::new();

        // read from redis without sleep
        let rs: Vec<LockableValue<T>> = self._lua_get_batch(keys, owner.as_str())?;
        for (i, r) in rs.into_iter().enumerate() {
            match r {
                LockableValue::Nil(s) => {
                    // normal value
                    if let Some(r) = s {
                        result.insert(i, r);
                    }
                }
                LockableValue::Value(_) => {
                    // locked for fetch
                    to_get.push(i);
                }
                LockableValue::Locked(_) => {
                    // locked by other
                    to_fetch.push(i);
                }
            }
        }

        if !to_fetch.is_empty() {
            // batch fetch
            let keys: Vec<String> = keys.iter().map(|x| x.to_string()).collect();
            let fetched = self._fetch_batch(&keys, &to_fetch, expire, owner.as_str(), func)?;
            for k in &to_fetch {
                if let Some(r) = fetched.get(&k) {
                    result.insert(*k, r.clone());
                }
            }
        }

        if !to_get.is_empty() {
            // read from redis and sleep to wait
            // let mut tasks: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
            let ths = ThreadPool::new(to_get.len());
            let (tx, rx) = mpsc::channel::<Pair<T>>();
            for idx in to_get {
                let key = match keys.get(idx) {
                    Some(r) => r.to_string(),
                    None => continue,
                };
                let owner = owner.clone();
                let tx = tx.clone();
                let c = self.clone();
                ths.execute(move || loop {
                    match c._lua_get::<LockableValue<T>>(key.as_str(), owner.as_str()) {
                        Ok(r) => match r {
                            LockableValue::Nil(s) => {
                                let _ = tx.send(Pair {
                                    idx,
                                    data: s,
                                    err: None,
                                });
                                return;
                            }
                            LockableValue::Value(_) => {
                                let _ = tx.send(Pair {
                                    idx,
                                    data: None,
                                    err: Some(crate::types::Errors::NeedFetch),
                                });
                                return;
                            }
                            LockableValue::Locked(_) => {
                                sleep(c.options.lock_sleep);
                                continue;
                            }
                        },
                        Err(e) => {
                            let _ = tx.send(Pair {
                                idx,
                                data: None,
                                err: Some(crate::types::Errors::Custom(e)),
                            });
                            return;
                        }
                    }
                })
            }

            drop(tx);
            while let Ok(p) = rx.recv() {
                if let Some(err) = p.err {
                    match err {
                        crate::types::Errors::NeedFetch => {
                            to_fetch.push(p.idx);
                            continue;
                        }
                        crate::types::Errors::Custom(e) => {
                            return Err(e);
                        }
                        crate::types::Errors::NeedAsyncFetch => {
                            continue;
                        }
                    }
                }
                if let Some(r) = p.data {
                    result.insert(p.idx, r);
                }
            }
        }

        if !to_fetch.is_empty() {
            // batch fetch
            let keys: Vec<String> = keys.iter().map(|x| x.to_string()).collect();
            let fetched = self._fetch_batch(&keys, &to_fetch, expire, owner.as_str(), func)?;
            for k in to_fetch {
                if let Some(r) = fetched.get(&k) {
                    result.insert(k, r.clone());
                }
            }
        }

        Ok(result)
    }

    #[instrument(skip(func))]
    pub fn fetch_batch<T, E, F>(
        &self,
        keys: &[&str],
        expire: Duration,
        func: F,
    ) -> Result<HashMap<usize, T>>
    where
        T: FromRedisValue + ToRedisArgs + Send + Sync + Clone + std::fmt::Debug + 'static,
        E: Into<anyhow::Error>,
        F: 'static
            + Copy
            + Send
            + Clone
            + Fn(&[usize]) -> core::result::Result<HashMap<usize, T>, E>,
    {
        if self.options.disable_cache_read {
            match func(&self._keys_idx(keys)) {
                Ok(r) => Ok(r),
                Err(e) => Err(e.into()),
            }
        } else if self.options.strong_consistency {
            self._strong_fetch_batch(keys, expire, func)
        } else {
            self._weak_fetch_batch(keys, expire, func)
        }
    }

    #[instrument]
    pub fn tag_as_deleted_batch(&self, keys: &[&str]) -> Result<()> {
        if self.options.disable_cache_delete {
            return Ok(());
        }
        let script = r#"-- luaDeleteBatch
        for i, key in ipairs(KEYS) do
            redis.call('HSET', key, 'lockUntil', 0)
            redis.call('HDEL', key, 'lockOwner')
            redis.call('EXPIRE', key, ARGV[1])
        end"#;
        let pool = self.pool.clone();
        let ref mut con = pool.get()?;
        if self.options.wait_replicas > 0 {
            let _ = call_lua(con, script, keys, &[self.options.delay.as_secs()])?;

            let replicas: i32 = redis::cmd("WAIT")
                .arg(&[
                    self.options.wait_replicas as u64,
                    self.options.wait_replicas_timeout.as_secs(),
                ])
                .query(con)?;
            if replicas < self.options.wait_replicas {
                return Err(anyhow!(format!(
                    "wait replicas {} failed. result replicas: {}",
                    self.options.wait_replicas, replicas
                ),));
            }
            return Ok(());
        }
        Ok(call_lua(
            con,
            script,
            keys,
            &[self.options.delay.as_secs()],
        )?)
    }
}

#[cfg(test)]
mod client_tests {
    use std::thread::{sleep, spawn};
    use std::time::Duration;

    use anyhow::Result;
    use redis::{RedisError, RedisResult};
    use tracing;
    use tracing_test::traced_test;

    use crate::types::Options;
    use crate::Client;

    fn gen_data_func(value: String, sleep_milli: u32) -> RedisResult<Option<String>> {
        sleep(Duration::new(0, sleep_milli * 1000));
        Ok(Some(value))
    }
    #[test]
    #[traced_test]
    fn test_weak_fetch() {
        let rc = Client::new("redis://127.0.0.1:6379/", Options::default()).unwrap();
        let ref mut con = rc.pool.get().unwrap();
        let _: () = redis::cmd("FLUSHDB").query(con).unwrap();

        let expected = "value1";
        let rdb_key = "client-test-key";
        let rc1 = rc.clone();
        spawn(move || {
            let res = rc1.fetch(rdb_key, Duration::new(60, 0), || {
                gen_data_func("value1".to_string(), 200)
            });
            assert!(res.is_ok());
            assert!(res.unwrap().is_some_and(|x| expected.to_string() == x));
        });

        sleep(Duration::new(0, 20_000));

        let res = rc.fetch(rdb_key, Duration::new(60, 0), || {
            gen_data_func("value1".to_string(), 201)
        });
        assert!(res.is_ok());
        assert!(res.unwrap().is_some_and(|x| expected.to_string() == x));

        let res = rc.tag_as_deleted(rdb_key);
        assert!(res.is_ok());

        let nv = "value2";
        let res = rc.fetch(rdb_key, Duration::new(60, 0), || {
            gen_data_func("value2".to_string(), 200)
        });
        assert!(res.is_ok());
        assert!(res.unwrap().is_some_and(|x| expected.to_string() == x));

        sleep(Duration::new(1, 0));

        let res = rc.fetch(rdb_key, Duration::new(60, 0), || {
            gen_data_func("ignored".to_owned(), 200)
        });
        assert!(res.is_ok());
        assert!(res.unwrap().is_some_and(|x| nv.to_string() == x));
    }

    #[test]
    #[traced_test]
    fn test_strong_fetch() {
        let rc = Client::new(
            "redis://127.0.0.1:6379/",
            Options {
                strong_consistency: true,
                ..Options::default()
            },
        )
        .unwrap();
        let ref mut con = rc.pool.get().unwrap();
        let _: () = redis::cmd("FLUSHDB").query(con).unwrap();

        // let began = now();
        let expected = "value1";
        let rdb_key = "client-test-key1";
        let rc1 = rc.clone();
        spawn(move || {
            let res = rc1.fetch(rdb_key, Duration::new(60, 0), || {
                gen_data_func("value1".to_string(), 200)
            });
            assert!(res.is_ok());
            assert!(res.unwrap().is_some_and(|x| expected.to_string() == x));
        });

        sleep(Duration::new(0, 20_000));

        let res = rc.fetch(rdb_key, Duration::new(60, 0), || {
            gen_data_func("value1".to_string(), 201)
        });
        assert!(res.is_ok());
        assert!(res.unwrap().is_some_and(|x| expected.to_string() == x));

        let res = rc.tag_as_deleted(rdb_key);
        assert!(res.is_ok());

        let nv = "value2";
        let res = rc.fetch(rdb_key, Duration::new(60, 0), || {
            gen_data_func("value2".to_string(), 200)
        });
        assert!(res.is_ok());
        assert!(res.unwrap().is_some_and(|x| nv.to_string() == x));

        let res = rc.fetch(rdb_key, Duration::new(60, 0), || {
            gen_data_func("ignored".to_owned(), 200)
        });
        assert!(res.is_ok());
        assert!(res.unwrap().is_some_and(|x| nv.to_string() == x));
    }

    #[test]
    #[traced_test]
    fn test_strong_error_fetch() {
        let rc = Client::new(
            "redis://127.0.0.1:6379/",
            Options {
                strong_consistency: true,
                ..Options::default()
            },
        )
        .unwrap();
        let ref mut con = rc.pool.get().unwrap();
        let _: () = redis::cmd("FLUSHDB").query(con).unwrap();
        let rdb_key = "client-test-key2";
        let res: Result<Option<String>> = rc.fetch(rdb_key, Duration::new(60, 0), || {
            Err(RedisError::from((
                redis::ErrorKind::TypeError,
                "fetch error",
            )))
        });
        assert!(res.is_err());
        let res: Result<Option<String>> = rc.fetch(rdb_key, Duration::new(60, 0), || {
            Ok::<Option<String>, anyhow::Error>(None)
        });
        assert!(res.is_ok());
    }

    #[test]
    #[traced_test]
    fn test_weak_error_fetch() {
        let rc = Client::new("redis://127.0.0.1:6379/", Options::default()).unwrap();
        let ref mut con = rc.pool.get().unwrap();
        let _: () = redis::cmd("FLUSHDB").query(con).unwrap();
        let rdb_key = "client-test-key2";
        let res: Result<Option<String>> = rc.fetch(rdb_key, Duration::new(60, 0), || {
            Err(RedisError::from((
                redis::ErrorKind::TypeError,
                "fetch error",
            )))
        });
        assert!(res.is_err());
        let res: Result<Option<String>> = rc.fetch(rdb_key, Duration::new(60, 0), || {
            Ok::<Option<String>, anyhow::Error>(None)
        });
        assert!(res.is_ok());
    }

    #[test]
    #[traced_test]
    fn test_raw_get() {
        let rc = Client::new("redis://127.0.0.1:6379/", Options::default()).unwrap();
        let res: Result<i32, anyhow::Error> = rc.raw_get("non_exist");
        assert!(res.is_err());
    }

    #[test]
    #[traced_test]
    fn test_raw_set() {
        let rc = Client::new("redis://127.0.0.1:6379/", Options::default()).unwrap();
        let res = rc.raw_set("eeeee", "value", Duration::from_secs(60));
        assert!(res.is_ok());
    }

    #[test]
    #[traced_test]
    fn test_lock() {
        let rc = Client::new(
            "redis://127.0.0.1:6379/",
            Options {
                strong_consistency: true,
                ..Options::default()
            },
        )
        .unwrap();

        let owner = "test_owner";
        let key = "test_lock";
        let res = rc.lock_for_update(key, owner);
        assert!(res.is_ok());
        let res = rc.lock_for_update(key, "other_owner");
        assert!(res.is_err());
        let res = rc.unlock_for_update(key, owner);
        assert!(res.is_ok());
    }
}

#[cfg(test)]
mod batch_tests {
    use std::collections::HashMap;
    use std::thread::{sleep, spawn};
    use std::time::Duration;

    use anyhow::Result;
    use redis::{RedisError, RedisResult};
    use tracing;
    use tracing_test::traced_test;

    use crate::types::Options;
    use crate::Client;

    fn gen_data_func(value: &[usize]) -> Result<HashMap<usize, String>> {
        sleep(Duration::new(0, 2000));
        let mut res = HashMap::new();
        for i in value {
            res.insert(*i, format!("value_{}", i));
        }
        Ok(res)
    }

    fn gen_data_func1(value: &[usize]) -> Result<HashMap<usize, String>> {
        sleep(Duration::new(0, 2000));
        let mut res = HashMap::new();
        for i in value {
            res.insert(*i, format!("eulav_{}", i));
        }
        Ok(res)
    }

    fn gen_data_func2(value: &[usize]) -> Result<HashMap<usize, String>> {
        sleep(Duration::new(0, 2000));
        let mut res = HashMap::new();
        for i in value {
            res.insert(*i, format!("vvvvv_{}", i));
        }
        Ok(res)
    }

    #[test]
    #[traced_test]
    fn test_weak_fetch_batch() {
        let rc = Client::new("redis://127.0.0.1:6379/", Options::default()).unwrap();
        let ref mut con = rc.pool.get().unwrap();
        let _: () = redis::cmd("FLUSHDB").query(con).unwrap();

        let rc1 = rc.clone();
        spawn(move || {
            let keys: Vec<&str> = vec!["key_0", "key_1", "key_2"];
            let mut values = HashMap::new();
            values.insert(0, "value_0".to_string());
            values.insert(1, "value_1".to_string());
            values.insert(2, "value_2".to_string());
            let res = rc1.fetch_batch(&keys, Duration::from_secs(60), gen_data_func);
            assert!(res.is_ok());
            assert_eq!(res.unwrap(), values);
        });

        sleep(Duration::new(0, 20_000));

        let keys = vec!["key_0", "key_1", "key_2"];
        let mut values = HashMap::new();
        values.insert(0, "value_0".to_string());
        values.insert(1, "value_1".to_string());
        values.insert(2, "value_2".to_string());
        let res = rc.fetch_batch(&keys, Duration::from_secs(60), gen_data_func);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), values);

        let res = rc.tag_as_deleted_batch(&keys);
        assert!(res.is_ok());

        let res = rc.fetch_batch(&keys, Duration::from_secs(60), gen_data_func1);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), values);

        let res = rc.fetch_batch(&keys, Duration::from_secs(60), gen_data_func2);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), values);

        sleep(Duration::new(1, 0));

        let mut values2 = HashMap::new();
        values2.insert(0, "vvvvv_0".to_string());
        values2.insert(1, "vvvvv_1".to_string());
        values2.insert(2, "vvvvv_2".to_string());
        let res = rc.fetch_batch(&keys, Duration::from_secs(60), gen_data_func2);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), values2);
    }

    #[test]
    #[traced_test]
    fn test_weak_fetch_batch_overlap() {}

    #[test]
    #[traced_test]
    fn test_strong_fetch_batch() {}

    #[test]
    #[traced_test]
    fn test_strong_fetch_batch_overlap() {}

    #[test]
    #[traced_test]
    fn test_strong_fetch_batch_overlap_expire() {}

    #[test]
    #[traced_test]
    fn test_strong_error_fetch_batch() {}
}
