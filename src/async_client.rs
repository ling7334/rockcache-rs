use anyhow::{anyhow, bail, Result};
use bb8_redis::{bb8::Pool, RedisMultiplexedConnectionManager};
use redis::{AsyncCommands, FromRedisValue, ToRedisArgs};
use std::collections::HashMap;
use std::future::Future;
use std::ops::DerefMut;
use std::pin::Pin;
use std::time::Duration;
use tokio::{sync::mpsc, task::JoinSet, time::sleep};
use tracing::{error, instrument, trace};
use uuid::Uuid;

use crate::types::{self, LockableValue, Options, Pair};
use crate::utils::{call_lua_async, now};
use singleflight::Group;

#[derive(Clone, Debug)]
/// Client represent a Rockscache client
pub struct AsyncClient {
    pool: Pool<RedisMultiplexedConnectionManager>,
    options: Options,
    group: Group,
}

impl AsyncClient {
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
    #[instrument]
    pub async fn new(uri: &str, options: Options) -> Result<Self> {
        if options.delay == Duration::ZERO || options.lock_expire == Duration::ZERO {
            panic!("cache options error: Delay and LockExpire should not be 0, you should call NewDefaultOptions() to get default options");
        }

        Ok(AsyncClient {
            pool: bb8_redis::bb8::Pool::builder()
                .build(RedisMultiplexedConnectionManager::new(uri).unwrap())
                .await?,
            options,
            group: Group::new(),
        })
    }

    /// tag_as_deleted a key, the key will expire after delay time.
    #[instrument]
    pub async fn tag_as_deleted(&self, key: &str) -> Result<bool> {
        if self.options.disable_cache_delete {
            return Ok(false);
        }
        let script = r#"
            -- delete
            redis.call('HSET', KEYS[1], 'lockUntil', 0)
            redis.call('HDEL', KEYS[1], 'lockOwner')
            redis.call('EXPIRE', KEYS[1], ARGV[1])
        "#;
        let delay_sec = self.options.delay.as_secs();
        let ref keys = [key];
        let ref args = [delay_sec];
        let pool = self.pool.clone();
        let mut pool_con = pool.get().await?;
        let con = pool_con.deref_mut();
        if self.options.wait_replicas > 0 {
            call_lua_async(con, script, keys, args).await?;

            let replicas: i32 = redis::cmd("WAIT")
                .arg(self.options.wait_replicas)
                .arg(self.options.wait_replicas_timeout.as_secs())
                .query_async(con)
                .await?;
            if replicas < self.options.wait_replicas {
                bail!(format!(
                    "wait replicas {} failed. result replicas: {}",
                    self.options.wait_replicas, replicas,
                ));
            }
        }
        call_lua_async(con, script, keys, args).await?;
        Ok(true)
    }

    /// fetch returns the value store in cache indexed by the key.
    /// If the key doest not exists, call fn to get result, store it in cache, then return.
    #[instrument(skip(func))]
    pub async fn fetch<T, E>(
        &self,
        key: &str,
        expire: Duration,
        func: impl Future<Output = std::result::Result<Option<T>, E>> + Send + 'static,
    ) -> Result<Option<T>>
    where
        T: FromRedisValue + ToRedisArgs + Clone + Send + Sync + std::fmt::Debug + 'static,
        E: Into<anyhow::Error> + Send + Sync + std::fmt::Debug,
    {
        let ex = expire
            - self.options.delay
            - Duration::from_secs_f64(
                rand::random::<f64>()
                    * self.options.random_expire_adjustment
                    * expire.as_secs_f64(),
            );
        self.group
            .async_work(key, async {
                if self.options.disable_cache_read {
                    match func.await {
                        Ok(r) => Ok(r),
                        Err(e) => Err(anyhow!(e)),
                    }
                } else if self.options.strong_consistency {
                    self._strong_fetch(key, ex, func).await
                } else {
                    self._weak_fetch(key, ex, func).await
                }
            })
            .await
    }

    #[instrument(skip(func))]
    async fn _strong_fetch<T, E>(
        &self,
        key: &str,
        ex: Duration,
        func: impl Future<Output = std::result::Result<Option<T>, E>>,
    ) -> Result<Option<T>>
    where
        T: FromRedisValue + ToRedisArgs + Clone + std::fmt::Debug,
        E: Into<anyhow::Error> + Send + Sync + std::fmt::Debug,
    {
        let owner = Uuid::new_v4().to_string();
        let owner = owner.as_str();

        loop {
            match self._lua_get::<LockableValue<T>>(key, owner).await {
                Ok(LockableValue::Nil(r)) => return Ok(r),
                Ok(LockableValue::Value(_)) => return self._fetch_new(key, ex, owner, func).await,
                Ok(LockableValue::Locked(_)) => {
                    // locked by other
                    sleep(self.options.lock_sleep).await;
                    continue;
                }
                Err(e) => bail!(e),
            }
        }
    }

    #[instrument(skip(func))]
    async fn _weak_fetch<T, E>(
        &self,
        key: &str,
        ex: Duration,
        func: impl Future<Output = std::result::Result<Option<T>, E>> + Send + 'static,
    ) -> Result<Option<T>>
    where
        T: FromRedisValue + ToRedisArgs + Clone + Send + Sync + std::fmt::Debug,
        E: Into<anyhow::Error> + Send + Sync + std::fmt::Debug,
    {
        let owner = Uuid::new_v4().to_string();

        loop {
            match self._lua_get::<LockableValue<T>>(key, owner.as_str()).await {
                Ok(LockableValue::Nil(r)) => match r {
                    Some(v) => {
                        trace!("LockableValue::Nil: {:?}", v);
                        return Ok(v.into());
                    }
                    None => {
                        trace!("LockableValue::Nil: None sleep");
                        sleep(self.options.lock_sleep).await;
                        continue;
                    }
                },
                Ok(LockableValue::Value(r)) => match r {
                    Some(v) => {
                        trace!("LockableValue::Value: {:?} _fetch_new return old", v);
                        let this = self.clone();
                        let key = key.to_owned();
                        let owner = owner.to_owned();
                        let afunc = async move {
                            let _ = this
                                ._fetch_new(key.as_str(), ex, owner.as_str(), func)
                                .await;
                            trace!("async fetch new complete");
                        };
                        tokio::spawn(afunc);
                        return Ok(v.into());
                    }
                    None => {
                        trace!("LockableValue::Value: None _fetch_new");
                        return self._fetch_new(key, ex, owner.as_str(), func).await;
                    }
                },
                Ok(LockableValue::Locked(r)) => match r {
                    Some(v) => {
                        trace!("LockableValue::Locked: {:?} return", v);
                        return Ok(v.into());
                    }
                    None => {
                        trace!("LockableValue::Locked: None sleep");
                        sleep(self.options.lock_sleep).await;
                        continue;
                    }
                },
                Err(e) => {
                    bail!(e);
                }
            }
        }
    }

    #[instrument]
    async fn _lua_get<T>(&self, key: &str, owner: &str) -> Result<T>
    where
        T: FromRedisValue,
    {
        let pool = self.pool.clone();
        let mut pool_con = pool.get().await?;
        let con = pool_con.deref_mut();

        match call_lua_async(
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
                now().to_string(),
                (now() + (self.options.lock_expire.as_secs())).to_string(),
                owner.to_string(),
            ],
        )
        .await
        {
            Ok(r) => Ok(r),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument]
    async fn _lua_set<T>(&self, key: &str, value: T, expire: i32, owner: &str) -> Result<bool>
    where
        T: ToRedisArgs + std::fmt::Debug,
    {
        let pool = self.pool.clone();
        let mut pool_con = pool.get().await?;
        let con = pool_con.deref_mut();

        let res: bool = call_lua_async(
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
        )
        .await?;
        Ok(res)
    }

    #[instrument(skip(func))]
    async fn _fetch_new<T, E>(
        &self,
        key: &str,
        ex: Duration,
        owner: &str,
        func: impl Future<Output = std::result::Result<Option<T>, E>>,
    ) -> Result<Option<T>>
    where
        T: FromRedisValue + ToRedisArgs + Clone + std::fmt::Debug,
        E: Into<anyhow::Error> + Send + Sync + std::fmt::Debug,
    {
        match func.await {
            Err(e) => {
                error! {"func execute error {:?}", e};
                let _ = self.unlock_for_update(key, owner).await?;
                return Err(e.into());
            }
            Ok(r) => {
                trace! {"func execute result {:?}", r};
                let mut expire = ex;
                if r.is_none() {
                    if self.options.empty_expire == Duration::ZERO {
                        let pool = self.pool.clone();
                        let mut con = pool.get().await?;
                        return match con.del::<&str, T>(key).await {
                            Ok(_) => Ok(None),
                            Err(e) => Err(e.into()),
                        };
                    }
                    expire = self.options.empty_expire;
                }

                match self
                    ._lua_set(key, r.to_owned(), expire.as_secs() as i32, owner)
                    .await
                {
                    Err(e) => {
                        error! {"_lua_set error {}", e};
                        return Err(e.into());
                    }
                    Ok(_) => return Ok(r),
                }
            }
        }
    }

    /// raw_get returns the value store in cache indexed by the key, no matter if the key locked or not
    #[instrument]
    pub async fn raw_get<T>(&self, key: &str) -> Result<T>
    where
        T: FromRedisValue,
    {
        let pool = self.pool.clone();
        let mut pool_con = pool.get().await?;
        let con = pool_con.deref_mut();
        match con.hget::<_, _, T>(key, "value").await {
            Ok(s) => Ok(s),
            Err(e) => Err(e.into()),
        }
    }
    /// raw_set sets the value store in cache indexed by the key, no matter if the key locked or not
    #[instrument]
    pub async fn raw_set<T>(&self, key: &str, value: T, expire: Duration) -> Result<bool>
    where
        T: ToRedisArgs + Send + Sync + std::fmt::Debug,
    {
        let pool = self.pool.clone();
        let mut pool_con = pool.get().await?;
        let con = pool_con.deref_mut();
        match con.hset::<_, _, _, ()>(key.clone(), "value", value).await {
            Err(e) => Err(e.into()),
            Ok(_) => {
                con.expire(key, expire.as_secs() as usize).await?;
                return Ok(true);
            }
        }
    }
    /// lock_for_update locks the key, used in very strict strong consistency mode
    #[instrument]
    pub async fn lock_for_update(&self, key: &str, owner: &str) -> Result<bool> {
        let pool = self.pool.clone();
        let mut pool_con = pool.get().await?;
        let con = pool_con.deref_mut();
        let lock_until = 10u64.pow(10);
        match call_lua_async::<_, _, _, String>(
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
            &[key.clone()],
            &[owner.to_string(), lock_until.to_string()],
        )
        .await
        {
            Ok(r) => {
                if r != "LOCKED" {
                    return Err(anyhow!(format!("{} has been locked by {}", key, r)));
                }
                Ok(true)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// unlock_for_update unlocks the key, used in very strict strong consistency mode
    #[instrument]
    pub async fn unlock_for_update(&self, key: &str, owner: &str) -> Result<bool> {
        let pool = self.pool.clone();
        let mut pool_con = pool.get().await?;
        let con = pool_con.deref_mut();
        match call_lua_async::<_, _, _, ()>(
            con,
            r#" -- luaUnlock
            local lo = redis.call('HGET', KEYS[1], 'lockOwner')
            if lo == ARGV[1] then
                redis.call('HSET', KEYS[1], 'lockUntil', 0)
                redis.call('HDEL', KEYS[1], 'lockOwner')
                redis.call('EXPIRE', KEYS[1], ARGV[2])
            end"#,
            &[key.clone()],
            &[
                owner.to_string(),
                self.options.lock_expire.as_secs().to_string(),
            ],
        )
        .await
        {
            Ok(_) => Ok(true),
            Err(e) => Err(e.into()),
        }
    }
}

impl AsyncClient {
    #[instrument]
    async fn _lua_get_batch<T: FromRedisValue>(
        &self,
        keys: Vec<String>,
        owner: &str,
    ) -> Result<Vec<T>> {
        let pool = self.pool.clone();
        let mut pool_con = pool.get().await?;
        let con = pool_con.deref_mut();
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
        match call_lua_async(
            con,
            script,
            keys,
            &[
                now().to_redis_args(),
                (now() + self.options.lock_expire.as_secs() as u64).to_redis_args(),
                owner.to_redis_args(),
            ],
        )
        .await
        {
            Ok(r) => Ok(r),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument]
    async fn _lua_set_batch<T>(
        &self,
        keys: Vec<String>,
        values: Vec<Option<T>>,
        expires: &[u64],
        owner: &str,
    ) -> Result<bool>
    where
        T: ToRedisArgs + std::fmt::Debug,
    {
        let mut vals = vec![owner.to_redis_args()];
        vals.extend(values.into_iter().map(|x| x.to_redis_args()));
        vals.extend(
            expires
                .into_iter()
                .map(|x| x.to_redis_args())
                .collect::<Vec<_>>(),
        );

        let pool = self.pool.clone();
        let mut pool_con = pool.get().await?;
        let con = pool_con.deref_mut();
        match call_lua_async(
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
        )
        .await
        {
            Ok(r) => Ok(r),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(func))]
    async fn _fetch_batch<T, E, F>(
        &self,
        keys: Vec<String>,
        idxs: &[usize],
        expire: Duration,
        owner: &str,
        func: F,
    ) -> Result<HashMap<usize, T>>
    where
        F: Fn(
            &[usize],
        ) -> Pin<
            Box<dyn Future<Output = core::result::Result<HashMap<usize, T>, E>> + Send + Sync>,
        >,
        T: FromRedisValue + ToRedisArgs + std::fmt::Debug,
        E: Into<anyhow::Error> + std::fmt::Debug,
    {
        let data = match func(idxs).await {
            Ok(r) => r,
            Err(e) => {
                for id in idxs {
                    let _ = self.unlock_for_update(id.to_string().as_str(), owner);
                }
                return Err(e.into());
            }
        };

        let mut batch_keys = Vec::new();
        let mut batch_values = Vec::new();
        let mut batch_expires = Vec::new();

        let pool = self.pool.clone();
        let mut pool_con = pool.get().await?;
        let con = pool_con.deref_mut();

        for i in idxs {
            let v = data.get(i);
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
                    let _ = con.del::<_, Option<T>>(keys[*i].clone()).await;
                    continue;
                }
                ex = self.options.empty_expire;

                // data.insert(*i); // in case idx not in data
            }
            batch_keys.push(keys[*i].clone());
            batch_values.push(v);
            batch_expires.push(ex.as_secs() as u64);
        }

        let _ = self._lua_set_batch(batch_keys, batch_values, &batch_expires, owner);
        Ok(data)
    }

    #[instrument]
    fn _keys_idx(&self, keys: Vec<String>) -> Vec<usize> {
        keys.iter().enumerate().map(|(idx, _)| idx).collect()
    }

    #[instrument(skip(func))]
    async fn _weak_fetch_batch<T, E, F>(
        &self,
        keys: Vec<String>,
        expire: Duration,
        func: F,
    ) -> Result<HashMap<usize, T>>
    where
        F: 'static
            + Send
            + Sync
            + Copy
            + Fn(
                &[usize],
            ) -> Pin<
                Box<dyn Future<Output = core::result::Result<HashMap<usize, T>, E>> + Send + Sync>,
            >,
        T: FromRedisValue + ToRedisArgs + Send + Sync + Copy + std::fmt::Debug + 'static,
        E: Into<anyhow::Error> + std::fmt::Debug,
    {
        let mut result: HashMap<usize, T> = HashMap::new();
        let owner = uuid::Uuid::new_v4().to_string();

        let mut to_get = Vec::new();
        let mut to_fetch = Vec::new();
        let mut to_fetch_async = Vec::new();

        // read from redis without sleep
        let res: Vec<LockableValue<T>> = self._lua_get_batch(keys.clone(), owner.as_str()).await?;
        let size = res.len();
        for (i, v) in res.iter().enumerate() {
            match v {
                LockableValue::Nil(r) => match r {
                    Some(s) => {
                        result.insert(i, *s);
                    }
                    None => {
                        to_get.push(i);
                        continue;
                    }
                },
                LockableValue::Value(r) => match r {
                    Some(s) => {
                        to_fetch_async.push(i);
                        result.insert(i, *s);
                    }
                    None => {
                        to_fetch.push(i);
                        continue;
                    }
                },
                LockableValue::Locked(r) => match r {
                    Some(s) => {
                        result.insert(i, *s);
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
            let keys = keys.clone();
            let func = func.clone();
            let owner = owner.clone();
            tokio::spawn(async move {
                let _ = this
                    ._fetch_batch(keys, &to_fetch_async, expire, owner.as_str(), func)
                    .await;
            });
            to_fetch_async = Vec::new(); // reset to_fetch
        }

        if !to_fetch.is_empty() {
            // batch fetch
            let to_fetch = to_fetch.clone();

            let fetched = self
                ._fetch_batch(keys.clone(), &to_fetch, expire, owner.as_str(), func)
                .await?;
            for k in to_fetch.into_iter() {
                if let Some(r) = fetched.get(&k) {
                    result.insert(k, *r);
                }
            }
            // to_fetch.clear(); // reset to_fetch
        }

        if !to_get.is_empty() {
            // read from redis and sleep to wait
            let (tx, mut rx) = mpsc::channel::<Pair<T>>(size);
            let mut tasks = JoinSet::new();
            for idx in to_get {
                let this = self.clone();
                let keys = keys.clone();
                let owner = owner.clone();
                let tx = tx.clone();
                tasks.spawn(async move {
                    loop {
                        match this
                            ._lua_get::<LockableValue<T>>(keys[idx].as_str(), owner.as_str())
                            .await
                        {
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
                                        err: Some(types::Errors::NeedAsyncFetch),
                                    });
                                    return;
                                }
                                None => {
                                    let _ = tx.send(Pair {
                                        idx,
                                        data: None,
                                        err: Some(types::Errors::NeedFetch),
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
                                    sleep(this.options.lock_sleep).await;
                                    continue;
                                }
                            },
                            Err(e) => {
                                let _ = tx.send(Pair {
                                    idx,
                                    data: None,
                                    err: Some(types::Errors::Custom(e.into())),
                                });
                                return;
                            }
                        }
                    }
                });
            }
            while let Some(_) = tasks.join_next().await {}
            drop(tx);

            while let Some(p) = rx.recv().await {
                if let Some(err) = p.err {
                    match err {
                        types::Errors::NeedFetch => {
                            to_fetch.push(p.idx);
                            continue;
                        }
                        types::Errors::NeedAsyncFetch => {
                            to_fetch_async.push(p.idx);
                            continue;
                        }
                        types::Errors::Custom(e) => return Err(e.into()),
                    }
                }
                if let Some(d) = p.data {
                    result.insert(p.idx, d);
                }
            }
        }

        if !to_fetch_async.is_empty() {
            let this = self.clone();
            let keys = keys.clone();
            let func = func.clone();
            let owner = owner.clone();
            tokio::spawn(async move {
                let _ = this._fetch_batch(keys, &to_fetch_async, expire, owner.as_str(), func);
            });
        }

        if !to_fetch.is_empty() {
            // batch fetch
            let fetched = self
                ._fetch_batch(keys, &to_fetch, expire, owner.as_str(), func)
                .await?;
            for k in to_fetch {
                if let Some(d) = fetched.get(&k) {
                    result.insert(k, *d);
                }
            }
        }

        Ok(result)
    }

    #[instrument(skip(func))]
    async fn _strong_fetch_batch<T, E, F>(
        &self,
        keys: Vec<String>,
        expire: Duration,
        func: F,
    ) -> Result<HashMap<usize, T>>
    where
        F: 'static
            + Send
            + Sync
            + Copy
            + Fn(
                &[usize],
            ) -> Pin<
                Box<dyn Future<Output = core::result::Result<HashMap<usize, T>, E>> + Send + Sync>,
            >,
        T: FromRedisValue + ToRedisArgs + Send + Sync + Copy + std::fmt::Debug + 'static,
        E: Into<anyhow::Error> + std::fmt::Debug,
    {
        let mut result: HashMap<usize, T> = HashMap::new();
        let owner: String = Uuid::new_v4().to_string();
        let mut to_get: Vec<usize> = Vec::new();
        let mut to_fetch: Vec<usize> = Vec::new();

        // read from redis without sleep
        let rs: Vec<LockableValue<T>> = self._lua_get_batch(keys.clone(), owner.as_str()).await?;
        let size = rs.len();
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
            };
        }

        if !to_fetch.is_empty() {
            // batch fetch
            let to_fetch = to_fetch.clone();
            let fetched = self
                ._fetch_batch(keys.clone(), &to_fetch, expire, owner.as_str(), func)
                .await?;
            for k in to_fetch {
                result.insert(k, fetched[&k].clone());
            }
        }

        if !to_get.is_empty() {
            // read from redis and sleep to wait
            let mut tasks = JoinSet::new();
            let (tx, mut rx) = mpsc::channel::<Pair<T>>(size);
            for idx in to_get {
                let keys = keys.clone();
                let owner = owner.clone();
                let tx = tx.clone();
                let this = self.clone();
                tasks.spawn(async move {
                    loop {
                        match this
                            ._lua_get::<LockableValue<T>>(keys[idx].as_str(), owner.as_str())
                            .await
                        {
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
                                        err: Some(types::Errors::NeedFetch),
                                    });
                                    return;
                                }
                                LockableValue::Locked(_) => {
                                    sleep(this.options.lock_sleep).await;
                                    continue;
                                }
                            },
                            Err(e) => {
                                let _ = tx.send(Pair {
                                    idx,
                                    data: None,
                                    err: Some(types::Errors::Custom(e.into())),
                                });
                                return;
                            }
                        }
                    }
                });
            }
            while let Some(_) = tasks.join_next().await {}
            drop(tx);
            while let Some(p) = rx.recv().await {
                if let Some(err) = p.err {
                    match err {
                        types::Errors::NeedFetch => {
                            to_fetch.push(p.idx);
                            continue;
                        }
                        types::Errors::Custom(e) => {
                            return Err(e);
                        }
                        types::Errors::NeedAsyncFetch => {
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
            let fetched = self
                ._fetch_batch(keys, &to_fetch, expire, owner.as_str(), func)
                .await?;
            for k in to_fetch {
                result.insert(k, fetched[&k]);
            }
        }

        Ok(result)
    }

    #[instrument(skip(func))]
    pub async fn fetch_batch<T, E, F>(
        &self,
        keys: Vec<String>,
        expire: Duration,
        func: F,
    ) -> Result<HashMap<usize, T>>
    where
        F: 'static
            + Send
            + Sync
            + Copy
            + Fn(
                &[usize],
            ) -> Pin<
                Box<dyn Future<Output = core::result::Result<HashMap<usize, T>, E>> + Send + Sync>,
            >,
        T: FromRedisValue + ToRedisArgs + Send + Sync + Copy + std::fmt::Debug + 'static,
        E: Into<anyhow::Error> + std::fmt::Debug,
    {
        if self.options.disable_cache_read {
            match func(&self._keys_idx(keys)).await {
                Ok(r) => Ok(r),
                Err(e) => Err(e.into()),
            }
        } else if self.options.strong_consistency {
            self._strong_fetch_batch(keys, expire, func).await
        } else {
            self._weak_fetch_batch(keys, expire, func).await
        }
    }

    #[instrument]
    pub async fn tag_as_deleted_batch(&self, keys: &[&str]) -> Result<bool> {
        if self.options.disable_cache_delete {
            return Ok(false);
        }
        let script = r#"-- luaDeleteBatch
        for i, key in ipairs(KEYS) do
            redis.call('HSET', key, 'lockUntil', 0)
            redis.call('HDEL', key, 'lockOwner')
            redis.call('EXPIRE', key, ARGV[1])
        end"#;
        let pool = self.pool.clone();
        let mut pool_con = pool.get().await?;
        let con = pool_con.deref_mut();
        if self.options.wait_replicas > 0 {
            let _ = call_lua_async(con, script, keys, &[self.options.delay.as_secs()]).await?;

            let replicas: i32 = redis::cmd("WAIT")
                .arg(&[
                    self.options.wait_replicas as u64,
                    self.options.wait_replicas_timeout.as_secs(),
                ])
                .query_async(con)
                .await?;
            if replicas < self.options.wait_replicas {
                return Err(anyhow!(format!(
                    "wait replicas {} failed. result replicas: {}",
                    self.options.wait_replicas, replicas
                ),));
            }
            return Ok(true);
        }
        call_lua_async(con, script, keys, &[self.options.delay.as_secs()]).await?;
        Ok(true)
    }
}

#[cfg(test)]
mod test {
    use super::{AsyncClient, Options};
    use std::ops::DerefMut;

    use std::time::Duration;
    use tokio::time::sleep;
    use tracing_test::traced_test;

    async fn gen_data_func(
        value: String,
        sleep_milli: u32,
    ) -> std::result::Result<Option<String>, std::io::Error> {
        sleep(Duration::new(0, sleep_milli * 1000)).await;
        Ok(Some(value))
    }

    #[tokio::test]
    #[traced_test]
    async fn test_fetch() {
        let rc = AsyncClient::new("redis://127.0.0.1:6379/", Options::default())
            .await
            .unwrap();
        let pool = rc.pool.clone();
        let mut pool_con = pool.get().await.unwrap();
        let con = pool_con.deref_mut();
        let _ = redis::cmd("FLUSHDB").query_async::<_, ()>(con).await;
        let expected = "value1";
        let rdb_key = "client-test-key";
        let res = rc
            .fetch(rdb_key, Duration::new(60, 0), async {
                gen_data_func("value1".to_string(), 201).await
            })
            .await;
        assert!(res.is_ok());
        assert!(res.unwrap().is_some_and(|x| expected.to_string() == x));
    }
}
