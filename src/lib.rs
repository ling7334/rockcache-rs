use r2d2;
use redis::{Client as RedisClient, Commands, ErrorKind, FromRedisValue, RedisError, RedisResult};
use singleflight::Group;
use std::thread::{sleep, spawn};
use std::time::Duration;
use uuid::Uuid;

mod utils;
use utils::{call_lua, now};
mod types;
use types::LockableValue;
pub use types::Options;

/// Client delay client
pub struct Client {
    pool: r2d2::Pool<RedisClient>,
    options: Options,
    group: Group,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            options: self.options.clone(),
            group: self.group.clone(),
        }
    }
}

fn from_r2d2_err<T>(result: Result<T, r2d2::Error>) -> RedisResult<T> {
    match result {
        Ok(r) => Ok(r),
        Err(e) => Err(RedisError::from((
            ErrorKind::ClientError,
            "Pooling Error",
            e.to_string(),
        ))),
    }
}

impl types::RocksCacheClient for Client {
    fn new(rdb: RedisClient, options: Options) -> Self {
        if options.delay == Duration::ZERO || options.lock_expire == Duration::ZERO {
            panic!("cache options error: Delay and LockExpire should not be 0, you should call NewDefaultOptions() to get default options");
        }

        Client {
            pool: r2d2::Pool::builder()
                .max_size(15)
                .min_idle(Some(3))
                .build(rdb)
                .unwrap(),
            options: options,
            group: Group::new(),
        }
    }

    fn tag_as_deleted(&self, key: String) -> RedisResult<()> {
        if self.options.disable_cache_delete {
            return Ok(());
        }
        let script = r#"
            -- delete
            redis.call('HSET', KEYS[1], 'lockUntil', 0)
            redis.call('HDEL', KEYS[1], 'lockOwner')
            redis.call('EXPIRE', KEYS[1], ARGV[1])
        "#;
        let delay_sec = self.options.delay.as_secs();
        let ref keys = vec![key];
        let ref args = vec![delay_sec];
        let pool = self.pool.clone();
        let ref mut con = from_r2d2_err(pool.get())?;
        if self.options.wait_replicas > 0 {
            let _: () = call_lua(con, script, keys, args).expect("call_lua failed");

            let replicas: i32 = redis::cmd("WAIT")
                .arg(self.options.wait_replicas)
                .arg(self.options.wait_replicas_timeout.as_secs())
                .query(con)
                .expect("WAIT failed");
            if replicas < self.options.wait_replicas {
                return Err(RedisError::from((
                    ErrorKind::TypeError,
                    "wait replicas",
                    format!(
                        "wait replicas {} failed. result replicas: {}",
                        self.options.wait_replicas, replicas,
                    ),
                )));
            }
        }
        call_lua(con, script, keys, args)
    }

    fn fetch<F>(&self, key: String, expire: Duration, func: F) -> RedisResult<String>
    where
        F: 'static + Send + Fn() -> Result<String, RedisError>,
    {
        let ex = expire
            - self.options.delay
            - Duration::from_secs_f64(
                rand::random::<f64>()
                    * self.options.random_expire_adjustment
                    * expire.as_secs_f64(),
            );
        let v = self
            .group
            .do_work(key.clone().as_str(), || {
                if self.options.disable_cache_read {
                    func()
                } else if self.options.strong_consistency {
                    self._strong_fetch(key, ex, func)
                } else {
                    self._weak_fetch(key, ex, func)
                }
            })
            .expect("failed to fetch data");
        Ok(v)
    }

    fn _strong_fetch<F>(&self, key: String, ex: Duration, func: F) -> RedisResult<String>
    where
        F: Fn() -> Result<String, RedisError>,
    {
        let owner = Uuid::new_v4().to_string();

        loop {
            match self._lua_get::<LockableValue<String>>(key.clone(), owner.clone()) {
                Ok(LockableValue::Nil(r)) => return Ok(r.unwrap_or_default()),
                Ok(LockableValue::Value(_)) => return self._fetch_new(key, ex, owner, func),
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

    fn _weak_fetch<F>(&self, key: String, ex: Duration, func: F) -> RedisResult<String>
    where
        F: 'static + Send + Fn() -> Result<String, RedisError>,
    {
        let owner = Uuid::new_v4().to_string();

        loop {
            match self._lua_get::<LockableValue<String>>(key.clone(), owner.clone()) {
                Ok(LockableValue::Nil(r)) => match r {
                    Some(v) => {
                        println!("LockableValue::Nil: {v}");
                        return Ok(v);
                    }
                    None => {
                        println!("LockableValue::Nil: None sleep");
                        sleep(self.options.lock_sleep);
                        continue;
                    }
                },
                Ok(LockableValue::Value(r)) => match r {
                    Some(v) => {
                        println!("LockableValue::Value: {v} _fetch_new return old");
                        let self1 = self.clone();
                        spawn(move || {
                            let _ = self1._fetch_new(key, ex, owner, func);
                        });
                        return Ok(v);
                    }
                    None => {
                        println!("LockableValue::Value: None _fetch_new");
                        return self._fetch_new(key, ex, owner, func);
                    }
                },
                Ok(LockableValue::Locked(r)) => match r {
                    Some(v) => {
                        println!("LockableValue::Locked: {v} return");
                        return Ok(v);
                    }
                    None => {
                        println!("LockableValue::Locked: None sleep");
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

    fn _lua_get<T>(&self, key: String, owner: String) -> RedisResult<T>
    where
        T: FromRedisValue,
    {
        // let ref mut con = self.rdb.get_connection()?;
        let pool = self.pool.clone();
        let ref mut con = from_r2d2_err(pool.get())?;
        call_lua(
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
                owner,
            ],
        )
    }

    fn _lua_set(&self, key: String, value: String, expire: i32, owner: String) -> RedisResult<()> {
        let pool = self.pool.clone();
        let ref mut con = from_r2d2_err(pool.get())?;
        call_lua(
            con,
            r#"-- luaSet
            local o = redis.call('HGET', KEYS[1], 'lockOwner')
            if o ~= ARGV[2] then
                    return
            end
            redis.call('HSET', KEYS[1], 'value', ARGV[1])
            redis.call('HDEL', KEYS[1], 'lockUntil')
            redis.call('HDEL', KEYS[1], 'lockOwner')
            redis.call('EXPIRE', KEYS[1], ARGV[3])
            "#,
            &[key],
            &[value, owner, expire.to_string()],
        )
    }

    fn _fetch_new<F>(
        &self,
        key: String,
        ex: Duration,
        owner: String,
        func: F,
    ) -> RedisResult<String>
    where
        F: Fn() -> Result<String, RedisError>,
    {
        match func() {
            Err(e) => {
                let _ = self.unlock_for_update(key, owner);
                Err(e)
            }
            Ok(r) => {
                if r.is_empty() {
                    if self.options.empty_expire == Duration::ZERO {
                        let pool = self.pool.clone();
                        let ref mut con = from_r2d2_err(pool.get())?;
                        return con.del(key);
                    }
                }
                match self._lua_set(key.clone(), r.clone(), ex.as_secs() as i32, owner.clone()) {
                    Err(e) => Err(e),
                    Ok(_) => Ok(r),
                }
            }
        }
    }

    fn raw_get(&self, key: String) -> RedisResult<String> {
        let pool = self.pool.clone();
        let ref mut con = from_r2d2_err(pool.get())?;
        con.hget(key, "value")
    }

    fn raw_set(&self, key: String, value: String, expire: Duration) -> RedisResult<()> {
        let pool = self.pool.clone();
        let ref mut con = from_r2d2_err(pool.get())?;
        match con.hset(key.clone(), "value", value) {
            Err(e) => Err(e),
            Ok(()) => con.expire(key, expire.as_secs() as usize),
        }
    }

    fn lock_for_update(&self, key: String, owner: String) -> RedisResult<()> {
        let pool = self.pool.clone();
        let ref mut con = from_r2d2_err(pool.get())?;
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
            &[key.clone()],
            &[owner, lock_until.to_string()],
        );
        match res {
            Ok(r) => {
                if r != "LOCKED" {
                    return Err(RedisError::from((
                        ErrorKind::ResponseError,
                        "Lock error",
                        format!("{} has been locked by {}", key, r),
                    )));
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn unlock_for_update(&self, key: String, owner: String) -> RedisResult<()> {
        let pool = self.pool.clone();
        let ref mut con = from_r2d2_err(pool.get())?;
        call_lua(
            con,
            r#" -- luaUnlock
            local lo = redis.call('HGET', KEYS[1], 'lockOwner')
            if lo == ARGV[1] then
                redis.call('HSET', KEYS[1], 'lockUntil', 0)
                redis.call('HDEL', KEYS[1], 'lockOwner')
                redis.call('EXPIRE', KEYS[1], ARGV[2])
            end"#,
            &[key.clone()],
            &[owner, self.options.lock_expire.as_secs().to_string()],
        )
    }
}

impl types::RocksCacheBatch for Client {
    fn _lua_get_batch<T>(&self, keys: Vec<String>, owner: String) -> RedisResult<Vec<T>> {
        todo!()
    }

    fn _lua_set_batch(
        &self,
        keys: Vec<String>,
        values: Vec<String>,
        expires: Vec<i32>,
        owner: String,
    ) -> RedisResult<()> {
        todo!()
    }

    fn _fetch_batch<F>(
        &self,
        keys: Vec<String>,
        idxs: Vec<i32>,
        expire: Duration,
        owner: String,
        func: F,
    ) -> RedisResult<std::collections::HashMap<i32, String>>
    where
        F: Fn(Vec<i32>) -> RedisResult<std::collections::HashMap<i32, String>>,
    {
        todo!()
    }

    fn _keys_idx(&self, keys: Vec<String>) -> Vec<i32> {
        todo!()
    }

    fn _weak_fetch_batch<F>(
        &self,
        keys: Vec<String>,
        expire: Duration,
        func: F,
    ) -> RedisResult<std::collections::HashMap<i32, String>>
    where
        F: Fn(Vec<i32>) -> RedisResult<std::collections::HashMap<i32, String>>,
    {
        todo!()
    }

    fn _strong_fetch_batch<F>(
        &self,
        keys: Vec<String>,
        expire: Duration,
        func: F,
    ) -> RedisResult<std::collections::HashMap<i32, String>>
    where
        F: Fn(Vec<i32>) -> RedisResult<std::collections::HashMap<i32, String>>,
    {
        todo!()
    }

    fn fetch_batch<F>(
        &self,
        keys: Vec<String>,
        expire: Duration,
        func: F,
    ) -> RedisResult<std::collections::HashMap<i32, String>>
    where
        F: Fn(Vec<i32>) -> RedisResult<std::collections::HashMap<i32, String>>,
    {
        todo!()
    }

    fn tag_as_deleted_batch(&self, keys: Vec<String>) -> RedisResult<()> {
        todo!()
    }
}

mod tests {
    use std::thread::{sleep, spawn};
    use std::time::Duration;

    use crate::types::RocksCacheClient;
    use crate::utils::now;

    use super::Client;
    use super::Options;
    use redis::Client as RedisClient;
    use redis::RedisResult;

    fn gen_data_func(value: String, sleepMilli: u32) -> RedisResult<String> {
        sleep(Duration::new(0, sleepMilli * 1000));
        Ok(value)
    }
    #[test]
    fn test_weak_fetch() {
        let rdb = RedisClient::open("redis://127.0.0.1:6379/").unwrap();
        let rc = Client::new(rdb.clone(), Options::default());
        let ref mut con = rdb.get_connection().unwrap();
        let _: () = redis::cmd("FLUSHDB").query(con).unwrap();

        // let began = now();
        let expected = "value1";
        let rdb_key = "client-test-key";
        let rc1 = rc.clone();
        spawn(move || {
            let res = rc1.fetch(rdb_key.to_string(), Duration::new(60, 0), || {
                gen_data_func("value1".to_string(), 200)
            });
            assert!(res.is_ok());
            assert_eq!(expected.to_string(), res.unwrap());
        });

        sleep(Duration::new(0, 20_000));

        let res = rc.fetch(rdb_key.to_string(), Duration::new(60, 0), || {
            gen_data_func("value1".to_string(), 201)
        });
        assert!(res.is_ok());
        assert_eq!(expected.to_string(), res.unwrap());

        let res = rc.tag_as_deleted(rdb_key.to_string());
        assert!(res.is_ok());

        let nv = "value2";
        let res = rc.fetch(rdb_key.to_string(), Duration::new(60, 0), || {
            gen_data_func("value2".to_string(), 200)
        });
        assert!(res.is_ok());
        assert_eq!(expected.to_string(), res.unwrap());

        sleep(Duration::new(0, 300_000));

        let res = rc.fetch(rdb_key.to_string(), Duration::new(60, 0), || {
            gen_data_func("ignored".to_owned(), 200)
        });
        assert!(res.is_ok());
        assert_eq!(nv.to_string(), res.unwrap());
    }
}
