use anyhow::{bail, Result};
use core::any::Any;
use core::future::Future;
use parking_lot::{Condvar, Mutex, RwLock, RwLockUpgradableReadGuard};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, instrument, trace};

#[derive(Clone, Debug)]
enum Status {
    Starting,
    LeaderDrop,
    Done,
}
#[derive(Clone, Debug)]
struct Call {
    status: Status,
    value: Arc<dyn Any + Send + Sync>,
}
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct Key(Arc<str>);

/// Group represents a class of work and creates a space in which units of work
/// can be executed with duplicate suppression.
#[derive(Clone, Debug)]
pub struct Group(Arc<RwLock<HashMap<Key, Arc<(Condvar, Mutex<Call>)>>>>);

impl Group {
    pub fn new() -> Group {
        Group(Arc::new(RwLock::new(HashMap::new())))
    }

    /// work executes and returns the results of the given function, making
    /// sure that only one execution is in-flight for a given key at a
    /// time. If a duplicate comes in, the duplicate caller waits for the
    /// original to complete and receives the same results.
    /// The return value shared indicates whether v was given to multiple callers.
    #[instrument(skip(work))]
    pub fn work<T, E, F>(&self, key: &str, work: F) -> Result<T>
    where
        T: Any + Send + Sync + Clone,
        E: Into<anyhow::Error> + Send + Sync,
        F: FnOnce() -> core::result::Result<T, E>,
    {
        let ref key = Key(Arc::from(key));
        let mut map = self.0.upgradable_read();

        trace!("Aquire read lock");
        match map.get(key) {
            Some(state) => {
                trace!("Reading...");
                let &(ref cvar, ref lock) = &*state.clone();
                let mut call = lock.lock();
                loop {
                    match call.status {
                        Status::Starting => {
                            trace!("Not return, waiting...");
                            cvar.wait(&mut call);
                            trace!("Work done noticed");
                            continue;
                        }
                        Status::LeaderDrop => {
                            trace!("Leader dropped");
                            let mut wmap = match RwLockUpgradableReadGuard::try_upgrade(map) {
                                Ok(r) => r,
                                Err(_) => {
                                    map = self.0.upgradable_read();
                                    continue;
                                }
                            };
                            wmap.entry(key.to_owned()).or_insert_with(|| {
                                Arc::new((
                                    Condvar::new(),
                                    Mutex::new(Call {
                                        status: Status::Starting,
                                        value: Arc::new(None::<T>),
                                    }),
                                ))
                            });
                            drop(wmap);
                            break;
                        }
                        Status::Done => {
                            if let Some(s) = call.value.downcast_ref::<T>() {
                                trace!("Value returned");
                                return Ok(s.clone());
                            } else {
                                error!("Error occur");
                                bail!("value not found");
                            }
                        }
                    }
                }
            }
            None => {
                let mut wmap = RwLockUpgradableReadGuard::upgrade(map);
                wmap.entry(key.to_owned()).or_insert_with(|| {
                    Arc::new((
                        Condvar::new(),
                        Mutex::new(Call {
                            status: Status::Starting,
                            value: Arc::new(None::<T>),
                        }),
                    ))
                });
                drop(wmap);
            }
        }
        let map = self.0.read();
        let entry = match map.get(key) {
            Some(r) => r,
            None => unreachable!(),
        };
        let &(ref cvar, ref lock) = &*entry.clone();
        drop(map);

        let mut call = lock.lock();

        trace!("working...");
        match work() {
            Ok(r) => {
                *call = Call {
                    status: Status::Done,
                    value: Arc::new(r),
                };
            }
            Err(e) => {
                *call = Call {
                    status: Status::LeaderDrop,
                    value: Arc::new(None::<T>),
                };
                cvar.notify_all();
                error!("Error occur during work");
                bail!(e);
            }
        };
        drop(call);

        trace!("Work done");
        cvar.notify_all();
        let mut wmap = self.0.write();
        let &(_, ref target) = &*match wmap.remove(key) {
            Some(r) => r,
            None => bail!("unable to remove entry"),
        };
        drop(wmap);

        trace!("Entry removed");
        let result = target.lock();
        match result.value.downcast_ref::<T>() {
            Some(s) => Ok(s.clone()),
            None => bail!("value not found"),
        }
    }

    /// async_work is like work but returns a future that will receive the
    /// results when they are ready.
    #[instrument(skip(fut))]
    pub async fn async_work<T, E>(
        &self,
        key: &str,
        fut: impl Future<Output = core::result::Result<T, E>>,
    ) -> Result<T>
    where
        T: Clone + Send + Sync + 'static,
        E: Into<anyhow::Error> + Send + Sync,
    {
        let ref key = Key(Arc::from(key));
        let mut map = self.0.upgradable_read();
        trace!("Aquire read lock");
        match map.get(key) {
            Some(state) => {
                trace!("Reading...");
                let &(ref cvar, ref lock) = &*state.clone();
                let mut call = lock.lock();
                loop {
                    match call.status {
                        Status::Starting => {
                            trace!("Not return, waiting...");
                            cvar.wait(&mut call);
                            trace!("Work done noticed");
                            continue;
                        }
                        Status::LeaderDrop => {
                            trace!("Leader dropped");
                            let mut wmap = match RwLockUpgradableReadGuard::try_upgrade(map) {
                                Ok(r) => r,
                                Err(_) => {
                                    map = self.0.upgradable_read();
                                    continue;
                                }
                            };
                            wmap.entry(key.to_owned()).or_insert_with(|| {
                                Arc::new((
                                    Condvar::new(),
                                    Mutex::new(Call {
                                        status: Status::Starting,
                                        value: Arc::new(None::<T>),
                                    }),
                                ))
                            });
                            drop(wmap);
                            break;
                        }
                        Status::Done => {
                            if let Some(s) = call.value.downcast_ref::<T>() {
                                trace!("Value returned");
                                return Ok(s.clone());
                            } else {
                                error!("Error occur");
                                bail!("value not found");
                            }
                        }
                    }
                }
            }
            None => {
                let mut wmap = RwLockUpgradableReadGuard::upgrade(map);
                wmap.entry(key.to_owned()).or_insert_with(|| {
                    Arc::new((
                        Condvar::new(),
                        Mutex::new(Call {
                            status: Status::Starting,
                            value: Arc::new(None::<T>),
                        }),
                    ))
                });
                drop(wmap);
            }
        }
        let map = self.0.read();
        let entry = match map.get(key) {
            Some(r) => r,
            None => unreachable!(),
        };
        let &(ref cvar, ref lock) = &*entry.clone();
        drop(map);

        let mut call = lock.lock();

        trace!("working...");
        match fut.await {
            Ok(r) => {
                *call = Call {
                    status: Status::Done,
                    value: Arc::new(r),
                };
            }
            Err(e) => {
                *call = Call {
                    status: Status::LeaderDrop,
                    value: Arc::new(None::<T>),
                };
                cvar.notify_all();
                error!("Error occur during work");
                bail!(e);
            }
        };
        drop(call);

        trace!("Work done");
        cvar.notify_all();
        let mut wmap = self.0.write();
        let &(_, ref target) = &*match wmap.remove(key) {
            Some(r) => r,
            None => bail!("unable to remove entry"),
        };

        drop(wmap);

        trace!("Entry removed");
        let result = target.lock();
        match result.value.downcast_ref::<T>() {
            Some(s) => Ok(s.clone()),
            None => bail!("value not found"),
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::bail;
    use tokio;
    use tracing;
    use tracing_test::traced_test;

    use super::Group;
    use std::io::Error as IOErr;
    use std::io::ErrorKind;

    #[test]
    #[traced_test]
    fn test_work() {
        let group = Group::new();
        let res = group.work::<i32, IOErr, _>("test_key", || Ok(0));
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 0);
    }

    #[test]
    #[traced_test]
    fn test_work_error() {
        let group = Group::new();
        let res = group.work::<(), IOErr, _>("test_key", || {
            Err(IOErr::new(ErrorKind::InvalidData, "test error"))
        });
        assert!(res.is_err());
        let res = group.work::<i32, IOErr, _>("test_key", || Ok(0));
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 0);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_async_work() {
        let group = Group::new();
        let res = group
            .async_work::<i32, std::io::Error>("test_key", async { Ok(0) })
            .await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 0);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_async_work_error() {
        let group = Group::new();
        let res = group
            .async_work::<(), _>("test_key", async { bail!("test error") })
            .await;
        assert!(res.is_err());
        let res = group
            .async_work::<i32, std::io::Error>("test_key", async { Ok(0) })
            .await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 0);
    }
}
