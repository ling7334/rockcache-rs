use parking_lot::{Condvar, Mutex, RwLock, RwLockUpgradableReadGuard};
use std::any::Any;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::result::Result;
use std::sync::Arc;
use tracing::{span, trace, Level};

#[derive(Debug)]
pub enum Status {
    Starting,
    LeaderDrop,
    Done,
}
#[derive(Debug)]
struct Call {
    wg: Status,
    value: Box<dyn Any + Send>,
}
#[derive(Debug)]
pub struct Group {
    m: Arc<RwLock<HashMap<String, Arc<(Condvar, Mutex<Call>)>>>>,
}

impl Clone for Group {
    fn clone(&self) -> Self {
        Self { m: self.m.clone() }
    }
}

impl Group {
    pub fn new() -> Group {
        Group {
            m: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    pub fn do_work<T, E, F>(&self, key: &str, work: F) -> Result<T, Error>
    where
        T: Any + Clone + Send,
        E: std::error::Error + Send + Sync + 'static,
        F: FnOnce() -> Result<T, E>,
    {
        let span = span!(Level::TRACE, "do_work", key = key);
        let _enter = span.enter();
        let mut map = self.m.upgradable_read();

        trace!("Aquire read lock");
        match map.get(key) {
            Some(state) => {
                trace!("Reading...");
                let &(ref cvar, ref lock) = &*state.clone();
                let mut call = lock.lock();
                loop {
                    match call.wg {
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
                                    map = self.m.upgradable_read();
                                    continue;
                                }
                            };
                            wmap.entry(key.to_owned()).or_insert_with(|| {
                                Arc::new((
                                    Condvar::new(),
                                    Mutex::new(Call {
                                        wg: Status::Starting,
                                        value: Box::new(None::<T>),
                                    }),
                                ))
                            });
                            drop(wmap);
                            break;
                            // match map.try_with_upgraded(|wmap| {
                            //     trace!("Lock upgraded");
                            //     wmap.entry(key.to_owned()).or_insert_with(|| {
                            //         Arc::new((
                            //             Condvar::new(),
                            //             Mutex::new(Call {
                            //                 wg: Status::Starting,
                            //                 value: Box::new(None::<T>),
                            //             }),
                            //         ))
                            //     });
                            //     trace!("entry inited");
                            //     Some(())
                            // }) {
                            //     Some(_) => break,
                            //     None => {
                            //         trace!("Lock not aquired, try again...");
                            //         continue;
                            //     }
                            // }
                        }
                        Status::Done => {
                            if let Some(s) = call.value.downcast_ref::<T>() {
                                trace!("Value returned");
                                return Ok(s.clone());
                            } else {
                                trace!("Error occur");
                                return Err(Error::new(ErrorKind::NotFound, "value not found"));
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
                            wg: Status::Starting,
                            value: Box::new(None::<T>),
                        }),
                    ))
                });
                drop(wmap);
            }
        }
        let map = self.m.read();
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
                    wg: Status::Done,
                    value: Box::new(r),
                };
            }
            Err(e) => {
                *call = Call {
                    wg: Status::LeaderDrop,
                    value: Box::new(None::<T>),
                };
                cvar.notify_all();
                trace!("Error occur during work");
                return Err(Error::new(ErrorKind::Other, e));
            }
        };
        drop(call);

        trace!("Work done");
        cvar.notify_all();
        let mut wmap = self.m.write();
        let &(_, ref target) = &*wmap
            .remove(key)
            .ok_or(Error::new(ErrorKind::Other, "unable to remove entry"))?;
        drop(wmap);

        trace!("Entry removed");
        let result = target.lock();
        if let Some(s) = result.value.downcast_ref::<T>() {
            return Ok(s.clone());
        } else {
            return Err(Error::new(ErrorKind::NotFound, "value not found"));
        }
    }
}

#[cfg(test)]
mod tests {
    use tracing;
    use tracing_test::traced_test;

    use super::Group;
    use std::io::Error as IOErr;
    use std::io::ErrorKind;

    #[test]
    #[traced_test]
    fn test_do_work() {
        let group = Group::new();
        let res = group.do_work::<i32, IOErr, _>("test_key", || Ok(0));
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 0);
    }

    #[test]
    #[traced_test]
    fn test_do_work_error() {
        let group = Group::new();
        let res = group.do_work::<(), IOErr, _>("test_key", || {
            Err(IOErr::new(ErrorKind::InvalidData, "test error"))
        });
        assert!(res.is_err());
        let res = group.do_work::<i32, IOErr, _>("test_key", || Ok(0));
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 0);
    }
}
