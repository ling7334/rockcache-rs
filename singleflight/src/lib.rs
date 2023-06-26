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
        let map = self.m.upgradable_read();

        trace!("Aquire read lock");
        if let Some(state) = map.get(key) {
            trace!("Reading...");
            let entry = state.clone();
            // drop(map);
            let &(ref cvar, ref lock) = &*entry;
            let mut call = lock.lock();
            loop {
                match call.wg {
                    Status::Starting => {
                        trace!("Not return, waiting...");
                        cvar.wait(&mut call);
                        trace!("Work done noticed");
                    }
                    Status::LeaderDrop => {
                        trace!("Leader dropped");
                        break;
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
        let mut wmap = RwLockUpgradableReadGuard::upgrade(map);
        trace!("lock upgrade");
        let state = wmap.entry(key.to_owned()).or_insert_with(|| {
            Arc::new((
                Condvar::new(),
                Mutex::new(Call {
                    wg: Status::Starting,
                    value: Box::new(None::<T>),
                }),
            ))
        });
        trace!("entry inited");
        let entry = state.clone();
        drop(wmap);

        let &(ref cvar, ref lock) = &*entry;
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
                trace!("Error occur");
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
