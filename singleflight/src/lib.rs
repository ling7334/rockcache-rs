use parking_lot::{Condvar, Mutex, RwLock, RwLockUpgradableReadGuard};
use std::any::Any;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::result::Result;
use std::sync::Arc;

struct Call {
    wg: bool,
    value: Box<dyn Any + Send>,
}
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
        let map = self.m.upgradable_read();
        if let Some(state) = map.get(key) {
            println!("reading...");
            let entry = state.clone();
            drop(map);
            let &(ref cvar, ref lock) = &*entry;
            let mut call = lock.lock();
            while !call.wg {
                println!("waiting...");
                cvar.wait(&mut call);
                println!("noticed");
            }
            if let Some(s) = call.value.downcast_ref::<T>() {
                return Ok(s.clone());
            } else {
                return Err(Error::new(ErrorKind::NotFound, "value not found"));
            }
        }
        let mut wmap = RwLockUpgradableReadGuard::upgrade(map);
        let state = wmap.entry(key.to_owned()).or_insert_with(|| {
            Arc::new((
                Condvar::new(),
                Mutex::new(Call {
                    wg: false,
                    value: Box::new(None::<T>),
                }),
            ))
        });
        let entry = state.clone();
        drop(wmap);

        let &(ref cvar, ref lock) = &*entry;
        let mut call = lock.lock();
        println!("working...");
        *call = Call {
            wg: true,
            value: Box::new(match work() {
                Ok(r) => r,
                Err(e) => {
                    let mut wmap = self.m.write();
                    let _ = wmap.remove(key);
                    cvar.notify_all();
                    return Err(Error::new(ErrorKind::Other, e));
                }
            }),
        };
        drop(call);
        println!("work done");
        cvar.notify_all();
        let mut wmap = self.m.write();
        let &(_, ref target) = &*wmap
            .remove(key)
            .ok_or(Error::new(ErrorKind::Other, "unable to remove entry"))?;
        drop(wmap);
        let result = target.lock();
        if let Some(s) = result.value.downcast_ref::<T>() {
            return Ok(s.clone());
        } else {
            return Err(Error::new(ErrorKind::NotFound, "value not found"));
        }
    }
}
