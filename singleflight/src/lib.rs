//! A rust port of singleflight, provides a duplicate function call suppression
//! mechanism.
//!
//! # Examples
//!
//! ```no_run
//! use futures::future::join_all;
//! use std::sync::Arc;
//! use std::thread::{spawn, sync};
//! use std::time::Duration;
//!
//! use singleflight::Group;
//!
//! const RES: usize = 7;
//!
//! fn expensive_fn() -> Result<usize, ()> {
//!     sleep(Duration::new(1, 500));
//!     Ok(RES)
//! }
//!
//! fn main() {
//! use std::sync::Arc;
//! use std::thread::spawn;
//! use singleflight::Group;
//! fn main() {
//!     let g = Arc::new(Group::<_, ()>::new());
//!     let mut handlers = Vec::new();
//!     for i in 0..10 {
//!         let g = g.clone();
//!         handlers.push(spawn(move || {
//!             let res = g.work("key", || Ok(i));
//!             let r = res.unwrap();
//!             println!("thread {} finished", r);
//!         }));
//!     }
//!     for h in handlers {
//!         h.join();
//!     }
//! }
//! ```
//!
//!
mod group;
pub use group::Group;
