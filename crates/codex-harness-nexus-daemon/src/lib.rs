//! The Codex Harness Nexus served: its meta socket bound at start, its
//! public socket bound once its configuration is ready, and the kernel's
//! watch on its sessions directories opened with it.
//!
//! The serving itself is nexus-serving's. What is written here is this
//! Nexus's bodies: the meta answer, the public answer, who the public socket
//! admits, and what becoming ready opens.

mod serving;
mod waking;

pub use serving::{Bounds, CodexHarness, Daemon, Serving};
pub use waking::{ReadyOpening, Supervising, Watching};
