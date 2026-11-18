//! The Codex Harness Nexus served: its meta socket bound at start, its
//! public socket bound once its configuration is ready, and the kernel's
//! watch on its sessions directories opened with it.
//!
//! The serving itself is nexus-serving's. What is written here is this
//! Nexus's bodies: the meta answer, the public answer, who the public socket
//! admits, and what becoming ready opens.

mod emitting;
mod serving;
mod waking;

pub use codex_harness_nexus::CodexHarness;
pub use emitting::{Answered, Carrying, PairedTranslator};
pub use serving::{Bounds, Daemon, Serving};
pub use waking::{ReadyOpening, Supervising, Watching};
