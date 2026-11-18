//! The Codex Harness Nexus served: started from its bootstrap, its meta
//! socket bound at start, its public socket bound once its configuration is
//! ready, the kernel's watch on its sessions directories opened with it, and
//! each emission carried to its paired translator.
//!
//! The serving itself is nexus-serving's. What is written here is this
//! Nexus's bodies: the meta answer, the public answer, who the public socket
//! admits, and what becoming ready opens.

mod bootstrap;
mod emitting;
mod serving;
mod waking;

pub use bootstrap::BootstrapReading;
pub use codex_harness_nexus::{CodexHarness, NexusBootstrap};
pub use emitting::{Answered, Carrying, PairedTranslator};
pub use serving::{Bounds, Daemon, Serving};
pub use waking::{ReadyOpening, Supervising, Watching};
