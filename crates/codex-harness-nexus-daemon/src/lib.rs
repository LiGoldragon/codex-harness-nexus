//! The Codex Harness Nexus served: started from its bootstrap, its meta
//! socket bound at start, its public socket bound once its configuration is
//! ready, the kernel's word on its sessions directories followed from the
//! moment they are set, and each emission carried to its paired translator.
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
pub use serving::{Bounds, Daemon, DaemonStarting, ListenerFailure, Serving};
pub use waking::{ReadyOpening, Supervising, WatchFollowing};
