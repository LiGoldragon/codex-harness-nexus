//! The Codex Harness Nexus: what it remembers of each Codex session, how an
//! operation is attempted through its translator's emissions, folded,
//! committed whole and published, and how a failed emission lane is told
//! apart from every answer the translator gives; and the name its sockets
//! take.

extern crate self as folding;
extern crate self as memory;
extern crate self as operation;

mod generated {
    pub mod emission;
    pub mod folding;
    pub mod memory;
    pub mod operation;
    pub mod runtime;
}
mod diagnosing;
mod failing;
mod folded;
mod naming;
mod nexus;
mod store;
mod views;
mod watching;

pub use diagnosing::Diagnosing;
pub use generated::emission;
pub use generated::folding::*;
pub use generated::memory::*;
pub use generated::operation::*;
pub use generated::runtime::*;
pub use naming::CodexHarness;
pub use nexus::{ConfigurationReading, HarnessNexus, Locking};
pub use store::{Familial, Opening, Store};
pub use watching::{
    Channelled, DirectoryWatch, Followed, Placement, Placing, Reach, Replacing, SessionsWatch,
    Settling, WatchWord, Watching,
};
