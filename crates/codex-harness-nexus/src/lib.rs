//! The Codex Harness Nexus: what it remembers of each Codex session, how an
//! operation is attempted through its translator's emissions, folded,
//! committed whole and published; the name its sockets take, and the
//! bootstrap the pair launcher starts it with.

extern crate self as folding;
extern crate self as memory;
extern crate self as operation;

mod generated {
    pub mod bootstrap;
    pub mod emission;
    pub mod folding;
    pub mod memory;
    pub mod operation;
    pub mod runtime;
}
mod folded;
mod naming;
mod nexus;
mod store;
mod views;
mod watching;

pub use generated::bootstrap::*;
pub use generated::emission;
pub use generated::folding::*;
pub use generated::memory::*;
pub use generated::operation::*;
pub use generated::runtime::*;
pub use naming::CodexHarness;
pub use nexus::HarnessNexus;
pub use store::{Familial, Opening, Store};
pub use watching::Watcher;
