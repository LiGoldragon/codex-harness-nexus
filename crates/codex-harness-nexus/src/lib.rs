//! The Codex Harness Nexus: what it remembers of each Codex session, how an
//! operation is attempted through its translator's emissions, folded,
//! committed whole and published.

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
mod folded;
mod nexus;
mod store;
mod views;

pub use generated::emission;
pub use generated::folding::*;
pub use generated::memory::*;
pub use generated::operation::*;
pub use generated::runtime::*;
pub use nexus::HarnessNexus;
pub use store::{Familial, Opening, Store};
