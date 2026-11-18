//! The Codex Harness Nexus's translator: the part that reads Codex's own
//! text, the rollout rows, its first session row and the identity
//! producer's answer, and carries each emission of the Nexus into the world.

mod generated {
    pub mod rollout;
}
mod emitting;
mod rollout;

pub use emitting::Translator;
pub use generated::rollout::*;
