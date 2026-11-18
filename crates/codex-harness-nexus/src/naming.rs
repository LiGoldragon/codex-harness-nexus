//! The Codex Harness Nexus as the shared serving and the shared translator
//! name it: codex-harness.sock, codex-harness-meta.sock and its translator's
//! two, each in its place's runtime directory; and its Memory,
//! codex-harness.memory, in its place's state directory.

use std::path::PathBuf;

use nexus_exchange::{ComponentName, ComponentTitle, Named};
use shared_translator::placement::{Place, state_path};

/// The Codex Harness Nexus, as its sockets are named.
pub struct CodexHarness;

impl Named for CodexHarness {
    fn name() -> ComponentName {
        ComponentName("codex-harness".into())
    }

    fn title() -> ComponentTitle {
        ComponentTitle("Codex Harness Nexus".into())
    }
}

/// Where a Nexus keeps its Memory in a place: named after the Nexus, as its
/// sockets are, `<name>.memory` in the place's state directory.
pub trait MemoryPlacing {
    fn memory_path<Component: Named>(&self) -> PathBuf;
}

impl MemoryPlacing for Place {
    fn memory_path<Component: Named>(&self) -> PathBuf {
        state_path(self).join(format!("{}.memory", Component::name().0))
    }
}
