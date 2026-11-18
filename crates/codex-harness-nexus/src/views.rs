//! What each value names for the capabilities that take it: the session it
//! belongs to, the operation it asks, the plan it commits, the value it sets.

use harness::Session;
use meta_signal_codex_harness_nexus::Set;
use signal_codex_harness_nexus::Hook;

use crate::generated::folding::FoldPlan;
use crate::generated::operation::{EventBatch, Operation};
use crate::generated::runtime::{Operable, Planned, Sessional, Settable};

impl Sessional for Session {
    fn session(&self) -> Session {
        self.clone()
    }
}

impl Sessional for Hook {
    fn session(&self) -> Session {
        match self {
            Hook::SessionStart(start) => start.session.clone(),
            Hook::PermissionRequest(session) | Hook::PostToolUse(session) => session.clone(),
            Hook::PostCompact(compact) => compact.session.clone(),
        }
    }
}

impl Sessional for EventBatch {
    fn session(&self) -> Session {
        self.session.clone()
    }
}

impl Operable for Operation {
    fn operation(&self) -> Operation {
        self.clone()
    }
}

impl Operable for Hook {
    fn operation(&self) -> Operation {
        Operation::Reporting(self.clone())
    }
}

impl Planned for FoldPlan {
    fn plan(&self) -> FoldPlan {
        self.clone()
    }
}

impl Settable for Set {
    fn setting(&self) -> Set {
        self.clone()
    }
}
