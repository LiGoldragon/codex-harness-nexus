//! What each value names for the capabilities that take it: the session it
//! belongs to, the operation it asks, the plan it commits, the value it sets.

use harness::SessionId;
use meta_signal_codex_harness_nexus::Set;
use signal_codex_harness_nexus::Hook;

use crate::generated::actor::WatchWord;
use crate::generated::actor::{
    ConfigurationSetting, Reportable, SessionsDirectoriesSetting, WatchWordInput,
};
use crate::generated::folding::FoldPlan;
use crate::generated::operation::{EventBatch, Operation};
use crate::generated::runtime::{Operable, Planned, Sessional, Settable};
use meta_signal_codex_harness_nexus::SessionsDirectories;

impl Sessional for SessionId {
    fn session_id(&self) -> SessionId {
        self.clone()
    }
}

impl Sessional for Hook {
    fn session_id(&self) -> SessionId {
        match self {
            Hook::SessionStart(start) => start.session_id.clone(),
            Hook::PermissionRequest(session) | Hook::PostToolUse(session) => session.clone(),
            Hook::PostCompact(compact) => compact.session_id.clone(),
        }
    }
}

impl Sessional for EventBatch {
    fn session_id(&self) -> SessionId {
        self.session_id.clone()
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

impl ConfigurationSetting for Set {
    fn setting(&self) -> Set {
        self.clone()
    }
}

impl SessionsDirectoriesSetting for SessionsDirectories {
    fn sessions_directories(&self) -> SessionsDirectories {
        self.clone()
    }
}

impl Reportable for Hook {
    fn hook(&self) -> Hook {
        self.clone()
    }
}

impl WatchWordInput for WatchWord {
    fn watch_word(&self) -> WatchWord {
        self.clone()
    }
}
