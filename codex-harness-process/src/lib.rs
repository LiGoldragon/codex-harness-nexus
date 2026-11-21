//! The processes of the Codex Harness Nexus: a hook's report and a rollout's
//! watch. Built with no operating system under it.

// The text side, where datom is spoken, is built with std; the Nexus's build is not.
#![cfg_attr(not(feature = "datom"), no_std)]
extern crate alloc;

mod generated;

pub use generated::*;

/// The version of these types: the memory and the trace are named for it.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The Nexus's name: its sockets, its files and its clients' configuration are named for it.
pub const NAME: &str = "codex-harness";

use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;

use flow::FlowId;
use harness::{Compaction, Event, Observable, Session};
use process::{Giving, Processable};

pub trait Identifiable {
    fn identify(&self) -> FlowId;
}

/// A Codex thread's id is a UUID of version 7, whose first characters are its
/// moment and are shared by every thread begun within hours; the system names
/// a Codex flow from its random tail: six characters from the twenty-fourth of
/// the id written without its dashes, as `flow-id codex` draws its first
/// candidate.
impl Identifiable for Session {
    fn identify(&self) -> FlowId {
        let bare: String = self.0.chars().filter(|character| *character != '-').map(|character| character.to_ascii_lowercase()).collect();
        FlowId(bare.chars().skip(23).take(6).collect())
    }
}

impl Hook {
    fn session(&self) -> &Session {
        match self {
            Hook::SessionStart(start) => &start.session,
            Hook::PermissionRequest(session) | Hook::PostToolUse(session) => session,
            Hook::PostCompact(compact) => &compact.session,
        }
    }
}

impl Identifiable for Hook {
    fn identify(&self) -> FlowId {
        self.session().identify()
    }
}

impl Observable for Hook {
    fn events(&self) -> Vec<Event> {
        match self {
            Hook::SessionStart(_) => vec![Event::Started],
            Hook::PermissionRequest(_) => vec![Event::PermissionAsked],
            Hook::PostToolUse(_) => vec![Event::ToolUsed],
            Hook::PostCompact(compact) => vec![Event::Compacted(Compaction { trigger: compact.trigger.clone(), tokens_option: None })],
        }
    }
}

impl Observable for Row {
    fn events(&self) -> Vec<Event> {
        match self {
            Row::TaskStarted => vec![Event::PromptSubmitted],
            Row::TokenCount(Some(tokens)) => vec![Event::ContextSize(*tokens)],
            Row::TaskComplete(Some(response)) => vec![Event::Responded(response.clone()), Event::TurnEnded],
            Row::TaskComplete(None) => vec![Event::TurnEnded],
            Row::TurnAborted => vec![Event::Interrupted],
            Row::TokenCount(None) | Row::Other => vec![],
        }
    }
}

impl Processable for ReportProcess {
    type Taken = ReportTaken;
    type Gift = Gift;
    type Outcome = Reporting;

    fn begin<N: Giving<Gift>>(&mut self, mut manager: N) -> Option<Reporting> {
        manager.give(Reported);
        if let Hook::SessionStart(start) = &self.hook {
            let rollout = match &start.file_path_option {
                Some(file_path) => Rollout::Located(file_path.clone()),
                None => Rollout::Unlocated,
            };
            manager.give(Watch { flow_id: start.session.identify(), session: start.session.clone(), rollout, offset: Offset(0) });
        }
        manager.give(Observe { flow_id: self.hook.identify(), event_vector: self.hook.events(), offset_option: None });
        None
    }

    fn take<N: Giving<Gift>>(&mut self, taken: ReportTaken, mut manager: N) -> Option<Reporting> {
        match taken {
            ReportTaken::Answer(Answer::Succeeded) => None,
            ReportTaken::Answer(Answer::Failed) => Some(Reporting::Unremembered),
            ReportTaken::Observation(observation) => {
                for changed in observation.0 {
                    manager.give(changed)
                }
                Some(Reporting::Observed)
            }
        }
    }
}

impl WatchProcess {
    fn follow<N: Giving<Gift>>(&self, file_path: &harness::FilePath, manager: &mut N) {
        manager.give(Follow { file_path: file_path.clone(), offset: self.offset.clone(), row_count: self.row_count.clone() });
    }
}

impl Processable for WatchProcess {
    type Taken = WatchTaken;
    type Gift = Gift;
    type Outcome = Watching;

    /// A located rollout is followed; an unlocated one is first asked of
    /// Codex's own record of its threads.
    fn begin<N: Giving<Gift>>(&mut self, mut manager: N) -> Option<Watching> {
        match (&self.rollout, &self.thread_database_option) {
            (Rollout::Located(file_path), _) => {
                self.follow(file_path, &mut manager);
                None
            }
            (Rollout::Unlocated, Some(thread_database)) => {
                manager.give(FindRollout { thread_database: thread_database.clone(), session: self.session.clone() });
                None
            }
            (Rollout::Unlocated, None) => Some(Watching::Unlocated),
        }
    }

    fn take<N: Giving<Gift>>(&mut self, taken: WatchTaken, mut manager: N) -> Option<Watching> {
        match taken {
            WatchTaken::RolloutFinding(Ok(RolloutFinding::Found(file_path))) => {
                self.rollout = Rollout::Located(file_path.clone());
                manager.give(Locate { flow_id: self.flow_id.clone(), file_path: file_path.clone() });
                self.follow(&file_path, &mut manager);
                None
            }
            WatchTaken::RolloutFinding(Ok(RolloutFinding::Unfound)) => Some(Watching::Unlocated),
            WatchTaken::RolloutFinding(Err(_)) => Some(Watching::Unfollowed),
            // A path not kept is found again at the next birth.
            WatchTaken::Answer(_) => None,
            WatchTaken::Following(Ok(following)) => {
                self.offset = following.offset;
                let event_vector = following.row_vector.iter().flat_map(Observable::events).collect();
                manager.give(Observe { flow_id: self.flow_id.clone(), event_vector, offset_option: Some(self.offset.clone()) });
                None
            }
            WatchTaken::Following(Err(_)) => Some(Watching::Unfollowed),
            WatchTaken::Observation(observation) => {
                for changed in observation.0 {
                    manager.give(changed)
                }
                None
            }
        }
    }
}

impl Processable for SubscribeProcess {
    type Taken = SubscribeTaken;
    type Gift = Gift;
    /// It ends one way, when its connection closes, and so declares no outcome.
    type Outcome = ();

    fn begin<N: Giving<Gift>>(&mut self, mut manager: N) -> Option<()> {
        manager.give(Recall);
        None
    }

    /// It answers with every session as it is now, then rests while its connection is open.
    fn take<N: Giving<Gift>>(&mut self, taken: SubscribeTaken, mut manager: N) -> Option<()> {
        match taken {
            SubscribeTaken::Recollection(recollection) => manager.give(Subscribed(recollection.0)),
        }
        None
    }
}

impl Processable for ConfigureProcess {
    type Taken = ConfigureTaken;
    type Gift = Gift;
    type Outcome = Configuring;

    fn begin<N: Giving<Gift>>(&mut self, mut manager: N) -> Option<Configuring> {
        manager.give(Configure(self.set.clone()));
        None
    }

    fn take<N: Giving<Gift>>(&mut self, taken: ConfigureTaken, mut manager: N) -> Option<Configuring> {
        match taken {
            ConfigureTaken::Answer(Answer::Succeeded) => {
                manager.give(Configured(self.set.clone()));
                Some(Configuring::Configured)
            }
            ConfigureTaken::Answer(Answer::Failed) => Some(Configuring::Unremembered),
        }
    }
}
