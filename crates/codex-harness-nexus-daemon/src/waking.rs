//! What a ready configuration opens, and what the kernel's word wakes. The
//! public socket opens once the configuration is ready; it does not wait
//! for a sessions directory, which Codex may make only once its first
//! session starts. The watch of the sessions directories is the core's,
//! opened as they are set; its words are followed here, on a thread of
//! their own, for the daemon's life. Each look at the Memory's rollouts
//! follows the watch it answers to, so no write falls between them.

use std::sync::Arc;

use codex_harness_nexus::{
    Locking as _, Operation, Performing as _, Readying as _, Remembering as _, Waking as _,
    Watching as _,
};
use meta_signal_codex_harness_nexus::Set;
use nexus_serving::Listening as _;
use signal_codex_harness_nexus::ReportFailure;

use crate::serving::{Daemon, Serving};

pub trait ReadyOpening {
    /// Opens the public socket, if the configuration is ready and it is not
    /// yet open, then looks once at every rollout the Memory names.
    fn open(&self) -> Result<(), Box<dyn std::error::Error>>;

    /// After a value was set: open, if this made the configuration ready;
    /// look again, if it moved the sessions directories of an open Nexus.
    fn configured(&self, set: &Set) -> Result<(), Box<dyn std::error::Error>>;
}

pub trait WatchFollowing {
    /// Watches the sessions directories the Memory recorded, as the daemon
    /// starts; one that can no longer be watched is told, since it was
    /// recorded and cannot be refused.
    fn watch_recorded(&self) -> Result<(), Box<dyn std::error::Error>>;

    /// Follows every word of the kernel on a thread of its own: each moves
    /// the watch where it must, and once the Nexus is open, a directory
    /// come to be there is looked at, and each file written is read.
    fn follow_watch(&self);
}

/// What the watch thread asks of the served Nexus.
pub trait Supervising {
    /// Performs each operation in turn; a refusal is reported and the rest
    /// go on.
    fn perform_all(&self, operations: Result<Vec<Operation>, ReportFailure>);
}

impl Supervising for Serving {
    fn perform_all(&self, operations: Result<Vec<Operation>, ReportFailure>) {
        match operations {
            Ok(operations) => {
                for operation in operations {
                    if let Err(failure) = self.nexus.perform(operation) {
                        eprintln!("Codex Harness Nexus operation refused: {failure:?}");
                    }
                }
            }
            Err(failure) => eprintln!("Codex Harness Nexus could not look: {failure:?}"),
        }
    }
}

impl ReadyOpening for Daemon {
    fn open(&self) -> Result<(), Box<dyn std::error::Error>> {
        let serving = &self.serving;
        let configuration = serving
            .nexus
            .store
            .configuration()
            .map_err(|diagnostic| diagnostic.0)?;
        if configuration.ready().is_none() || serving.opened.get().is_some() {
            return Ok(());
        }
        let listener = serving.listeners.stage_public()?;
        if serving.opened.set(()).is_err() {
            return Ok(());
        }
        serving.listeners.publish_public(listener);
        serving.perform_all(serving.nexus.startup());
        Ok(())
    }

    fn configured(&self, set: &Set) -> Result<(), Box<dyn std::error::Error>> {
        match set {
            Set::SessionsDirectories(_) if self.serving.opened.get().is_some() => {
                self.serving.perform_all(self.serving.nexus.startup());
                Ok(())
            }
            _ => self.open(),
        }
    }
}

impl WatchFollowing for Daemon {
    fn watch_recorded(&self) -> Result<(), Box<dyn std::error::Error>> {
        let configuration = self
            .serving
            .nexus
            .store
            .configuration()
            .map_err(|diagnostic| diagnostic.0)?;
        if let Some(directories) = configuration.sessions_directories_option {
            self.serving
                .nexus
                .watch(&directories)
                .map_err(|unwatchable| {
                    format!("{}: {}", unwatchable.file_path.0, unwatchable.diagnostic.0)
                })?;
        }
        Ok(())
    }

    fn follow_watch(&self) {
        let serving = Arc::clone(&self.serving);
        std::thread::spawn(move || {
            let words = serving.words.locked();
            while let Ok(word) = words.recv() {
                let followed = serving.nexus.follow(word);
                for failure in &followed.failures {
                    eprintln!("Codex Harness Nexus watch: {}", failure.0);
                }
                if serving.opened.get().is_none() {
                    continue;
                }
                if !followed.arrived.is_empty() {
                    serving.perform_all(serving.nexus.startup());
                }
                serving.perform_all(serving.nexus.woken(followed.written));
            }
        });
    }
}
