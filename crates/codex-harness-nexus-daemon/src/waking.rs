//! What a ready configuration opens: the public socket, once, and the
//! kernel's watch on the sessions directories, each time they are set. The
//! watch is opened before the one startup look, so no write falls between
//! them; a watch superseded by a newer one ends at its next word.

use std::sync::{Arc, MutexGuard};

use codex_harness_nexus::{
    Awaiting as _, Locking as _, Operation, Performing as _, Readying as _, Remembering as _,
    Waking as _, Watcher,
};
use harness::FilePath;
use meta_signal_codex_harness_nexus::Set;
use nexus_serving::Listening as _;
use signal_codex_harness_nexus::ReportFailure;

use crate::serving::{Daemon, Serving};

pub trait ReadyOpening {
    /// Opens what a ready configuration opens, if it is ready and not yet
    /// open.
    fn open(&self) -> Result<(), Box<dyn std::error::Error>>;

    /// After a value was set: open, if this made the configuration ready;
    /// watch anew, if it moved the sessions directories of an open Nexus.
    fn configured(&self, set: &Set) -> Result<(), Box<dyn std::error::Error>>;
}

pub trait Watching {
    /// Watches these directories in place of any watch before, then looks
    /// once at every rollout the Memory names.
    fn watch(&self, directories: Vec<FilePath>) -> Result<(), Box<dyn std::error::Error>>;
}

/// What the watch threads ask of the served Nexus.
pub trait Supervising {
    /// The generation of the current watch.
    fn generation(&self) -> MutexGuard<'_, u64>;

    /// Performs each operation in turn; a refusal is reported and the rest
    /// go on.
    fn perform_all(&self, operations: Result<Vec<Operation>, ReportFailure>);
}

impl Supervising for Serving {
    fn generation(&self) -> MutexGuard<'_, u64> {
        self.watch_generation.locked()
    }

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
        let Some(ready) = configuration.ready() else {
            return Ok(());
        };
        if serving.opened.get().is_some() {
            return Ok(());
        }
        // Open only once the watch is open: a watch that fails leaves the
        // Nexus unopened, and the staged socket is dropped with its file.
        let listener = serving.listeners.stage_public()?;
        self.watch(ready.sessions_directories.0)?;
        if serving.opened.set(()).is_err() {
            return Ok(());
        }
        serving.listeners.publish_public(listener);
        Ok(())
    }

    fn configured(&self, set: &Set) -> Result<(), Box<dyn std::error::Error>> {
        match set {
            Set::SessionsDirectories(directories) if self.serving.opened.get().is_some() => {
                self.watch(directories.0.clone())
            }
            _ => self.open(),
        }
    }
}

impl Watching for Daemon {
    fn watch(&self, directories: Vec<FilePath>) -> Result<(), Box<dyn std::error::Error>> {
        let watcher = Watcher::open(directories).map_err(|diagnostic| diagnostic.0)?;
        let current = {
            let mut generation = self.serving.generation();
            *generation += 1;
            *generation
        };
        let serving = Arc::clone(&self.serving);
        std::thread::spawn(move || {
            loop {
                let written = match watcher.written() {
                    Ok(written) => written,
                    Err(diagnostic) => {
                        eprintln!("Codex Harness Nexus watch ended: {}", diagnostic.0);
                        return;
                    }
                };
                if *serving.generation() != current {
                    return;
                }
                serving.perform_all(serving.nexus.woken(written));
            }
        });
        self.serving.perform_all(self.serving.nexus.startup());
        Ok(())
    }
}
