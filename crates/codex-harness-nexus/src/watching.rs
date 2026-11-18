//! The kernel's word that a rollout was written. Each sessions directory is
//! watched whole, its subdirectories with it; the watch wakes the Nexus,
//! which reads only what was written. Nothing here keeps a clock.

use std::path::Path;
use std::sync::mpsc::{Receiver, channel};

use harness::FilePath;
use notify::event::{CreateKind, EventKind};
use notify::{RecommendedWatcher, RecursiveMode, Watcher as _};
use signal_codex_harness_nexus::{Diagnostic, ReportFailure};

use crate::generated::memory::Rollout;
use crate::generated::operation::Operation;
use crate::generated::runtime::{Awaiting, Remembering, Waking};
use crate::nexus::HarnessNexus;

/// The watch on every sessions directory, and the events it sends.
pub struct Watcher {
    pub watcher: RecommendedWatcher,
    pub events: Receiver<notify::Result<notify::Event>>,
}

impl Awaiting for Watcher {
    fn open(directories: Vec<FilePath>) -> Result<Self, Diagnostic> {
        let (sender, events) = channel();
        let mut watcher = notify::recommended_watcher(move |event| {
            let _ = sender.send(event);
        })
        .map_err(|error| Diagnostic(error.to_string()))?;
        for directory in directories {
            watcher
                .watch(Path::new(&directory.0), RecursiveMode::Recursive)
                .map_err(|error| Diagnostic(format!("{}: {error}", directory.0)))?;
        }
        Ok(Self { watcher, events })
    }

    /// Waits for the kernel's next word and names each file written.
    fn written(&self) -> Result<Vec<FilePath>, Diagnostic> {
        let event = self
            .events
            .recv()
            .map_err(|error| Diagnostic(error.to_string()))?
            .map_err(|error| Diagnostic(error.to_string()))?;
        Ok(match event.kind {
            EventKind::Create(CreateKind::Folder) => vec![],
            EventKind::Create(_) | EventKind::Modify(_) => event
                .paths
                .into_iter()
                .map(|path| FilePath(path.to_string_lossy().into_owned()))
                .collect(),
            _ => vec![],
        })
    }
}

impl<Translator> Waking for HarnessNexus<Translator> {
    /// One look at startup: each watched rollout is read from its offset,
    /// and each pending one attaches if it now exists.
    fn startup(&self) -> Result<Vec<Operation>, ReportFailure> {
        Ok(self
            .store
            .recall_all()
            .map_err(ReportFailure::Unrecorded)?
            .into_iter()
            .filter_map(|record| match record.rollout {
                Rollout::Watched(watch) => Some(Operation::Reading(watch)),
                Rollout::Pending(path) if Path::new(&path.0).exists() => {
                    Some(Operation::Discovering(path))
                }
                Rollout::Pending(_) | Rollout::Unlocated => None,
            })
            .collect())
    }

    /// A written file that a session watches is read; any other is
    /// discovered, which attaches it only to a session that waits for it.
    fn woken(&self, written: Vec<FilePath>) -> Result<Vec<Operation>, ReportFailure> {
        let records = self.store.recall_all().map_err(ReportFailure::Unrecorded)?;
        let mut operations: Vec<Operation> = vec![];
        for path in written {
            let operation = records
                .iter()
                .find_map(|record| match &record.rollout {
                    Rollout::Watched(watch) if watch.file_path == path => {
                        Some(Operation::Reading(watch.clone()))
                    }
                    _ => None,
                })
                .unwrap_or(Operation::Discovering(path));
            if !operations.contains(&operation) {
                operations.push(operation);
            }
        }
        Ok(operations)
    }
}
