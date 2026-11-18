//! The kernel's word on the sessions directories. Codex makes its sessions
//! directory lazily, so a configured directory may not be there yet: its
//! watch rests on its nearest existing ancestor, and moves down at each
//! creation until it rests on the directory itself, which is then watched
//! whole, its subdirectories with it. A directory deleted or moved away
//! returns its watch to its nearest existing ancestor. The watch wakes the
//! Nexus, which reads only what was written. Nothing here keeps a clock.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;

use harness::FilePath;
use meta_signal_codex_harness_nexus::{
    AwaitedDirectories, ConfigurationChange, SessionsDirectories, UnwatchableDirectory,
};
use nexus::Publishing as _;
use notify::event::{CreateKind, EventKind, ModifyKind, RenameMode};
use notify::{RecommendedWatcher, RecursiveMode, Watcher as _};
use signal_codex_harness_nexus::{Diagnostic, ReportFailure};

use crate::generated::actor::{
    Placement, PlacementPositioning as _, PlacementState as _, Reach, ReachCoverage as _,
    WatchChangeKind, WatchConsequence, WatchEvent, WatchFailure, WatchGeneration, WatchResult,
    WatchWord,
};
use crate::generated::memory::Rollout;
use crate::generated::operation::Operation;
use crate::generated::runtime::{Remembering, Waking};
use crate::nexus::{HarnessNexus, Locking as _};

/// One watch of every configured sessions directory: the kernel's watcher,
/// where each directory's watch rests, and each directory watched and how
/// far.
pub struct DirectoryWatch {
    pub generation: WatchGeneration,
    pub watcher: RecommendedWatcher,
    pub placements: Vec<Placement>,
    pub watched: BTreeMap<PathBuf, Reach>,
}

/// The Nexus's watch of its sessions directories: the channel every watch
/// it opens speaks into, the generations drawn, and the current watch, once
/// sessions directories are known.
pub struct SessionsWatch {
    pub sender: Sender<WatchWord>,
    pub drawn: WatchGeneration,
    pub current: Option<DirectoryWatch>,
}

impl PlacementPositioning for FilePath {
    fn placed(&self) -> Placement {
        let path = Path::new(&self.0);
        let resting = path
            .ancestors()
            .find(|ancestor| ancestor.is_dir())
            .unwrap_or(path);
        Placement {
            file_path: self.clone(),
            file_path_option: Some(FilePath(resting.to_string_lossy().into_owned())),
        }
    }
}

impl PlacementState for Placement {
    fn arrived(&self) -> bool {
        self.file_path_option
            .as_ref()
            .is_some_and(|resting| Path::new(&resting.0) == Path::new(&self.file_path.0))
    }

    fn reach(&self) -> Reach {
        if self.arrived() {
            Reach::Whole
        } else {
            Reach::Entries
        }
    }
}

impl ReachCoverage for Reach {
    fn reaches(&self, needed: Reach) -> bool {
        matches!(
            (self, needed),
            (Reach::Whole, _) | (Reach::Entries, Reach::Entries)
        )
    }
}

impl From<&Reach> for RecursiveMode {
    fn from(reach: &Reach) -> Self {
        match reach {
            Reach::Entries => RecursiveMode::NonRecursive,
            Reach::Whole => RecursiveMode::Recursive,
        }
    }
}

impl From<Reach> for RecursiveMode {
    fn from(reach: Reach) -> Self {
        Self::from(&reach)
    }
}

impl From<notify::Event> for WatchEvent {
    fn from(event: notify::Event) -> Self {
        let watch_change_kind = match event.kind {
            EventKind::Create(CreateKind::Folder) => WatchChangeKind::CreatedFolder,
            EventKind::Create(_) => WatchChangeKind::Created,
            EventKind::Remove(_) => WatchChangeKind::Removed,
            EventKind::Modify(ModifyKind::Name(RenameMode::From)) => WatchChangeKind::RenamedFrom,
            EventKind::Modify(_) => WatchChangeKind::Modified,
            _ => WatchChangeKind::Other,
        };
        Self {
            watch_change_kind,
            file_path_vector: event
                .paths
                .into_iter()
                .map(|path| FilePath(path.to_string_lossy().into_owned()))
                .collect(),
        }
    }
}

impl From<notify::Result<notify::Event>> for WatchResult {
    fn from(event: notify::Result<notify::Event>) -> Self {
        match event {
            Ok(event) => Self::WatchEvent(event.into()),
            Err(error) => Self::WatchFailure(WatchFailure {
                file_path_option: error
                    .paths
                    .first()
                    .map(|path| FilePath(path.to_string_lossy().into_owned())),
                diagnostic: Diagnostic(error.to_string()),
            }),
        }
    }
}

pub trait Replacing {
    /// Opens a watch of these directories, each resting where it can; a
    /// directory there that cannot be watched refuses the whole. No
    /// directory is no watch.
    fn opened(
        &mut self,
        directories: &SessionsDirectories,
    ) -> Result<Option<DirectoryWatch>, UnwatchableDirectory>;

    /// Takes this watch in place of the one before, which ends with it.
    fn replaced(&mut self, watch: Option<DirectoryWatch>);

    /// The configured directories not yet there.
    fn awaited(&self) -> AwaitedDirectories;
}

impl Replacing for SessionsWatch {
    fn opened(
        &mut self,
        directories: &SessionsDirectories,
    ) -> Result<Option<DirectoryWatch>, UnwatchableDirectory> {
        let Some(first) = directories.0.first() else {
            return Ok(None);
        };
        let Some(next_generation) = self.drawn.0.checked_add(1) else {
            return Err(UnwatchableDirectory {
                file_path: first.clone(),
                diagnostic: meta_signal_codex_harness_nexus::Diagnostic(
                    "watch generation exhausted".to_owned(),
                ),
            });
        };
        self.drawn = WatchGeneration(next_generation);
        let generation = self.drawn;
        let sender = self.sender.clone();
        // The kernel's watcher is refused only at its own limit, which the
        // first directory is told under.
        let watcher = notify::recommended_watcher(move |event| {
            let _ = sender.send(WatchWord {
                watch_generation: generation,
                watch_result: event.into(),
            });
        })
        .map_err(|error| UnwatchableDirectory {
            file_path: first.clone(),
            diagnostic: meta_signal_codex_harness_nexus::Diagnostic(error.to_string()),
        })?;
        let mut watch = DirectoryWatch {
            generation,
            watcher,
            placements: directories
                .0
                .iter()
                .map(PlacementPositioning::placed)
                .collect(),
            watched: BTreeMap::new(),
        };
        match watch.settle().into_iter().next() {
            Some(unwatchable) => Err(unwatchable),
            None => Ok(Some(watch)),
        }
    }

    fn replaced(&mut self, watch: Option<DirectoryWatch>) {
        self.current = watch;
    }

    fn awaited(&self) -> AwaitedDirectories {
        self.current
            .as_ref()
            .map_or(AwaitedDirectories(vec![]), DirectoryWatch::awaited)
    }
}

pub trait Settling {
    /// Moves each directory's watch to where it rests now, watching the new
    /// place before the old is let go, and looks again after each move, so
    /// a component made while the watch moved is not missed. A directory
    /// there that cannot be watched keeps its watch where it was, and is
    /// told.
    fn settle(&mut self) -> Vec<UnwatchableDirectory>;

    /// A watched directory removed or moved away no longer holds its
    /// watch: it is let go, every directory resting on it rests nowhere,
    /// and settling places each anew, so one made again at once is still
    /// told as come to be there.
    fn forget(&mut self, event: &WatchEvent);

    /// The files this word says were written inside a configured
    /// directory that is there.
    fn written(&self, event: &WatchEvent) -> Vec<FilePath>;

    /// The configured directories not yet there.
    fn awaited(&self) -> AwaitedDirectories;
}

impl Settling for DirectoryWatch {
    fn settle(&mut self) -> Vec<UnwatchableDirectory> {
        let mut failures: Vec<UnwatchableDirectory> = vec![];
        loop {
            let mut moved = false;
            for index in 0..self.placements.len() {
                let wanted = self.placements[index].file_path.placed();
                let Some(resting_path) = wanted.file_path_option.as_ref() else {
                    continue;
                };
                let resting = PathBuf::from(&resting_path.0);
                let reach = wanted.reach();
                let covered = self
                    .watched
                    .get(&resting)
                    .is_some_and(|watched| watched.reaches(reach.clone()));
                if wanted == self.placements[index] && covered {
                    continue;
                }
                if !covered {
                    match self.watcher.watch(&resting, (&reach).into()) {
                        Ok(()) => {
                            self.watched.insert(resting.clone(), reach);
                        }
                        // Gone between the look and the watch: look again.
                        Err(error) if matches!(error.kind, notify::ErrorKind::PathNotFound) => {
                            moved = true;
                            continue;
                        }
                        Err(error) => {
                            let unwatchable = UnwatchableDirectory {
                                file_path: FilePath(resting.to_string_lossy().into_owned()),
                                diagnostic: meta_signal_codex_harness_nexus::Diagnostic(
                                    error.to_string(),
                                ),
                            };
                            if !failures.contains(&unwatchable) {
                                failures.push(unwatchable);
                            }
                            continue;
                        }
                    }
                }
                self.placements[index] = wanted;
                moved = true;
            }
            // Only what some directory still rests on stays watched.
            let resting: Vec<PathBuf> = self
                .placements
                .iter()
                .filter_map(|placement| {
                    placement
                        .file_path_option
                        .as_ref()
                        .map(|resting| PathBuf::from(&resting.0))
                })
                .collect();
            let released: Vec<PathBuf> = self
                .watched
                .keys()
                .filter(|path| !resting.contains(path))
                .cloned()
                .collect();
            for path in released {
                let _ = self.watcher.unwatch(&path);
                self.watched.remove(&path);
            }
            if !moved {
                return failures;
            }
        }
    }

    fn forget(&mut self, event: &WatchEvent) {
        let gone = matches!(
            &event.watch_change_kind,
            WatchChangeKind::Removed | WatchChangeKind::RenamedFrom
        );
        if !gone {
            return;
        }
        for file_path in &event.file_path_vector {
            let path = PathBuf::from(&file_path.0);
            if self.watched.remove(&path).is_some() {
                let _ = self.watcher.unwatch(&path);
                for placement in &mut self.placements {
                    if placement
                        .file_path_option
                        .as_ref()
                        .is_some_and(|resting| Path::new(&resting.0) == path.as_path())
                    {
                        placement.file_path_option = None;
                    }
                }
            }
        }
    }

    fn written(&self, event: &WatchEvent) -> Vec<FilePath> {
        match &event.watch_change_kind {
            WatchChangeKind::CreatedFolder => vec![],
            WatchChangeKind::Created | WatchChangeKind::Modified => event
                .file_path_vector
                .iter()
                .filter(|file_path| {
                    let path = Path::new(&file_path.0);
                    self.placements.iter().any(|placement| {
                        let directory = Path::new(&placement.file_path.0);
                        placement.arrived() && path.starts_with(directory) && path != directory
                    })
                })
                .cloned()
                .collect(),
            _ => vec![],
        }
    }

    fn awaited(&self) -> AwaitedDirectories {
        AwaitedDirectories(
            self.placements
                .iter()
                .filter(|placement| !placement.arrived())
                .map(|placement| placement.file_path.clone())
                .collect(),
        )
    }
}

/// The Nexus's side of its watch.
pub trait Watching {
    /// Watches these directories in place of any watch before: as the
    /// Nexus starts, the directories its Memory recorded.
    fn watch(&self, directories: &SessionsDirectories) -> Result<(), UnwatchableDirectory>;

    /// Follows one word of the kernel: lets go of a directory gone, moves
    /// each watch to where it rests now, and tells a change in what is
    /// awaited to the configuration's subscribers.
    fn follow(&self, word: WatchWord) -> Vec<WatchConsequence>;
}

impl<Translator> Watching for HarnessNexus<Translator> {
    fn watch(&self, directories: &SessionsDirectories) -> Result<(), UnwatchableDirectory> {
        let mut sessions = self.sessions_watch.locked();
        let watch = sessions.opened(directories)?;
        sessions.replaced(watch);
        if let Some(publisher) = self.configurations.locked().as_ref() {
            publisher.publish(ConfigurationChange::AwaitedDirectories(sessions.awaited()));
        }
        Ok(())
    }

    fn follow(&self, word: WatchWord) -> Vec<WatchConsequence> {
        let mut sessions = self.sessions_watch.locked();
        let Some(watch) = sessions
            .current
            .as_mut()
            .filter(|watch| watch.generation == word.watch_generation)
        else {
            return vec![];
        };
        let event = match word.watch_result {
            WatchResult::WatchEvent(event) => event,
            WatchResult::WatchFailure(failure) => {
                return vec![WatchConsequence::WatchFailure(failure)];
            }
        };
        let before = watch.awaited();
        watch.forget(&event);
        // A directory whose watch was lost is awaited until it is placed
        // anew, so one made again at once is told as come to be there.
        let unplaced = watch.awaited();
        let written = watch.written(&event);
        let failures = watch
            .settle()
            .into_iter()
            .map(|unwatchable| WatchFailure {
                file_path_option: Some(unwatchable.file_path),
                diagnostic: Diagnostic(unwatchable.diagnostic.0),
            })
            .collect();
        let after = watch.awaited();
        if before != after
            && let Some(publisher) = self.configurations.locked().as_ref()
        {
            publisher.publish(ConfigurationChange::AwaitedDirectories(after.clone()));
        }
        let arrived: Vec<FilePath> = unplaced
            .0
            .into_iter()
            .filter(|directory| !after.0.contains(directory))
            .collect();
        let mut consequences: Vec<WatchConsequence> = failures
            .into_iter()
            .map(WatchConsequence::WatchFailure)
            .collect();
        if !arrived.is_empty() {
            consequences.push(WatchConsequence::Arrived(arrived));
        }
        if !written.is_empty() {
            consequences.push(WatchConsequence::Written(written));
        }
        consequences
    }
}

impl<Translator> Waking for HarnessNexus<Translator> {
    /// One look at startup, and again whenever a sessions directory comes
    /// to be there: each watched rollout is read from its offset, and each
    /// pending one attaches if it now exists.
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
