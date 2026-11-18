//! The kernel's word on the sessions directories. Codex makes its sessions
//! directory lazily, so a configured directory may not be there yet: its
//! watch rests on its nearest existing ancestor, and moves down at each
//! creation until it rests on the directory itself, which is then watched
//! whole, its subdirectories with it. A directory deleted or moved away
//! returns its watch to its nearest existing ancestor. The watch wakes the
//! Nexus, which reads only what was written. Nothing here keeps a clock.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, Sender, channel};

use harness::FilePath;
use meta_signal_codex_harness_nexus::{
    AwaitedDirectories, ConfigurationChange, SessionsDirectories, UnwatchableDirectory,
};
use nexus::Publishing as _;
use notify::event::{CreateKind, EventKind, ModifyKind, RenameMode};
use notify::{RecommendedWatcher, RecursiveMode, Watcher as _};
use signal_codex_harness_nexus::{Diagnostic, ReportFailure};

use crate::diagnosing::Diagnosing as _;
use crate::generated::memory::Rollout;
use crate::generated::operation::Operation;
use crate::generated::runtime::{Remembering, Waking};
use crate::nexus::{HarnessNexus, Locking as _};

/// One word of the kernel, stamped with the generation of the watch that
/// heard it, so a word from a watch since replaced is let go.
pub struct WatchWord {
    pub generation: u64,
    pub event: notify::Result<notify::Event>,
}

/// One configured sessions directory, as it was configured, and the
/// directory its watch rests on: itself once it exists, until then its
/// nearest existing ancestor; none, once the directory it rested on was
/// removed, until it is placed anew.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Placement {
    pub directory: FilePath,
    pub resting: Option<PathBuf>,
}

/// How far a watch reaches from the directory it rests on: its entries, to
/// hear the next path component made; or its whole tree, once the
/// configured directory itself is there.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Reach {
    Entries,
    Whole,
}

/// One watch of every configured sessions directory: the kernel's watcher,
/// where each directory's watch rests, and each directory watched and how
/// far.
pub struct DirectoryWatch {
    pub generation: u64,
    pub watcher: RecommendedWatcher,
    pub placements: Vec<Placement>,
    pub watched: BTreeMap<PathBuf, Reach>,
}

/// The Nexus's watch of its sessions directories: the channel every watch
/// it opens speaks into, the generations drawn, and the current watch, once
/// sessions directories are known.
pub struct SessionsWatch {
    pub sender: Sender<WatchWord>,
    pub drawn: u64,
    pub current: Option<DirectoryWatch>,
}

/// What one word of the kernel told: the files written inside a directory
/// that is there, the directories that came to be there, and each failure
/// met while the watch moved.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Followed {
    pub written: Vec<FilePath>,
    pub arrived: Vec<FilePath>,
    pub failures: Vec<Diagnostic>,
}

pub trait Placing: Sized {
    /// Where a directory's watch rests now: on the directory, if it is
    /// there; else on its nearest existing ancestor.
    fn placed(directory: FilePath) -> Self;

    /// The directory a watch of this one rests on now: the directory, if
    /// it is there; else its nearest existing ancestor.
    fn nearest(directory: &FilePath) -> PathBuf;

    /// Whether the watch rests on the configured directory itself.
    fn arrived(&self) -> bool;

    /// How far the watch reaches from where it rests.
    fn reach(&self) -> Reach;
}

impl Placing for Placement {
    fn placed(directory: FilePath) -> Self {
        Self {
            resting: Some(Self::nearest(&directory)),
            directory,
        }
    }

    fn nearest(directory: &FilePath) -> PathBuf {
        // An absolute path's ancestors end at the root, which is always
        // there.
        let path = Path::new(&directory.0);
        path.ancestors()
            .find(|ancestor| ancestor.is_dir())
            .unwrap_or(path)
            .to_path_buf()
    }

    fn arrived(&self) -> bool {
        self.resting.as_deref() == Some(Path::new(&self.directory.0))
    }

    fn reach(&self) -> Reach {
        if self.arrived() {
            Reach::Whole
        } else {
            Reach::Entries
        }
    }
}

impl From<Reach> for RecursiveMode {
    fn from(reach: Reach) -> Self {
        match reach {
            Reach::Entries => RecursiveMode::NonRecursive,
            Reach::Whole => RecursiveMode::Recursive,
        }
    }
}

pub trait Channelled: Sized {
    /// The Nexus's watch, watching nothing yet, and the receiving end of
    /// the channel every watch it opens speaks into.
    fn channelled() -> (Self, Receiver<WatchWord>);
}

impl Channelled for SessionsWatch {
    fn channelled() -> (Self, Receiver<WatchWord>) {
        let (sender, words) = channel();
        (
            Self {
                sender,
                drawn: 0,
                current: None,
            },
            words,
        )
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
        self.drawn += 1;
        let generation = self.drawn;
        let sender = self.sender.clone();
        // The kernel's watcher is refused only at its own limit, which the
        // first directory is told under.
        let watcher = notify::recommended_watcher(move |event| {
            let _ = sender.send(WatchWord { generation, event });
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
                .cloned()
                .map(Placement::placed)
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
    fn forget(&mut self, event: &notify::Event);

    /// The files this word says were written inside a configured
    /// directory that is there.
    fn written(&self, event: &notify::Event) -> Vec<FilePath>;

    /// The configured directories not yet there.
    fn awaited(&self) -> AwaitedDirectories;
}

impl Settling for DirectoryWatch {
    fn settle(&mut self) -> Vec<UnwatchableDirectory> {
        let mut failures: Vec<UnwatchableDirectory> = vec![];
        loop {
            let mut moved = false;
            for index in 0..self.placements.len() {
                let directory = self.placements[index].directory.clone();
                let resting = Placement::nearest(&directory);
                let wanted = Placement {
                    directory,
                    resting: Some(resting.clone()),
                };
                let reach = wanted.reach();
                let covered = self
                    .watched
                    .get(&resting)
                    .is_some_and(|watched| *watched >= reach);
                if wanted == self.placements[index] && covered {
                    continue;
                }
                if !covered {
                    match self.watcher.watch(&resting, reach.into()) {
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
                .filter_map(|placement| placement.resting.clone())
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

    fn forget(&mut self, event: &notify::Event) {
        let gone = matches!(
            event.kind,
            EventKind::Remove(_) | EventKind::Modify(ModifyKind::Name(RenameMode::From))
        );
        if !gone {
            return;
        }
        for path in &event.paths {
            if self.watched.remove(path).is_some() {
                let _ = self.watcher.unwatch(path);
                for placement in &mut self.placements {
                    if placement.resting.as_ref() == Some(path) {
                        placement.resting = None;
                    }
                }
            }
        }
    }

    fn written(&self, event: &notify::Event) -> Vec<FilePath> {
        match event.kind {
            EventKind::Create(CreateKind::Folder) => vec![],
            EventKind::Create(_) | EventKind::Modify(_) => event
                .paths
                .iter()
                .filter(|path| {
                    self.placements.iter().any(|placement| {
                        let directory = Path::new(&placement.directory.0);
                        placement.arrived() && path.starts_with(directory) && *path != directory
                    })
                })
                .map(|path| FilePath(path.to_string_lossy().into_owned()))
                .collect(),
            _ => vec![],
        }
    }

    fn awaited(&self) -> AwaitedDirectories {
        AwaitedDirectories(
            self.placements
                .iter()
                .filter(|placement| !placement.arrived())
                .map(|placement| placement.directory.clone())
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
    fn follow(&self, word: WatchWord) -> Followed;
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

    fn follow(&self, word: WatchWord) -> Followed {
        let mut sessions = self.sessions_watch.locked();
        let Some(watch) = sessions
            .current
            .as_mut()
            .filter(|watch| watch.generation == word.generation)
        else {
            return Followed::default();
        };
        let event = match word.event {
            Ok(event) => event,
            Err(error) => {
                return Followed {
                    failures: vec![error.diagnostic()],
                    ..Followed::default()
                };
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
            .map(|unwatchable| {
                Diagnostic(format!(
                    "{}: {}",
                    unwatchable.file_path.0, unwatchable.diagnostic.0
                ))
            })
            .collect();
        let after = watch.awaited();
        if before != after
            && let Some(publisher) = self.configurations.locked().as_ref()
        {
            publisher.publish(ConfigurationChange::AwaitedDirectories(after.clone()));
        }
        Followed {
            written,
            arrived: unplaced
                .0
                .into_iter()
                .filter(|directory| !after.0.contains(directory))
                .collect(),
            failures,
        }
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
