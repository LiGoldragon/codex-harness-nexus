//! Native notification handles and generated placement behavior.
//!
//! A sessions directory is watched recursively once it exists; until then,
//! its nearest existing ancestor is watched and the placement moves down as
//! directory-created events arrive. An explicit rollout path is watched at
//! its nearest existing parent, including when the path is outside every
//! configured sessions directory. Its events match that path exactly.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;

use harness::FilePath;
use meta_signal_codex_harness_nexus::{
    AwaitedDirectories, SessionsDirectories, UnwatchableDirectories, UnwatchableDirectory,
};
use notify::event::{CreateKind, EventKind, ModifyKind, RenameMode};
use notify::{RecommendedWatcher, RecursiveMode, Watcher as _};
use signal_codex_harness_nexus::Diagnostic;

use crate::generated::actor::{
    Placement, PlacementPositioning, PlacementState, Reach, ReachCoverage, Settling,
    WatchChangeKind, WatchEvent, WatchFailure, WatchGeneration, WatchPlacement, WatchPlan,
    WatchPlanBuilding, WatchResult, WatchTarget, WatchWord,
};
use crate::generated::memory::{Rollout, SessionRecord};

/// One native kernel watcher plus its typed target placements.
pub struct DirectoryWatch {
    pub generation: WatchGeneration,
    pub watcher: RecommendedWatcher,
    pub placements: Vec<Placement>,
    pub watched: BTreeMap<PathBuf, Reach>,
    pub failures: Vec<WatchFailure>,
}

/// The watch state owned exclusively by the core actor.
pub struct SessionsWatch {
    pub sender: Sender<WatchWord>,
    pub drawn: WatchGeneration,
    pub current: Option<DirectoryWatch>,
    pub unwatchable: Vec<UnwatchableDirectory>,
}

impl PlacementPositioning for WatchPlacement {
    fn placed(&self) -> Placement {
        let target = Path::new(&self.file_path.0);
        let start = match self.watch_target {
            WatchTarget::SessionsDirectory => target,
            WatchTarget::RolloutFile => target.parent().unwrap_or(target),
        };
        let resting = start.ancestors().find(|ancestor| ancestor.is_dir());
        Placement {
            file_path: self.file_path.clone(),
            watch_target: self.watch_target.clone(),
            file_path_option: resting.map(|path| FilePath(path.to_string_lossy().into_owned())),
        }
    }
}

impl PlacementState for Placement {
    fn arrived(&self) -> bool {
        matches!(self.watch_target, WatchTarget::SessionsDirectory)
            && self
                .file_path_option
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
                watch_target_option: None,
                file_path_option: error
                    .paths
                    .first()
                    .map(|path| FilePath(path.to_string_lossy().into_owned())),
                diagnostic: Diagnostic(error.to_string()),
            }),
        }
    }
}

impl Settling for DirectoryWatch {
    fn settle(&mut self) -> Vec<WatchFailure> {
        let mut failures = Vec::new();
        loop {
            let mut moved = false;
            for index in 0..self.placements.len() {
                let target = WatchPlacement {
                    file_path: self.placements[index].file_path.clone(),
                    watch_target: self.placements[index].watch_target.clone(),
                };
                let wanted = target.placed();
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
                        Err(error) if matches!(error.kind, notify::ErrorKind::PathNotFound) => {
                            moved = true;
                            continue;
                        }
                        Err(error) => {
                            let failure = WatchFailure {
                                watch_target_option: Some(target.watch_target),
                                file_path_option: Some(target.file_path),
                                diagnostic: Diagnostic(error.to_string()),
                            };
                            if !failures.contains(&failure) {
                                failures.push(failure);
                            }
                            continue;
                        }
                    }
                }
                self.placements[index] = wanted;
                moved = true;
            }
            let resting: Vec<PathBuf> = self
                .placements
                .iter()
                .filter_map(|placement| {
                    placement
                        .file_path_option
                        .as_ref()
                        .map(|path| PathBuf::from(&path.0))
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
                self.failures.clone_from(&failures);
                return failures;
            }
        }
    }

    fn forget(&mut self, event: WatchEvent) {
        if !matches!(
            event.watch_change_kind,
            WatchChangeKind::Removed | WatchChangeKind::RenamedFrom
        ) {
            return;
        }
        for file_path in event.file_path_vector {
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

    fn written(&self, event: WatchEvent) -> Vec<FilePath> {
        match event.watch_change_kind {
            WatchChangeKind::CreatedFolder => Vec::new(),
            WatchChangeKind::Created | WatchChangeKind::Modified => event
                .file_path_vector
                .into_iter()
                .filter(|file_path| {
                    let path = Path::new(&file_path.0);
                    self.placements
                        .iter()
                        .any(|placement| match placement.watch_target {
                            WatchTarget::SessionsDirectory => {
                                let directory = Path::new(&placement.file_path.0);
                                placement.arrived()
                                    && path.starts_with(directory)
                                    && path != directory
                            }
                            WatchTarget::RolloutFile => path == Path::new(&placement.file_path.0),
                        })
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    fn awaited(&self) -> AwaitedDirectories {
        AwaitedDirectories(
            self.placements
                .iter()
                .filter(|placement| {
                    placement.watch_target == WatchTarget::SessionsDirectory && !placement.arrived()
                })
                .map(|placement| placement.file_path.clone())
                .collect(),
        )
    }
}

impl crate::generated::actor::SessionsWatching for SessionsWatch {
    fn opened(&mut self, plan: WatchPlan) -> Result<Option<DirectoryWatch>, UnwatchableDirectory> {
        if plan.watch_placement_vector.is_empty() {
            return Ok(None);
        }
        let first = plan.watch_placement_vector[0].file_path.clone();
        let Some(next_generation) = self.drawn.0.checked_add(1) else {
            return Err(UnwatchableDirectory {
                file_path: first,
                diagnostic: meta_signal_codex_harness_nexus::Diagnostic(
                    "watch generation exhausted".to_owned(),
                ),
            });
        };
        self.drawn = WatchGeneration(next_generation);
        let generation = self.drawn;
        let sender = self.sender.clone();
        let watcher = notify::recommended_watcher(move |event| {
            let _ = sender.send(WatchWord {
                watch_generation: generation,
                watch_result: event.into(),
            });
        })
        .map_err(|error| UnwatchableDirectory {
            file_path: first,
            diagnostic: meta_signal_codex_harness_nexus::Diagnostic(error.to_string()),
        })?;
        let mut watch = DirectoryWatch {
            generation,
            watcher,
            placements: plan
                .watch_placement_vector
                .into_iter()
                .map(PlacementPositioning::placed)
                .collect(),
            watched: BTreeMap::new(),
            failures: Vec::new(),
        };
        let failures = watch.settle();
        if let Some(failure) = failures
            .iter()
            .find(|failure| failure.watch_target_option == Some(WatchTarget::SessionsDirectory))
        {
            return Err(UnwatchableDirectory {
                file_path: failure.file_path_option.clone().unwrap_or(first),
                diagnostic: meta_signal_codex_harness_nexus::Diagnostic(
                    failure.diagnostic.0.clone(),
                ),
            });
        }
        Ok(Some(watch))
    }

    fn replaced(&mut self, watch: Option<DirectoryWatch>) {
        self.current = watch;
    }

    fn retarget(&mut self, plan: WatchPlan) -> Vec<WatchFailure> {
        match self.current.as_mut() {
            Some(current) => {
                current.placements = plan
                    .watch_placement_vector
                    .into_iter()
                    .map(PlacementPositioning::placed)
                    .collect();
                current.settle()
            }
            None => match self.opened(plan) {
                Ok(watch) => {
                    self.replaced(watch);
                    self.current
                        .as_ref()
                        .map_or_else(Vec::new, |watch| watch.failures.clone())
                }
                Err(failure) => vec![WatchFailure {
                    watch_target_option: Some(WatchTarget::SessionsDirectory),
                    file_path_option: Some(failure.file_path),
                    diagnostic: Diagnostic(failure.diagnostic.0),
                }],
            },
        }
    }

    fn awaited(&self) -> AwaitedDirectories {
        self.current
            .as_ref()
            .map_or_else(|| AwaitedDirectories(Vec::new()), Settling::awaited)
    }

    fn unwatchable(&self) -> UnwatchableDirectories {
        let mut directories = self.unwatchable.clone();
        if let Some(current) = &self.current {
            for failure in current.failures.iter().filter(|failure| {
                failure.watch_target_option == Some(WatchTarget::SessionsDirectory)
            }) {
                let Some(file_path) = &failure.file_path_option else {
                    continue;
                };
                let directory = UnwatchableDirectory {
                    file_path: file_path.clone(),
                    diagnostic: meta_signal_codex_harness_nexus::Diagnostic(
                        failure.diagnostic.0.clone(),
                    ),
                };
                if !directories.contains(&directory) {
                    directories.push(directory);
                }
            }
        }
        UnwatchableDirectories(directories)
    }
}

impl WatchPlanBuilding for SessionsWatch {
    fn planned(
        &self,
        sessions_directories_option: Option<SessionsDirectories>,
        records: Vec<SessionRecord>,
    ) -> WatchPlan {
        let mut watch_placement_vector: Vec<WatchPlacement> = sessions_directories_option
            .into_iter()
            .flat_map(|directories| directories.0)
            .map(|file_path| WatchPlacement {
                file_path,
                watch_target: WatchTarget::SessionsDirectory,
            })
            .collect();
        for record in records {
            let file_path_option = match record.rollout {
                Rollout::Pending(file_path) => Some(file_path),
                Rollout::Watched(watch) => Some(watch.file_path),
                Rollout::Unlocated => None,
            };
            let Some(file_path) = file_path_option else {
                continue;
            };
            let placement = WatchPlacement {
                file_path,
                watch_target: WatchTarget::RolloutFile,
            };
            if !watch_placement_vector.contains(&placement) {
                watch_placement_vector.push(placement);
            }
        }
        WatchPlan {
            watch_placement_vector,
        }
    }
}
