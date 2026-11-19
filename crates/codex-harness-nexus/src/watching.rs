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
    WatchChangeKind, WatchEvent, WatchFailure, WatchGeneration, WatchLifecycle, WatchOpening,
    WatchPlacement, WatchPlan, WatchPlanBuilding, WatchResult, WatchTarget, WatchWord,
};
use crate::generated::memory::{Rollout, SessionRecord};

/// One native kernel watcher plus its typed target placements.
pub struct DirectoryWatch {
    pub generation: WatchGeneration,
    pub lifecycle: WatchLifecycle,
    pub watcher: RecommendedWatcher,
    pub placements: Vec<Placement>,
    pub watched: BTreeMap<PathBuf, Reach>,
    pub failures: Vec<WatchFailure>,
}

/// The watch state owned exclusively by the core actor.
pub struct SessionsWatch {
    pub sender: Sender<WatchWord>,
    pub drawn: WatchGeneration,
    pub current: Option<Vec<DirectoryWatch>>,
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

impl WatchOpening for SessionsWatch {
    fn watch(&mut self, placement: WatchPlacement) -> Result<DirectoryWatch, WatchFailure> {
        let failed = |diagnostic: String| WatchFailure {
            watch_target_option: Some(placement.watch_target.clone()),
            file_path_option: Some(placement.file_path.clone()),
            diagnostic: Diagnostic(diagnostic),
        };
        let Some(next_generation) = self.drawn.0.checked_add(1) else {
            return Err(failed("watch generation exhausted".to_owned()));
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
        .map_err(|error| failed(error.to_string()))?;
        let mut watch = DirectoryWatch {
            generation,
            lifecycle: WatchLifecycle::Staged,
            watcher,
            placements: vec![placement.placed()],
            watched: BTreeMap::new(),
            failures: Vec::new(),
        };
        if let Some(failure) = watch.settle().into_iter().next() {
            Err(failure)
        } else {
            Ok(watch)
        }
    }
}

impl crate::generated::actor::SessionsWatching for SessionsWatch {
    fn opened(
        &mut self,
        plan: WatchPlan,
    ) -> Result<Option<Vec<DirectoryWatch>>, UnwatchableDirectory> {
        let mut watch_vector = Vec::new();
        for placement in plan.watch_placement_vector {
            match self.watch(placement.clone()) {
                Ok(mut watch) => {
                    watch.lifecycle = WatchLifecycle::Active;
                    watch_vector.push(watch);
                }
                Err(failure) if placement.watch_target == WatchTarget::SessionsDirectory => {
                    return Err(UnwatchableDirectory {
                        file_path: failure.file_path_option.unwrap_or(placement.file_path),
                        diagnostic: meta_signal_codex_harness_nexus::Diagnostic(
                            failure.diagnostic.0,
                        ),
                    });
                }
                Err(_) => {}
            }
        }
        Ok((!watch_vector.is_empty()).then_some(watch_vector))
    }

    fn replaced(&mut self, watch_vector_option: Option<Vec<DirectoryWatch>>) {
        self.current = watch_vector_option;
    }

    fn staged(&mut self, plan: WatchPlan) -> Vec<WatchFailure> {
        let mut failure_vector = Vec::new();
        for placement in &plan.watch_placement_vector {
            let wanted = placement.placed();
            let current_placement_is_healthy =
                self.current.as_ref().into_iter().flatten().any(|watch| {
                    watch.lifecycle == WatchLifecycle::Active
                        && watch.failures.is_empty()
                        && watch.placements.iter().any(|active| {
                            active.file_path == placement.file_path
                                && active.watch_target == placement.watch_target
                                && active == &wanted
                        })
                        && wanted.file_path_option.as_ref().is_some_and(|resting| {
                            watch
                                .watched
                                .get(Path::new(&resting.0))
                                .is_some_and(|reach| reach.reaches(wanted.reach()))
                        })
                });
            if current_placement_is_healthy {
                continue;
            }
            match self.watch(placement.clone()) {
                Ok(watch) => self.current.get_or_insert_with(Vec::new).push(watch),
                Err(failure) => failure_vector.push(failure),
            }
        }
        failure_vector
    }

    fn merged(&mut self, plan: WatchPlan, failure_vector: Vec<WatchFailure>) {
        let (mut current, mut candidate_vector): (Vec<DirectoryWatch>, Vec<DirectoryWatch>) = self
            .current
            .take()
            .unwrap_or_default()
            .into_iter()
            .partition(|watch| watch.lifecycle == WatchLifecycle::Active);
        current.retain(|watch| {
            watch.placements.iter().any(|active| {
                plan.watch_placement_vector.iter().any(|planned| {
                    active.file_path == planned.file_path
                        && active.watch_target == planned.watch_target
                })
            })
        });
        for mut candidate in candidate_vector.drain(..) {
            if let Some(placement) = candidate.placements.first() {
                current.retain(|active| {
                    !active.placements.iter().any(|existing| {
                        existing.file_path == placement.file_path
                            && existing.watch_target == placement.watch_target
                    })
                });
                candidate.lifecycle = WatchLifecycle::Active;
                current.push(candidate);
            }
        }
        self.current = (!current.is_empty()).then_some(current);

        let planned_session_paths: Vec<FilePath> = plan
            .watch_placement_vector
            .iter()
            .filter(|placement| placement.watch_target == WatchTarget::SessionsDirectory)
            .map(|placement| placement.file_path.clone())
            .collect();
        self.unwatchable.retain(|directory| {
            planned_session_paths.contains(&directory.file_path)
                && !self.current.as_ref().is_some_and(|current| {
                    plan.watch_placement_vector
                        .iter()
                        .filter(|planned| {
                            planned.watch_target == WatchTarget::SessionsDirectory
                                && planned.file_path == directory.file_path
                        })
                        .any(|planned| {
                            let wanted = planned.placed();
                            current.iter().any(|watch| {
                                watch.lifecycle == WatchLifecycle::Active
                                    && watch.failures.is_empty()
                                    && watch.placements.contains(&wanted)
                                    && wanted.file_path_option.as_ref().is_some_and(|resting| {
                                        watch
                                            .watched
                                            .get(Path::new(&resting.0))
                                            .is_some_and(|reach| reach.reaches(wanted.reach()))
                                    })
                            })
                        })
                })
        });
        for failure in failure_vector {
            if failure.watch_target_option != Some(WatchTarget::SessionsDirectory) {
                continue;
            }
            let Some(file_path) = failure.file_path_option else {
                continue;
            };
            self.unwatchable
                .retain(|directory| directory.file_path != file_path);
            self.unwatchable.push(UnwatchableDirectory {
                file_path,
                diagnostic: meta_signal_codex_harness_nexus::Diagnostic(failure.diagnostic.0),
            });
        }
    }

    fn discarded(&mut self) {
        if let Some(current) = self.current.as_mut() {
            current.retain(|watch| watch.lifecycle == WatchLifecycle::Active);
        }
        if self.current.as_ref().is_some_and(Vec::is_empty) {
            self.current = None;
        }
    }

    fn awaited(&self) -> AwaitedDirectories {
        let mut directories = Vec::new();
        for watch in self.current.as_ref().into_iter().flatten() {
            if watch.lifecycle != WatchLifecycle::Active {
                continue;
            }
            for directory in watch.awaited().0 {
                if !directories.contains(&directory) {
                    directories.push(directory);
                }
            }
        }
        AwaitedDirectories(directories)
    }

    fn unwatchable(&self) -> UnwatchableDirectories {
        let mut directories = self.unwatchable.clone();
        for failure in self
            .current
            .as_ref()
            .into_iter()
            .flatten()
            .filter(|watch| watch.lifecycle == WatchLifecycle::Active)
            .flat_map(|watch| watch.failures.iter())
            .filter(|failure| failure.watch_target_option == Some(WatchTarget::SessionsDirectory))
        {
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
