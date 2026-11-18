//! The running Nexus: its Memory, its translator, and the publishers its
//! subscribers follow. Each operation is attempted through the translator's
//! emissions, folded into the session it names, committed whole, and only
//! then published.

use std::sync::{Mutex, MutexGuard};

use harness::{FilePath, Observable, Session};
use meta_signal_codex_harness_nexus::{
    Configuration, ConfigurationChange, ConfigurationReadout, ConfigurationRefusal, Set,
};
use nexus::{Capacity, CapacityRefusal, Publisher, Publishing};
use signal_codex_harness_nexus::{
    Changed, Diagnostic, Hook, Observation, Observations, Output, ReportFailure,
};

use crate::generated::emission::{
    ClaimIdentity_Data, Emittable, FlowsRoot, IdentityClaim_Data, IdentityProducer,
    RolloutFinding_Data, RolloutIdentity_Data, RolloutReading_Data,
};
use crate::generated::folding::{FoldRefusal, Folding};
use crate::generated::memory::{Rollout, SessionRecord, Watch};
use crate::generated::operation::{EventBatch, Operation, Outcome, ReportedLaneFailed_Data};
use crate::generated::runtime::{
    Attempting, NamingPaths, Observing, Operable, Performed, Performing, ReadyConfiguration,
    Readying, Remembering, Sessional,
};
use crate::store::Store;
use crate::watching::{Replacing as _, SessionsWatch};

/// The Nexus whole. Its translator carries every emission; its publishers
/// exist once the subscriber capacity is configured; its watch of the
/// sessions directories is opened as they are set.
///
/// Its locks are taken in one order: the serial lock, then the watch, then
/// a publisher.
pub struct HarnessNexus<Translator> {
    pub store: Store,
    pub translator: Translator,
    pub serial: Mutex<()>,
    pub observations: Mutex<Option<Publisher<Observations, Changed>>>,
    pub configurations: Mutex<Option<Publisher<ConfigurationReadout, ConfigurationChange>>>,
    pub sessions_watch: Mutex<SessionsWatch>,
}

impl Readying for Configuration {
    fn ready(&self) -> Option<ReadyConfiguration> {
        Some(ReadyConfiguration {
            flows_root: self.flows_root_option.clone()?,
            identity_producer: self.identity_producer_option.clone()?,
            sessions_directories: self.sessions_directories_option.clone()?,
            thread_databases: self.thread_databases_option.clone()?,
            capacity: self.capacity_option?,
        })
    }
}

/// The configuration as it is read out: what is recorded, and the sessions
/// directories recorded but not yet there, which the Nexus awaits.
pub trait ConfigurationReading {
    fn readout(&self) -> Result<ConfigurationReadout, Diagnostic>;
}

impl<Translator> ConfigurationReading for HarnessNexus<Translator> {
    fn readout(&self) -> Result<ConfigurationReadout, Diagnostic> {
        let configuration = self.store.configuration()?;
        Ok(ConfigurationReadout {
            configuration,
            awaited_directories: self.sessions_watch.locked().awaited(),
        })
    }
}

/// A mutex held through a panic elsewhere: its value is still the
/// Nexus's, so the guard is taken back from the poisoning.
pub trait Locking<Value> {
    fn locked(&self) -> MutexGuard<'_, Value>;
}

impl<Value> Locking<Value> for Mutex<Value> {
    fn locked(&self) -> MutexGuard<'_, Value> {
        self.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl<Translator> Attempting for HarnessNexus<Translator>
where
    Session: Emittable<Translator, Outcome = RolloutFinding_Data>,
    ClaimIdentity_Data: Emittable<Translator, Outcome = IdentityClaim_Data>,
    Watch: Emittable<Translator, Outcome = RolloutReading_Data>,
    FilePath: Emittable<Translator, Outcome = RolloutIdentity_Data>,
{
    fn attempt<N: Operable>(&self, input: N) -> Result<Outcome, ReportFailure> {
        let ready = self
            .store
            .configuration()
            .map_err(ReportFailure::Unrecorded)?
            .ready()
            .ok_or(ReportFailure::Unconfigured)?;
        match input.operation() {
            Operation::Reporting(hook) => {
                let session = hook.session();
                let record = self
                    .store
                    .recall(session.clone())
                    .map_err(ReportFailure::Unrecorded)?;
                let flow_id = match (&record, &hook) {
                    (Some(record), _) => record.observed.flow_id.clone(),
                    (None, Hook::SessionStart(_)) => match (ClaimIdentity_Data {
                        session: session.clone(),
                        flows_root: FlowsRoot(ready.flows_root.0.clone()),
                        identity_producer: IdentityProducer(ready.identity_producer.0.clone()),
                    })
                    .emit(&self.translator)
                    {
                        IdentityClaim_Data::Claimed(flow_id) => flow_id,
                        IdentityClaim_Data::Unclaimed(failure) => {
                            return Err(ReportFailure::Unidentified(failure));
                        }
                        // A new session's record needs its identity, so a
                        // lane that fails here records nothing.
                        IdentityClaim_Data::LaneFailed(lane_failure) => {
                            return Err(ReportFailure::LaneFailed(lane_failure));
                        }
                    },
                    (None, _) => return Err(ReportFailure::Unregistered(session)),
                };
                let (rollout_option, lane_failure_option) = match (&hook, &record) {
                    // A rollout already watched stays as it is watched.
                    (
                        _,
                        Some(SessionRecord {
                            rollout: Rollout::Watched(_),
                            ..
                        }),
                    ) => (None, None),
                    (Hook::SessionStart(start), _) => match &start.file_path_option {
                        Some(path) => (Some(Rollout::Pending(path.clone())), None),
                        None => match session.emit(&self.translator) {
                            RolloutFinding_Data::Found(path) => {
                                (Some(Rollout::Pending(path)), None)
                            }
                            RolloutFinding_Data::Unfound => (Some(Rollout::Unlocated), None),
                            // The session is recorded whatever the lane does:
                            // its rollout stays as the Memory holds it,
                            // unlocated for a new session, and the watch
                            // finds it by its first row at its next write.
                            RolloutFinding_Data::LaneFailed(lane_failure) => {
                                (None, Some(lane_failure))
                            }
                        },
                    },
                    _ => (None, None),
                };
                let event_batch = EventBatch {
                    session,
                    flow_id,
                    observation_vector: hook.events().into_iter().map(Observation::Event).collect(),
                    rollout_option,
                };
                Ok(match lane_failure_option {
                    None => Outcome::Reported(event_batch),
                    Some(lane_failure) => Outcome::ReportedLaneFailed(ReportedLaneFailed_Data {
                        event_batch,
                        lane_failure,
                    }),
                })
            }
            Operation::Reading(watch) => {
                let record = self
                    .store
                    .recall_all()
                    .map_err(ReportFailure::Unrecorded)?
                    .into_iter()
                    .find(|record| record.observed.flow_id == watch.flow_id)
                    .ok_or_else(|| {
                        ReportFailure::Unrecorded(Diagnostic(format!(
                            "no session holds flow {}",
                            watch.flow_id.0
                        )))
                    })?;
                match watch.emit(&self.translator) {
                    RolloutReading_Data::Read(read) => Ok(Outcome::Read(EventBatch {
                        session: record.session,
                        flow_id: watch.flow_id.clone(),
                        observation_vector: read.observation_vector,
                        rollout_option: Some(Rollout::Watched(Watch {
                            flow_id: watch.flow_id,
                            file_path: watch.file_path,
                            byte_offset: read.byte_offset,
                        })),
                    })),
                    RolloutReading_Data::Unreadable(diagnostic) => {
                        Err(ReportFailure::Unrecorded(diagnostic))
                    }
                    RolloutReading_Data::LaneFailed(lane_failure) => {
                        Err(ReportFailure::LaneFailed(lane_failure))
                    }
                }
            }
            Operation::Discovering(path) => {
                let records = self.store.recall_all().map_err(ReportFailure::Unrecorded)?;
                let attached = |record: SessionRecord| {
                    Outcome::Discovered(Some(EventBatch {
                        session: record.session,
                        flow_id: record.observed.flow_id.clone(),
                        observation_vector: vec![],
                        rollout_option: Some(Rollout::Watched(Watch {
                            flow_id: record.observed.flow_id,
                            file_path: path.clone(),
                            byte_offset: signal_codex_harness_nexus::ByteOffset(0),
                        })),
                    }))
                };
                // The path a hook named attaches as it is, read for nothing.
                if let Some(record) = records
                    .iter()
                    .find(|record| record.rollout == Rollout::Pending(path.clone()))
                {
                    return Ok(attached(record.clone()));
                }
                // A path no hook named is identified by its first row, and
                // attaches only to a known session still unlocated.
                match path.emit(&self.translator) {
                    RolloutIdentity_Data::Identified(session) => Ok(records
                        .into_iter()
                        .find(|record| {
                            record.session == session && record.rollout == Rollout::Unlocated
                        })
                        .map_or(Outcome::Discovered(None), attached)),
                    RolloutIdentity_Data::Unidentified => Ok(Outcome::Discovered(None)),
                    RolloutIdentity_Data::LaneFailed(lane_failure) => {
                        Err(ReportFailure::LaneFailed(lane_failure))
                    }
                }
            }
        }
    }
}

/// A fold refused for a session never registered is told as such; any
/// other refusal is a Memory that could not be recorded.
impl From<FoldRefusal> for ReportFailure {
    fn from(refusal: FoldRefusal) -> Self {
        match refusal {
            FoldRefusal::Unregistered(session) => ReportFailure::Unregistered(session),
            refusal => ReportFailure::Unrecorded(Diagnostic(format!("{refusal:?}"))),
        }
    }
}

impl<Translator> Performing for HarnessNexus<Translator>
where
    Session: Emittable<Translator, Outcome = RolloutFinding_Data>,
    ClaimIdentity_Data: Emittable<Translator, Outcome = IdentityClaim_Data>,
    Watch: Emittable<Translator, Outcome = RolloutReading_Data>,
    FilePath: Emittable<Translator, Outcome = RolloutIdentity_Data>,
{
    fn perform<N: Operable>(&self, input: N) -> Result<Performed, ReportFailure> {
        let (performed, attached) = {
            let _serial = self.serial.locked();
            // A discovery attaches a rollout; only that is read again at once.
            let (batch, attached, lane_failure_option) = match self.attempt(input)? {
                Outcome::Reported(batch) | Outcome::Read(batch) => (batch, None, None),
                Outcome::ReportedLaneFailed(reported) => {
                    (reported.event_batch, None, Some(reported.lane_failure))
                }
                Outcome::Discovered(Some(batch)) => {
                    let attached = match &batch.rollout_option {
                        Some(Rollout::Watched(watch)) => Some(watch.clone()),
                        _ => None,
                    };
                    (batch, attached, None)
                }
                Outcome::Discovered(None) => {
                    return Ok(Performed {
                        changed_vector: vec![],
                        lane_failure_option: None,
                    });
                }
            };
            let record = self
                .store
                .recall(batch.session())
                .map_err(ReportFailure::Unrecorded)?;
            let plan = batch.fold(record)?;
            let changes = self.store.commit(plan).map_err(ReportFailure::Unrecorded)?;
            self.publish(changes.clone());
            (
                Performed {
                    changed_vector: changes,
                    lane_failure_option,
                },
                attached,
            )
        };
        // A rollout just attached is read at once, from its start. The
        // attachment needs no lane, so it stays committed when the lane
        // fails the read, which the rollout's next write tries again.
        match attached {
            Some(watch) => match self.perform(Operation::Reading(watch)) {
                Ok(read) => Ok(Performed {
                    changed_vector: [performed.changed_vector, read.changed_vector].concat(),
                    lane_failure_option: performed.lane_failure_option.or(read.lane_failure_option),
                }),
                Err(ReportFailure::LaneFailed(lane_failure)) => Ok(Performed {
                    lane_failure_option: Some(lane_failure),
                    ..performed
                }),
                Err(failure) => Err(failure),
            },
            None => Ok(performed),
        }
    }
}

/// The observations publisher is made from Memory when the capacity is
/// first known, and each committed change is published to it.
impl<Translator> Observing for HarnessNexus<Translator> {
    fn publish(&self, changes: Vec<Changed>) {
        if let Some(publisher) = self.observations.locked().as_ref() {
            for change in changes {
                publisher.publish(change);
            }
        }
    }

    /// Called under the serial lock, so no commit falls between the
    /// snapshot and the publisher made from it.
    fn prepare(&self) -> Result<meta_signal_codex_harness_nexus::Capacity, ReportFailure> {
        let capacity = self
            .store
            .configuration()
            .map_err(ReportFailure::Unrecorded)?
            .capacity_option
            .ok_or(ReportFailure::Unconfigured)?;
        let mut publisher = self.observations.locked();
        if publisher.is_none() {
            let state = Observations(
                self.store
                    .recall_all()
                    .map_err(ReportFailure::Unrecorded)?
                    .into_iter()
                    .map(|record| record.observed)
                    .collect(),
            );
            *publisher = Some(
                Publisher::create(state, Capacity(capacity.0))
                    .map_err(|_| ReportFailure::Unconfigured)?,
            );
        }
        Ok(capacity)
    }
}

impl<Translator> signal_codex_harness_nexus::Answering for HarnessNexus<Translator>
where
    Session: Emittable<Translator, Outcome = RolloutFinding_Data>,
    ClaimIdentity_Data: Emittable<Translator, Outcome = IdentityClaim_Data>,
    Watch: Emittable<Translator, Outcome = RolloutReading_Data>,
    FilePath: Emittable<Translator, Outcome = RolloutIdentity_Data>,
{
    /// The contract answers Reported for `Ok`; every other answer is the
    /// `Err` side, a report recorded though the lane failed where it was
    /// not needed among them.
    fn report(&self, hook: &Hook) -> Result<(), Output> {
        match self.perform(hook.clone()) {
            Ok(Performed {
                lane_failure_option: None,
                ..
            }) => Ok(()),
            Ok(Performed {
                lane_failure_option: Some(lane_failure),
                ..
            }) => Err(Output::ReportedLaneFailed(lane_failure)),
            Err(failure) => Err(Output::ReportRefused(failure)),
        }
    }

    fn subscribe(&self) -> Result<signal::Stream<Observations, Changed>, Output> {
        let _serial = self.serial.locked();
        self.prepare().map_err(Output::ReportRefused)?;
        self.observations
            .locked()
            .as_ref()
            .map(Publishing::open)
            .ok_or(Output::ReportRefused(ReportFailure::Unconfigured))
    }
}

impl<Translator> meta_signal_codex_harness_nexus::Answering for HarnessNexus<Translator> {
    /// Every setting but the text budget is the Nexus's; the text budget is
    /// its translator's own, answered there and refused here.
    fn configure(&self, set: &Set) -> Result<Set, meta_signal_codex_harness_nexus::Output> {
        use meta_signal_codex_harness_nexus::Output as MetaOutput;
        if let Set::TextBudget(_) = set {
            return Err(MetaOutput::ConfigureRefused(
                ConfigurationRefusal::TranslatorSetting,
            ));
        }
        if let Set::SubscriberCapacity(capacity) = set
            && capacity.0 <= 0
        {
            return Err(MetaOutput::ConfigureRefused(
                ConfigurationRefusal::InvalidCapacity(*capacity),
            ));
        }
        if let Some(path) = set
            .paths()
            .into_iter()
            .find(|path| !path.0.starts_with('/'))
        {
            return Err(MetaOutput::ConfigureRefused(
                ConfigurationRefusal::InvalidPath(path),
            ));
        }
        let _serial = self.serial.locked();
        // A capacity takes effect on the open publishers before it is kept,
        // so a capacity they refuse is refused whole and never recorded.
        if let Set::SubscriberCapacity(capacity) = set {
            let refused = |_: CapacityRefusal| {
                MetaOutput::ConfigureRefused(ConfigurationRefusal::InvalidCapacity(*capacity))
            };
            if let Some(publisher) = self.observations.locked().as_mut() {
                publisher.resize(Capacity(capacity.0)).map_err(refused)?;
            }
            if let Some(publisher) = self.configurations.locked().as_mut() {
                publisher.resize(Capacity(capacity.0)).map_err(refused)?;
            }
        }
        // Sessions directories are watched before they are kept: one there
        // that cannot be watched is refused whole and never recorded, and
        // one not yet there is awaited.
        let mut sessions = self.sessions_watch.locked();
        let opened = match set {
            Set::SessionsDirectories(directories) => {
                Some(sessions.opened(directories).map_err(|unwatchable| {
                    MetaOutput::ConfigureRefused(ConfigurationRefusal::UnwatchableDirectory(
                        unwatchable,
                    ))
                })?)
            }
            _ => None,
        };
        if let Err(diagnostic) = self.store.configure(set.clone()) {
            return Err(MetaOutput::ConfigureRefused(
                ConfigurationRefusal::Unrecorded(meta_signal_codex_harness_nexus::Diagnostic(
                    diagnostic.0,
                )),
            ));
        }
        let configurations = self.configurations.locked();
        if let Some(publisher) = configurations.as_ref() {
            publisher.publish(ConfigurationChange::Set(set.clone()));
        }
        if let Some(watch) = opened {
            sessions.replaced(watch);
            if let Some(publisher) = configurations.as_ref() {
                publisher.publish(ConfigurationChange::AwaitedDirectories(sessions.awaited()));
            }
        }
        Ok(set.clone())
    }

    fn subscribe(
        &self,
    ) -> Result<
        signal::Stream<ConfigurationReadout, ConfigurationChange>,
        meta_signal_codex_harness_nexus::Output,
    > {
        use meta_signal_codex_harness_nexus::Output as MetaOutput;
        let _serial = self.serial.locked();
        let configuration = self.store.configuration().map_err(|diagnostic| {
            MetaOutput::ConfigureRefused(ConfigurationRefusal::Unrecorded(
                meta_signal_codex_harness_nexus::Diagnostic(diagnostic.0),
            ))
        })?;
        let capacity = configuration
            .capacity_option
            .ok_or(MetaOutput::ConfigureRefused(
                ConfigurationRefusal::CapacityUnset,
            ))?;
        let sessions = self.sessions_watch.locked();
        let mut publisher = self.configurations.locked();
        if publisher.is_none() {
            let readout = ConfigurationReadout {
                configuration,
                awaited_directories: sessions.awaited(),
            };
            *publisher = Some(
                Publisher::create(readout, Capacity(capacity.0)).map_err(|_| {
                    MetaOutput::ConfigureRefused(ConfigurationRefusal::InvalidCapacity(capacity))
                })?,
            );
        }
        publisher
            .as_ref()
            .map(Publishing::open)
            .ok_or(MetaOutput::ConfigureRefused(
                ConfigurationRefusal::CapacityUnset,
            ))
    }
}

/// The paths a configuration value names, each to be absolute.
impl NamingPaths for Set {
    fn paths(&self) -> Vec<FilePath> {
        match self {
            Set::TextBudget(_) => vec![],
            Set::FlowsRoot(root) => vec![FilePath(root.0.clone())],
            Set::IdentityProducer(producer) => vec![FilePath(producer.0.clone())],
            Set::SessionsDirectories(directories) => directories.0.clone(),
            Set::ThreadDatabases(databases) => databases.0.clone(),
            Set::SubscriberCapacity(_) => vec![],
        }
    }
}
