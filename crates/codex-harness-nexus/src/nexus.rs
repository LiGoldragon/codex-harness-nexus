//! The running Nexus: its Memory, its translator, and the publishers its
//! subscribers follow. Each operation is attempted through the translator's
//! emissions, folded into the session it names, committed whole, and only
//! then published.

use std::sync::{Mutex, MutexGuard};

use harness::{FilePath, Observable, Session};
use meta_signal_codex_harness_nexus::{Configuration, ConfigurationRefusal, Set};
use nexus::{Capacity, Publisher, Publishing};
use signal_codex_harness_nexus::{
    Changed, Diagnostic, Hook, Observation, Observations, Output, ReportFailure,
};

use crate::generated::emission::{
    ClaimIdentity_Data, Emittable, FlowsRoot, IdentityClaim_Data, IdentityProducer,
    RolloutFinding_Data, RolloutIdentity_Data, RolloutReading_Data,
};
use crate::generated::folding::{FoldRefusal, Folding};
use crate::generated::memory::{Rollout, SessionRecord, Watch};
use crate::generated::operation::{EventBatch, Operation, Outcome};
use crate::generated::runtime::{
    Attempting, NamingPaths, Observing, Operable, Performing, ReadyConfiguration, Readying,
    Remembering, Sessional,
};
use crate::store::Store;

/// The Nexus whole. Its translator carries every emission; its publishers
/// exist once the subscriber capacity is configured.
pub struct HarnessNexus<Translator> {
    pub store: Store,
    pub translator: Translator,
    pub serial: Mutex<()>,
    pub observations: Mutex<Option<Publisher<Observations, Changed>>>,
    pub configurations: Mutex<Option<Publisher<Configuration, Set>>>,
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

fn unrecorded(diagnostic: Diagnostic) -> ReportFailure {
    ReportFailure::Unrecorded(diagnostic)
}

fn locked<Value>(mutex: &Mutex<Value>) -> MutexGuard<'_, Value> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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
            .map_err(unrecorded)?
            .ready()
            .ok_or(ReportFailure::Unconfigured)?;
        match input.operation() {
            Operation::Reporting(hook) => {
                let session = hook.session();
                let record = self.store.recall(session.clone()).map_err(unrecorded)?;
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
                    },
                    (None, _) => return Err(ReportFailure::Unregistered(session)),
                };
                let rollout_option = match (&hook, &record) {
                    // A rollout already watched stays as it is watched.
                    (
                        _,
                        Some(SessionRecord {
                            rollout: Rollout::Watched(_),
                            ..
                        }),
                    ) => None,
                    (Hook::SessionStart(start), _) => Some(match &start.file_path_option {
                        Some(path) => Rollout::Pending(path.clone()),
                        None => match session.emit(&self.translator) {
                            RolloutFinding_Data::Found(path) => Rollout::Pending(path),
                            RolloutFinding_Data::Unfound => Rollout::Unlocated,
                        },
                    }),
                    _ => None,
                };
                Ok(Outcome::Reported(EventBatch {
                    session,
                    flow_id,
                    observation_vector: hook.events().into_iter().map(Observation::Event).collect(),
                    rollout_option,
                }))
            }
            Operation::Reading(watch) => {
                let record = self
                    .store
                    .recall_all()
                    .map_err(unrecorded)?
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
                    RolloutReading_Data::Unreadable(diagnostic) => Err(unrecorded(diagnostic)),
                }
            }
            Operation::Discovering(path) => {
                let records = self.store.recall_all().map_err(unrecorded)?;
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
                }
            }
        }
    }
}

fn fold_refused(refusal: FoldRefusal) -> ReportFailure {
    match refusal {
        FoldRefusal::Unregistered(session) => ReportFailure::Unregistered(session),
        refusal => ReportFailure::Unrecorded(Diagnostic(format!("{refusal:?}"))),
    }
}

impl<Translator> Performing for HarnessNexus<Translator>
where
    Session: Emittable<Translator, Outcome = RolloutFinding_Data>,
    ClaimIdentity_Data: Emittable<Translator, Outcome = IdentityClaim_Data>,
    Watch: Emittable<Translator, Outcome = RolloutReading_Data>,
    FilePath: Emittable<Translator, Outcome = RolloutIdentity_Data>,
{
    fn perform<N: Operable>(&self, input: N) -> Result<Vec<Changed>, ReportFailure> {
        let changes = {
            let _serial = locked(&self.serial);
            // A discovery attaches a rollout; only that is read again at once.
            let (batch, attached) = match self.attempt(input)? {
                Outcome::Reported(batch) | Outcome::Read(batch) => (batch, None),
                Outcome::Discovered(Some(batch)) => {
                    let attached = match &batch.rollout_option {
                        Some(Rollout::Watched(watch)) => Some(watch.clone()),
                        _ => None,
                    };
                    (batch, attached)
                }
                Outcome::Discovered(None) => return Ok(vec![]),
            };
            let record = self.store.recall(batch.session()).map_err(unrecorded)?;
            let plan = batch.fold(record).map_err(fold_refused)?;
            let changes = self.store.commit(plan).map_err(unrecorded)?;
            self.publish(changes.clone());
            (changes, attached)
        };
        // A rollout just attached is read at once, from its start.
        match changes {
            (mut changes, Some(watch)) => {
                changes.extend(self.perform(Operation::Reading(watch))?);
                Ok(changes)
            }
            (changes, None) => Ok(changes),
        }
    }
}

/// The observations publisher is made from Memory when the capacity is
/// first known, and each committed change is published to it.
impl<Translator> Observing for HarnessNexus<Translator> {
    fn publish(&self, changes: Vec<Changed>) {
        if let Some(publisher) = locked(&self.observations).as_ref() {
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
            .map_err(unrecorded)?
            .capacity_option
            .ok_or(ReportFailure::Unconfigured)?;
        let mut publisher = locked(&self.observations);
        if publisher.is_none() {
            let state = Observations(
                self.store
                    .recall_all()
                    .map_err(unrecorded)?
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
    fn report(&self, hook: &Hook) -> Result<(), Output> {
        self.perform(hook.clone())
            .map(|_| ())
            .map_err(Output::ReportRefused)
    }

    fn subscribe(&self) -> Result<signal::Stream<Observations, Changed>, Output> {
        let _serial = locked(&self.serial);
        self.prepare().map_err(Output::ReportRefused)?;
        locked(&self.observations)
            .as_ref()
            .map(Publishing::open)
            .ok_or(Output::ReportRefused(ReportFailure::Unconfigured))
    }
}

impl<Translator> meta_signal_codex_harness_nexus::Answering for HarnessNexus<Translator> {
    fn configure(&self, set: &Set) -> Result<Set, meta_signal_codex_harness_nexus::Output> {
        use meta_signal_codex_harness_nexus::Output as MetaOutput;
        let refused = |refusal| Err(MetaOutput::ConfigureRefused(refusal));
        if let Set::SubscriberCapacity(capacity) = set
            && capacity.0 <= 0
        {
            return refused(ConfigurationRefusal::InvalidCapacity(*capacity));
        }
        if let Some(path) = set
            .paths()
            .into_iter()
            .find(|path| !path.0.starts_with('/'))
        {
            return refused(ConfigurationRefusal::InvalidPath(path));
        }
        let _serial = locked(&self.serial);
        if let Err(diagnostic) = self.store.configure(set.clone()) {
            return refused(ConfigurationRefusal::Unrecorded(
                meta_signal_codex_harness_nexus::Diagnostic(diagnostic.0),
            ));
        }
        if let Set::SubscriberCapacity(capacity) = set {
            if let Some(publisher) = locked(&self.observations).as_mut() {
                let _ = publisher.resize(Capacity(capacity.0));
            }
            if let Some(publisher) = locked(&self.configurations).as_mut() {
                let _ = publisher.resize(Capacity(capacity.0));
            }
        }
        if let Some(publisher) = locked(&self.configurations).as_ref() {
            publisher.publish(set.clone());
        }
        Ok(set.clone())
    }

    fn subscribe(
        &self,
    ) -> Result<signal::Stream<Configuration, Set>, meta_signal_codex_harness_nexus::Output> {
        use meta_signal_codex_harness_nexus::Output as MetaOutput;
        let _serial = locked(&self.serial);
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
        let mut publisher = locked(&self.configurations);
        if publisher.is_none() {
            *publisher = Some(
                Publisher::create(configuration, Capacity(capacity.0)).map_err(|_| {
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
            Set::FlowsRoot(root) => vec![FilePath(root.0.clone())],
            Set::IdentityProducer(producer) => vec![FilePath(producer.0.clone())],
            Set::SessionsDirectories(directories) => directories.0.clone(),
            Set::ThreadDatabases(databases) => databases.0.clone(),
            Set::SubscriberCapacity(_) => vec![],
        }
    }
}
