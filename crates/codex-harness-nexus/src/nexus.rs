//! The serialized core actor: its Memory, translator, watch, readiness and
//! subscribers are one state, changed only by typed messages.

use std::convert::Infallible;
use std::fmt::Debug;
use std::path::Path;

use harness::{FilePath, Observable, SessionId};
use kameo::actor::ActorRef;
use kameo::message::{Context, Message};
use meta_signal_codex_harness_nexus as meta;
use meta_signal_codex_harness_nexus::{
    Configuration, ConfigurationChange, ConfigurationReadout, ConfigurationRefusal, Set,
};
use nexus::{Capacity, CapacityRefusal, Publisher, Publishing};
use signal::Following;
use signal_codex_harness_nexus::{
    Changed, Diagnostic, FoldRefusal, Hook, Observation, Observations, Output, ReportFailure,
    StaleReading_Data,
};

use crate::generated::actor::{
    ActorFailure, ConfigurationReading, ConfigurationSetting, ConfigureRequest, FollowWatchRequest,
    InitializeRequest, ListenerFailure, NexusControlFailure, NexusPort, NexusRuntime,
    OpenPublicRequest, Opened, PerformRequest, PublicOpening, ReadConfigurationRequest,
    ReportRequest, Reportable, SessionsDirectoriesSetting, SessionsWatching, Settling,
    SubscribeConfigurationRequest, SubscribeObservationsRequest, WatchConsequence, WatchFailure,
    WatchPlanBuilding, WatchSessionsRequest, WatchTarget, WatchWord, WatchWordInput,
};
use crate::generated::emission::{
    ClaimIdentity_Data, Emittable, FlowsRoot, IdentityClaim_Data, IdentityProducer,
    RolloutFinding_Data, RolloutIdentity_Data, RolloutReading_Data,
};
use crate::generated::folding::Folding;
use crate::generated::memory::{Rollout, SessionRecord, Watch};
use crate::generated::operation::{EventBatch, Operation, Outcome, ReportedLaneFailed_Data};
use crate::generated::runtime::{
    Attempting, NamingPaths, Observing, Operable, Performed, Performing, ReadyConfiguration,
    Readying, Remembering, Sessional, Settable, Waking,
};
use crate::store::Store;
use crate::watching::SessionsWatch;

/// The native Kameo actor state. All mutable core state belongs to this
/// single actor and its generated `NexusRuntime` capabilities.
pub struct CoreActor<Translator, Opening> {
    pub store: Store,
    pub translator: Translator,
    pub observations: Option<Publisher<Observations, Changed>>,
    pub configurations: Option<Publisher<ConfigurationReadout, ConfigurationChange>>,
    pub sessions_watch: SessionsWatch,
    pub opened: Opened,
    pub public_opening: Opening,
}

/// An orphan-safe contact wrapper around the actor's native handle.
pub struct HarnessNexus<Translator, Opening> {
    pub actor: ActorRef<CoreActor<Translator, Opening>>,
}

impl<Translator, Opening> Clone for HarnessNexus<Translator, Opening> {
    fn clone(&self) -> Self {
        Self {
            actor: self.actor.clone(),
        }
    }
}

impl<M> From<kameo::error::SendError<M, Infallible>> for ActorFailure
where
    M: Debug,
{
    fn from(failure: kameo::error::SendError<M, Infallible>) -> Self {
        Self {
            diagnostic: Diagnostic(format!("core actor request failed: {failure:?}")),
        }
    }
}

impl Readying for Configuration {
    fn ready(&self) -> Option<ReadyConfiguration> {
        Some(ReadyConfiguration {
            flows_root: self.flows_root_option.clone()?,
            identity_producer: self.identity_producer_option.clone()?,
            sessions_directories: self.sessions_directories_option.clone()?,
            capacity: self.capacity_option?,
        })
    }
}

impl<Translator, Opening> ConfigurationReading for CoreActor<Translator, Opening> {
    fn readout(&self) -> Result<ConfigurationReadout, signal_codex_harness_nexus::MemoryFault> {
        Ok(ConfigurationReadout {
            configuration: self.store.configuration()?,
            awaited_directories: self.sessions_watch.awaited(),
            unwatchable_directories: self.sessions_watch.unwatchable(),
        })
    }
}

impl<Translator, Opening> Attempting for CoreActor<Translator, Opening>
where
    SessionId: Emittable<Translator, Outcome = RolloutFinding_Data>,
    ClaimIdentity_Data: Emittable<Translator, Outcome = IdentityClaim_Data>,
    Watch: Emittable<Translator, Outcome = RolloutReading_Data>,
    FilePath: Emittable<Translator, Outcome = RolloutIdentity_Data>,
{
    fn attempt<N: Operable>(&mut self, input: N) -> Result<Outcome, ReportFailure> {
        let ready = self
            .store
            .configuration()
            .map_err(ReportFailure::Unrecorded)?
            .ready()
            .ok_or(ReportFailure::Unconfigured)?;
        match input.operation() {
            Operation::Reporting(hook) => {
                let session_id = hook.session_id();
                let record = self
                    .store
                    .recall(session_id.clone())
                    .map_err(ReportFailure::Unrecorded)?;
                let flow_id = match (&record, &hook) {
                    (Some(record), _) => record.observed.flow_id.clone(),
                    (None, Hook::SessionStart(_)) => match (ClaimIdentity_Data {
                        session_id: session_id.clone(),
                        flows_root: FlowsRoot(ready.flows_root.0.clone()),
                        identity_producer: IdentityProducer(ready.identity_producer.0.clone()),
                    })
                    .emit(&self.translator)
                    {
                        IdentityClaim_Data::Claimed(flow_id) => flow_id,
                        IdentityClaim_Data::Unclaimed(failure) => {
                            return Err(ReportFailure::Unidentified(failure));
                        }
                        IdentityClaim_Data::LaneFailed(lane_failure) => {
                            return Err(ReportFailure::LaneFailed(lane_failure));
                        }
                    },
                    (None, _) => return Err(ReportFailure::Unregistered(session_id)),
                };
                let (rollout_option, lane_failure_option) = match (&hook, &record) {
                    (
                        _,
                        Some(SessionRecord {
                            rollout: Rollout::Watched(_),
                            ..
                        }),
                    ) => (None, None),
                    (
                        Hook::SessionStart(start),
                        Some(SessionRecord {
                            rollout: Rollout::Pending(_),
                            ..
                        }),
                    ) if start.file_path_option.is_none() => (None, None),
                    (Hook::SessionStart(start), _) => match &start.file_path_option {
                        Some(path) => (Some(Rollout::Pending(path.clone())), None),
                        None => match session_id.emit(&self.translator) {
                            RolloutFinding_Data::Found(path) => {
                                (Some(Rollout::Pending(path)), None)
                            }
                            RolloutFinding_Data::Unfound => (Some(Rollout::Unlocated), None),
                            RolloutFinding_Data::LaneFailed(lane_failure) => {
                                (None, Some(lane_failure))
                            }
                        },
                    },
                    _ => (None, None),
                };
                let event_batch = EventBatch {
                    session_id,
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
                    .ok_or_else(|| ReportFailure::UnknownFlow(watch.flow_id.clone()))?;
                let current_offset = match &record.rollout {
                    Rollout::Watched(current) if current.file_path == watch.file_path => {
                        Some(current.byte_offset)
                    }
                    _ => None,
                };
                if current_offset != Some(watch.byte_offset) {
                    return Err(FoldRefusal::StaleReading(StaleReading_Data {
                        byte_offset: watch.byte_offset,
                        byte_offset_option: current_offset,
                    })
                    .into());
                }
                match watch.emit(&self.translator) {
                    RolloutReading_Data::Read(read) => Ok(Outcome::Read(EventBatch {
                        session_id: record.session_id,
                        flow_id: watch.flow_id.clone(),
                        observation_vector: read.observation_vector,
                        rollout_option: Some(Rollout::Watched(Watch {
                            flow_id: watch.flow_id,
                            file_path: watch.file_path,
                            byte_offset: read.byte_offset,
                        })),
                    })),
                    RolloutReading_Data::Unreadable(diagnostic) => Err(ReportFailure::Unreadable(
                        signal_codex_harness_nexus::RolloutFailure::Unread(diagnostic),
                    )),
                    RolloutReading_Data::LaneFailed(lane_failure) => {
                        Err(ReportFailure::LaneFailed(lane_failure))
                    }
                }
            }
            Operation::Discovering(path) => {
                let records = self.store.recall_all().map_err(ReportFailure::Unrecorded)?;
                let attached = |record: SessionRecord| {
                    Outcome::Discovered(Some(EventBatch {
                        session_id: record.session_id,
                        flow_id: record.observed.flow_id.clone(),
                        observation_vector: vec![],
                        rollout_option: Some(Rollout::Watched(Watch {
                            flow_id: record.observed.flow_id,
                            file_path: path.clone(),
                            byte_offset: signal_codex_harness_nexus::ByteOffset(0),
                        })),
                    }))
                };
                if let Some(record) = records
                    .iter()
                    .find(|record| record.rollout == Rollout::Pending(path.clone()))
                {
                    return Ok(attached(record.clone()));
                }
                match path.emit(&self.translator) {
                    RolloutIdentity_Data::Identified(session_id) => Ok(records
                        .into_iter()
                        .find(|record| {
                            record.session_id == session_id && record.rollout == Rollout::Unlocated
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

impl<Translator, Opening> Performing for CoreActor<Translator, Opening>
where
    SessionId: Emittable<Translator, Outcome = RolloutFinding_Data>,
    ClaimIdentity_Data: Emittable<Translator, Outcome = IdentityClaim_Data>,
    Watch: Emittable<Translator, Outcome = RolloutReading_Data>,
    FilePath: Emittable<Translator, Outcome = RolloutIdentity_Data>,
{
    fn perform<N: Operable>(&mut self, input: N) -> Result<Performed, ReportFailure> {
        let explicit_path = match input.operation() {
            Operation::Reporting(Hook::SessionStart(start)) => start.file_path_option.clone(),
            _ => None,
        };
        let (performed, attached) = {
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
                .recall(batch.session_id())
                .map_err(ReportFailure::Unrecorded)?;
            let plan = batch.fold(record)?;
            let changes = self.store.commit(plan).map_err(ReportFailure::Unrecorded)?;
            self.publish(changes.clone());
            let configuration = self
                .store
                .configuration()
                .map_err(ReportFailure::Unrecorded)?;
            let records = self.store.recall_all().map_err(ReportFailure::Unrecorded)?;
            let watch_plan = self
                .sessions_watch
                .planned(configuration.sessions_directories_option, records);
            let failures = self.sessions_watch.retarget(watch_plan);
            for failure in failures {
                eprintln!(
                    "Codex Harness Nexus rollout watch {:?} at {:?}: {}",
                    failure.watch_target_option, failure.file_path_option, failure.diagnostic.0
                );
            }
            (
                Performed {
                    changed_vector: changes,
                    lane_failure_option,
                },
                attached,
            )
        };
        let mut performed = performed;
        if let Some(path) = explicit_path.filter(|path| Path::new(&path.0).exists()) {
            match self.perform(Operation::Discovering(path)) {
                Ok(discovered) => {
                    performed.changed_vector.extend(discovered.changed_vector);
                    performed.lane_failure_option = performed
                        .lane_failure_option
                        .or(discovered.lane_failure_option);
                }
                Err(failure) => return Err(failure),
            }
        } else if let Some(watch) = attached {
            match self.perform(Operation::Reading(watch)) {
                Ok(read) => {
                    performed.changed_vector.extend(read.changed_vector);
                    performed.lane_failure_option =
                        performed.lane_failure_option.or(read.lane_failure_option);
                }
                Err(failure) => return Err(failure),
            }
        }
        Ok(performed)
    }
}

impl<Translator, Opening> Observing for CoreActor<Translator, Opening> {
    fn publish(&mut self, changes: Vec<Changed>) {
        if let Some(publisher) = self.observations.as_ref() {
            for change in changes {
                publisher.publish(change);
            }
        }
    }

    fn prepare(&mut self) -> Result<meta::Capacity, ReportFailure> {
        let capacity = self
            .store
            .configuration()
            .map_err(ReportFailure::Unrecorded)?
            .capacity_option
            .ok_or(ReportFailure::Unconfigured)?;
        if self.observations.is_none() {
            let state = Observations(
                self.store
                    .recall_all()
                    .map_err(ReportFailure::Unrecorded)?
                    .into_iter()
                    .map(|record| record.observed)
                    .collect(),
            );
            self.observations =
                Some(Publisher::create(state, Capacity(capacity.0)).map_err(|_| {
                    ReportFailure::PublisherRefused(signal_codex_harness_nexus::SubscriberCapacity(
                        capacity.0,
                    ))
                })?);
        }
        Ok(capacity)
    }
}

impl<Translator, Opening> Waking for CoreActor<Translator, Opening> {
    fn startup(&mut self) -> Result<Vec<Operation>, ReportFailure> {
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

    fn woken(&mut self, written: Vec<FilePath>) -> Result<Vec<Operation>, ReportFailure> {
        let records = self.store.recall_all().map_err(ReportFailure::Unrecorded)?;
        let mut operations = Vec::new();
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

impl<Translator, Opening> NexusRuntime for CoreActor<Translator, Opening>
where
    Opening: PublicOpening,
    SessionId: Emittable<Translator, Outcome = RolloutFinding_Data>,
    ClaimIdentity_Data: Emittable<Translator, Outcome = IdentityClaim_Data>,
    Watch: Emittable<Translator, Outcome = RolloutReading_Data>,
    FilePath: Emittable<Translator, Outcome = RolloutIdentity_Data>,
{
    fn read_configuration(
        &self,
    ) -> Result<ConfigurationReadout, signal_codex_harness_nexus::MemoryFault> {
        ConfigurationReading::readout(self)
    }

    fn configure<N: ConfigurationSetting>(
        &mut self,
        input: N,
    ) -> Result<Configuration, ConfigurationRefusal> {
        let set = input.setting();
        if matches!(set, Set::TextBudget(_)) {
            return Err(ConfigurationRefusal::TranslatorSetting);
        }
        if let Set::SubscriberCapacity(capacity) = &set
            && capacity.0 <= 0
        {
            return Err(ConfigurationRefusal::InvalidCapacity(*capacity));
        }
        if let Some(path) = set
            .paths()
            .into_iter()
            .find(|path| !path.0.starts_with('/'))
        {
            return Err(ConfigurationRefusal::InvalidPath(path));
        }
        let opened_watch = match &set {
            Set::SessionsDirectories(directories) => {
                let records = self
                    .store
                    .recall_all()
                    .map_err(ConfigurationRefusal::Unrecorded)?;
                let plan = self
                    .sessions_watch
                    .planned(Some(directories.clone()), records);
                Some(
                    self.sessions_watch
                        .opened(plan)
                        .map_err(ConfigurationRefusal::UnwatchableDirectory)?,
                )
            }
            _ => None,
        };
        if let Set::SubscriberCapacity(capacity) = &set {
            let refused = |_| ConfigurationRefusal::InvalidCapacity(*capacity);
            if let Some(publisher) = self.observations.as_mut() {
                publisher.resize(Capacity(capacity.0)).map_err(refused)?;
            }
            if let Some(publisher) = self.configurations.as_mut() {
                publisher.resize(Capacity(capacity.0)).map_err(refused)?;
            }
        }
        self.store
            .configure(set.clone())
            .map_err(ConfigurationRefusal::Unrecorded)?;
        if let Some(watch) = opened_watch {
            self.sessions_watch.replaced(watch);
            self.sessions_watch.unwatchable.clear();
        }
        if let Some(publisher) = self.configurations.as_ref() {
            publisher.publish(ConfigurationChange::Set(set.clone()));
            if matches!(set, Set::SessionsDirectories(_)) {
                publisher.publish(ConfigurationChange::AwaitedDirectories(
                    self.sessions_watch.awaited(),
                ));
                publisher.publish(ConfigurationChange::UnwatchableDirectories(
                    self.sessions_watch.unwatchable(),
                ));
            }
        }
        Ok(self
            .store
            .configuration()
            .map_err(ConfigurationRefusal::Unrecorded)?)
    }

    fn watch_sessions<N: SessionsDirectoriesSetting>(
        &mut self,
        input: N,
    ) -> Result<Opened, NexusControlFailure> {
        let directories = input.sessions_directories();
        let records = self
            .store
            .recall_all()
            .map_err(NexusControlFailure::MemoryFault)?;
        let plan = self.sessions_watch.planned(Some(directories), records);
        match self.sessions_watch.opened(plan) {
            Ok(watch) => {
                self.sessions_watch.replaced(watch);
                self.sessions_watch.unwatchable.clear();
                Ok(Opened(true))
            }
            Err(unwatchable) => {
                self.sessions_watch.unwatchable = vec![unwatchable.clone()];
                Err(NexusControlFailure::UnwatchableDirectory(unwatchable))
            }
        }
    }

    fn follow_watch(&mut self, word: WatchWord) -> Vec<WatchConsequence> {
        if !self
            .sessions_watch
            .current
            .as_ref()
            .is_some_and(|watch| watch.generation == word.watch_generation)
        {
            return vec![];
        }
        let event = match word.watch_result {
            crate::generated::actor::WatchResult::WatchEvent(event) => event,
            crate::generated::actor::WatchResult::WatchFailure(failure) => {
                return vec![WatchConsequence::WatchFailure(failure)];
            }
        };
        let before_awaited = self.sessions_watch.awaited();
        let before_unwatchable = self.sessions_watch.unwatchable();
        let (unplaced, written, failures, after_awaited) = {
            let Some(watch) = self.sessions_watch.current.as_mut() else {
                return vec![];
            };
            watch.forget(event.clone());
            let unplaced = watch.awaited();
            let written = watch.written(event);
            let failures = watch.settle();
            let after_awaited = watch.awaited();
            (unplaced, written, failures, after_awaited)
        };
        let after_unwatchable = self.sessions_watch.unwatchable();
        if let Some(publisher) = self.configurations.as_ref() {
            if before_awaited != after_awaited {
                publisher.publish(ConfigurationChange::AwaitedDirectories(
                    after_awaited.clone(),
                ));
            }
            if before_unwatchable != after_unwatchable {
                publisher.publish(ConfigurationChange::UnwatchableDirectories(
                    after_unwatchable,
                ));
            }
        }
        let arrived: Vec<FilePath> = unplaced
            .0
            .into_iter()
            .filter(|directory| !after_awaited.0.contains(directory))
            .collect();
        let mut consequences: Vec<WatchConsequence> = failures
            .into_iter()
            .map(WatchConsequence::WatchFailure)
            .collect();
        if !arrived.is_empty() {
            consequences.push(WatchConsequence::Arrived(arrived.clone()));
        }
        if !written.is_empty() {
            consequences.push(WatchConsequence::Written(written.clone()));
        }
        if self.opened.0 {
            let operations = if !arrived.is_empty() {
                self.startup()
            } else {
                self.woken(written)
            };
            match operations {
                Ok(operations) => {
                    for operation in operations {
                        if let Err(failure) = self.perform(operation) {
                            eprintln!("Codex Harness Nexus operation refused: {failure:?}");
                        }
                    }
                }
                Err(failure) => {
                    eprintln!("Codex Harness Nexus could not look: {failure:?}");
                }
            }
        }
        consequences
    }

    fn open_public(&mut self) -> Result<Opened, NexusControlFailure> {
        if self.opened.0 {
            return Ok(self.opened);
        }
        let configuration = self
            .store
            .configuration()
            .map_err(NexusControlFailure::MemoryFault)?;
        if configuration.ready().is_none() {
            return Ok(Opened(false));
        }
        let opened = self
            .public_opening
            .open_public()
            .map_err(NexusControlFailure::ListenerFailure)?;
        self.opened = opened;
        if self.opened.0 {
            match self.startup() {
                Ok(operations) => {
                    for operation in operations {
                        if let Err(failure) = self.perform(operation) {
                            eprintln!("Codex Harness Nexus operation refused: {failure:?}");
                        }
                    }
                }
                Err(failure) => eprintln!("Codex Harness Nexus could not look: {failure:?}"),
            }
        }
        Ok(self.opened)
    }

    fn initialize(&mut self) -> Result<Opened, NexusControlFailure> {
        let configuration = self
            .store
            .configuration()
            .map_err(NexusControlFailure::MemoryFault)?;
        if let Some(directories) = configuration.sessions_directories_option.clone() {
            self.watch_sessions(directories)
                .map_err(NexusControlFailure::UnwatchableDirectory)?;
        }
        self.open_public()
    }

    fn perform<N: Operable>(&mut self, input: N) -> Result<Performed, ReportFailure> {
        Performing::perform(self, input)
    }

    fn report<N: Reportable>(&mut self, input: N) -> Result<Performed, ReportFailure> {
        Performing::perform(self, input.hook())
    }

    fn subscribe_observations(
        &mut self,
    ) -> Result<signal::Stream<Observations, Changed>, ReportFailure> {
        self.prepare()?;
        self.observations
            .as_ref()
            .map(Publishing::open)
            .ok_or(ReportFailure::Unconfigured)
    }

    fn subscribe_configuration(
        &mut self,
    ) -> Result<signal::Stream<ConfigurationReadout, ConfigurationChange>, ConfigurationRefusal>
    {
        let configuration = self
            .store
            .configuration()
            .map_err(ConfigurationRefusal::Unrecorded)?;
        let capacity = configuration
            .capacity_option
            .ok_or(ConfigurationRefusal::CapacityUnset)?;
        if self.configurations.is_none() {
            let readout = ConfigurationReadout {
                configuration,
                awaited_directories: self.sessions_watch.awaited(),
                unwatchable_directories: self.sessions_watch.unwatchable(),
            };
            self.configurations = Some(
                Publisher::create(readout, Capacity(capacity.0))
                    .map_err(|_| ConfigurationRefusal::InvalidCapacity(capacity))?,
            );
        }
        self.configurations
            .as_ref()
            .map(Publishing::open)
            .ok_or(ConfigurationRefusal::CapacityUnset)
    }
}

impl<Translator, Opening> kameo::Actor for CoreActor<Translator, Opening>
where
    Translator: Send + 'static,
    Opening: Send + 'static,
{
    type Args = Self;
    type Error = Infallible;

    async fn on_start(args: Self::Args, _actor: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(args)
    }
}

macro_rules! actor_message {
    ($message:ty, $reply:ty, $body:block) => {
        impl<Translator, Opening> Message<$message> for CoreActor<Translator, Opening>
        where
            Translator: Send + 'static,
            Opening: PublicOpening + Send + Sync + 'static,
            SessionId: Emittable<Translator, Outcome = RolloutFinding_Data>,
            ClaimIdentity_Data: Emittable<Translator, Outcome = IdentityClaim_Data>,
            Watch: Emittable<Translator, Outcome = RolloutReading_Data>,
            FilePath: Emittable<Translator, Outcome = RolloutIdentity_Data>,
        {
            type Reply = $reply;

            async fn handle(
                &mut self,
                message: $message,
                _context: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                $body
            }
        }
    };
}

actor_message!(ReadConfigurationRequest, Result<ConfigurationReadout, signal_codex_harness_nexus::MemoryFault>, { NexusRuntime::read_configuration(self) });
actor_message!(ConfigureRequest, Result<Configuration, ConfigurationRefusal>, { NexusRuntime::configure(self, message.set) });
actor_message!(WatchSessionsRequest, Result<Opened, NexusControlFailure>, { NexusRuntime::watch_sessions(self, message.sessions_directories) });
actor_message!(FollowWatchRequest, Vec<WatchConsequence>, {
    NexusRuntime::follow_watch(self, message.watch_word)
});
actor_message!(OpenPublicRequest, Result<Opened, NexusControlFailure>, { NexusRuntime::open_public(self) });
actor_message!(InitializeRequest, Result<Opened, NexusControlFailure>, { NexusRuntime::initialize(self) });
actor_message!(PerformRequest, Result<Performed, ReportFailure>, { NexusRuntime::perform(self, message.operation) });
actor_message!(ReportRequest, Result<Performed, ReportFailure>, { NexusRuntime::report(self, message.hook) });
actor_message!(
    SubscribeObservationsRequest,
    Result<signal::Stream<Observations, Changed>, ReportFailure>,
    { NexusRuntime::subscribe_observations(self) }
);
actor_message!(
    SubscribeConfigurationRequest,
    Result<signal::Stream<ConfigurationReadout, ConfigurationChange>, ConfigurationRefusal>,
    { NexusRuntime::subscribe_configuration(self) }
);

impl<Translator, Opening> NexusPort for HarnessNexus<Translator, Opening>
where
    Translator: Send + 'static,
    Opening: PublicOpening + Send + Sync + 'static,
    SessionId: Emittable<Translator, Outcome = RolloutFinding_Data>,
    ClaimIdentity_Data: Emittable<Translator, Outcome = IdentityClaim_Data>,
    Watch: Emittable<Translator, Outcome = RolloutReading_Data>,
    FilePath: Emittable<Translator, Outcome = RolloutIdentity_Data>,
{
    fn read_configuration(&self) -> Result<ConfigurationReadout, ConfigurationRefusal> {
        self.actor
            .ask(ReadConfigurationRequest {})
            .blocking_send()
            .map_err(|failure| {
                ConfigurationRefusal::ActorUnavailable(meta::Diagnostic(format!("{failure:?}")))
            })?
            .map_err(ConfigurationRefusal::Unrecorded)
    }

    fn configure<N: ConfigurationSetting>(
        &self,
        input: N,
    ) -> Result<Configuration, ConfigurationRefusal> {
        self.actor
            .ask(ConfigureRequest {
                set: input.setting(),
            })
            .blocking_send()
            .map_err(|failure| {
                ConfigurationRefusal::ActorUnavailable(meta::Diagnostic(format!("{failure:?}")))
            })?
    }

    fn watch_sessions<N: SessionsDirectoriesSetting>(
        &self,
        input: N,
    ) -> Result<Opened, NexusControlFailure> {
        self.actor
            .ask(WatchSessionsRequest {
                sessions_directories: input.sessions_directories(),
            })
            .blocking_send()
            .map_err(ActorFailure::from)?
    }

    fn follow_watch<N: WatchWordInput>(
        &self,
        input: N,
    ) -> Result<Vec<WatchConsequence>, ActorFailure> {
        self.actor
            .ask(FollowWatchRequest {
                watch_word: input.watch_word(),
            })
            .blocking_send()
            .map_err(ActorFailure::from)
    }

    fn open_public(&self) -> Result<Opened, NexusControlFailure> {
        self.actor
            .ask(OpenPublicRequest {})
            .blocking_send()
            .map_err(ActorFailure::from)?
    }

    fn initialize(&self) -> Result<Opened, NexusControlFailure> {
        self.actor
            .ask(InitializeRequest {})
            .blocking_send()
            .map_err(ActorFailure::from)?
    }

    fn perform<N: Operable>(&self, input: N) -> Result<Performed, ReportFailure> {
        self.actor
            .ask(PerformRequest {
                operation: input.operation(),
            })
            .blocking_send()
            .map_err(|failure| {
                ReportFailure::ActorUnavailable(Diagnostic(format!("{failure:?}")))
            })?
    }

    fn report<N: Reportable>(&self, input: N) -> Result<Performed, ReportFailure> {
        self.actor
            .ask(ReportRequest { hook: input.hook() })
            .blocking_send()
            .map_err(|failure| {
                ReportFailure::ActorUnavailable(Diagnostic(format!("{failure:?}")))
            })?
    }

    fn subscribe_observations(
        &self,
    ) -> Result<signal::Stream<Observations, Changed>, ReportFailure> {
        self.actor
            .ask(SubscribeObservationsRequest {})
            .blocking_send()
            .map_err(|failure| {
                ReportFailure::ActorUnavailable(Diagnostic(format!("{failure:?}")))
            })?
    }

    fn subscribe_configuration(
        &self,
    ) -> Result<signal::Stream<ConfigurationReadout, ConfigurationChange>, ConfigurationRefusal>
    {
        self.actor
            .ask(SubscribeConfigurationRequest {})
            .blocking_send()
            .map_err(|failure| {
                ConfigurationRefusal::ActorUnavailable(meta::Diagnostic(format!("{failure:?}")))
            })?
    }
}

impl<Translator, Opening> signal_codex_harness_nexus::Answering
    for HarnessNexus<Translator, Opening>
where
    Translator: Send + 'static,
    Opening: PublicOpening + Send + Sync + 'static,
    SessionId: Emittable<Translator, Outcome = RolloutFinding_Data>,
    ClaimIdentity_Data: Emittable<Translator, Outcome = IdentityClaim_Data>,
    Watch: Emittable<Translator, Outcome = RolloutReading_Data>,
    FilePath: Emittable<Translator, Outcome = RolloutIdentity_Data>,
{
    fn report(&self, hook: &Hook) -> Result<(), Output> {
        match NexusPort::report(self, hook) {
            Ok(Performed {
                lane_failure_option: None,
                ..
            }) => Ok(()),
            Ok(Performed {
                lane_failure_option: Some(lane_failure),
                ..
            }) => Err(Output::ReportedLaneFailed(lane_failure)),
            Err(ReportFailure::Unreadable(failure)) => Err(Output::ReportedUnread(failure)),
            Err(failure) => Err(Output::ReportRefused(failure)),
        }
    }

    fn subscribe(&self) -> Result<signal::Stream<Observations, Changed>, Output> {
        NexusPort::subscribe_observations(self).map_err(Output::ReportRefused)
    }
}

impl<Translator, Opening> meta_signal_codex_harness_nexus::Answering
    for HarnessNexus<Translator, Opening>
where
    Translator: Send + 'static,
    Opening: PublicOpening + Send + Sync + 'static,
    SessionId: Emittable<Translator, Outcome = RolloutFinding_Data>,
    ClaimIdentity_Data: Emittable<Translator, Outcome = IdentityClaim_Data>,
    Watch: Emittable<Translator, Outcome = RolloutReading_Data>,
    FilePath: Emittable<Translator, Outcome = RolloutIdentity_Data>,
{
    fn configure(&self, set: &Set) -> Result<Set, meta::Output> {
        use meta::Output as MetaOutput;
        if matches!(set, Set::TextBudget(_)) {
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
        NexusPort::configure(self, set.clone())
            .map(|_| set.clone())
            .map_err(MetaOutput::ConfigureRefused)
    }

    fn subscribe(
        &self,
    ) -> Result<signal::Stream<ConfigurationReadout, ConfigurationChange>, meta::Output> {
        NexusPort::subscribe_configuration(self).map_err(meta::Output::ConfigureRefused)
    }
}

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
