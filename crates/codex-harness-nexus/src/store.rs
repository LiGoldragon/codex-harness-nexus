//! The Nexus's Memory on memory-engine: one family of sessions, one of the
//! malformed rows read past, and the configuration, each batch committed
//! whole or not at all.

use std::path::Path;

use memory_engine::{
    Engine, EngineOpen, EngineRecord, FamilyName, QueryPlan, RecordKey, SchemaHash, SchemaVersion,
    TableDescriptor, TableName, TableReference,
};
use meta_signal_codex_harness_nexus::Configuration;
use signal::Following;
use signal_codex_harness_nexus::{
    Changed, Diagnostic, MemoryFault, MemoryFault_DuplicateSessions_Data, RecordCount,
};

use crate::diagnosing::Diagnosing as _;
use crate::generated::folding::SessionChange;
use crate::generated::memory::{
    Familial, MalformedRecord, SessionRecord, StoreOpening, StorePath, StoredConfiguration,
};
use crate::generated::runtime::{Planned, Remembering, Sessional, Settable};

const SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1);
const SESSIONS: TableName = TableName::new("codex_harness_sessions_v1");
const MALFORMED: TableName = TableName::new("codex_harness_malformed_rows_v1");
const CONFIGURATION: TableName = TableName::new("codex_harness_configuration_v1");
const CONFIGURATION_KEY: &str = "configuration";

impl EngineRecord for SessionRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.session_id.0.clone())
    }
}

/// A malformed row is one place in one rollout: its flow and its offset.
impl EngineRecord for MalformedRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!("{}@{}", self.flow_id.0, self.byte_offset.0))
    }
}

impl EngineRecord for StoredConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(CONFIGURATION_KEY)
    }
}

impl Familial for SessionRecord {
    fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            SESSIONS,
            FamilyName::new("codex-harness-session"),
            SchemaHash::for_label("codex-harness-session-v1"),
        )
    }
}

impl Familial for MalformedRecord {
    fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            MALFORMED,
            FamilyName::new("codex-harness-malformed-row"),
            SchemaHash::for_label("codex-harness-malformed-row-v1"),
        )
    }
}

impl Familial for StoredConfiguration {
    fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            CONFIGURATION,
            FamilyName::new("codex-harness-configuration"),
            SchemaHash::for_label("codex-harness-configuration-v1"),
        )
    }
}

/// The engine and a reference to each family it holds.
pub struct Store {
    engine: Engine,
    sessions: TableReference<SessionRecord>,
    malformed: TableReference<MalformedRecord>,
    configuration: TableReference<StoredConfiguration>,
}

impl StorePath for harness::FilePath {
    fn path(&self) -> harness::FilePath {
        self.clone()
    }
}

/// A store is opened at its path, every family declared in the open.
impl StoreOpening for Store {
    fn open<N: StorePath>(input: N) -> Result<Self, Diagnostic> {
        let file_path = input.path();
        let path = Path::new(&file_path.0);
        if let Some(directory) = path.parent() {
            std::fs::create_dir_all(directory).map_err(|error| error.diagnostic())?;
        }
        let mut engine = Engine::open(
            EngineOpen::new(path, SCHEMA_VERSION)
                .with_family(&SessionRecord::descriptor())
                .with_family(&MalformedRecord::descriptor())
                .with_family(&StoredConfiguration::descriptor()),
        )
        .map_err(|error| error.diagnostic())?;
        let sessions = engine
            .register_table(SessionRecord::descriptor())
            .map_err(|error| error.diagnostic())?;
        let malformed = engine
            .register_table(MalformedRecord::descriptor())
            .map_err(|error| error.diagnostic())?;
        let configuration = engine
            .register_table(StoredConfiguration::descriptor())
            .map_err(|error| error.diagnostic())?;
        Ok(Self {
            engine,
            sessions,
            malformed,
            configuration,
        })
    }
}

impl Remembering for Store {
    fn recall<N: Sessional>(&self, input: N) -> Result<Option<SessionRecord>, MemoryFault> {
        let snapshot = self
            .engine
            .match_records(QueryPlan::key(
                self.sessions,
                RecordKey::new(input.session_id().0),
            ))
            .map_err(|error| MemoryFault::EngineFailed(error.diagnostic()))?;
        match snapshot.records() {
            [] => Ok(None),
            [record] => Ok(Some(record.clone())),
            records => Err(MemoryFault::DuplicateSessions(
                MemoryFault_DuplicateSessions_Data {
                    session_id: input.session_id(),
                    record_count: RecordCount(i64::try_from(records.len()).map_err(|_| {
                        MemoryFault::EngineFailed(Diagnostic(
                            "session record count exceeds the supported range".to_owned(),
                        ))
                    })?),
                },
            )),
        }
    }

    fn recall_all(&self) -> Result<Vec<SessionRecord>, MemoryFault> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.sessions))
            .map_err(|error| MemoryFault::EngineFailed(error.diagnostic()))?
            .records()
            .to_vec())
    }

    fn commit<N: Planned>(&mut self, input: N) -> Result<Vec<Changed>, MemoryFault> {
        let plan = input.plan();
        let commit = self.engine.begin_atomic_commit();
        let mut commit = match plan.session_change {
            SessionChange::Registered(record) => commit.assert(self.sessions, record),
            SessionChange::Updated(record) => commit.mutate(self.sessions, record),
        };
        for record in plan.malformed_record_vector {
            commit = commit.assert(self.malformed, record);
        }
        self.engine
            .commit_atomic(commit)
            .map_err(|error| MemoryFault::EngineFailed(error.diagnostic()))?;
        Ok(plan.changed_vector)
    }

    fn configuration(&self) -> Result<Configuration, MemoryFault> {
        let snapshot = self
            .engine
            .match_records(QueryPlan::all(self.configuration))
            .map_err(|error| MemoryFault::EngineFailed(error.diagnostic()))?;
        match snapshot.records() {
            [] => Ok(Configuration {
                text_budget_option: None,
                flows_root_option: None,
                identity_producer_option: None,
                sessions_directories_option: None,
                thread_databases_option: None,
                capacity_option: None,
            }),
            [stored] => Ok(stored.configuration.clone()),
            records => Err(MemoryFault::DuplicateConfigurations(RecordCount(
                i64::try_from(records.len()).map_err(|_| {
                    MemoryFault::EngineFailed(Diagnostic(
                        "configuration record count exceeds the supported range".to_owned(),
                    ))
                })?,
            ))),
        }
    }

    fn configure<N: Settable>(&mut self, input: N) -> Result<Configuration, MemoryFault> {
        let present = !self
            .engine
            .match_records(QueryPlan::all(self.configuration))
            .map_err(|error| MemoryFault::EngineFailed(error.diagnostic()))?
            .records()
            .is_empty();
        let configuration = Configuration::after(self.configuration()?, input.setting());
        let stored = StoredConfiguration {
            configuration: configuration.clone(),
        };
        let commit = self.engine.begin_atomic_commit();
        let commit = if present {
            commit.mutate(self.configuration, stored)
        } else {
            commit.assert(self.configuration, stored)
        };
        self.engine
            .commit_atomic(commit)
            .map_err(|error| MemoryFault::EngineFailed(error.diagnostic()))?;
        Ok(configuration)
    }
}
