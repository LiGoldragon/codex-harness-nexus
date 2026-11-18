//! Each emission of the Codex Harness Nexus, carried into the world by its
//! translator process: the identity producer run, the rollout read from its
//! offset to its last whole row, and a rollout's first row read for its
//! session.

use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::process::Command;

use codex_harness_nexus::emission::{
    ClaimIdentity_Data, Emittable, IdentityClaim_Data, RolloutFinding_Data, RolloutIdentity_Data,
    RolloutReading_Data, RolloutReading_Data_Read_Data,
};
use codex_harness_nexus::{Diagnosing as _, Watch};
use flow::FlowId;
use harness::{FilePath, Observable, Session};
use signal_codex_harness_nexus::{
    ByteOffset, Diagnostic, IdentityFailure, Observation, Observation_Malformed_Data, ParseFailure,
    ProducerOutput, Refused_Data,
};

use crate::generated::rollout::{Interpretable, RowText};
use crate::translating::CodexHarnessTranslator;

/// Codex's own thread database is not read yet: a session whose hook names
/// no path waits for its rollout to appear in a sessions directory.
impl Emittable<CodexHarnessTranslator> for Session {
    fn emit(&self, _translator: &CodexHarnessTranslator) -> RolloutFinding_Data {
        RolloutFinding_Data::Unfound
    }
}

impl Emittable<CodexHarnessTranslator> for ClaimIdentity_Data {
    fn emit(&self, _translator: &CodexHarnessTranslator) -> IdentityClaim_Data {
        let output = match Command::new(&self.identity_producer.0)
            .arg("codex")
            .arg("--flows-root")
            .arg(&self.flows_root.0)
            .env("CODEX_SESSION_ID", &self.session.0)
            .output()
        {
            Ok(output) => output,
            Err(error) => {
                return IdentityClaim_Data::Unclaimed(IdentityFailure::NotStarted(
                    error.diagnostic(),
                ));
            }
        };
        if !output.status.success() {
            return IdentityClaim_Data::Unclaimed(IdentityFailure::Refused(Refused_Data {
                exit_status: signal_codex_harness_nexus::ExitStatus(
                    output.status.code().map_or(-1, i64::from),
                ),
                diagnostic: Diagnostic(String::from_utf8_lossy(&output.stderr).into_owned()),
            }));
        }
        let text = match String::from_utf8(output.stdout) {
            Ok(text) => text,
            Err(error) => {
                return IdentityClaim_Data::Unclaimed(IdentityFailure::UnreadableOutput(
                    error.diagnostic(),
                ));
            }
        };
        let identity = text.trim_end_matches(['\r', '\n']);
        if identity.is_empty()
            || !identity
                .chars()
                .all(|character| matches!(character, '0'..='9' | 'a'..='f'))
        {
            return IdentityClaim_Data::Unclaimed(IdentityFailure::MalformedIdentity(
                ProducerOutput(text),
            ));
        }
        IdentityClaim_Data::Claimed(FlowId(identity.to_owned()))
    }
}

/// The observations of every whole row after the offset, and the offset
/// past the last of them; a row not yet ended waits for the next write.
pub trait ReadingRows {
    fn rows(&self, offset: ByteOffset) -> RolloutReading_Data;
}

impl ReadingRows for [u8] {
    fn rows(&self, offset: ByteOffset) -> RolloutReading_Data {
        let whole = self
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |last| last + 1);
        let mut observation_vector = vec![];
        let mut start = 0;
        for line in self[..whole].split_inclusive(|byte| *byte == b'\n') {
            let row_offset = ByteOffset(offset.0 + start as i64);
            start += line.len();
            let row = std::str::from_utf8(&line[..line.len() - 1])
                .map_err(|error| ParseFailure::InvalidJson(error.diagnostic()))
                .and_then(|text| RowText(text.to_owned()).row());
            match row {
                Ok(row) => {
                    observation_vector.extend(row.events().into_iter().map(Observation::Event))
                }
                Err(parse_failure) => {
                    observation_vector.push(Observation::Malformed(Observation_Malformed_Data {
                        byte_offset: row_offset,
                        parse_failure,
                    }))
                }
            }
        }
        RolloutReading_Data::Read(RolloutReading_Data_Read_Data {
            observation_vector,
            byte_offset: ByteOffset(offset.0 + whole as i64),
        })
    }
}

impl Emittable<CodexHarnessTranslator> for Watch {
    fn emit(&self, _translator: &CodexHarnessTranslator) -> RolloutReading_Data {
        let unreadable =
            |error: std::io::Error| RolloutReading_Data::Unreadable(error.diagnostic());
        let Ok(start) = u64::try_from(self.byte_offset.0) else {
            return RolloutReading_Data::Unreadable(Diagnostic(format!(
                "negative offset {}",
                self.byte_offset.0
            )));
        };
        let mut file = match File::open(&self.file_path.0) {
            Ok(file) => file,
            Err(error) => return unreadable(error),
        };
        match file.metadata() {
            Ok(metadata) if metadata.len() < start => {
                return RolloutReading_Data::Unreadable(Diagnostic(format!(
                    "the rollout is {} bytes, shorter than its offset {start}",
                    metadata.len()
                )));
            }
            Ok(_) => {}
            Err(error) => return unreadable(error),
        }
        if let Err(error) = file.seek(SeekFrom::Start(start)) {
            return unreadable(error);
        }
        let mut bytes = vec![];
        if let Err(error) = file.read_to_end(&mut bytes) {
            return unreadable(error);
        }
        bytes.rows(self.byte_offset)
    }
}

/// A rollout's first whole row names its session when it is session_meta.
impl Emittable<CodexHarnessTranslator> for FilePath {
    fn emit(&self, _translator: &CodexHarnessTranslator) -> RolloutIdentity_Data {
        let Ok(file) = File::open(&self.0) else {
            return RolloutIdentity_Data::Unidentified;
        };
        let mut line = String::new();
        match BufReader::new(file).read_line(&mut line) {
            Ok(_) if line.ends_with('\n') => {}
            _ => return RolloutIdentity_Data::Unidentified,
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) else {
            return RolloutIdentity_Data::Unidentified;
        };
        if value.pointer("/type").and_then(serde_json::Value::as_str) != Some("session_meta") {
            return RolloutIdentity_Data::Unidentified;
        }
        value
            .pointer("/payload/id")
            .and_then(serde_json::Value::as_str)
            .map_or(RolloutIdentity_Data::Unidentified, |id| {
                RolloutIdentity_Data::Identified(Session(id.to_owned()))
            })
    }
}
