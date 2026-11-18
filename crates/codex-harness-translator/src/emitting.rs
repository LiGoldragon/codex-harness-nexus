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
use harness::{FilePath, Observable, SessionId};
use signal_codex_harness_nexus::{
    ByteOffset, Diagnostic, IdentityFailure, Observation, Observation_Malformed_Data, ParseFailure,
    ProducerOutput, Refused_Data,
};

use crate::generated::rollout::{
    ByteOffsetInput, Interpretable, ReadingRows, RolloutByte, RolloutBytes, RolloutLength,
    RolloutReadFailure, RolloutShorterThanOffset_Data, RowText,
};
use crate::translating::CodexHarnessTranslator;

/// Codex's own thread database is not read yet: a session whose hook names
/// no path waits for its rollout to appear in a sessions directory.
impl Emittable<CodexHarnessTranslator> for SessionId {
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
            .env("CODEX_SESSION_ID", &self.session_id.0)
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
impl ByteOffsetInput for ByteOffset {
    fn byte_offset(&self) -> ByteOffset {
        *self
    }
}

impl ReadingRows for RolloutBytes {
    fn rows<N: ByteOffsetInput>(&self, input: N) -> RolloutReading_Data {
        let offset = input.byte_offset();
        let bytes = self
            .0
            .iter()
            .map(|byte| u8::try_from(byte.0))
            .collect::<Result<Vec<_>, _>>();
        let Ok(bytes) = bytes else {
            return RolloutReading_Data::Unreadable(Diagnostic(format!(
                "{:?}",
                RolloutReadFailure::InvalidByteValue
            )));
        };
        if offset.0 < 0 {
            return RolloutReading_Data::Unreadable(Diagnostic(format!(
                "{:?}",
                RolloutReadFailure::NegativeOffset(offset)
            )));
        }
        let whole = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |last| last + 1);
        let mut observation_vector = vec![];
        let mut start = 0;
        for line in bytes[..whole].split_inclusive(|byte| *byte == b'\n') {
            let row_offset = match i64::try_from(start)
                .ok()
                .and_then(|delta| offset.0.checked_add(delta))
            {
                Some(row_offset) => ByteOffset(row_offset),
                None => {
                    return RolloutReading_Data::Unreadable(Diagnostic(format!(
                        "{:?}",
                        RolloutReadFailure::OffsetOverflow
                    )));
                }
            };
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
        let Some(byte_offset) = i64::try_from(whole)
            .ok()
            .and_then(|length| offset.0.checked_add(length))
        else {
            return RolloutReading_Data::Unreadable(Diagnostic(format!(
                "{:?}",
                RolloutReadFailure::OffsetOverflow
            )));
        };
        RolloutReading_Data::Read(RolloutReading_Data_Read_Data {
            observation_vector,
            byte_offset: ByteOffset(byte_offset),
        })
    }
}

impl Emittable<CodexHarnessTranslator> for Watch {
    fn emit(&self, _translator: &CodexHarnessTranslator) -> RolloutReading_Data {
        let unreadable =
            |error: std::io::Error| RolloutReading_Data::Unreadable(error.diagnostic());
        let Ok(start) = u64::try_from(self.byte_offset.0) else {
            return RolloutReading_Data::Unreadable(Diagnostic(format!(
                "{:?}",
                RolloutReadFailure::NegativeOffset(self.byte_offset)
            )));
        };
        let mut file = match File::open(&self.file_path.0) {
            Ok(file) => file,
            Err(error) => return unreadable(error),
        };
        match file.metadata() {
            Ok(metadata) if metadata.len() < start => {
                let Ok(rollout_length) = i64::try_from(metadata.len()) else {
                    return RolloutReading_Data::Unreadable(Diagnostic(format!(
                        "{:?}",
                        RolloutReadFailure::OffsetOverflow
                    )));
                };
                return RolloutReading_Data::Unreadable(Diagnostic(format!(
                    "{:?}",
                    RolloutReadFailure::RolloutShorterThanOffset(RolloutShorterThanOffset_Data {
                        rollout_length: RolloutLength(rollout_length),
                        byte_offset: self.byte_offset,
                    })
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
        RolloutBytes(
            bytes
                .into_iter()
                .map(|byte| RolloutByte(i64::from(byte)))
                .collect(),
        )
        .rows(self.byte_offset)
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
                RolloutIdentity_Data::Identified(SessionId(id.to_owned()))
            })
    }
}
