use harness::{Event, Flow, Observed, State, Tokens, Transitionable};
use signal_codex_harness_nexus::{Changed, Changed_Malformed_Data, Observation, Transition_Data};

use crate::generated::folding::{
    FoldPlan, FoldRefusal, Folding, IdentityMismatch_Data, OffsetReversed_Data, SessionChange,
    SessionMismatch_Data, WatchIdentityMismatch_Data,
};
use crate::generated::memory::{MalformedRecord, Rollout, SessionRecord};
use crate::generated::operation::EventBatch;

impl Folding for EventBatch {
    fn fold(&self, input: Option<SessionRecord>) -> Result<FoldPlan, FoldRefusal> {
        let registering = input.is_none();
        let mut record =
            match input {
                Some(record) => record,
                None => {
                    if !self.observation_vector.iter().any(|observation| {
                        matches!(observation, Observation::Event(Event::Started))
                    }) {
                        return Err(FoldRefusal::Unregistered(self.session.clone()));
                    }
                    SessionRecord {
                        session: self.session.clone(),
                        observed: Observed {
                            flow_id: self.flow_id.clone(),
                            state: State::Dry,
                            tokens: Tokens(0),
                            background_work_vector: vec![],
                        },
                        rollout: Rollout::Unlocated,
                    }
                }
            };
        if record.session != self.session {
            return Err(FoldRefusal::SessionMismatch(SessionMismatch_Data {
                first_session: record.session,
                second_session: self.session.clone(),
            }));
        }
        if record.observed.flow_id != self.flow_id {
            return Err(FoldRefusal::IdentityMismatch(IdentityMismatch_Data {
                first_flow_id: record.observed.flow_id,
                second_flow_id: self.flow_id.clone(),
            }));
        }
        if let Some(rollout) = &self.rollout_option {
            if let Rollout::Watched(watch) = rollout {
                if watch.flow_id != self.flow_id {
                    return Err(FoldRefusal::WatchIdentityMismatch(
                        WatchIdentityMismatch_Data {
                            first_flow_id: watch.flow_id.clone(),
                            second_flow_id: self.flow_id.clone(),
                        },
                    ));
                }
                if watch.byte_offset.0 < 0 {
                    return Err(FoldRefusal::InvalidOffset(watch.byte_offset));
                }
                if let Rollout::Watched(previous) = &record.rollout
                    && previous.file_path == watch.file_path
                    && watch.byte_offset.0 < previous.byte_offset.0
                {
                    return Err(FoldRefusal::OffsetReversed(OffsetReversed_Data {
                        first_byte_offset: previous.byte_offset,
                        second_byte_offset: watch.byte_offset,
                    }));
                }
            }
            record.rollout = rollout.clone();
        }
        let mut malformed_records = vec![];
        let mut changes = vec![];
        for observation in &self.observation_vector {
            match observation {
                Observation::Event(event) => {
                    if matches!(event, Event::TurnEnded)
                        && matches!(
                            record.observed.state,
                            State::Flowing(Flow::Rested | Flow::Waiting)
                        )
                    {
                        continue;
                    }
                    record.observed.after(event.clone());
                    changes.push(Changed::Transition(Transition_Data {
                        flow_id: self.flow_id.clone(),
                        event: event.clone(),
                        state: record.observed.state.clone(),
                    }));
                }
                Observation::Malformed(malformed) => {
                    if malformed.byte_offset.0 < 0 {
                        return Err(FoldRefusal::InvalidOffset(malformed.byte_offset));
                    }
                    malformed_records.push(MalformedRecord {
                        flow_id: self.flow_id.clone(),
                        byte_offset: malformed.byte_offset,
                        parse_failure: malformed.parse_failure.clone(),
                    });
                    changes.push(Changed::Malformed(Changed_Malformed_Data {
                        flow_id: self.flow_id.clone(),
                        byte_offset: malformed.byte_offset,
                        parse_failure: malformed.parse_failure.clone(),
                    }));
                }
            }
        }
        Ok(FoldPlan {
            session_change: if registering {
                SessionChange::Registered(record)
            } else {
                SessionChange::Updated(record)
            },
            malformed_record_vector: malformed_records,
            changed_vector: changes,
        })
    }
}
