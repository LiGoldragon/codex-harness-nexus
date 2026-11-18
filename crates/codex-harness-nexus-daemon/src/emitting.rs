//! The Nexus's side of its emissions: each one a typed value, sent to its
//! paired translator process on the emission lane, and its outcome read
//! back. What the translator does with it is the translator's; the Nexus
//! knows only the value it sent and the value it was answered with, or that
//! the lane failed.

use codex_harness_nexus::Watch;
use codex_harness_nexus::emission::{
    ClaimIdentity_Data, Emission, Emittable, IdentityClaim_Data, Outcome, RolloutFinding_Data,
    RolloutIdentity_Data, RolloutReading_Data,
};
use harness::{FilePath, Session};
use nexus_exchange::ValidAdmission;
use nexus_serving::{EmissionLane, Emitting as _, LaneFailing, LaneFailure, LaneFault};

/// The translator process the Nexus is paired with, as the Nexus reaches
/// it: the emission lane to its public socket, and the admission every
/// emission frame is written and read within.
pub struct PairedTranslator {
    pub lane: EmissionLane,
    pub admission: ValidAdmission,
}

/// The outcome one emission is answered with, taken from the outcome the
/// translator sent, a failed lane among them; any other outcome is handed
/// back.
pub trait Answered: LaneFailing + Sized {
    fn answered(outcome: Outcome) -> Result<Self, Outcome>;
}

impl Answered for RolloutFinding_Data {
    fn answered(outcome: Outcome) -> Result<Self, Outcome> {
        match outcome {
            Outcome::RolloutFinding(finding) => Ok(finding),
            Outcome::LaneFailed(lane_failure) => Ok(Self::lane_failed(lane_failure)),
            other => Err(other),
        }
    }
}

impl Answered for IdentityClaim_Data {
    fn answered(outcome: Outcome) -> Result<Self, Outcome> {
        match outcome {
            Outcome::IdentityClaim(claim) => Ok(claim),
            Outcome::LaneFailed(lane_failure) => Ok(Self::lane_failed(lane_failure)),
            other => Err(other),
        }
    }
}

impl Answered for RolloutReading_Data {
    fn answered(outcome: Outcome) -> Result<Self, Outcome> {
        match outcome {
            Outcome::RolloutReading(reading) => Ok(reading),
            Outcome::LaneFailed(lane_failure) => Ok(Self::lane_failed(lane_failure)),
            other => Err(other),
        }
    }
}

impl Answered for RolloutIdentity_Data {
    fn answered(outcome: Outcome) -> Result<Self, Outcome> {
        match outcome {
            Outcome::RolloutIdentity(identity) => Ok(identity),
            Outcome::LaneFailed(lane_failure) => Ok(Self::lane_failed(lane_failure)),
            other => Err(other),
        }
    }
}

pub trait Carrying {
    /// Sends one emission and reads back the outcome it is answered with. A
    /// lane that fails is the answer's own variant; an outcome of another
    /// emission is one the emission's answer was never read from, so the
    /// lane went unanswered.
    fn carry<Answer: Answered>(&self, emission: Emission) -> Answer;
}

impl Carrying for PairedTranslator {
    fn carry<Answer: Answered>(&self, emission: Emission) -> Answer {
        let ValidAdmission {
            deadline,
            frame_capacity,
            ..
        } = self.admission;
        let outcome: Outcome = self.lane.emit(&emission, deadline, frame_capacity);
        Answer::answered(outcome).unwrap_or_else(|other| {
            Answer::lane_failed(LaneFailure::Unanswered(LaneFault(format!(
                "{emission:?} answered with {other:?}"
            ))))
        })
    }
}

/// Each emission's outcome is the translator's answer as it came, or the
/// lane's failure, which the Nexus tells apart from every answer.
impl Emittable<PairedTranslator> for Session {
    fn emit(&self, translator: &PairedTranslator) -> RolloutFinding_Data {
        translator.carry(Emission::FindRollout(self.clone()))
    }
}

impl Emittable<PairedTranslator> for ClaimIdentity_Data {
    fn emit(&self, translator: &PairedTranslator) -> IdentityClaim_Data {
        translator.carry(Emission::ClaimIdentity(self.clone()))
    }
}

impl Emittable<PairedTranslator> for Watch {
    fn emit(&self, translator: &PairedTranslator) -> RolloutReading_Data {
        translator.carry(Emission::ReadRollout(self.clone()))
    }
}

impl Emittable<PairedTranslator> for FilePath {
    fn emit(&self, translator: &PairedTranslator) -> RolloutIdentity_Data {
        translator.carry(Emission::IdentifyRollout(self.clone()))
    }
}
