//! The Nexus's side of its emissions: each one a typed value, sent to its
//! paired translator process on the emission lane, and its outcome read
//! back. What the translator does with it is the translator's; the Nexus
//! knows only the value it sent and the value it was answered with.

use codex_harness_nexus::Watch;
use codex_harness_nexus::emission::{
    ClaimIdentity_Data, Emission, Emittable, IdentityClaim_Data, Outcome, RolloutFinding_Data,
    RolloutIdentity_Data, RolloutReading_Data,
};
use harness::{FilePath, Session};
use nexus_exchange::ValidAdmission;
use nexus_serving::{EmissionLane, Emitting as _};
use signal_codex_harness_nexus::{Diagnostic, IdentityFailure};

/// The translator process the Nexus is paired with, as the Nexus reaches
/// it: the emission lane to its public socket, and the admission every
/// emission frame is written and read within.
pub struct PairedTranslator {
    pub lane: EmissionLane,
    pub admission: ValidAdmission,
}

/// The outcome one emission is answered with, taken from the outcome the
/// translator sent; any other outcome is handed back.
pub trait Answered: Sized {
    fn answered(outcome: Outcome) -> Result<Self, Outcome>;
}

impl Answered for RolloutFinding_Data {
    fn answered(outcome: Outcome) -> Result<Self, Outcome> {
        match outcome {
            Outcome::RolloutFinding(finding) => Ok(finding),
            other => Err(other),
        }
    }
}

impl Answered for IdentityClaim_Data {
    fn answered(outcome: Outcome) -> Result<Self, Outcome> {
        match outcome {
            Outcome::IdentityClaim(claim) => Ok(claim),
            other => Err(other),
        }
    }
}

impl Answered for RolloutReading_Data {
    fn answered(outcome: Outcome) -> Result<Self, Outcome> {
        match outcome {
            Outcome::RolloutReading(reading) => Ok(reading),
            other => Err(other),
        }
    }
}

impl Answered for RolloutIdentity_Data {
    fn answered(outcome: Outcome) -> Result<Self, Outcome> {
        match outcome {
            Outcome::RolloutIdentity(identity) => Ok(identity),
            other => Err(other),
        }
    }
}

pub trait Carrying {
    /// Sends one emission and reads back the outcome it is answered with. A
    /// lane that fails, or an outcome of another emission, is the
    /// diagnostic.
    fn carry<Answer: Answered>(&self, emission: Emission) -> Result<Answer, Diagnostic>;
}

impl Carrying for PairedTranslator {
    fn carry<Answer: Answered>(&self, emission: Emission) -> Result<Answer, Diagnostic> {
        let ValidAdmission {
            deadline,
            frame_capacity,
            ..
        } = self.admission;
        let outcome: Outcome = match self.lane.emit(&emission, deadline, frame_capacity) {
            Ok(outcome) => outcome,
            Err(failure) => {
                let unanswered = format!("unanswered {emission:?}: {failure:?}");
                return Err(Diagnostic(unanswered));
            }
        };
        match Answer::answered(outcome) {
            Ok(answer) => Ok(answer),
            Err(other) => {
                let misanswered = format!("{emission:?} answered with {other:?}");
                Err(Diagnostic(misanswered))
            }
        }
    }
}

/// A rollout the translator could not be asked to find is not found; the
/// session waits for it to appear in a sessions directory.
impl Emittable<PairedTranslator> for Session {
    fn emit(&self, translator: &PairedTranslator) -> RolloutFinding_Data {
        match translator.carry(Emission::FindRollout(self.clone())) {
            Ok(finding) => finding,
            Err(diagnostic) => {
                eprintln!("Codex Harness Nexus found no rollout: {}", diagnostic.0);
                RolloutFinding_Data::Unfound
            }
        }
    }
}

/// An identity the translator could not be asked to claim is unclaimed, its
/// producer not started.
impl Emittable<PairedTranslator> for ClaimIdentity_Data {
    fn emit(&self, translator: &PairedTranslator) -> IdentityClaim_Data {
        match translator.carry(Emission::ClaimIdentity(self.clone())) {
            Ok(claim) => claim,
            Err(diagnostic) => {
                IdentityClaim_Data::Unclaimed(IdentityFailure::NotStarted(diagnostic))
            }
        }
    }
}

/// A rollout the translator could not be asked to read is unreadable.
impl Emittable<PairedTranslator> for Watch {
    fn emit(&self, translator: &PairedTranslator) -> RolloutReading_Data {
        match translator.carry(Emission::ReadRollout(self.clone())) {
            Ok(reading) => reading,
            Err(diagnostic) => RolloutReading_Data::Unreadable(diagnostic),
        }
    }
}

/// A rollout the translator could not be asked to identify is unidentified,
/// and attaches to no session.
impl Emittable<PairedTranslator> for FilePath {
    fn emit(&self, translator: &PairedTranslator) -> RolloutIdentity_Data {
        match translator.carry(Emission::IdentifyRollout(self.clone())) {
            Ok(identity) => identity,
            Err(diagnostic) => {
                eprintln!("Codex Harness Nexus identified no rollout: {}", diagnostic.0);
                RolloutIdentity_Data::Unidentified
            }
        }
    }
}
