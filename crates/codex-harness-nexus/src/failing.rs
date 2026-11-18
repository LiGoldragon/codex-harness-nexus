//! A failed emission lane, told apart from every answer the translator
//! gives: each emission's outcome, and the outcome the lane carries, has
//! one variant of its own for it, and the operation that sent the emission
//! is refused naming the failure.
//!
//! Hand-written: Ethos names the trait, and an association's body is not
//! something it can generate.

use nexus_exchange::{LaneFailing, LaneFailure};
use signal_codex_harness_nexus::{Diagnostic, ReportFailure};

use crate::emission::{
    IdentityClaim_Data, Outcome, RolloutFinding_Data, RolloutIdentity_Data, RolloutReading_Data,
};

impl LaneFailing for Outcome {
    fn lane_failed(lane_failure: LaneFailure) -> Self {
        Self::LaneFailed(lane_failure)
    }
}

impl LaneFailing for RolloutFinding_Data {
    fn lane_failed(lane_failure: LaneFailure) -> Self {
        Self::LaneFailed(lane_failure)
    }
}

impl LaneFailing for IdentityClaim_Data {
    fn lane_failed(lane_failure: LaneFailure) -> Self {
        Self::LaneFailed(lane_failure)
    }
}

impl LaneFailing for RolloutReading_Data {
    fn lane_failed(lane_failure: LaneFailure) -> Self {
        Self::LaneFailed(lane_failure)
    }
}

impl LaneFailing for RolloutIdentity_Data {
    fn lane_failed(lane_failure: LaneFailure) -> Self {
        Self::LaneFailed(lane_failure)
    }
}

/// The refusal of an operation whose emission's lane failed. The public
/// contract names no failure of the translator lane, so it is told as a
/// report not recorded, its diagnostic naming the lane's failure whole.
pub trait LaneReporting {
    fn report_failure(&self) -> ReportFailure;
}

impl LaneReporting for LaneFailure {
    fn report_failure(&self) -> ReportFailure {
        ReportFailure::Unrecorded(Diagnostic(format!(
            "the translator's emission lane failed: {self:?}"
        )))
    }
}
