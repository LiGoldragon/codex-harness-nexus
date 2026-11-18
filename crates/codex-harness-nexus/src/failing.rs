//! A failed emission lane, told apart from every answer the translator
//! gives: each emission's outcome, and the outcome the lane carries, has
//! one variant of its own for it. The operation that sent the emission is
//! refused as `ReportFailure::LaneFailed` when its effect needs the lane,
//! and is recorded, answered `Output::ReportedLaneFailed`, when it does not.
//!
//! Hand-written: Ethos names the trait, and an association's body is not
//! something it can generate.

use nexus_exchange::{LaneFailing, LaneFailure};

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
