//! A failed emission lane reaches the Nexus as the lane's failure, apart
//! from every answer the translator gives: a translator not reached is
//! unsubmitted, one that answers with a failure frame has declined, and one
//! that closes without an answer, or answers another emission, leaves the
//! emission unanswered. None of them reads as a rollout not found. An
//! operation whose effect needs the lane is refused as the lane's failure;
//! one whose effect does not is recorded, and answered with the failure.

use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::Mutex;
use std::thread::JoinHandle;

use codex_harness_nexus::emission::{
    ClaimIdentity_Data, Emittable as _, FlowsRoot, IdentityClaim_Data, IdentityProducer, Outcome,
    RolloutFinding_Data, RolloutIdentity_Data,
};
use codex_harness_nexus::{
    Channelled as _, HarnessNexus, Opening as _, Operation, Remembering as _, Rollout,
    SessionsWatch, Store, Waking as _,
};
use codex_harness_nexus_daemon::PairedTranslator;
use flow::FlowId;
use harness::{FilePath, Session};
use meta_signal_codex_harness_nexus as meta;
use nexus_exchange::lane::{Frame, Kind, Lane, LaneReading as _, LaneWriting as _};
use nexus_exchange::{
    Admission, FrameCapacityBytes, FrameDeadlineMilliseconds, LaneFailure, LaneFault,
    RetryDelayMilliseconds, ValidAdmission,
};
use nexus_serving::EmissionLane;
use signal::{ByteViewable as _, Signalizable as _};
use signal_codex_harness_nexus::{Answering as _, Hook, Output, ReportFailure, SessionStart_Data};

/// A directory of its own for one test, holding the translator sockets it
/// listens on, removed when the test ends.
struct Lanes {
    directory: PathBuf,
}

impl Drop for Lanes {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

trait Laning: Sized {
    /// A fresh directory for the test `label`.
    fn made(label: &str) -> Self;

    /// The admission every emission frame is read and written within.
    fn admission() -> ValidAdmission;

    /// A translator that is not there: its socket was never bound.
    fn unreached(&self) -> PairedTranslator;

    /// A translator on the socket `name` that reads each emission in turn,
    /// one connection each, and then writes its answer, or closes without
    /// answering where it is `None`.
    fn answering(
        &self,
        name: &str,
        answers: Vec<Option<Frame>>,
    ) -> (PairedTranslator, JoinHandle<()>);

    /// A Nexus on `translator`, configured whole, its sessions directory
    /// made in the test's own.
    fn configured(&self, translator: PairedTranslator) -> HarnessNexus<PairedTranslator>;
}

impl Laning for Lanes {
    fn made(label: &str) -> Self {
        let directory =
            std::env::temp_dir().join(format!("codex-harness-lane-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("test directory");
        Self { directory }
    }

    fn admission() -> ValidAdmission {
        ValidAdmission::try_from(&Admission {
            frame_deadline_milliseconds: FrameDeadlineMilliseconds(70_000),
            frame_capacity_bytes: FrameCapacityBytes(8_388_608),
            retry_delay_milliseconds: RetryDelayMilliseconds(25),
        })
        .expect("a valid admission")
    }

    fn unreached(&self) -> PairedTranslator {
        PairedTranslator {
            lane: EmissionLane::from(self.directory.join("absent.sock")),
            admission: Self::admission(),
        }
    }

    fn answering(
        &self,
        name: &str,
        answers: Vec<Option<Frame>>,
    ) -> (PairedTranslator, JoinHandle<()>) {
        let socket = self.directory.join(name);
        let listener = UnixListener::bind(&socket).expect("translator listener");
        let admission = Self::admission();
        let answering = std::thread::spawn(move || {
            for answer in answers {
                let (mut stream, _) = listener.accept().expect("the Nexus connects");
                stream
                    .read_lane_frame(admission.frame_capacity)
                    .expect("the emission is read whole");
                if let Some(answer) = answer {
                    stream
                        .write_lane_frame(&answer, admission.frame_capacity)
                        .expect("the answer is written");
                }
            }
        });
        let translator = PairedTranslator {
            lane: EmissionLane::from(socket),
            admission,
        };
        (translator, answering)
    }

    fn configured(&self, translator: PairedTranslator) -> HarnessNexus<PairedTranslator> {
        let sessions = self.directory.join("sessions");
        std::fs::create_dir(&sessions).expect("a sessions directory");
        let (sessions_watch, _words) = SessionsWatch::channelled();
        let nexus = HarnessNexus {
            store: Store::open(&self.directory.join("codex-harness.memory")).expect("the Memory"),
            translator,
            serial: Mutex::new(()),
            observations: Mutex::new(None),
            configurations: Mutex::new(None),
            sessions_watch: Mutex::new(sessions_watch),
        };
        for set in [
            meta::Set::FlowsRoot(meta::FlowsRoot("/flows".into())),
            meta::Set::IdentityProducer(meta::IdentityProducer("/flow-id".into())),
            meta::Set::SessionsDirectories(meta::SessionsDirectories(vec![FilePath(
                sessions.to_string_lossy().into_owned(),
            )])),
            meta::Set::ThreadDatabases(meta::ThreadDatabases(vec![])),
            meta::Set::SubscriberCapacity(meta::Capacity(16)),
        ] {
            let answer = meta::Answerable::answer(&meta::Input::Configure(set.clone()), &nexus);
            assert!(
                matches!(answer, meta::Answer::Once(meta::Output::Configured(_))),
                "{set:?} is configured"
            );
        }
        nexus
    }
}

/// An emission's answer frame, as the translator writes it.
trait Answering {
    fn frame(kind: Kind, payload: Vec<u8>) -> Self;
}

impl Answering for Frame {
    fn frame(kind: Kind, payload: Vec<u8>) -> Self {
        Self {
            lane: Lane::EmissionSignal,
            kind,
            payload,
        }
    }
}

/// The one session, and the one identity claim, every test emits for.
trait Fixture {
    fn fixture() -> Self;
}

impl Fixture for Session {
    fn fixture() -> Self {
        Self("01a1e4cb-c2d2-7d91-86ea-0b4c85ce34be".into())
    }
}

impl Fixture for ClaimIdentity_Data {
    fn fixture() -> Self {
        Self {
            session: Session::fixture(),
            flows_root: FlowsRoot("/flows".into()),
            identity_producer: IdentityProducer("/flow-id".into()),
        }
    }
}

#[test]
fn a_translator_not_reached_is_an_unsubmitted_lane_and_never_a_rollout_unfound() {
    let lanes = Lanes::made("unreached");
    let translator = lanes.unreached();
    assert!(matches!(
        Session::fixture().emit(&translator),
        RolloutFinding_Data::LaneFailed(LaneFailure::Unsubmitted(_))
    ));
    assert!(matches!(
        ClaimIdentity_Data::fixture().emit(&translator),
        IdentityClaim_Data::LaneFailed(LaneFailure::Unsubmitted(_))
    ));
}

#[test]
fn a_translator_answering_a_failure_has_declined_the_emission() {
    let lanes = Lanes::made("declined");
    let (translator, answering) = lanes.answering(
        "declining.sock",
        vec![Some(Frame::frame(
            Kind::Failed,
            b"translator is awaiting settings".to_vec(),
        ))],
    );
    let identity = FilePath("/sessions/rollout.jsonl".into()).emit(&translator);
    answering.join().expect("the declining translator");
    assert_eq!(
        identity,
        RolloutIdentity_Data::LaneFailed(LaneFailure::Declined(LaneFault(
            "translator is awaiting settings".into()
        )))
    );
}

#[test]
fn a_translator_closing_unanswered_or_answering_another_emission_leaves_it_unanswered() {
    let lanes = Lanes::made("unanswered");
    let (silent, answering) = lanes.answering("silent.sock", vec![None]);
    let claimed = ClaimIdentity_Data::fixture().emit(&silent);
    answering.join().expect("the silent translator");
    assert!(matches!(
        claimed,
        IdentityClaim_Data::LaneFailed(LaneFailure::Unanswered(_))
    ));

    let other = Outcome::RolloutIdentity(RolloutIdentity_Data::Unidentified)
        .signalize()
        .expect("an outcome of another emission");
    let (misanswering, answering) = lanes.answering(
        "misanswering.sock",
        vec![Some(Frame::frame(Kind::Answered, other.bytes().to_vec()))],
    );
    let finding = Session::fixture().emit(&misanswering);
    answering.join().expect("the misanswering translator");
    assert!(matches!(
        finding,
        RolloutFinding_Data::LaneFailed(LaneFailure::Unanswered(_))
    ));
}

#[test]
fn a_new_session_whose_identity_claim_lane_failed_is_refused_lane_failed_and_never_recorded() {
    let lanes = Lanes::made("refused");
    let nexus = lanes.configured(lanes.unreached());
    let refused = nexus.report(&Hook::SessionStart(SessionStart_Data {
        session: Session::fixture(),
        file_path_option: None,
    }));
    assert!(
        matches!(
            refused,
            Err(Output::ReportRefused(ReportFailure::LaneFailed(
                LaneFailure::Unsubmitted(_)
            )))
        ),
        "a new session needs its identity, so the lane's failure refuses it: {refused:?}"
    );
    assert_eq!(
        nexus.store.recall(Session::fixture()).expect("the Memory"),
        None,
        "nothing is recorded"
    );
}

#[test]
fn a_session_start_whose_rollout_lookup_lane_failed_is_recorded_unlocated_and_answered_with_it() {
    let lanes = Lanes::made("unlocated");
    let claimed = Outcome::IdentityClaim(IdentityClaim_Data::Claimed(FlowId("4c85ce".into())))
        .signalize()
        .expect("the identity claimed");
    // The identity is claimed; the rollout's lookup is left unanswered.
    let (translator, answering) = lanes.answering(
        "claiming.sock",
        vec![
            Some(Frame::frame(Kind::Answered, claimed.bytes().to_vec())),
            None,
        ],
    );
    let nexus = lanes.configured(translator);
    let reported = nexus.report(&Hook::SessionStart(SessionStart_Data {
        session: Session::fixture(),
        file_path_option: None,
    }));
    answering.join().expect("the claiming translator");
    assert!(
        matches!(
            reported,
            Err(Output::ReportedLaneFailed(LaneFailure::Unanswered(_)))
        ),
        "the report is answered with the lane's failure: {reported:?}"
    );
    let record = nexus
        .store
        .recall(Session::fixture())
        .expect("the Memory")
        .expect("the session is recorded whatever the lane does");
    assert_eq!(record.observed.flow_id, FlowId("4c85ce".into()));
    assert_eq!(record.rollout, Rollout::Unlocated);
    // Its rollout is looked at once it is written, as any unlocated one is.
    let rollout = FilePath(
        lanes
            .directory
            .join("sessions/rollout.jsonl")
            .to_string_lossy()
            .into_owned(),
    );
    assert_eq!(
        nexus.woken(vec![rollout.clone()]),
        Ok(vec![Operation::Discovering(rollout)])
    );
}
