//! A failed emission lane reaches the Nexus as the lane's failure, apart
//! from every answer the translator gives: a translator not reached is
//! unsubmitted, one that answers with a failure frame has declined, and one
//! that closes without an answer, or answers another emission, leaves the
//! emission unanswered. None of them reads as a rollout not found, and an
//! operation whose lane failed is refused naming the failure.

use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::Mutex;
use std::thread::JoinHandle;

use codex_harness_nexus::emission::{
    ClaimIdentity_Data, Emittable as _, FlowsRoot, IdentityClaim_Data, IdentityProducer, Outcome,
    RolloutFinding_Data, RolloutIdentity_Data,
};
use codex_harness_nexus::{Channelled as _, HarnessNexus, Opening as _, SessionsWatch, Store};
use codex_harness_nexus_daemon::PairedTranslator;
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

    /// A translator on the socket `name` that reads one emission and then
    /// writes `answer`, or closes without answering when it is `None`.
    fn answering(&self, name: &str, answer: Option<Frame>) -> (PairedTranslator, JoinHandle<()>);
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

    fn answering(&self, name: &str, answer: Option<Frame>) -> (PairedTranslator, JoinHandle<()>) {
        let socket = self.directory.join(name);
        let listener = UnixListener::bind(&socket).expect("translator listener");
        let admission = Self::admission();
        let answering = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("the Nexus connects");
            stream
                .read_lane_frame(admission.frame_capacity)
                .expect("the emission is read whole");
            if let Some(answer) = answer {
                stream
                    .write_lane_frame(&answer, admission.frame_capacity)
                    .expect("the answer is written");
            }
        });
        let translator = PairedTranslator {
            lane: EmissionLane::from(socket),
            admission,
        };
        (translator, answering)
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
        Some(Frame::frame(
            Kind::Failed,
            b"translator is awaiting settings".to_vec(),
        )),
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
    let (silent, answering) = lanes.answering("silent.sock", None);
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
        Some(Frame::frame(Kind::Answered, other.bytes().to_vec())),
    );
    let finding = Session::fixture().emit(&misanswering);
    answering.join().expect("the misanswering translator");
    assert!(matches!(
        finding,
        RolloutFinding_Data::LaneFailed(LaneFailure::Unanswered(_))
    ));
}

#[test]
fn a_session_start_whose_lane_failed_is_refused_naming_the_failure() {
    let lanes = Lanes::made("refused");
    let sessions = lanes.directory.join("sessions");
    std::fs::create_dir(&sessions).expect("a sessions directory");
    let (sessions_watch, _words) = SessionsWatch::channelled();
    let nexus = HarnessNexus {
        store: Store::open(&lanes.directory.join("codex-harness.memory")).expect("the Memory"),
        translator: lanes.unreached(),
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
    let refused = nexus.report(&Hook::SessionStart(SessionStart_Data {
        session: Session::fixture(),
        file_path_option: None,
    }));
    let Err(Output::ReportRefused(ReportFailure::Unrecorded(diagnostic))) = refused else {
        panic!("a lane failure refuses the report: {refused:?}");
    };
    assert!(
        diagnostic.0.contains("Unsubmitted"),
        "the refusal names the lane's failure: {}",
        diagnostic.0
    );
}
