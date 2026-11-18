//! A bootstrap the daemon refuses leaves the filesystem as it was: the
//! admission and the place name are checked before the place is prepared,
//! and preparation comes before any socket.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use codex_harness_nexus_daemon::NexusBootstrap;
use nexus_exchange::{
    Admission, FrameCapacityBytes, FrameDeadlineMilliseconds, RetryDelayMilliseconds,
};
use shared_translator::{
    FrameCapacity, FrameWriting, PeerIdentity, Place, PlaceName, ProcessId, Runtime, Signalizable,
    State, UserId,
};

/// A directory of its own for one test, removed when the test ends.
struct Root {
    path: PathBuf,
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

trait Rooted: Sized {
    /// A fresh directory for the test `label`.
    fn made(label: &str) -> Self;

    /// The place named `name`, its runtime and state inside the root.
    fn place(&self, name: &str) -> Place;

    /// A legacy state directory holding one record, which a preparation
    /// would move.
    fn legacy_state(&self) -> PathBuf;

    /// Asserts the legacy state is in place and nothing else was made.
    fn untouched(&self, legacy: &Path);
}

impl Rooted for Root {
    fn made(label: &str) -> Self {
        let name = format!("codex-harness-bootstrap-{label}-{}", std::process::id());
        let path = std::env::temp_dir().join(name);
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("test root");
        Self { path }
    }

    fn place(&self, name: &str) -> Place {
        Place {
            place_name: PlaceName(name.into()),
            runtime: Runtime(self.path.join("run/nexus/alpha").display().to_string()),
            state: State(self.path.join("state/nexus/alpha").display().to_string()),
        }
    }

    fn legacy_state(&self) -> PathBuf {
        let legacy = self.path.join("state/metaflow/alpha");
        std::fs::create_dir_all(&legacy).expect("legacy state");
        std::fs::write(legacy.join("record"), b"kept").expect("record");
        legacy
    }

    fn untouched(&self, legacy: &Path) {
        assert!(
            legacy.join("record").is_file(),
            "the legacy state is still in place"
        );
        assert!(
            !self.path.join("state/nexus").exists(),
            "no current state or lock directory was made"
        );
        assert!(
            !self.path.join("run").exists(),
            "no runtime directory was made"
        );
    }
}

trait Starting {
    /// The bootstrap for `place`, its frames read and written within
    /// `deadline` milliseconds.
    fn bootstrap(place: Place, deadline: i64) -> Self;

    /// Feeds the bootstrap to the daemon and answers whether it exited
    /// successfully.
    fn started(&self) -> bool;
}

impl Starting for NexusBootstrap {
    fn bootstrap(place: Place, deadline: i64) -> Self {
        Self {
            place,
            admission: Admission {
                frame_deadline_milliseconds: FrameDeadlineMilliseconds(deadline),
                frame_capacity_bytes: FrameCapacityBytes(8_388_608),
                retry_delay_milliseconds: RetryDelayMilliseconds(25),
            },
            peer_identity: PeerIdentity {
                user_id: UserId(0),
                process_id: ProcessId(1),
            },
        }
    }

    fn started(&self) -> bool {
        let mut child = Command::new(env!("CARGO_BIN_EXE_codex-harness-nexus"))
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn the daemon");
        let mut input = child.stdin.take().expect("daemon standard input");
        input
            .write_frame(
                &self.signalize().expect("archive the bootstrap"),
                FrameCapacity::from(signal::MAXIMUM_SIGNAL_BYTES),
            )
            .expect("write the bootstrap frame");
        input.flush().expect("flush");
        drop(input);
        child.wait().expect("daemon status").success()
    }
}

#[test]
fn an_invalid_admission_is_refused_before_the_place_is_prepared() {
    let root = Root::made("admission");
    let legacy = root.legacy_state();
    let bootstrap = NexusBootstrap::bootstrap(root.place("alpha"), 0);
    assert!(!bootstrap.started());
    root.untouched(&legacy);
}

#[test]
fn an_invalid_place_name_is_refused_before_the_place_is_prepared() {
    let root = Root::made("name");
    let legacy = root.legacy_state();
    let bootstrap = NexusBootstrap::bootstrap(root.place("../escape"), 70_000);
    assert!(!bootstrap.started());
    root.untouched(&legacy);
}
