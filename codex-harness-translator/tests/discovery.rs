//! The translator's discovery under a rollout root: a session is a
//! `rollout-<moment>-<uuid>.jsonl` at any depth under it, reported once it
//! appears, or at the start when a live process holds it open for writing.

use std::collections::BTreeSet;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use codex_harness_process::{Discover_Data, Discovered_Data};
use codex_harness_translator::{discover, rollout_session};
use harness::{FilePath, SessionId};
use transport::Framed;

const CLOSED: &str = "01a1fc39-854c-7721-8339-0495bf000001";
const HELD: &str = "01a1fc39-854c-7721-8339-0495bf000002";
const NEW_DAY: &str = "01a1fc39-854c-7721-8339-0495bf000003";
const OLD_DAY: &str = "01a1fc39-854c-7721-8339-0495bf000004";
const OUTSIDE: &str = "01a1fc39-854c-7721-8339-0495bf000005";

fn scratch(name: &str) -> PathBuf {
    let scratch = std::env::temp_dir().join(format!("codex-harness-discovery-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).expect("the scratch");
    scratch
}

fn rollout(directory: &Path, session: &str) -> PathBuf {
    directory.join(format!("rollout-2026-11-28T04-09-56-{session}.jsonl"))
}

fn write(path: &Path) {
    std::fs::create_dir_all(path.parent().expect("a parent")).expect("the parent");
    std::fs::write(path, "{\"type\":\"session_meta\",\"payload\":{}}\n").expect("the file");
}

fn discovered(path: &Path, session: &str) -> Discovered_Data {
    Discovered_Data { session_id: SessionId(session.into()), file_path: FilePath(path.to_string_lossy().into_owned()) }
}

#[test]
fn a_rollout_is_named_for_its_moment_and_its_thread() {
    let session = |name: &str| rollout_session(Path::new(name));
    assert_eq!(session(&format!("/r/2026/11/28/rollout-2026-11-28T04-09-56-{CLOSED}.jsonl")), Some(SessionId(CLOSED.into())));
    assert_eq!(session(&format!("/r/{CLOSED}.jsonl")), None);
    assert_eq!(session(&format!("/r/rollout-{CLOSED}.jsonl")), None);
    assert_eq!(session(&format!("/r/rollout-2026-11-28T04-09-56-{}.jsonl", CLOSED.to_uppercase())), None);
    assert_eq!(session(&format!("/r/rollout-2026-11-28T04-09-56-{CLOSED}.jsonl (deleted)")), None);
    assert_eq!(session(&format!("/r/rollout-2026-11-28T04-09-56-{CLOSED}.json")), None);
}

#[test]
fn a_rollout_that_appears_or_is_held_is_discovered_and_nothing_else_is() {
    let scratch = scratch("appears");
    let root = scratch.join("sessions");
    let old_day = root.join("2026/11/27");
    let closed = rollout(&old_day, CLOSED);
    let held = rollout(&old_day, HELD);
    write(&closed);
    write(&held);
    let _holding = std::fs::OpenOptions::new().append(true).open(&held).expect("the held rollout");
    let outside = scratch.join("outside");
    write(&rollout(&outside, OUTSIDE));

    let (mut nexus, translator) = UnixStream::pair().expect("a pair");
    let discovering = Discover_Data { file_path: FilePath(root.to_string_lossy().into_owned()) };
    let discovery = std::thread::spawn(move || discover(translator, discovering));
    nexus.set_read_timeout(Some(Duration::from_secs(5))).expect("a timeout");
    std::thread::sleep(Duration::from_millis(200));

    let new_day = rollout(&root.join("2026/11/28"), NEW_DAY);
    let old_new = rollout(&old_day, OLD_DAY);
    write(&new_day);
    write(&old_new);
    write(&old_day.join("history.jsonl"));
    std::os::unix::fs::symlink(&outside, root.join("2026/linked")).expect("a link");

    let mut seen = BTreeSet::new();
    for _ in 0..3 {
        let found = Discovered_Data::read_from(&mut nexus).expect("a read").expect("a discovery");
        seen.insert((found.session_id.0.clone(), found.file_path.0.clone()));
    }
    let expected: BTreeSet<_> = [discovered(&held, HELD), discovered(&new_day, NEW_DAY), discovered(&old_new, OLD_DAY)].into_iter().map(|found| (found.session_id.0, found.file_path.0)).collect();
    assert_eq!(seen, expected);

    nexus.set_read_timeout(Some(Duration::from_millis(500))).expect("a timeout");
    assert!(Discovered_Data::read_from(&mut nexus).is_err(), "nothing more is discovered");
    drop(nexus);
    assert!(discovery.join().expect("the discovery").is_ok());
    let _ = std::fs::remove_dir_all(&scratch);
}

#[test]
fn a_root_that_appears_is_new_whole() {
    let scratch = scratch("root");
    let root = scratch.join("sessions");
    let (mut nexus, translator) = UnixStream::pair().expect("a pair");
    let discovering = Discover_Data { file_path: FilePath(root.to_string_lossy().into_owned()) };
    let discovery = std::thread::spawn(move || discover(translator, discovering));
    nexus.set_read_timeout(Some(Duration::from_secs(5))).expect("a timeout");
    std::thread::sleep(Duration::from_millis(200));

    let first = rollout(&root.join("2026/11/28"), NEW_DAY);
    write(&first);
    assert_eq!(Discovered_Data::read_from(&mut nexus).expect("a read"), Some(discovered(&first, NEW_DAY)));
    drop(nexus);
    assert!(discovery.join().expect("the discovery").is_ok());
    let _ = std::fs::remove_dir_all(&scratch);
}
