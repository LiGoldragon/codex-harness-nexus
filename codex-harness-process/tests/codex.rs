//! The life of a Codex hook's report and of a rollout's watch, as the manager
//! traces them.

use codex_harness_process::*;
use harness::{Event, FilePath, Response, SessionId, Tokens};
use process::{Caller, Gave, Given, Identity, Living, Path, ProcessId, ProcessManager, Root, Trace, Traced, Tracing, Unobserved, UserId};

#[derive(Default)]
struct World(Vec<Traced<Process>>);

impl Tracing<Process> for World {
    fn trace(&mut self, trace: Traced<Process>) {
        self.0.push(trace);
    }
}

const THREAD: &str = "01a1fc39-854c-7721-8339-0495bf476131";

fn root() -> Identity {
    Identity { root: Root(1), path: Path(Vec::new()) }
}

fn session_id() -> SessionId {
    SessionId(THREAD.into())
}

fn gifts(world: &World) -> Vec<Gift> {
    world
        .0
        .iter()
        .filter_map(|trace| match trace {
            Trace::Gave(Gave { gift, .. }) => Some(gift.clone()),
            _ => None,
        })
        .collect()
}

fn ended(world: &World) -> Vec<Outcome> {
    world
        .0
        .iter()
        .filter_map(|trace| match trace {
            Trace::Ended(ended) => Some(ended.outcome.clone()),
            _ => None,
        })
        .collect()
}

fn report_for(session_id: SessionId, hook: Hook) -> World {
    let mut manager = ProcessManager::default();
    let mut world = World::default();
    let process = Process::Report(ReportProcess { caller: Caller { process_id: ProcessId(48211), user_id: UserId(1000) }, session_id, hook });
    manager.bear(Living { identity: root(), process }, &mut world);
    world
}

fn report(hook: Hook) -> World {
    report_for(session_id(), hook)
}

fn watch(rollout: Rollout, thread_database_option: Option<ThreadDatabase>) -> (ProcessManager<Process>, World) {
    let mut manager = ProcessManager::default();
    let mut world = World::default();
    let process = Process::Watch(WatchProcess { session_id: session_id(), rollout, read_from: ReadFrom::Offset(Offset(0)), row_count: RowCount(64), thread_database_option });
    manager.bear(Living { identity: root(), process }, &mut world);
    (manager, world)
}

#[test]
fn two_codex_threads_with_the_same_prefix_and_old_abbreviation_keep_their_whole_ids() {
    let second = "01a1fc39-854c-7721-8339-0495bf476132";
    for thread in [THREAD, second] {
        let session_id = SessionId(thread.into());
        let world = report_for(session_id.clone(), Hook::SessionStart(SessionStart_Data {
            file_path_option: None,
            source: Source::Startup,
        }));
        let gifts = gifts(&world);
        let [Gift::Reported, Gift::Watch(watch), Gift::Observe(observe)] = gifts.as_slice() else {
            panic!("expected Reported, Watch and Observe");
        };
        assert_eq!(watch.session_id, session_id);
        assert_eq!(observe.session_id, session_id);
    }
}

#[test]
fn a_session_start_with_its_rollout_keeps_a_located_watch() {
    let rollout = FilePath("/home/example/.codex/sessions/2026/11/20/rollout-2026-11-20T21-49-50-01a1fc39-854c-7721-8339-0495bf476131.jsonl".into());
    let world = report(Hook::SessionStart(SessionStart_Data { file_path_option: Some(rollout.clone()), source: Source::Startup }));
    assert_eq!(
        gifts(&world),
        vec![
            Gift::Reported,
            Gift::Watch(Watch { session_id: session_id(), rollout: Rollout::Located(rollout), read_from: ReadFrom::Offset(Offset(0)) }),
            Gift::Observe(Observe { session_id: session_id(), event_vector: vec![Event::Started], offset_option: None }),
        ]
    );
}

#[test]
fn a_session_start_without_its_rollout_keeps_an_unlocated_watch() {
    let world = report(Hook::SessionStart(SessionStart_Data { file_path_option: None, source: Source::Startup }));
    assert_eq!(gifts(&world)[1], Gift::Watch(Watch { session_id: session_id(), rollout: Rollout::Unlocated, read_from: ReadFrom::Offset(Offset(0)) }));
}

#[test]
fn a_resumed_session_is_resumed_and_read_from_its_rollout_end() {
    let rollout = FilePath("/t/rollout.jsonl".into());
    let world = report(Hook::SessionStart(SessionStart_Data { file_path_option: Some(rollout.clone()), source: Source::Resume }));
    assert_eq!(
        gifts(&world)[1..],
        [
            Gift::Watch(Watch { session_id: session_id(), rollout: Rollout::Located(rollout), read_from: ReadFrom::FileEnd }),
            Gift::Observe(Observe { session_id: session_id(), event_vector: vec![Event::Resumed], offset_option: None }),
        ]
    );
}

/// The start Codex announces again after a compaction leaves the session as it is.
#[test]
fn a_session_start_after_a_compaction_gives_no_event() {
    let rollout = FilePath("/t/rollout.jsonl".into());
    let world = report(Hook::SessionStart(SessionStart_Data { file_path_option: Some(rollout.clone()), source: Source::Compact }));
    assert_eq!(
        gifts(&world)[1..],
        [
            Gift::Watch(Watch { session_id: session_id(), rollout: Rollout::Located(rollout), read_from: ReadFrom::FileEnd }),
            Gift::Observe(Observe { session_id: session_id(), event_vector: vec![], offset_option: None }),
        ]
    );
}

#[test]
fn a_compaction_is_observed_with_its_trigger() {
    let world = report(Hook::PostCompact(harness::Trigger::Automatic));
    let compaction = harness::Compaction { trigger: harness::Trigger::Automatic, tokens_option: None };
    assert_eq!(gifts(&world)[1], Gift::Observe(Observe { session_id: session_id(), event_vector: vec![Event::Compacted(compaction)], offset_option: None }));
}

#[test]
fn an_unlocated_watch_finds_its_rollout_keeps_it_and_follows_it() {
    let database = ThreadDatabase("/home/example/.codex/state_5.sqlite".into());
    let (mut manager, mut world) = watch(Rollout::Unlocated, Some(database.clone()));
    let rollout = FilePath("/home/example/.codex/sessions/rollout.jsonl".into());
    let taken = Taken::RolloutFinding(Ok(RolloutFinding_Data::Found(rollout.clone())));
    manager.hand(Given { identity: root(), taken }, &mut world);
    assert_eq!(
        gifts(&world),
        vec![
            Gift::FindRollout(FindRollout_Data { thread_database: database, session_id: session_id() }),
            Gift::Locate(Locate { session_id: session_id(), file_path: rollout.clone() }),
            Gift::Follow(Follow_Data { file_path: rollout, read_from: ReadFrom::Offset(Offset(0)), row_count: RowCount(64) }),
        ]
    );
    assert!(ended(&world).is_empty());
}

#[test]
fn an_unlocated_watch_with_no_thread_database_ends_unlocated() {
    let (_, world) = watch(Rollout::Unlocated, None);
    assert_eq!(ended(&world), vec![Outcome::Watch(Watching::Unlocated)]);
}

#[test]
fn a_rollout_the_record_does_not_hold_ends_the_watch_unlocated() {
    let (mut manager, mut world) = watch(Rollout::Unlocated, Some(ThreadDatabase("/t/state_5.sqlite".into())));
    manager.hand(Given { identity: root(), taken: Taken::RolloutFinding(Ok(RolloutFinding_Data::Unfound)) }, &mut world);
    assert_eq!(ended(&world), vec![Outcome::Watch(Watching::Unlocated)]);
}

#[test]
fn a_turn_read_from_the_rollout_is_observed_with_its_new_offset() {
    let rollout = FilePath("/t/rollout.jsonl".into());
    let (mut manager, mut world) = watch(Rollout::Located(rollout), None);
    let following = Following_Data {
        row_vector: vec![Row::TaskStarted, Row::Other, Row::TokenCount(Some(Tokens(19147))), Row::TokenCount(None), Row::TaskComplete(Some(Response("Listed.".into()))), Row::TurnAborted],
        offset: Offset(4096),
    };
    manager.hand(Given { identity: root(), taken: Taken::Following(Ok(following)) }, &mut world);
    assert_eq!(
        gifts(&world)[1],
        Gift::Observe(Observe {
            session_id: session_id(),
            event_vector: vec![Event::PromptSubmitted, Event::ContextSize(Tokens(19147)), Event::Responded(Response("Listed.".into())), Event::TurnEnded, Event::Interrupted],
            offset_option: Some(Offset(4096)),
        })
    );
}

#[test]
fn a_translator_that_was_not_there_ends_the_watch_unfollowed() {
    let (mut manager, mut world) = watch(Rollout::Located(FilePath("/t/rollout.jsonl".into())), None);
    manager.hand(Given { identity: root(), taken: Taken::Following(Err(Unobserved::Unsubmitted)) }, &mut world);
    assert_eq!(ended(&world), vec![Outcome::Watch(Watching::Unfollowed)]);
}
