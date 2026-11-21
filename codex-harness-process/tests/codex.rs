//! The life of a Codex hook's report and of a rollout's watch, as the manager
//! traces them.

use codex_harness_process::*;
use flow::FlowId;
use harness::{Event, FilePath, Response, SessionId, Tokens};
use process::{Gave, Given, Identity, Living, Path, ProcessManager, Root, Trace, Traced, Tracing};

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

fn flow_id() -> FlowId {
    FlowId("5bf476".into())
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

fn report(hook: Hook) -> World {
    let mut manager = ProcessManager::default();
    let mut world = World::default();
    let process = Process::Report(ReportProcess { caller: Caller { process_id: ProcessId(48211), user_id: UserId(1000) }, hook });
    manager.bear(Living { identity: root(), process }, &mut world);
    world
}

fn watch(rollout: Rollout, thread_database_option: Option<ThreadDatabase>) -> (ProcessManager<Process>, World) {
    let mut manager = ProcessManager::default();
    let mut world = World::default();
    let process = Process::Watch(WatchProcess { flow_id: flow_id(), session_id: session_id(), rollout, offset: Offset(0), row_count: RowCount(64), thread_database_option });
    manager.bear(Living { identity: root(), process }, &mut world);
    (manager, world)
}

#[test]
fn a_codex_flow_is_named_from_the_random_tail_of_its_thread_id_as_flow_id_codex_names_it() {
    assert_eq!(session_id().identify(), flow_id());
}

/// Two sessions Codex began four minutes apart share their first six
/// characters, and are two flows.
#[test]
fn two_codex_sessions_begun_close_together_are_two_flows() {
    let first = SessionId("01a1fc7b-1137-73b1-81ce-cafdb261d078".into());
    let second = SessionId("01a1fc7f-bbad-7730-b6bc-02328450444f".into());
    assert_ne!(first.identify(), second.identify());
}

#[test]
fn a_session_start_with_its_rollout_keeps_a_located_watch() {
    let rollout = FilePath("/home/example/.codex/sessions/2026/11/20/rollout-2026-11-20T21-49-50-01a1fc39-854c-7721-8339-0495bf476131.jsonl".into());
    let world = report(Hook::SessionStart(SessionStart { session_id: session_id(), file_path_option: Some(rollout.clone()) }));
    assert_eq!(
        gifts(&world),
        vec![
            Gift::Reported,
            Gift::Watch(Watch { flow_id: flow_id(), session_id: session_id(), rollout: Rollout::Located(rollout), offset: Offset(0) }),
            Gift::Observe(Observe { flow_id: flow_id(), event_vector: vec![Event::Started], offset_option: None }),
        ]
    );
}

#[test]
fn a_session_start_without_its_rollout_keeps_an_unlocated_watch() {
    let world = report(Hook::SessionStart(SessionStart { session_id: session_id(), file_path_option: None }));
    assert_eq!(gifts(&world)[1], Gift::Watch(Watch { flow_id: flow_id(), session_id: session_id(), rollout: Rollout::Unlocated, offset: Offset(0) }));
}

#[test]
fn a_compaction_is_observed_with_its_trigger() {
    let world = report(Hook::PostCompact(PostCompact { session_id: session_id(), trigger: harness::Trigger::Automatic }));
    let compaction = harness::Compaction { trigger: harness::Trigger::Automatic, tokens_option: None };
    assert_eq!(gifts(&world)[1], Gift::Observe(Observe { flow_id: flow_id(), event_vector: vec![Event::Compacted(compaction)], offset_option: None }));
}

#[test]
fn an_unlocated_watch_finds_its_rollout_keeps_it_and_follows_it() {
    let database = ThreadDatabase("/home/example/.codex/state_5.sqlite".into());
    let (mut manager, mut world) = watch(Rollout::Unlocated, Some(database.clone()));
    let rollout = FilePath("/home/example/.codex/sessions/rollout.jsonl".into());
    let taken = Taken::RolloutFinding(Ok(RolloutFinding::Found(rollout.clone())));
    manager.hand(Given { identity: root(), taken }, &mut world);
    assert_eq!(
        gifts(&world),
        vec![
            Gift::FindRollout(FindRollout { thread_database: database, session_id: session_id() }),
            Gift::Locate(Locate { flow_id: flow_id(), file_path: rollout.clone() }),
            Gift::Follow(Follow { file_path: rollout, offset: Offset(0), row_count: RowCount(64) }),
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
    manager.hand(Given { identity: root(), taken: Taken::RolloutFinding(Ok(RolloutFinding::Unfound)) }, &mut world);
    assert_eq!(ended(&world), vec![Outcome::Watch(Watching::Unlocated)]);
}

#[test]
fn a_turn_read_from_the_rollout_is_observed_with_its_new_offset() {
    let rollout = FilePath("/t/rollout.jsonl".into());
    let (mut manager, mut world) = watch(Rollout::Located(rollout), None);
    let following = Following {
        row_vector: vec![Row::TaskStarted, Row::Other, Row::TokenCount(Some(Tokens(19147))), Row::TokenCount(None), Row::TaskComplete(Some(Response("Listed.".into()))), Row::TurnAborted],
        offset: Offset(4096),
    };
    manager.hand(Given { identity: root(), taken: Taken::Following(Ok(following)) }, &mut world);
    assert_eq!(
        gifts(&world)[1],
        Gift::Observe(Observe {
            flow_id: flow_id(),
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
