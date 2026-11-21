//! Codex's own words, read into the Nexus's types: a hook's payload and a
//! rollout's rows, in the shapes Codex 0.169 writes them.

use codex_harness_process::*;
use codex_harness_translator::{Translatable, Untranslated};
use harness::{FilePath, Response, Session, Tokens, Trigger};

const THREAD: &str = "01a1fc39-854c-7721-8339-0495bf476131";

#[test]
fn a_session_start_names_its_rollout_or_null() {
    let named = format!(r#"{{"hook_event_name":"SessionStart","session_id":"{THREAD}","transcript_path":"/t/rollout.jsonl","source":"startup"}}"#);
    let null = format!(r#"{{"hook_event_name":"SessionStart","session_id":"{THREAD}","transcript_path":null,"source":"startup"}}"#);
    assert_eq!(Hook::translate(&named), Ok(Hook::SessionStart(SessionStart { session: Session(THREAD.into()), file_path_option: Some(FilePath("/t/rollout.jsonl".into())) })));
    assert_eq!(Hook::translate(&null), Ok(Hook::SessionStart(SessionStart { session: Session(THREAD.into()), file_path_option: None })));
}

#[test]
fn a_compaction_names_its_trigger() {
    let payload = format!(r#"{{"hook_event_name":"PostCompact","session_id":"{THREAD}","trigger":"auto"}}"#);
    assert_eq!(Hook::translate(&payload), Ok(Hook::PostCompact(PostCompact { session: Session(THREAD.into()), trigger: Trigger::Automatic })));
}

#[test]
fn a_hook_the_nexus_does_not_hear_is_refused_by_name() {
    let payload = format!(r#"{{"hook_event_name":"Stop","session_id":"{THREAD}"}}"#);
    assert_eq!(Hook::translate(&payload), Err(Untranslated::UnknownEvent("Stop".into())));
}

#[test]
fn each_rollout_row_is_told_by_its_payload_type() {
    let rows = [
        r#"{"type":"event_msg","payload":{"type":"task_started","turn_id":"t"}}"#,
        r#"{"type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":19147}}}}"#,
        r#"{"type":"event_msg","payload":{"type":"token_count","info":null}}"#,
        r#"{"type":"event_msg","payload":{"type":"task_complete","last_agent_message":"Listed."}}"#,
        r#"{"type":"event_msg","payload":{"type":"task_complete","last_agent_message":null}}"#,
        r#"{"type":"event_msg","payload":{"type":"turn_aborted","reason":"interrupted"}}"#,
        r#"{"type":"response_item","payload":{"type":"message"}}"#,
    ];
    let read: Vec<Row> = rows.iter().map(|row| Row::translate(row).expect("a row")).collect();
    assert_eq!(
        read,
        vec![
            Row::TaskStarted,
            Row::TokenCount(Some(Tokens(19147))),
            Row::TokenCount(None),
            Row::TaskComplete(Some(Response("Listed.".into()))),
            Row::TaskComplete(None),
            Row::TurnAborted,
            Row::Other,
        ]
    );
}
