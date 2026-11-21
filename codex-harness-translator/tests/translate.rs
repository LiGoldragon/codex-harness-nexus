//! Codex's own words, read into the Nexus's types: a hook's payload and a
//! rollout's rows, in the shapes Codex 0.169 writes them.

use codex_harness_process::*;
use codex_harness_translator::{Translatable, Untranslated};
use harness::{FilePath, Response, SessionId, Tokens, Trigger};

const THREAD: &str = "01a1fc39-854c-7721-8339-0495bf476131";

#[test]
fn a_session_start_names_its_rollout_or_null() {
    let named = format!(r#"{{"hook_event_name":"SessionStart","session_id":"{THREAD}","transcript_path":"/t/rollout.jsonl","source":"startup"}}"#);
    let null = format!(r#"{{"hook_event_name":"SessionStart","session_id":"{THREAD}","transcript_path":null,"source":"startup"}}"#);
    assert_eq!(Hook::translate(&named), Ok(Hook::SessionStart(SessionStart { session_id: SessionId(THREAD.into()), file_path_option: Some(FilePath("/t/rollout.jsonl".into())) })));
    assert_eq!(Hook::translate(&null), Ok(Hook::SessionStart(SessionStart { session_id: SessionId(THREAD.into()), file_path_option: None })));
}

#[test]
fn a_compaction_names_its_trigger() {
    let payload = format!(r#"{{"hook_event_name":"PostCompact","session_id":"{THREAD}","trigger":"auto"}}"#);
    assert_eq!(Hook::translate(&payload), Ok(Hook::PostCompact(PostCompact { session_id: SessionId(THREAD.into()), trigger: Trigger::Automatic })));
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

/// The hooks Codex 0.169.0 fired in an interactive session, verbatim but for
/// fields the Nexus does not read (flows/6fa457/harness-evidence/codex-hooks.md).
#[test]
fn the_hooks_of_a_real_session_are_read() {
    const REAL: &str = "01a1fc7f-bbad-7730-b6bc-02328450444f";
    const ROLLOUT: &str = "/tmp/hooks-1A6J/home/sessions/2026/11/20/rollout-2026-11-20T23-06-31-01a1fc7f-bbad-7730-b6bc-02328450444f.jsonl";
    let start = format!(r#"{{"session_id":"{REAL}","transcript_path":"{ROLLOUT}","cwd":"/tmp/hooks-1A6J/work","hook_event_name":"SessionStart","model":"gpt-6.2-sol","permission_mode":"default","source":"startup"}}"#);
    let permission = format!(r#"{{"session_id":"{REAL}","turn_id":"01a1fc7f-f96f-7880-b936-5168f8fb067e","transcript_path":"{ROLLOUT}","cwd":"/tmp/hooks-1A6J/work","hook_event_name":"PermissionRequest","model":"gpt-6.2-sol","permission_mode":"default","tool_name":"Bash","tool_input":{{"command":"touch /home/li/hooks-probe-ce63a9.txt"}}}}"#);
    let tool = format!(r#"{{"session_id":"{REAL}","turn_id":"01a1fc7f-f96f-7880-b936-5168f8fb067e","transcript_path":"{ROLLOUT}","cwd":"/tmp/hooks-1A6J/work","hook_event_name":"PostToolUse","model":"gpt-6.2-sol","permission_mode":"default","tool_name":"Bash","tool_input":{{"command":"ls -la"}},"tool_response":"","tool_use_id":"exec-c194a840-858b-451b-94f9-7ea2dbed7966"}}"#);
    let compact = format!(r#"{{"session_id":"{REAL}","turn_id":"01a1fc80-7f62-7d62-8b5e-e852a88d730c","transcript_path":"{ROLLOUT}","cwd":"/tmp/hooks-1A6J/work","hook_event_name":"PostCompact","model":"gpt-6.2-sol","trigger":"manual"}}"#);
    let session_id = || SessionId(REAL.into());
    assert_eq!(Hook::translate(&start), Ok(Hook::SessionStart(SessionStart { session_id: session_id(), file_path_option: Some(FilePath(ROLLOUT.into())) })));
    assert_eq!(Hook::translate(&permission), Ok(Hook::PermissionRequest(session_id())));
    assert_eq!(Hook::translate(&tool), Ok(Hook::PostToolUse(session_id())));
    assert_eq!(Hook::translate(&compact), Ok(Hook::PostCompact(PostCompact { session_id: session_id(), trigger: Trigger::Manual })));
}
