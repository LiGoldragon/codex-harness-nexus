//! The Codex Harness Nexus's translator: the one place that holds Codex's
//! words. It reads a hook's payload into a Hook, follows a rollout for its
//! Nexus, reading each new row, and finds a rollout in Codex's own record of
//! its threads.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Seek, SeekFrom};
use std::io::Write;
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::process::ExitCode;

use codex_harness_process::*;
use harness::{FilePath, Response, SessionId, Tokens, Trigger};
use rustix::event::{PollFd, PollFlags};
use rustix::fs::inotify;
use serde_json::Value;
use transport::{Framed, Named, Place, Placed};

pub struct CodexHarness;

impl Named for CodexHarness {
    const NAME: &'static str = NAME;
}

#[derive(Debug, PartialEq, Eq)]
pub enum Untranslated {
    Unreadable,
    UnknownEvent(String),
    MissingField(&'static str),
}

/// What reads the harness's own words into a type of the Nexus.
pub trait Translatable: Sized {
    fn translate(text: &str) -> Result<Self, Untranslated>;
}

fn text(value: &Value, field: &'static str) -> Result<String, Untranslated> {
    value.get(field).and_then(Value::as_str).map(str::to_owned).ok_or(Untranslated::MissingField(field))
}

impl Translatable for Hook {
    /// Codex's hook payload: its event's name, the thread's id, and for a
    /// session's start the rollout's path, which may be null.
    fn translate(payload: &str) -> Result<Hook, Untranslated> {
        let value: Value = serde_json::from_str(payload).map_err(|_| Untranslated::Unreadable)?;
        let session_id = || text(&value, "session_id").map(SessionId);
        Ok(match text(&value, "hook_event_name")?.as_str() {
            "SessionStart" => Hook::SessionStart(SessionStart {
                session_id: session_id()?,
                file_path_option: value.get("transcript_path").and_then(Value::as_str).filter(|path| !path.trim().is_empty()).map(|path| FilePath(path.to_owned())),
                source: match text(&value, "source").ok().as_deref() {
                    Some("resume") => Source::Resume,
                    Some("compact") => Source::Compact,
                    _ => Source::Startup,
                },
            }),
            "PermissionRequest" => Hook::PermissionRequest(session_id()?),
            "PostToolUse" => Hook::PostToolUse(session_id()?),
            "PostCompact" => Hook::PostCompact(PostCompact {
                session_id: session_id()?,
                trigger: match text(&value, "trigger")?.as_str() {
                    "manual" => Trigger::Manual,
                    "auto" => Trigger::Automatic,
                    other => return Err(Untranslated::UnknownEvent(format!("compaction trigger {other}"))),
                },
            }),
            other => return Err(Untranslated::UnknownEvent(other.to_owned())),
        })
    }
}

impl Translatable for Row {
    /// A rollout row is told by payload.type; every other row is Other. A
    /// count of no input, as Codex writes after a compaction, is no size.
    fn translate(line: &str) -> Result<Row, Untranslated> {
        let value: Value = serde_json::from_str(line).map_err(|_| Untranslated::Unreadable)?;
        Ok(match value.pointer("/payload/type").and_then(Value::as_str) {
            Some("task_started") => Row::TaskStarted,
            Some("turn_aborted") => Row::TurnAborted,
            Some("token_count") => Row::TokenCount(value.pointer("/payload/info/last_token_usage/input_tokens").and_then(Value::as_i64).filter(|input| *input > 0).map(Tokens)),
            Some("task_complete") => Row::TaskComplete(value.pointer("/payload/last_agent_message").and_then(Value::as_str).map(|response| Response(response.to_owned()))),
            _ => Row::Other,
        })
    }
}

/// Reads the whole lines after the offset, at most the row count of them.
fn rows_after(path: &str, offset: &Offset, row_count: &RowCount) -> io::Result<Following> {
    let mut file = BufReader::new(File::open(path)?);
    file.seek(SeekFrom::Start(offset.0.max(0) as u64))?;
    let mut rows = Following { row_vector: Vec::new(), offset: offset.clone() };
    let mut line = Vec::new();
    while (rows.row_vector.len() as i64) < row_count.0 {
        line.clear();
        let read = file.read_until(b'\n', &mut line)?;
        if read == 0 || line.last() != Some(&b'\n') {
            break;
        }
        rows.offset.0 += read as i64;
        rows.row_vector.push(Row::translate(&String::from_utf8_lossy(&line)).unwrap_or(Row::Other));
    }
    Ok(rows)
}

/// A lasting emission: an outcome each time the rollout grows, until the
/// Nexus closes the connection. A rollout not yet written is waited for:
/// its nearest existing ancestor is watched until each missing part appears.
/// A path with no ancestor at all ends the connection, and the emission is Unanswered.
pub fn follow(mut stream: UnixStream, follow: Follow) -> io::Result<()> {
    let path = std::path::PathBuf::from(&follow.file_path.0);
    let watching = inotify::init(inotify::CreateFlags::CLOEXEC)?;
    let mut appearing: Option<(std::path::PathBuf, i32)> = None;
    let mut followed = false;
    let mut read_from = follow.read_from.clone();
    let mut events = [0u8; 4096];
    loop {
        while !followed {
            if inotify::add_watch(&watching, &path, inotify::WatchFlags::MODIFY).is_ok() {
                followed = true;
                if let Some((_, watch)) = appearing.take() {
                    inotify::remove_watch(&watching, watch)?;
                }
                break;
            }
            let Some(nearest) = path.ancestors().skip(1).find(|ancestor| ancestor.is_dir()).map(std::path::Path::to_owned) else {
                return Err(io::Error::new(io::ErrorKind::NotFound, "no ancestor of the rollout exists"));
            };
            if appearing.as_ref().is_some_and(|(directory, _)| *directory == nearest) {
                break;
            }
            if let Some((_, watch)) = appearing.take() {
                inotify::remove_watch(&watching, watch)?;
            }
            let watch = inotify::add_watch(&watching, &nearest, inotify::WatchFlags::CREATE | inotify::WatchFlags::MOVED_TO)?;
            appearing = Some((nearest, watch));
        }
        if followed {
            let offset = match &read_from {
                ReadFrom::Offset(offset) => offset.clone(),
                ReadFrom::FileEnd => Offset(std::fs::metadata(&path)?.len().try_into().unwrap_or(i64::MAX)),
            };
            let rows = rows_after(&follow.file_path.0, &offset, &follow.row_count)?;
            read_from = ReadFrom::Offset(rows.offset.clone());
            let full = rows.row_vector.len() as i64 >= follow.row_count.0;
            if !rows.row_vector.is_empty() {
                rows.write_to(&mut stream)?;
            }
            if full {
                continue;
            }
        }
        let mut waiting = [PollFd::new(&watching, PollFlags::IN), PollFd::new(&stream, PollFlags::IN)];
        rustix::event::poll(&mut waiting, None)?;
        if !waiting[1].revents().is_empty() {
            return Ok(());
        }
        rustix::io::read(&watching, &mut events)?;
    }
}

/// The translator itself as a caller: its process, and the user that owns it.
pub fn itself() -> io::Result<Caller> {
    use std::os::unix::fs::MetadataExt;
    let user_id = std::fs::metadata("/proc/self")?.uid();
    Ok(Caller { process_id: ProcessId(std::process::id().into()), user_id: UserId(user_id.into()) })
}

/// The hook's caller, from the connection's peer credentials.
pub fn caller(stream: &UnixStream) -> io::Result<Caller> {
    let credentials = rustix::net::sockopt::socket_peercred(stream)?;
    Ok(Caller { process_id: ProcessId(credentials.pid.as_raw_nonzero().get().into()), user_id: UserId(credentials.uid.as_raw().into()) })
}

/// The CLI, written once for the ordinary and the meta socket: one datom
/// written to the translator; each reply printed as it arrives, one datom a
/// line, until the translator ends the exchange.
pub fn call(text_socket: Placed) -> ExitCode {
    let mut arguments = std::env::args().skip(1);
    let (Some(datom), None) = (arguments.next(), arguments.next()) else {
        eprintln!("Usage");
        return ExitCode::from(2);
    };
    let place = match Place::from_configuration::<CodexHarness>() {
        Ok(place) => place,
        Err(unplaced) => {
            eprintln!("{unplaced:?}");
            return ExitCode::from(2);
        }
    };
    let Ok(mut stream) = UnixStream::connect(place.path::<CodexHarness>(text_socket)) else {
        eprintln!("TranslatorDown");
        return ExitCode::FAILURE;
    };
    if stream.write_all(datom.as_bytes()).is_err() || stream.shutdown(Shutdown::Write).is_err() {
        return ExitCode::FAILURE;
    }
    let mut out = std::io::stdout();
    for line in BufReader::new(stream).lines().map_while(Result::ok) {
        if writeln!(out, "{line}").and_then(|()| out.flush()).is_err() {
            break;
        }
    }
    ExitCode::SUCCESS
}

/// Finds a thread's rollout in Codex's own record of its threads, opened for
/// reading alone; a thread the record does not hold, or a record that cannot
/// be read, is Unfound.
pub fn find(find_rollout: &FindRollout) -> RolloutFinding {
    let opened = rusqlite::Connection::open_with_flags(&find_rollout.thread_database.0, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX);
    let found = opened.and_then(|database| {
        database.query_row("SELECT rollout_path FROM threads WHERE id = ?1", [&find_rollout.session_id.0], |row| row.get::<_, String>(0))
    });
    match found {
        Ok(path) => RolloutFinding::Found(FilePath(path)),
        Err(_) => RolloutFinding::Unfound,
    }
}
