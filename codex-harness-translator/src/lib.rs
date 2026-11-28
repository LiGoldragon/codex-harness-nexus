//! The Codex Harness Nexus's translator: the one place that holds Codex's
//! words. It reads a hook's payload into a Hook, follows a rollout for its
//! Nexus, reading each new row, finds a rollout in Codex's own record of
//! its threads, and discovers each session's rollout under Codex's root.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Seek, SeekFrom};
use std::io::Write;
use std::mem::MaybeUninit;
use std::net::Shutdown;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use codex_harness_process::*;
use harness::{FilePath, Response, SessionId, Tokens, Trigger};
use process::{Caller, ProcessId, UserId};
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
            "SessionStart" => Hook::SessionStart(SessionStart_Data {
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
            "PostCompact" => Hook::PostCompact(PostCompact_Data {
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
fn rows_after(path: &str, offset: &Offset, row_count: &RowCount) -> io::Result<Following_Data> {
    let mut file = BufReader::new(File::open(path)?);
    file.seek(SeekFrom::Start(offset.0.max(0) as u64))?;
    let mut rows = Following_Data { row_vector: Vec::new(), offset: *offset };
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

/// A path watched once it exists. Until then its nearest existing ancestor is
/// watched for what appears in it, and each time something does the path is
/// tried again.
struct Appearing {
    path: PathBuf,
    ancestor: Option<(PathBuf, i32)>,
}

impl Appearing {
    fn new(path: PathBuf) -> Appearing {
        Appearing { path, ancestor: None }
    }

    /// The path's watch once it exists, None while it is waited for. A path
    /// with no ancestor at all is NotFound.
    fn watch(&mut self, watching: &OwnedFd, flags: inotify::WatchFlags) -> io::Result<Option<i32>> {
        loop {
            if let Ok(watch) = inotify::add_watch(watching, &self.path, flags) {
                if let Some((_, ancestor)) = self.ancestor.take() {
                    inotify::remove_watch(watching, ancestor)?;
                }
                return Ok(Some(watch));
            }
            let Some(nearest) = self.path.ancestors().skip(1).find(|ancestor| ancestor.is_dir()).map(Path::to_owned) else {
                return Err(io::Error::new(io::ErrorKind::NotFound, "no ancestor of the path exists"));
            };
            if self.ancestor.as_ref().is_some_and(|(directory, _)| *directory == nearest) {
                return Ok(None);
            }
            if let Some((_, ancestor)) = self.ancestor.take() {
                inotify::remove_watch(watching, ancestor)?;
            }
            let ancestor = inotify::add_watch(watching, &nearest, inotify::WatchFlags::CREATE | inotify::WatchFlags::MOVED_TO)?;
            self.ancestor = Some((nearest, ancestor));
        }
    }
}

/// Waits for what the watches report or for the Nexus to close the connection,
/// true when it has closed.
fn closed(watching: &OwnedFd, stream: &UnixStream) -> io::Result<bool> {
    let mut waiting = [PollFd::new(watching, PollFlags::IN), PollFd::new(stream, PollFlags::IN)];
    rustix::event::poll(&mut waiting, None)?;
    Ok(!waiting[1].revents().is_empty())
}

/// A lasting emission: an outcome each time the rollout grows, until the
/// Nexus closes the connection. A rollout not yet written is waited for:
/// its nearest existing ancestor is watched until each missing part appears.
/// A path with no ancestor at all ends the connection, and the emission is Unanswered.
pub fn follow(mut stream: UnixStream, follow: Follow_Data) -> io::Result<()> {
    let watching = inotify::init(inotify::CreateFlags::CLOEXEC)?;
    let mut appearing = Appearing::new(PathBuf::from(&follow.file_path.0));
    let mut followed = false;
    let mut read_from = follow.read_from.clone();
    let mut events = [0u8; 4096];
    loop {
        if !followed {
            followed = appearing.watch(&watching, inotify::WatchFlags::MODIFY)?.is_some();
        }
        if followed {
            let offset = match &read_from {
                ReadFrom::Offset(offset) => *offset,
                ReadFrom::FileEnd => Offset(std::fs::metadata(&appearing.path)?.len().try_into().unwrap_or(i64::MAX)),
            };
            let rows = rows_after(&follow.file_path.0, &offset, &follow.row_count)?;
            read_from = ReadFrom::Offset(rows.offset);
            let full = rows.row_vector.len() as i64 >= follow.row_count.0;
            if !rows.row_vector.is_empty() {
                rows.write_to(&mut stream)?;
            }
            if full {
                continue;
            }
        }
        if closed(&watching, &stream)? {
            return Ok(());
        }
        rustix::io::read(&watching, &mut events)?;
    }
}

/// A session's rollout, as Codex writes it: a file named `rollout-`, its
/// moment, then the thread's id, a canonical lowercase UUID, then `.jsonl`.
/// The id is the session's, as its hooks and its first row, session_meta, name it.
pub fn rollout_session(path: &Path) -> Option<SessionId> {
    let name = path.file_name()?.to_str()?;
    let stem = name.strip_prefix("rollout-")?.strip_suffix(".jsonl")?;
    let id = stem.get(stem.len().checked_sub(36)?..)?;
    let moment = &stem[..stem.len() - 36];
    let canonical = id.char_indices().all(|(position, character)| match position {
        8 | 13 | 18 | 23 => character == '-',
        _ => matches!(character, '0'..='9' | 'a'..='f'),
    });
    (canonical && moment.len() > 1 && moment.ends_with('-')).then(|| SessionId(id.to_owned()))
}

/// The rollouts a live process holds open for writing, as Codex holds the
/// rollout of each session it is running: every file descriptor of every
/// process this user may read. A file only read, or no longer written, is no
/// live session's.
fn held_rollouts() -> Vec<PathBuf> {
    let Ok(processes) = std::fs::read_dir("/proc") else { return Vec::new() };
    let mut held = Vec::new();
    for process in processes.flatten() {
        if !process.file_name().to_str().is_some_and(|name| name.bytes().all(|byte| byte.is_ascii_digit())) {
            continue;
        }
        let Ok(descriptors) = std::fs::read_dir(process.path().join("fd")) else { continue };
        for descriptor in descriptors.flatten() {
            let Ok(target) = std::fs::read_link(descriptor.path()) else { continue };
            if rollout_session(&target).is_none() {
                continue;
            }
            let information = std::fs::read_to_string(process.path().join("fdinfo").join(descriptor.file_name())).unwrap_or_default();
            let written = information
                .lines()
                .find_map(|line| line.strip_prefix("flags:"))
                .and_then(|flags| u32::from_str_radix(flags.trim(), 8).ok())
                .is_some_and(|flags| flags & 0o3 != 0);
            if written {
                held.push(target);
            }
        }
    }
    held
}

/// What one discovery holds: the root, waited for until it exists; each
/// directory under it, by its watch; and each rollout reported.
struct Discovery {
    stream: UnixStream,
    watching: OwnedFd,
    root: Appearing,
    directories: BTreeMap<i32, PathBuf>,
    reported: BTreeSet<PathBuf>,
}

impl Discovery {
    /// The root is watched for what is created or moved into it.
    const ROOTED: inotify::WatchFlags = inotify::WatchFlags::CREATE.union(inotify::WatchFlags::MOVED_TO).union(inotify::WatchFlags::ONLYDIR);
    /// A directory under it is too, and never through a link: what is outside
    /// the root is no session of it.
    const WATCHED: inotify::WatchFlags = Discovery::ROOTED.union(inotify::WatchFlags::DONT_FOLLOW);

    /// Each directory directly in a directory, as it is now, never a link.
    fn directories_in(directory: &Path) -> Vec<PathBuf> {
        let Ok(entries) = std::fs::read_dir(directory) else { return Vec::new() };
        entries.flatten().filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir())).map(|entry| entry.path()).collect()
    }

    /// A directory and every directory under it are watched; what they hold
    /// is reported only when the directory is new.
    fn watch(&mut self, directory: PathBuf, new: bool) -> io::Result<()> {
        let flags = if directory == self.root.path {
            Discovery::ROOTED
        } else if std::fs::symlink_metadata(&directory).is_ok_and(|metadata| metadata.is_dir()) {
            Discovery::WATCHED
        } else {
            return Ok(());
        };
        if let Ok(watch) = inotify::add_watch(&self.watching, &directory, flags) {
            self.directories.insert(watch, directory.clone());
        }
        if new && let Ok(entries) = std::fs::read_dir(&directory) {
            for entry in entries.flatten() {
                if entry.file_type().is_ok_and(|kind| kind.is_file()) {
                    self.report(entry.path())?;
                }
            }
        }
        for inner in Discovery::directories_in(&directory) {
            self.watch(inner, new)?;
        }
        Ok(())
    }

    /// A session's rollout is reported once; a link, or what is not a rollout, never.
    fn report(&mut self, path: PathBuf) -> io::Result<()> {
        let Some(session_id) = rollout_session(&path) else { return Ok(()) };
        if !std::fs::symlink_metadata(&path).is_ok_and(|metadata| metadata.is_file()) || !self.reported.insert(path.clone()) {
            return Ok(());
        }
        Discovered_Data { session_id, file_path: FilePath(path.to_string_lossy().into_owned()) }.write_to(&mut self.stream)
    }

    /// The root has come to exist: every directory in it is watched. What is
    /// in a root that appeared after the discovery began is new, and reported;
    /// what was there before is reported only when a live process holds it.
    fn rooted(&mut self, appeared: bool) -> io::Result<()> {
        let root = self.root.path.clone();
        self.watch(root.clone(), appeared)?;
        let Ok(canonical) = std::fs::canonicalize(&root) else { return Ok(()) };
        for held in held_rollouts() {
            if let Ok(inner) = held.strip_prefix(&canonical) {
                self.report(root.join(inner))?;
            }
        }
        Ok(())
    }

    /// What the watches have reported since the last read: each watch, what it
    /// saw and the name it saw it on.
    fn events(&self, buffer: &mut [MaybeUninit<u8>]) -> io::Result<Vec<(i32, inotify::ReadFlags, Option<std::ffi::OsString>)>> {
        use std::os::unix::ffi::OsStrExt;
        let mut reader = inotify::Reader::new(&self.watching, buffer);
        let mut events = Vec::new();
        loop {
            let event = reader.next()?;
            let name = event.file_name().map(|name| std::ffi::OsStr::from_bytes(name.to_bytes()).to_owned());
            events.push((event.wd(), event.events(), name));
            if reader.is_buffer_empty() {
                return Ok(events);
            }
        }
    }
}

/// A lasting emission: a Discovered for each session's rollout that appears
/// under the root, at any depth, as Codex nests them by day, until the Nexus
/// closes the connection. A directory that appears is new whole: each rollout
/// in it is reported. A rollout that was there when the discovery began is
/// reported only when a live process holds it open for writing, a session
/// begun while no discovery ran. A root that does not exist yet is waited for,
/// as a rollout is; what then appears in it is new.
pub fn discover(stream: UnixStream, discover: Discover_Data) -> io::Result<()> {
    let watching = inotify::init(inotify::CreateFlags::CLOEXEC)?;
    let mut discovery = Discovery { stream, watching, root: Appearing::new(PathBuf::from(&discover.file_path.0)), directories: BTreeMap::new(), reported: BTreeSet::new() };
    let mut rooted = discovery.root.watch(&discovery.watching, Discovery::ROOTED)?.is_some();
    if rooted {
        discovery.rooted(false)?;
    }
    let mut buffer = [MaybeUninit::<u8>::uninit(); 4096];
    loop {
        if closed(&discovery.watching, &discovery.stream)? {
            return Ok(());
        }
        let events = discovery.events(&mut buffer)?;
        if !rooted {
            if discovery.root.watch(&discovery.watching, Discovery::ROOTED)?.is_some() {
                rooted = true;
                discovery.rooted(true)?;
            }
            continue;
        }
        for (watch, seen, name) in events {
            if seen.contains(inotify::ReadFlags::IGNORED) {
                discovery.directories.remove(&watch);
                continue;
            }
            let (Some(name), Some(directory)) = (name, discovery.directories.get(&watch)) else { continue };
            let path = directory.join(name);
            if seen.contains(inotify::ReadFlags::ISDIR) {
                discovery.watch(path, true)?;
            } else {
                discovery.report(path)?;
            }
        }
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
pub fn find(find_rollout: &FindRollout_Data) -> RolloutFinding_Data {
    let opened = rusqlite::Connection::open_with_flags(&find_rollout.thread_database.0, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX);
    let found = opened.and_then(|database| {
        database.query_row("SELECT rollout_path FROM threads WHERE id = ?1", [&find_rollout.session_id.0], |row| row.get::<_, String>(0))
    });
    match found {
        Ok(path) => RolloutFinding_Data::Found(FilePath(path)),
        Err(_) => RolloutFinding_Data::Unfound,
    }
}
