//! The additive administrative surface of Codex Harness. This process has no
//! hook receiver, observation/watch store, translator connection or model list.
#[path = "generated/meta.rs"]
mod meta;
use datom_codec::Datomizable;
use meta::{Answerable, Answering, Input, Output, Refusal, ServerAddress, Set};
use protos::{Protosizable, Textualizable};
use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::{
        fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};
use transport::{Framed, Place};

const DIRECTORY: &str = "codex-harness-selector-control";
const WIRE_LIMIT: u64 = 8192;
struct Control {
    state: PathBuf,
    runtime: PathBuf,
    _writer: File,
    _listener_owner: File,
}
fn invalid(text: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, text)
}
fn directory(parent: &Path) -> io::Result<PathBuf> {
    // Only the dedicated child is created; neither existing observer Place is prepared.
    let dir = parent.join(DIRECTORY);
    match fs::DirBuilder::new().mode(0o700).create(&dir) {
        Ok(()) => File::open(parent)?.sync_all()?,
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e),
    }
    let meta = fs::symlink_metadata(&dir)?;
    if !meta.is_dir() || meta.permissions().mode() & 0o077 != 0 {
        return Err(invalid("control requires its private 0700 directory"));
    }
    Ok(dir)
}
fn lock(path: &Path) -> io::Result<File> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(path)?;
    if !f.metadata()?.is_file() {
        return Err(invalid("invalid ownership file"));
    }
    f.try_lock()
        .map_err(|e| io::Error::other(format!("control already owned or unavailable: {e}")))?;
    Ok(f)
}
fn atomic(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let tmp = dir.join(format!("{name}.next"));
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    fs::rename(tmp, dir.join(name))?;
    File::open(dir)?.sync_all()
}
impl Control {
    fn open(place: &Place) -> io::Result<Self> {
        let state = directory(Path::new(&place.state.0))?;
        let runtime = directory(Path::new(&place.runtime.0))?;
        if state == runtime {
            return Err(invalid("state and runtime must differ"));
        }
        let w = lock(&state.join("writer.lock"))?;
        let l = lock(&runtime.join("listener.lock"))?;
        Ok(Self {
            state,
            runtime,
            _writer: w,
            _listener_owner: l,
        })
    }
    fn read(&self) -> io::Result<Option<ServerAddress>> {
        let path = self.state.join("starting-server-v1.signal");
        let mut f = match OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(path)
        {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let len = f.metadata()?.len();
        if !(4..=WIRE_LIMIT).contains(&len) {
            return Err(invalid("bad Memory size"));
        }
        let mut bytes = Vec::new();
        (&mut f).take(WIRE_LIMIT + 1).read_to_end(&mut bytes)?;
        if bytes.len() < 4
            || bytes.len() as u64 != len
            || u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize != bytes.len() - 4
        {
            return Err(invalid("bad Memory frame"));
        }
        Option::<ServerAddress>::read_from(&mut &bytes[..])?
            .ok_or_else(|| invalid("truncated Memory"))
    }
    fn socket(&self) -> PathBuf {
        self.runtime.join("meta.sock")
    }
    fn retire_stale(&self) -> io::Result<()> {
        let socket = self.socket();
        let meta = match fs::symlink_metadata(&socket) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        if !meta.file_type().is_socket()
            || fs::read_to_string(self.runtime.join("owned-listener"))?
                != format!("{} {}", meta.dev(), meta.ino())
        {
            return Err(invalid("refusing foreign listener"));
        }
        match UnixStream::connect(&socket) {
            Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => fs::remove_file(&socket),
            _ => Err(invalid("refusing possibly live listener")),
        }
    }
    fn reply(&self, mut stream: UnixStream) -> io::Result<()> {
        stream.set_read_timeout(Some(Duration::from_secs(3)))?;
        stream.set_write_timeout(Some(Duration::from_secs(3)))?;
        let mut request = String::new();
        let len = (&mut stream)
            .take(WIRE_LIMIT + 1)
            .read_to_string(&mut request)?;
        let output = if len as u64 > WIRE_LIMIT {
            Output::Refused(Refusal::InvalidRequest)
        } else {
            match transport::actualize::<Input>(&request) {
                Ok(input) => input.answer(self),
                Err(_) => Output::Refused(Refusal::InvalidRequest),
            }
        };
        writeln!(
            stream,
            "{}",
            output.datomize(vec![]).protosize().textualize()
        )
    }
    fn serve(&self) -> io::Result<()> {
        // Refuse corrupt configuration rather than interpreting it as no setting.
        self.read()?;
        self.retire_stale()?;
        let path = self.socket();
        let l = UnixListener::bind(&path)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        let m = fs::symlink_metadata(&path)?;
        atomic(
            &self.runtime,
            "owned-listener",
            format!("{} {}", m.dev(), m.ino()).as_bytes(),
        )?;
        for stream in l.incoming() {
            match stream {
                Ok(s) => {
                    if let Err(e) = self.reply(s) {
                        eprintln!("control request: {e}");
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}
impl Answering for Control {
    fn configure(&self, set: &Set) -> Result<Set, Output> {
        self.read()
            .map_err(|_| Output::Refused(Refusal::UnreadableMemory))?;
        let value = match set {
            Set::StartingServer(a)
                if Path::new(&a.0).is_absolute() && !a.0.chars().any(char::is_control) =>
            {
                Some(a.clone())
            }
            Set::NoStartingServer => None,
            _ => return Err(Output::Refused(Refusal::InvalidServerAddress)),
        };
        let mut bytes = Vec::new();
        value
            .write_to(&mut bytes)
            .and_then(|_| {
                if bytes.len() as u64 > WIRE_LIMIT {
                    return Err(invalid("oversized Memory"));
                }
                atomic(&self.state, "starting-server-v1.signal", &bytes)
            })
            .map_err(|_| Output::Refused(Refusal::Uncommitted))?;
        Ok(set.clone())
    }
    fn observe_starting_server(&self) -> Result<Option<ServerAddress>, Output> {
        self.read()
            .map_err(|_| Output::Refused(Refusal::UnreadableMemory))
    }
}
fn run() -> io::Result<()> {
    let a: Vec<_> = env::args().skip(1).collect();
    let (retire, raw) = match a.as_slice() {
        [place] => (false, place),
        [mode, place] if mode == "retire" => (true, place),
        _ => {
            return Err(invalid(
                "usage: codex-harness-selector-control [retire] '<Place>'",
            ));
        }
    };
    let place = transport::actualize::<Place>(raw).map_err(|_| invalid("invalid Place"))?;
    let c = Control::open(&place)?;
    if retire { c.retire_stale() } else { c.serve() }
}
fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("codex-harness-selector-control: {e}");
            ExitCode::FAILURE
        }
    }
}
