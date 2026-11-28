//! The life of a discovery, as the manager traces it: each rollout discovered
//! under the root becomes a Watch of its session, read from its first row.

use codex_harness_process::*;
use flow::FlowId;
use harness::{FilePath, SessionId};
use process::{Answer, Gave, Given, Identity, Living, Ownable, Owner, ProcessManager, Roots, Trace, Traced, Tracing, Unobserved};

#[derive(Default)]
struct World(Vec<Traced<Process>>);

impl Tracing<Process> for World {
    fn trace(&mut self, trace: Traced<Process>) {
        self.0.push(trace);
    }
}

const THREAD: &str = "01a1fc39-854c-7721-8339-0495bf476131";

fn discovery() -> Process {
    Process::Discover(DiscoverProcess { file_path: FilePath("/home/li/.codex/sessions".into()) })
}

#[test]
fn a_discovered_rollout_is_watched_from_its_first_row() {
    let identity: Identity = Roots::default().draw();
    let mut manager = ProcessManager::default();
    let mut world = World::default();
    manager.bear(Living { identity: identity.clone(), process: discovery() }, &mut world);

    let rollout = FilePath(format!("/home/li/.codex/sessions/2026/11/28/rollout-2026-11-28T04-09-56-{THREAD}.jsonl"));
    let found = Taken::Discovered(Ok(Discovered_Data { session_id: SessionId(THREAD.into()), file_path: rollout.clone() }));
    manager.hand(Given { identity: identity.clone(), taken: found.clone() }, &mut world);
    manager.hand(Given { identity: identity.clone(), taken: Taken::Answer(Answer::Succeeded) }, &mut world);

    let gave = |gift| Trace::Gave(Gave { identity: identity.clone(), gift });
    assert_eq!(
        world.0,
        vec![
            Trace::Born(Living { identity: identity.clone(), process: discovery() }),
            gave(Gift::Discover(Discover_Data { file_path: FilePath("/home/li/.codex/sessions".into()) })),
            Trace::Took(Given { identity: identity.clone(), taken: found }),
            gave(Gift::Watch(Watch { flow_id: FlowId("5bf476".into()), session_id: SessionId(THREAD.into()), rollout: Rollout::Located(rollout), read_from: ReadFrom::Offset(Offset(0)) })),
            Trace::Took(Given { identity: identity.clone(), taken: Taken::Answer(Answer::Succeeded) }),
        ]
    );
}

#[test]
fn a_discovery_whose_emission_fails_ends_undiscovered() {
    let identity: Identity = Roots::default().draw();
    let mut manager = ProcessManager::default();
    let mut world = World::default();
    manager.bear(Living { identity: identity.clone(), process: discovery() }, &mut world);
    manager.hand(Given { identity: identity.clone(), taken: Taken::Discovered(Err(Unobserved::Unanswered)) }, &mut world);
    assert_eq!(world.0.last(), Some(&Trace::Ended(process::Ended { identity, outcome: Outcome::Discover(Discovering::Undiscovered) })));
}

#[test]
fn a_discovery_is_owned_by_the_memory() {
    assert_eq!(discovery().owner(), Owner::Memory);
}
