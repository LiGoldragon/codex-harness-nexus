The ethos of each root of the Codex Harness Nexus, in the forms of ethos-zero 31.1.1
(github:LiGoldragon/ethos-zero, 8d69f1074401f3503c7311894c177b5b57e6a161).

All six roots are checked and generated together, into codex-harness-process/src/generated/,
which is checked in; nothing of the processes is written by hand but their bodies, in
codex-harness-process/src/lib.rs:

```sh
ethos-zero 'Generate.{ [ «ethos/signal.ethos» «ethos/meta.ethos» «ethos/emission.ethos» «ethos/library.ethos» «ethos/memory.ethos» «ethos/process.ethos» ] «codex-harness-process/src/generated» }'
```

- signal.ethos is the Nexus's own contract with its translator and its subscribers. Each
  input is answered by the output of its place: Report by Reported, Subscribe by Subscribed,
  and Listening, the library's input every Nexus has (its translator tells it that it has
  begun to listen), by Acknowledged. Changed, the output a subscriber is sent as each session
  changes, answers no input and stands last. Its inline payload is shared with the Memory's Observation.
- meta.ethos is the meta contract: Configure, answered by Configured.
- emission.ethos is the Emission root: Follow a rollout, and FindRollout in Codex's own record
  of its threads, the ThreadDatabase set on the meta surface.
- library.ethos holds what the roots share: Offset, ReadFrom and RowCount.
- memory.ethos has the Memory's five sections. Its records: Watch, which keeps its SessionId
  and its Rollout, Located or Unlocated, since a session's start may name no rollout;
  Observed; Locate, the rollout found for a Watch; and Keep, the setting a Configure keeps.
  Its changes, Observe and Recall, are answered by Observation and Recollection, by place.
  What returns for a record is the process library's Answer; for a change, its answer or
  Unremembered.
- process.ethos is the Process root: Report, Watch, Subscribe and Configure. Every name it
  imports is declared by the other five roots, so its module is generated whole, and the crate
  includes it as processes, since the process library is the crate process.
