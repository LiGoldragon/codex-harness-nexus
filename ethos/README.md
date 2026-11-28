The ethos of each root of the Codex Harness Nexus, in the forms of ethos-zero 31.1.1
(github:LiGoldragon/ethos-zero, 8d69f1074401f3503c7311894c177b5b57e6a161).

The signal, meta, emission, library and memory roots are checked and generated together, into
codex-harness-process/src/generated/, which is checked in; their process bodies are in
codex-harness-process/src/lib.rs:

```sh
ethos-zero 'Generate.{ [ «ethos/signal.ethos» «ethos/meta.ethos» «ethos/emission.ethos» «ethos/library.ethos» «ethos/memory.ethos» ] «codex-harness-process/src/generated» }'
```

- signal.ethos is the Nexus's own contract with its translator: Report is answered by Reported,
  and Listening, the library's input every Nexus has (its translator tells it that it has begun
  to listen), by Acknowledged. Subscriptions are served separately by signal-harness.
- meta.ethos is the meta contract: Configure, answered by Configured.
- emission.ethos is the Emission root: Follow a rollout, and FindRollout in Codex's own record
  of its threads, the ThreadDatabase set on the meta surface.
- library.ethos holds what the roots share: Offset, ReadFrom and RowCount.
- memory.ethos has the Memory's five sections. Its records: Watch, which keeps its SessionId
  and its Rollout, Located or Unlocated, since a session's start may name no rollout;
  Observed; Locate, the rollout found for a Watch; and Keep, the setting a Configure keeps.
  Its Observation changes are the same `signal-harness::Changed` values sent to subscribers.
  Its changes, Observe and Recall, are answered by Observation and Recollection, by place.
  What returns for a record is the process library's Answer; for a change, its answer or
  Unremembered.
- process.ethos is the Process root: Report, Watch, Subscribe and Configure. It imports
  Subscribe, Subscribed and Changed from signal-harness. Ethos-zero does not resolve imports
  across crates, so process.ethos is kept as the semantic source but is not generated; its
  process-module output lives in codex-harness-process/src/processes.rs, where the internal
  Changed record is held. memory.ethos imports that record from `codex_harness:processes`.
