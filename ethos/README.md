The ethos of each root of the Codex Harness Nexus; generated.rs in codex-harness-process is
written by hand from them. Checked with ethos-zero at github:LiGoldragon/ethos-zero 415c37036:

- signal.ethos, meta.ethos, emission.ethos (an Emission written as a Signal): Checked.
  Listening, in the Signal's inputs, is the library's input every Nexus has: its translator
  tells it that it has begun to listen, and it is answered at once by Acknowledged.
- memory.ethos: Checked. A Watch keeps its Session and its Rollout, Located or Unlocated:
  a session's start may name no rollout, and the Watch's process then finds it in Codex's own
  record of its threads, the ThreadDatabase set on the meta surface.
- library.ethos: Checked. The changes and their answers, Observe, Observation, Locate,
  Configure, Recall, Recollection, Answer, and Offset and RowCount, are declared here until
  the Memory page places them.
- process.ethos: Rejected, Root. ethos-zero knows no Process root; the file follows Process.4,
  unchecked until it is ruled.
