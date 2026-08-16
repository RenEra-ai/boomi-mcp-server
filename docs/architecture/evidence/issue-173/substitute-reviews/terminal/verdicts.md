# Terminal substitute validation — charter and verdict (issue #173)

Reviewed delta: `a996b4dc61edfc01e43bde7858ed06b0ab3b1d6f..2c7feccccc87466793bbe235181218b2b9b13ccc`
(the round-1 correction batch). One independent fresh-context Claude reviewer, read-only.
Charter scoped to blocking-class defects and landing safety, per CLAUDE.md's rule that
non-blocking residue gets ONE batched pass and never reopens a gate.

## Verdict

`VERDICT: BLOCKING ISSUES FOUND` — one P1, plus recorded residue.

## Findings

- **P1 — the commit renames a manifest-registered test node without the manifest
  transaction, so the required gate fails at this tip** (`PYTEST_NODE_MISSING` at a
  pristine checkout). Found INDEPENDENTLY and concurrently by the composite wave gate
  (ledger row W1-1) and by this reviewer. Nothing else catches it: `pytest` is green either
  way, and `manifests --base` alone validates manifest-internal consistency, not the
  collection. (T1-1)
- The reviewer additionally validated the SHAPE of the fix: tombstone-and-append is refused
  (`MANIFEST_TRANSITION_ILLEGAL` — a row appended within the unlanded range was never
  active at the baseline), and an in-place edit is refused only against an intra-slice base
  that is never a CI baseline. One appended row carrying the final node id is the only
  legal form.
- P3 residue, all recorded as ledger rows T1-2 … T1-7: neither round-1 code fix had an
  in-tree witness; the defect-class table contradicted the rows it claimed to derive from;
  the non-ASCII assertions depended on git's `core.quotePath` default; `_REVISION_ID_RE`
  used positional slicing; the template paragraph was not restated after the P1 scoping;
  and two residual escapes (an evil-merge-introduced ledger; `cat-file -e` fail-open) plus
  the instance-patch shape of the flags fix.

## Verified clean by this reviewer

The P1 fix in BOTH directions (fresh ledger now green at the tip; the historical delimiter
parser still fails for a COMMITTED ledger); both flags load-bearing; the archive fixture
now pins `-z` in both directions; the regex lift inert across a 268-id corpus; ledger
integrity exact including the byte-identical restoration of `INH-CR-2`; the withdrawn
deferral genuinely void, measured; counts and scope correct; the slice still dark.
