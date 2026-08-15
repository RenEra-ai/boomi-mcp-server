# Audit ledger — issue #NNN (M__.__)  <!-- template; instantiate at Stage-1 step 0 -->

This file is created IN THE STAGE-1.5 BASELINE COMMIT, before the first Stage-2 round —
never mid-slice. Two of the completion workflow's rules are unsatisfiable without that:
the loop roster must exist "in advance, in the audit record, before the first correction
is applied", and a deferral must cite a checkpoint that "already exists as an in-tree
ledger row" — #152 filed two deferrals against checkpoints that existed only in the
session transcript because this file arrived two-thirds through the slice, and both had
to be re-decided at close. Instantiate the template, fill the baseline, commit.

Conventions this template inherits from #152's end state (adopt them from row one, not
after a terminal loop rediscovers them): no hand-maintained totals or instance lists —
counts are derived from the rows; every derived field carries its deriving anchor
inline; platform-behaviour claims carry a provenance marker (`measured here` /
`documented, not measured` / `assumption`); measured output is quoted, never re-keyed
from memory; audit-record-integrity findings are NON-blocking from round one (outside
the eight blocking classes); diagnostic codes named anywhere in this file must exist in
the gate's `DIAGNOSTIC_CODES` and sit inside a backtick span or fence body (the scanner
enforces both, per occurrence).

## Baseline (Stage-1 step 0)

- Issue: #NNN — <title>
- Step-0 baseline: `<full 40-char sha, pasted literally from git rev-parse HEAD>`
- Slice kind: dark | behaviour-affecting
- Artifact trust boundary: <what the slice's deliverable CREATES AND OWNS vs what it
  CONSUMES from outside — declared here so a finding that hardens a self-owned artifact
  gets a boundary verdict at reconciliation instead of a fix batch by default; #152
  spent a commit cluster hardening its own O_EXCL-owned artifact>
- Expected defect classes (pre-enumerated from prior slices so a second instance
  triggers structurally ON ARRIVAL): hand-enumeration shadowing a derivable authority;
  hand-modelled platform behaviour in served text; <add per slice>

## Loop roster (fixed BEFORE the first correction; a gate not listed here cannot mint a
## loop mid-run — a roster addition is itself a recorded checkpoint decision)

1. Stage-1 QA (live scoped pass, or darkness proof per round for a dark slice)
2. Stage-2 repo Codex review (delta-scoped; every round COLLECTED, never read from wait)
3. <each declared additive/downstream gate, in declared order — e.g. §6 architect review>
4. Composite wave gate (full suite + every active golden + determinism + manifests)
5. Terminal correction loop — ONLY via a recorded roster-addition checkpoint

## Defect-class ledger (empty at instantiation; a class is a (mechanism,
## runtime-authority) pair, assigned at reconciliation, revisable with original retained)

| Class | Mechanism | Runtime authority | Instances (derived from rows) | Resolution |
| --- | --- | --- | --- | --- |

Second-instance check: run against this table AT ROW-WRITE TIME, and also when a second
finding lands in the same file/subsystem within a loop (mechanism-family question:
"what single authority do these hand-model?") — narrow per-round class names delayed
#152's trigger to instance 4–7. On the second instance the structural fix is mandatory
in that batch (or the immediately-next dedicated batch for dispatch/compiler/
materialization/shared-apply code), with sibling sweep, non-vacuity witness, and an
authority-derived coverage claim.

## Finding rows (one per raw finding; append-only; exactly one disposition each)

| ID | Source gate + run dir + attestation | Verbatim summary | Original label | Blocking class | Defect class | Derived tier (anchor inline) | SHA/delta | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |

Dispositions: `fixed` · `finding-refuted` · `severity-refuted` · `not-validated` ·
`deferred` (issue, reason class, placement). A refutation names the disputed claim and
the concrete evidence. An original label is never edited — a revision is a new dated
line with the original retained.

## Checkpoints (a row is written IN FLIGHT at every third evaluation of each loop —
## 3, 6, 9, … — in the batch it governs, never reconstructed at close)

| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |
| --- | --- | --- | --- | --- |

Each rationale records: per-tier counts and breadth, new/resolved/recurring defect
classes (derived from the rows), the trend vector, explicit rule-outs of the other
outcomes, and a NAMED finite next correction. The decision comes after the owed
validation and before the next mutation — recorded mid-correction is recorded as such.

## Deferrals

Pointer-only — reason class, placement, and lineage live on the finding row and in the
filed issue; no second hand-copied table (#152's went stale against its own
re-decision). RULE: a deferral's issue body QUOTES the commit SHA of the
already-committed checkpoint row it cites; the issue body is never the first place the
deferral exists. `window-exhausted` is single-use per finding: the next appearance must
be fixed, refuted, or escalated.

## Evidence index

Collected run directories are archived (byte-verified, allowlisted sidecars) under
`docs/architecture/evidence/issue-NNN/` with `index.jsonl` + `SHA256SUMS`, in the batch
that collects them — `/tmp` is session-lifetime, and
`test_audit_ledger_attestations_have_durable_matching_evidence` re-verifies every
archived attestation per collector type. An attestation this file claims and the
archive cannot back is a fabricated row.

## Final-tree validation (filled at close; every roster gate current on the FINAL sha)

| Gate | Evidence (quoted output / run URL / archived round) | SHA |
| --- | --- | --- |
