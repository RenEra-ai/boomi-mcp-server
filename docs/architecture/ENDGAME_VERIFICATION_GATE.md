# The endgame verification gate

Issue #152 (M12.13). Status: shipped. Owner: repository.

Two committed ledgers and one fail-closed command make the mechanical half of the
M12 endgame verification regime automatic:

| Artifact | Purpose |
|---|---|
| `.github/workflows/tests.yml` | the required `Python 3.11 non-KB` check on `dev` |
| `scripts/wave_gate.py` | the gate: `ci`, `wave`, and the non-gate `manifests` |
| `tests/fixtures/wave_gate/test_nodes.jsonl` | required pytest node ids + floors |
| `tests/fixtures/wave_gate/goldens.jsonl` | the golden corpus inventory |
| `tests/_wave_gate_golden_corpus.py` | the closed case registry (row → bytes) |
| `tests/test_wave_gate.py` | the negative matrix |
| `tests/test_wave_gate_goldens.py` | per-commit execution of every active golden |

## 1. Why floors and not counts

This repo's suite runs only under an exact invocation (`PYTHONPATH=src`, an
interpreter that can import FastMCP). A job that "runs pytest" with the wrong
path collects a fraction of the suite — or nothing — and exits 0. **Aggregate-a-set
fails open; assert-a-floor fails closed.** Every check here compares what it found
against a value committed in advance. Nothing derives its own expectation at
runtime.

## 2. Commands

```
python scripts/wave_gate.py ci   --github-event "$GITHUB_EVENT_PATH"
python scripts/wave_gate.py wave --base COMMIT [--bootstrap] [--require-plan-fingerprint]
python scripts/wave_gate.py manifests (--base COMMIT | --github-event PATH) [--bootstrap]
```

`ci` — baseline → manifest format → transition → tree self-consistency →
collection (floor, then required nodes, then reconciliation) → the full non-KB
suite → worktree unchanged.

`wave` — everything `ci` does, then every ACTIVE golden rendered **twice** in
separate child processes under different `PYTHONHASHSEED` values, compared
pass-to-pass (determinism) and then to its committed bytes; then the #153
plan-fingerprint seam.

`manifests` — **not a gate.** The manifest checks only, for a fast local
pre-check. It prints a `NOT A GATE` banner and CI does not use it.

There is deliberately no `--update`, `--skip`, `--force` or `--minimum`. A gate
that can rewrite its own ledger is not append-only, and `tests/test_wave_gate.py`
asserts those flags stay absent.

### Exit codes

| Code | Meaning |
|---|---|
| 0 | every requested check passed |
| 1 | a validation ran and failed (pytest, floor, required node, golden bytes, nondeterminism, fingerprint, hygiene) |
| 2 | a contract failure that prevented validation from being meaningful (usage, baseline, bootstrap, manifest format, manifest transition) |

Every failure prints a stable diagnostic code as the first stderr token:

`BASELINE_EVENT_INVALID` · `BASELINE_ZERO_SHA` · `BASELINE_UNAVAILABLE` ·
`BASELINE_MERGE_BASE_MISSING` · `BASELINE_MERGE_BASE_AMBIGUOUS` ·
`BOOTSTRAP_NOT_ALLOWED` · `MANIFEST_MISSING` · `MANIFEST_FORMAT_INVALID` ·
`MANIFEST_TRANSITION_ILLEGAL` · `MANIFEST_FLOOR_INVALID` ·
`PYTEST_COLLECTION_FAILED` · `PYTEST_COLLECTION_EMPTY` ·
`PYTEST_COLLECTION_DUPLICATE` · `PYTEST_COLLECTION_FLOOR` · `PYTEST_NODE_MISSING` ·
`PYTEST_NODE_TOMBSTONED_BUT_PRESENT` · `PYTEST_FAILED` ·
`PYTEST_SUMMARY_UNPARSEABLE` · `PYTEST_SKIPPED_EXCEEDS_CAP` ·
`PYTEST_PASSED_BELOW_FLOOR` · `GOLDEN_FILE_MISSING` · `GOLDEN_FILE_UNDECLARED` ·
`GOLDEN_RENDER_FAILED` · `GOLDEN_OUTPUT_SET_MISMATCH` · `GOLDEN_NONDETERMINISTIC` ·
`GOLDEN_MISMATCH` · `PLAN_FINGERPRINT_PENDING` · `PLAN_FINGERPRINT_MISMATCH` ·
`WORKTREE_DIRTY`

## 3. Manifest format

JSON Lines, UTF-8, LF, final newline required, no BOM, no blank lines, no
comments, no leading/trailing whitespace, no duplicate JSON keys. Line 1 is the
header; every later line is a row. **Field order is part of the format** — a
reordered object is rejected rather than normalised, so a diff of the file stays
legible and a rewrite is visible.

`tests/fixtures/wave_gate/test_nodes.jsonl`

```json
{"kind":"manifest","schema_version":1,"manifest":"pytest-nodes","minimum_active":N,"minimum_collected":M,"maximum_skipped":K,"bootstrap_base":"<40-hex>"}
{"kind":"test","id":"pytest-000001","node_id":"tests/path/test_file.py::test_name","state":"active"}
```

### Why `maximum_skipped` exists

A required node id proves a test still EXISTS and is collectable. It says nothing
about whether it RAN. pytest collects a skipped test, counts it in the total, and
exits 0 — so a single module-level `pytestmark = pytest.mark.skip` neutralises
every test in that module while collection, the floor, the required-node check
and the exit code all stay green. Measured on this suite: one such line disarmed
**119 required tests** with the whole gate passing. Deleting the same test is
correctly refused; skipping it was not.

That is the aggregate-fails-open shape this gate exists to prevent, one level up,
so it gets the same answer: a committed bound. The gate parses pytest's outcome
summary and asserts `skipped <= maximum_skipped` and
`passed >= collected - maximum_skipped` (derived, not a second free number). A
run with no parseable summary is `PYTEST_SUMMARY_UNPARSEABLE` — an unaccountable
result is not a pass.

The cap is **30**, not zero, because environment-conditional skips are legitimate
and genuinely differ between a developer machine and a runner: this suite has 22
runtime `pytest.skip()` sites plus
`test_build_context_completeness.py::test_no_tracked_file_dropped_from_build_context`,
gated on `gcloud` being on PATH — true locally, false on `ubuntu-24.04`. Measured
18 skips here; expect ~19 on a bare runner. 30 leaves headroom for that while
sitting far below any mass-skip.

`maximum_skipped` may be **lowered** freely (tightening) but **never raised** by a
transition — raising the cap is precisely the move that would launder a mass-skip
past the gate, so it must be its own reviewed change.

`tests/fixtures/wave_gate/goldens.jsonl`

```json
{"kind":"manifest","schema_version":1,"manifest":"goldens","minimum_active":N,"bootstrap_base":"<40-hex>"}
{"kind":"golden","id":"golden-000001","input_case":"process_flow:branch_fanout","renderer":"process-component-v1","expected_file":"tests/fixtures/golden_xml/branch_fanout.xml","owner":"repository","disposition":"survivor","state":"active"}
```

* `id` — `<prefix>-NNNNNN`, **positional and contiguous** from 1. Never reused,
  reordered, renumbered or repointed. Because ids are positional, deleting any
  row but the last also breaks the sequence, so such a deletion is caught by the
  format check rather than the transition check. Both refuse; both exit 2.
* `renderer` — closed enum: `process-component-v1` (a complete Process
  component), `process-xml-v1` (a bare process document), `component-xml-v1`
  (a non-process component).
* `owner` — `repository` or `#<issue>`.
* `disposition` — `survivor` | `transitional_oracle` | `deletion_only`.
* `state` — `active` | `tombstone`.

**Immutable is not the same as unique.** Every payload field is immutable per
row; only `node_id`, `input_case` and `expected_file` must additionally be
unique. `renderer`, `owner` and `disposition` are shared by many rows.

### Two axes, and why

`disposition` and `state` are separate because a lone flag cannot represent an
**ACTIVE transitional oracle** — a golden that still runs today and retires with
its subject later. The gate runs and counts every `active` row *regardless of
disposition*; `disposition` records who owns the row's eventual retirement.

Current assignment: 58 `survivor`/`repository`, and two `transitional_oracle`
owned by **#159** — `try_catch_dlq_document_cache_archetype.xml` and
`try_catch_notify_dlq_document_cache_archetype.xml`. That pair is the only one
the repo positively evidences as retiring with a named subject
(`capability-parity-backlog.md:146`: "the only goldens pinning `ConnectionOverride`
bytes are the two `*_archetype.xml` files, which retire with the archetype";
`created-issue-recipes.md:125`).

**No row is committed as `deletion_only`,** deliberately. `survivor` is the only
value that can be corrected later without an illegal edit — a survivor that turns
out to retire is simply tombstoned by its owning slice, which is a legal
transition — whereas an immutable `deletion_only` stamped on a golden whose
withdrawal is still an open decision can never be taken back. The nearest
candidate, `try_catch_dlq_retry_count_2.xml`, is recorded at
`139-criterion-audit.md:89` as *possibly* unreproducible canonically and "would
have to be recorded as a deliberate withdrawal" — a pending decision, not a
settled one. The enum member exists, is parsed, and is negative-tested across all
three values in `tests/test_wave_gate.py`.

## 4. Baseline resolution — one rule per context, no fallback

| Context | Baseline |
|---|---|
| `pull_request` | the **unique** merge base of `head.sha` and `base.sha`; zero or several is a refusal |
| `push` | `github.event.before`, **verbatim** |
| local | an explicit `--base`, **required** |

A push must never use a merge base: on a push to `dev`, the merge base of HEAD
against `dev` is HEAD itself, so the gate would compare the new tip with itself
and validate nothing. A missing, malformed, all-zero (branch creation /
force-push) or unresolvable baseline **fails closed**; it is never silently
replaced, and it can never invoke the bootstrap exception.

Baseline manifests are read with `git show <base>:<path>`; current manifests are
read from the **worktree**, so CI validates the checked-out merge result and a
local run validates uncommitted work.

## 5. Legal and illegal transitions

Rows are compared **by position**. For each baseline row the current row at the
same index must carry the same `id` and identical payload fields. Then:

* legal: `active → active`, `active → tombstone`, `tombstone → tombstone`
* illegal: `tombstone → active`, any deletion, any insertion before the end, any
  reorder, any renumbering, any payload mutation, an appended row born
  `tombstone`

**A tombstone records a retirement that has already happened, not an intention.**
A tombstoned golden's file must be absent, and a tombstoned node id must not be
collected (`PYTEST_NODE_TOMBSTONED_BUT_PRESENT`). Without the second half the two
parts of a retirement could be split across changes: tombstone a test that is
still there — legally lowering both floors — and the deletion later needs no
manifest edit at all, because the floor reduction was prepaid and a tombstoned
node is not required.

New rows are permitted only **after** every baseline row, with the next
sequential ids and state `active`.

Floor arithmetic:

* `minimum_active` (both manifests) — exactly `old + appended − newly_tombstoned`.
* `minimum_collected` (node manifest) — may be raised freely; may drop by at most
  the number of rows tombstoned in the same change.

**Deleting a golden together with its manifest row** is self-consistent against
the tree and therefore invisible to any check that only looks at HEAD. The
base→head comparison runs *before* the tree checks, which is what catches it.

The gate cannot prove from git alone that a change is "the owning slice". It
reports every tombstone transition with the row's immutable `owner` and
`disposition`; branch protection and review enforce that the transition belongs
to the named issue.

## 6. The bootstrap exception

Permitted exactly once: the change that introduces **both** manifests. Every
condition must hold.

* Neither manifest exists at the validated baseline (a half-introduction is
  refused).
* `git log <baseline> -- <path>` is empty for both — neither path was ever
  touched anywhere in the baseline's ancestry. A later delete-and-recreate has
  the paths in its ancestry, so it cannot be laundered into a fresh start.
* **Neither manifest exists on the TARGET**, when the run context has one. The
  ancestry probe alone looks only BACKWARDS, and that is not enough: once the
  manifests have landed, a baseline predating them still finds both paths absent
  there — forever — so bootstrap would be granted again and every transition
  check skipped, letting a rewritten ledger pass. (Measured before the fix:
  mutating an immutable `owner` on an existing row exited 0 that way.)
  For a **push** the baseline IS the branch tip, so the ordinary all-present
  check already covers it. For a **PR** it does not: a branch cut before the
  landing keeps a merge base that predates it forever, and GitHub checks out
  `refs/pull/N/merge` — a tree that HAS the manifests — while `head.sha` stays
  un-merged. The gate therefore carries the PR's `base.sha` alongside the merge
  base and refuses bootstrap when the manifests already exist there.

> A rule deliberately **not** used: "at most one commit in `<baseline>..HEAD` may
> touch a manifest". It appears to confine bootstrap to the introduction, but it
> refuses ordinary multi-commit development of the very change that introduces
> the ledger — the slice could not validate itself after its second commit. The
> discriminator is whether the ledger has LANDED, not how many commits touched
> it. Pinned by
> `test_multi_commit_development_of_the_introduction_stays_bootstrappable`.
>
> **Residual, stated rather than papered over:** a LOCAL `wave --base <sha
> predating the landing> --bootstrap` on a hand-edited ledger is still accepted.
> That is an operator explicitly invoking the one-time exception against a stale
> baseline they typed themselves, and local runs are advisory — CI is the gate,
> and no CI context can reach that state once the ledger has landed.
* Both current manifests parse and are self-consistent.
* Both headers declare the same `bootstrap_base`, equal to the validated
  baseline.
* A local run must additionally pass `--bootstrap`.

`bootstrap_base` is `9080e3c2d0fcc82b01f781b2352d60995ba58ad8`.

> **Operational precondition.** This repo lands with a fast-forward push to
> `dev`, so the push event's `github.event.before` equals that SHA **only if
> nothing else lands on `dev` first**. If `dev` advances independently before
> this change lands, bootstrap fails closed (`BOOTSTRAP_NOT_ALLOWED`) and both
> headers must be regenerated against the new baseline. This is a refusal, not a
> silent mis-validation.

Local bootstrap deliberately does **not** require "an uncommitted introduction at
`HEAD == bootstrap_base`, or a single introduction commit whose parent is
`bootstrap_base`". A working branch carries several commits before the manifests
land, so neither arm would hold and the gate would be unrunnable locally on the
very change it bootstraps. The ancestry proof already forbids re-bootstrap; the
immediate-introduction rule adds nothing and costs runnability.

## 7. The golden case registry

`tests/_wave_gate_golden_corpus.py` maps `input_case` → a callable returning raw
bytes. It is **closed**: a manifest row names a key in that dict, never an
arbitrary module or callable.

The registry **imports the owning test modules and calls their existing
module-level case helpers** (`_dataprocess_config`, `LISTENER_CHAINS`, `_ANCHORS`,
`SYNC_CASES`, …) rather than restating their contents. There is therefore exactly
one definition of every case input in the tree. Only each case's *invocation*
arguments — component name, folder, which variant helper — are named here, and
those are pinned byte-for-byte by the committed golden. Importing all thirteen
producer modules costs 0.39 s (measured), paid once per render child.

Renderers must **emit**, never read: no renderer touches `expected_file`, and
`test_wave_gate_goldens.py` asserts the renderer half of the module never
mentions the golden directory at all. A renderer that echoed its own answer would
match forever, including after a regression.

Determinism is checked with two child processes under `PYTHONHASHSEED=1` and
`=2`. Two renders inside one interpreter share every module-level cache and would
agree even if emission depended on import order.

`tests/test_wave_gate_goldens.py` parametrizes over **all** rows — active and
tombstoned — keyed by the immutable manifest id. Active rows render and
byte-compare; tombstoned rows assert their file is gone. This keeps the pytest
node set independent of golden state: tombstoning a golden would otherwise delete
a node id and trip `PYTEST_NODE_MISSING`, forcing #159/#160 into a pointless
paired edit.

## 8. The #153 plan-fingerprint seam

`run_plan_fingerprint_checks(require, provider=None)` in `scripts/wave_gate.py`.
With no provider registered it reports `pending:#153` and continues, so #152 can
be green before the fingerprint type exists; `wave --require-plan-fingerprint`
turns that into a hard `PLAN_FINGERPRINT_PENDING` failure — which is how #153
proves the seam actually activates.

#153 sets `PLAN_FINGERPRINT_PROVIDER` to an object exposing:

```python
cases() -> list[str]
mutations(case) -> list[str]
fingerprint(case, *, account, environment, mutation=None) -> str
```

The gate then asserts **both** halves: the same plan under two different
account/environment identities yields the SAME fingerprint (relocatable), and
every declared semantic mutation yields a DIFFERENT one (discriminating). A
provider declaring no cases or no mutations is refused — checking only stability
would be satisfied by a constant. Do not change the command names or the check
order when activating it.

## 9. Regenerating a manifest

The gate has no mutation mode; regeneration is a documented, throwaway
procedure, run only by the slice that changes the corpus.

**Golden manifest** — render every registry case and match its bytes against the
corpus (sound only because every golden has a distinct hash, which the generator
asserts first), assign ids by lexicographic `expected_file`, and write the rows.
The generator used for the bootstrap is reproduced in the issue #152 record.

**Node manifest** — regenerate on **Python 3.11**, the authoritative interpreter:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3.11 -m pytest tests \
  --ignore=tests/kb --collect-only -q -p no:cacheprovider
```

Sort the node ids lexicographically, number them from `pytest-000001`, and set
`minimum_active` / `minimum_collected` to the active row count.

**Appending** a test or golden: add rows at the END with the next ids and state
`active`, and raise `minimum_active` by exactly the number appended.
**Retiring** one: flip `state` to `tombstone` in place (never delete the row),
delete the artifact, and lower `minimum_active` by exactly the number tombstoned.

## 10. Branch protection

The workflow cannot enforce itself. Configure the `dev` ruleset to require the
status check named **`Python 3.11 non-KB`** and to require branches to be up to
date before merging. That check name is the stable contract #153 and #154 cite as
their "full non-KB Python 3.11 suite in CI" gate item; renaming the job silently
drops protection.

## 11. Interaction with the #149 reachability freeze

`tests/_m12_12_legacy_inventory.py::python_sources` scans repo-root `*.py`,
`src/boomi_mcp/**` and **`scripts/**`** — so adding `scripts/wave_gate.py` moves
`scan_contract.python_source_count`, and the #149 freeze fixture
(`tests/fixtures/m12_12/legacy_reachability_inventory.json`) records the new
value (205 → 206). That is the freeze working as designed: a change to the scan
universe requires a deliberate, reviewed baseline edit. `tests/` is **not**
scanned, so the other files this slice adds do not affect it.

Any future file added under `scripts/` will need the same one-scalar update.
