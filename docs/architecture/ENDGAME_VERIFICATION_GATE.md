# The endgame verification gate

Issue #152 (M12.13). Owner: repository.

> **Status: implemented, landed on `dev`, and evidenced in CI.** The gate runs on
> pushes to `dev` that start it (a `[skip ci]` head commit starts none — but since
> #172 such a tip cannot reach `dev` at all; see §10 gap 1);
> the landing run passed on Python 3.11.15 with the full non-KB suite. Its ability
> to FAIL was proven by seeded defects on scratch branches — four produced the
> diagnostic the plan expected and one produced a different, correct one; see the
> rollout evidence in [`ISSUE_152_AUDIT_LEDGER.md`](ISSUE_152_AUDIT_LEDGER.md).
>
> **Since #171 there are TWO routes, one gate.** A push to `dev` is DETECTION on
> the pushed tip. A push to `scratch/**` that STARTS a run is a PREFLIGHT on a
> candidate before it is fast-forwarded, baselined on the exact fetched `origin/dev` commit and
> required to descend from it. There is deliberately **no `pull_request`
> trigger** — a PR run validates the synthetic merge tree rather than the commit
> that would land, and this repo does not use pull requests
> ([`ISSUE_171_AUDIT_LEDGER.md`](ISSUE_171_AUDIT_LEDGER.md)).
>
> **Since #172 the check is REQUIRED on `dev`** — a ruleset with one
> `required_status_checks` rule naming `Python 3.11 non-KB`, `enforcement: active`,
> no bypass actors. That was settled by MEASUREMENT, not prediction: both controls
> of §10's experiment passed (an unchecked `[skip ci]` tip is refused; a commit
> already green from a `scratch/**` preflight is accepted from another ref). So the
> gate now PREVENTS on `dev` as well as detecting, and **every landing must first
> earn a green check on a `scratch/**` preflight push**. §10 records the captures.

Two committed ledgers and one fail-closed command make the mechanical half of the
M12 endgame verification regime automatic:

| Artifact | Purpose |
|---|---|
| `.github/workflows/tests.yml` | the `Python 3.11 non-KB` check: REQUIRED on `dev` since #172, preflight on `scratch/**` |
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
python scripts/wave_gate.py ci   (--base COMMIT | --github-event PATH)
python scripts/wave_gate.py wave --base COMMIT [--bootstrap] [--require-plan-fingerprint]
python scripts/wave_gate.py manifests (--base COMMIT | --github-event PATH) [--bootstrap]
```

`ci` — baseline → manifest format → transition → tree self-consistency →
collection (floor, then required nodes, then reconciliation) → the full non-KB
suite → worktree unchanged.

`ci` takes **exactly one** baseline selector, the same shape `manifests` uses:
`--github-event` is the workflow's `dev` arm (the platform supplies the
baseline) and `--base` is the workflow's `scratch/**` preflight arm (the
baseline is the exact fetched `origin/dev` commit). Supplying **neither or
both** is an argparse usage error — exit 2, with the stable first stderr token
`GATE_USAGE_INVALID`.

`ci --base` additionally requires (all three, because a preflight that does not
validate an integration delta validates nothing):

* a **clean** checkout — the bytes under test must be exactly the committed
  candidate, not the runner's edits;
* the baseline must be an **ancestor of `HEAD`** (equality included) — otherwise
  there is no delta, the manifest transition is computed against an unrelated
  tree, and a diverged branch preflights green and is then unmergeable;
* inside GitHub Actions, `HEAD` must equal the platform's `GITHUB_SHA` — the one
  checkout identity a workflow step cannot forge by editing the tree.

`wave --base` and `manifests --base` deliberately keep their dirty-tree support:
there the operator chose the baseline and the uncommitted work IS the subject.

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
`GATE_USAGE_INVALID` · `GATE_UNEXPECTED_ERROR` · `BOOTSTRAP_NOT_ALLOWED` ·
`SCRATCH_NOT_OURS` · `SCRATCH_FOREIGN_ENTRIES` ·
`SCRATCH_INSIDE_REPO` ·
`SCRATCH_CONTAINMENT_UNPROVEN` ·
`SCRATCH_RETARGETED` ·
`MANIFEST_MISSING` ·
`MANIFEST_FORMAT_INVALID` ·
`MANIFEST_TRANSITION_ILLEGAL` · `MANIFEST_FLOOR_INVALID` ·
`PYTEST_COLLECTION_FAILED` · `PYTEST_COLLECTION_EMPTY` ·
`PYTEST_COLLECTION_DUPLICATE` · `PYTEST_COLLECTION_FLOOR` · `PYTEST_EXECUTION_UNRECONCILED` · `PYTEST_NODE_MISSING` · `PYTEST_OUTCOME_UNACCOUNTED` ·
`CHECKOUT_EVENT_MISMATCH` · `PYTEST_NODE_TOMBSTONED_BUT_PRESENT` · `PYTEST_FAILED` ·
`PYTEST_SUMMARY_AMBIGUOUS` · `PYTEST_SUMMARY_UNPARSEABLE` · `PYTEST_SKIPPED_EXCEEDS_CAP` ·
`PYTEST_PASSED_BELOW_FLOOR` · `GOLDEN_FILE_MISSING` · `GOLDEN_FILE_UNDECLARED` ·
`GOLDEN_RENDER_FAILED` · `GOLDEN_OUTPUT_SET_MISMATCH` · `GOLDEN_NONDETERMINISTIC` ·
`GOLDEN_MISMATCH` · `PLAN_FINGERPRINT_PENDING` · `PLAN_FINGERPRINT_MISMATCH` ·
`WORKTREE_DIRTY` · `GATE_DIAGNOSTIC_UNRENDERABLE`

The last of those is emitted, never raised: if the gate fails and its own
diagnostic cannot be rendered, the fallback line still leads with a documented
code so log and test consumers can classify it. The exit status is decided before
any message is built, so it is authoritative regardless.

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

Current assignment: 57 `survivor`/`repository`, one `deletion_only` owned by
**#160** (`try_catch_dlq_error_subprocess.xml`), and two `transitional_oracle`
owned by **#159** — `try_catch_dlq_document_cache_archetype.xml` and
`try_catch_notify_dlq_document_cache_archetype.xml`. That pair is the only one
the repo positively evidences as retiring with a named subject
(`capability-parity-backlog.md:146`: "the only goldens pinning `ConnectionOverride`
bytes are the two `*_archetype.xml` files, which retire with the archetype";
`created-issue-recipes.md:125`).

**`deletion_only` is committed only where the withdrawal is already settled.** The
architect plan settles exactly one — `try_catch_dlq_error_subprocess.xml`, owned by
#160 — and it must be stamped **before** the bootstrap lands, because the field is
immutable and #160 could never correct it afterwards. An earlier revision of this
section generalised a caution about a DIFFERENT golden into "no row is committed as
`deletion_only`", which contradicted both the plan and the committed manifest.

The caution itself stands, for goldens whose withdrawal is still open: `survivor` is the
only value that can be corrected later without an illegal edit — a survivor that turns
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
| `push` — workflow: `dev` | `github.event.before`, **verbatim** |
| local via `ci --base` — workflow: `scratch/**` | the explicit commit; clean tree, and it must be an ancestor of `HEAD` |
| local via `wave` / `manifests --base` | the explicit commit; dirty trees supported |
| `pull_request` | the **unique** merge base of `head.sha` and `base.sha`; zero or several is a refusal — implemented and unit-tested, but **reachable from no trigger** |

A push must never use a merge base: on a push to `dev`, the merge base of HEAD
against `dev` is HEAD itself, so the gate would compare the new tip with itself
and validate nothing. A missing, malformed, all-zero (branch creation /
force-push) or unresolvable baseline **fails closed**; it is never silently
replaced, and it can never invoke the bootstrap exception.

**Why the scratch arm uses the exact fetched `origin/dev`.** Not
`github.event.before`: on the push that CREATES a scratch branch that field is
the all-zero sha, and on later incremental pushes it is the previous scratch
tip — so the gate would validate only the newest increment rather than the whole
candidate. *(Provenance: documented GitHub behaviour, not measured here. The gate
refuses an all-zero baseline outright, which is measured — see the negative
matrix — so this rationale explains the choice without the choice resting on it.)* Not a merge base either: a branch that has diverged from `dev` still
has one, so it would preflight green and then fail to fast-forward. The exact
`origin/dev` commit plus an ancestry proof is the pair that models this
repository's actual integration rule.

The honest caveat: `origin/dev` is authoritative only **as fetched at workflow
start**. If `dev` advances afterwards, the preflight's verdict describes a
superseded baseline — rebase the candidate and re-run rather than pushing on a
stale green.

Baseline manifests are read with `git show <base>:<path>`; current manifests are
read from the **worktree**, so CI validates the checked-out merge result and a
local run validates uncommitted work.

**Golden-corpus authority direction — inverted by #165.** `tests/_wave_gate_golden_corpus.py`
OWNS every case definition and imports ZERO test modules; the golden-producing test
modules consume the definitions from it (or from the committed JSON fixtures it binds —
see §7). The invariant is stated without a module count on purpose: #165's issue text
says "thirteen", which was the pre-inversion registry's twelve direct imports plus one
transitive, and a hand-typed tally goes stale the next time a module is added or retired.
The registry used to import those test modules and call their helpers,
which made removing an owning test module render its golden unrenderable — exactly the
removal `transitional_oracle` (#159) and `deletion_only` (#160) are designed to survive.
`test_wave_gate_goldens.py::test_every_active_golden_renders_with_all_test_modules_unimportable`
now proves the survival property directly: every active golden renders to its committed
bytes in a child where every `test_*` module is unimportable. Import cost is measured,
not estimated: the bare corpus import is 0.02–0.03 s (production imports are lazy inside
the factories); a full render of every case active at the time (60 of them) in one child is
0.43 s (a measurement, so it keeps its count; raw outputs
in `docs/architecture/evidence/issue-165/measurements/`).

**Worktree hygiene is a cross-check, not the guarantee.** The gate's read-only
property is STRUCTURAL and ENFORCED, not merely asserted: the scratch directory's
resolved path is checked to be outside the worktree before use (`TMPDIR` can
otherwise place it inside, and cleanup would then hide the write from the
fingerprint entirely — `SCRATCH_INSIDE_REPO`). That check compares `(st_dev,
st_ino)` identity, NOT path spelling: on a case-insensitive filesystem
`realpath()` keeps the spelling it was given, so `TMPDIR=/users/…/repo` against
`/Users/…/repo` passes a lexical prefix test while landing physically inside the
worktree. An ancestor that cannot be stat'd is not evidence of safety either, so
that fails closed as `SCRATCH_CONTAINMENT_UNPROVEN`. The path the gate then uses is the
resolved one that was checked, never the spelling `mkdtemp()` returned — verifying one
name and writing through another leaves the used path unproven.

**The scratch must be the directory the gate created.** `mkdtemp()` returns a name, and
between its return and the `os.open()` that follows, a same-user process can replace the
directory at that name. Containment cannot tell the difference — an attacker's directory
outside the repository passes it perfectly well — so the gate would adopt somebody else's
files as scratch space and `dispose()` would recursively delete them. `SCRATCH_NOT_OURS`
refuses anything that is not what `mkdtemp()` makes: the inode observed immediately after
creation, mode 0700, owned by us, and **empty**. The emptiness check is what defeats the
swapped-directory-full-of-real-files case even if the identity observation were itself
raced. The failure path discards through the DESCRIPTOR — never `rmtree` on the candidate
pathname, which is precisely the name the gate has just decided it distrusts.

**A name is not a directory.** Resolving the path once is not enough: a process running
as the same user — no execution inside the gate's process tree required — can rename the
verified parent and leave a symlink to the repository in its place, after which the same
string denotes a directory INSIDE the tree. So the gate holds an open descriptor on the
directory that passed the check, and `_ScratchDir.__fspath__` re-proves the binding every
time the value becomes a string. That single chokepoint covers every existing
`os.path.join(...)`/`shutil.rmtree(...)` call without hardening them one by one; the
gate's own writes additionally go through the descriptor, which cannot be redirected.
The descriptor is acquired BEFORE containment is judged, and it is the opened object that
gets judged — validating a path and then opening it leaves a window in which the parent is
retargeted between the two, after which every later comparison agrees because both sides
name the replacement. Containment itself is decided by climbing `..` through descriptors,
which walks the real tree rather than the name.

**Containment is not stable, and the gate does not pretend otherwise.** A same-user
process can move the verified directory itself into the worktree; no amount of descriptor
anchoring prevents that. What the gate guarantees instead is that it never HIDES the
consequence, and three properties combine to make that true:

1. A broken binding is `SCRATCH_RETARGETED`, a gate failure.
2. `dispose()` removes **only what the gate created** — an inventory recorded at
   creation, deepest first — and refuses with `SCRATCH_FOREIGN_ENTRIES` if anything else
   is present. A recursive sweep of whatever happens to be in the scratch destroys data
   the gate never owned: reproduced by moving an unrelated subtree containing
   `precious.txt` into a valid scratch, which the sweep deleted while reporting success.
   It removes nothing **through a broken binding** — contents are unlinked
   relative to the held descriptor, never by name. Stated precisely, because the earlier
   wording overclaimed: the recorded-inventory removal runs BETWEEN the two binding
   checks, so a
   directory moved into the worktree after the first check can have its contents deleted
   before the second check fails. The run still goes red — that is the guarantee — but
   "nothing is deleted" and "the write survives for the fingerprint" hold for a binding
   that is already broken when disposal begins, not for one broken mid-disposal. Deleting
   through a swapped name would delete inside the repository, and would erase the only
   trace that anything happened. The destructive step is BRACKETED by identity and
   containment checks rather than merely preceded by one: a check before deleting means
   a directory already moved into the worktree is never emptied, and the check after
   means a move that lands mid-delete still turns the run red. A failing `rmdir` is
   treated as a broken binding for the same reason — swallowing it would leave an empty
   directory that git does not track, so the fingerprint would match and the gate would
   pass over a retargeting it had already seen. Removal targets the entry whose inode IS
   the held directory (looked up in the live parent, `follow_symlinks=False`), never a
   remembered name — and because POSIX offers no remove-by-descriptor, no guard placed
   *before* `rmdir` can be atomic with it. The gate therefore proves the OUTCOME: after
   the call the held directory must no longer be listed in that parent, and `..` from it
   must still name that parent. Anything else is a removal that hit something other than
   the scratch, and fails closed. Asserting the result is what ends this class; each
   additional pre-check only ever revealed the next window. `dispose()` is total by
   construction — it returns a verdict and never raises, because it runs in a `finally`
   where an escape would replace a pending failure with a traceback and skip the closing
   fingerprint. How `..` behaves for a removed directory is filesystem-specific and is
   MEASURED once per run inside the verified scratch (`_probe_dotdot_at`); an unreadable
   `..` is not proof of unlinking. **Known residual, tracked in #164:** the two outcome
   observations are separate syscalls, so a same-user process racing the gate can
   interleave between them. It can cost at most an empty untracked directory, which git
   does not track and which changes no tracked content and no gate assertion.
3. Retargeting is recorded as a PENDING failure so the closing worktree fingerprint still
   runs. Raising immediately would skip it on precisely the path where a repository
   mutation is most plausible, costing the gate its `WORKTREE_DIRTY` evidence.

So the gate goes red. Where the binding was already broken when disposal began, the write
also survives for the fingerprint to see; where it breaks mid-disposal, the contents may
already be gone and the red comes from the failed second check instead. Going red — not
stable containment, and not guaranteed evidence retention in every interleaving — is the
property that holds.

**What the fingerprint does NOT defend against, stated plainly.** It is a hygiene check
against the gate's own writes and against a misconfigured or subverted scratch path. It
is NOT a general defence against a same-user adversary, and cannot be: such an adversary
can write into the worktree directly, without involving the gate at all. Measured, with
no symlink, no `TMPDIR` and no scratch directory involved:

```
wrote_inside_repo_during_run   : True
fingerprint_equal_after_cleanup: True
check_worktree_unchanged       : PASSED (blind)
modify_then_restore blind      : True
```

A before/after snapshot is structurally blind to any write that is undone before the
closing snapshot, and no amount of hardening inside the gate changes that. The honest
boundary is therefore narrow and worth stating exactly: **the gate guarantees that IT
does not write into the worktree, and refuses to run rather than write through a path it
cannot vouch for.** It does not guarantee that nothing else did. Test code is trusted
because it is executed — the gate exists to run it. It writes only there, runs children
with `PYTHONDONTWRITEBYTECODE=1` and `-p no:cacheprovider`, and never invokes a
mutating git command. The before/after fingerprint (HEAD + porcelain status +
digests of the full binary patches + a SHA-256 per untracked file) is a runtime
cross-check of that structure and raises the cost of an undetected mutation — it
does not reduce it to zero. Git reports `lstat()` failures as arbitrary errno
text and its directory iterator can treat a `readdir()` error as end-of-directory
with no diagnostic at all, so a subtree can be omitted with nothing to detect.
Do not read `WORKTREE_DIRTY` passing as proof that nothing changed; read it as
"nothing that this check can see changed".

**The checkout is bound to the event.** In an event context the gate asserts that
`HEAD` is the commit the event names — the push's `after`, or for a PR either the
head or a `refs/pull/N/merge` commit whose parents are the head and the base —
and that the worktree is **clean**. Without both, the baseline comes from the
event while the evidence comes from whatever happens to be checked out, so a PR
carrying an illegal rewrite could be validated against a different, valid tree.
`after` is validated as strictly as `before`: degrading a malformed value to
"unknown" would make the binding silently skip itself.

Since #171 the binding is wider than "an event context": **every `ci` run** is bound
to the platform's `GITHUB_SHA` when one is available, and a `ci` run additionally
requires a clean worktree whatever its baseline kind — including `--base`. What keeps
their dirty-tree support is `wave --base` and `manifests --base` SPECIFICALLY, because
there the operator chose the baseline and the uncommitted work is the subject. An
earlier revision of this sentence said "local `--base` runs" without that
qualification, which stopped being true when `ci` gained the selector.

## 5. Legal and illegal transitions

Rows are compared **by position**. For each baseline row the current row at the
same index must carry the same `id` and identical payload fields. Then:

* legal: `active → active`, `active → tombstone`, `tombstone → tombstone`
* illegal: `tombstone → active`, any deletion, any insertion before the end, any
  reorder, any renumbering, any payload mutation

**A tombstone records a retirement that has already happened, not an intention.**
A tombstoned golden's file must be absent, and a tombstoned node id must not be
collected (`PYTEST_NODE_TOMBSTONED_BUT_PRESENT`). This makes one tombstone plus
one append — floors unchanged, since `old + 1 − 1 = old` — the canonical shape
for RENAMING a test, and #171 is the worked example: `pytest-009550` retired,
`pytest-009789` appended, `minimum_active` and `minimum_collected` both still
9788. The retirement is only legal because the rename happens in the SAME
change; tombstoning a test that is still there is exactly what the
collected-node check refuses. Without the second half the two
parts of a retirement could be split across changes: tombstone a test that is
still there — legally lowering both floors — and the deletion later needs no
manifest edit at all, because the floor reduction was prepaid and a tombstoned
node is not required.

New rows are permitted only **after** every baseline row, with the next
sequential ids, and a new row must arrive **`active`**. Appending an already
tombstoned row is `MANIFEST_TRANSITION_ILLEGAL`: a tombstone is a RETIREMENT
RECORD, and there is nothing to retire for an identity that was never in the
manifest — the row would permanently reserve an id for something that never
collected, with floors unchanged.

An earlier revision permitted born-tombstoned rows so that a push adding a test in
one commit and removing it in a later one stayed legal. That solved a problem which
does not exist: from the range's endpoints such a test simply never existed, so it
needs **no row at all**.

Floor arithmetic:

* `minimum_active` (both manifests) — exactly
  `old + appended_ACTIVE − newly_tombstoned`. Only appends that arrive `active`
  count; a born-tombstoned row adds nothing to the active total.
* `minimum_collected` (node manifest) — may be raised freely; may drop by at most
  the number of rows tombstoned in the same change.

**Deleting a golden together with its manifest row** is self-consistent against
the tree and therefore invisible to any check that only looks at HEAD. The
base→head comparison runs *before* the tree checks, which is what catches it.

The gate cannot prove from git alone that a change is "the owning slice". It
reports every tombstone transition with the row's immutable `owner` and
`disposition` — and there it stops. This repository has no branch protection and
no pull-request review (§10), so **nothing mechanical enforces that a transition
belongs to the issue its row names**; the gate makes the transition loud and
attributable, and the owner enforces it at push time. Stated plainly because the
alternative is a documented guarantee no mechanism provides.

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
> The local arm is NOT closed by any heuristic — see the `--bootstrap` bullet
> above for why eight formulations were tried and removed. It is an explicit
> operator assertion, labelled as one at runtime; the CI arms hold the authority.
* Both current manifests parse and are self-consistent.
* Both headers declare the same `bootstrap_base`, equal to the validated
  baseline.
* A local run must additionally pass `--bootstrap`. That flag is an OPERATOR
  ASSERTION, not a verified condition, and the run says so on stderr.

  There is deliberately **no local check that the exception is still unspent**.
  Eight successive formulations of "has this ledger landed?" were each defeated:
  ancestry-only (re-claimable forever after landing); a commit-count rule
  (rejected ordinary multi-commit development of the introduction itself);
  exempting `*/<branch>` mirrors (reopened the hole on `dev`, which is exactly
  where this repo lands); enumerating ref namespaces (missed `refs/tags`);
  `git rev-parse --abbrev-ref` (returns `heads/<name>` for a branch that shares a
  tag's name); matching the introducing COMMIT rather than the path (a branch that
  recreates the ledger has different SHAs); `--all --not <own_ref>` (subtracts any
  commit merged in, hiding the other branch's addition); and default history
  simplification (prunes an addition that arrived via `merge -s ours`).

  They did not fail through sloppiness. Locally the OPERATOR chooses the
  baseline, so no rule can separate "legitimately introducing the ledger" from
  "asserting a stale baseline" — and being wrong in the refusing direction blocks
  the introduction the exception exists for. The question is ill-posed here.

  The authority therefore lives where the baseline is supplied by the platform
  rather than chosen by the person being checked — the `ci` arms, which are
  strict: `push` compares against the branch tip it builds on, and
  `pull_request` additionally requires the target to carry no manifests. A local
  `wave --bootstrap` still runs the suite, the goldens and the determinism check;
  only the manifest-transition portion is unvalidated, and in a genuine bootstrap
  there is no transition to validate.

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
very change it bootstraps — the first of the eight failed formulations catalogued
above.

## 7. The golden case registry

`tests/_wave_gate_golden_corpus.py` maps `input_case` → a callable returning raw
bytes. It is **closed**: a manifest row names a key in that dict, never an
arbitrary module or callable.

The registry **owns the case definitions; the owning test modules consume them**
(#165 inverted the original direction). Each test module keeps a small alias
block (`_base_config = _corpus.pfb_base_config`, `LISTENER_CHAINS =
_corpus.LISTENER_CHAINS`, …) so its own assertions and non-golden tests are
unchanged, and the corpus imports ZERO test modules — deleting an owning test
module leaves every golden renderable, which is the property #159/#160 depend
on and which `test_wave_gate_goldens.py` proves with an import-blocked child
(checked by module name AND by resolved file, so a file-path import cannot
smuggle the dependency back). The corpus carries only what a case needs: a
fixture used by no case belongs in the test module, or it becomes dead corpus
code the day its owner is deleted. Case-to-case independence is likewise
asserted in BOTH directions, not described: one test renders every case and
requires every module-level container in the corpus to be unchanged afterwards
(the consequence), and a second records the arguments every corpus FUNCTION
receives during that render and refuses any object that is — by identity — a
module-level container or a container nested inside one (the antecedent). The
consequence test alone was green with the copies removed — today's builders
happen not to mutate their input — so it regression-tests the property without
pinning the mechanism. The two guards whose failure branch was
found unreachable now carry COMMITTED negative controls rather than hand-run
ones: the import blocker is run disarmed and required to refuse, and the
antecedent's watch-set construction and leak walker are driven against a
synthetic sharing graph plus real module state. Guards here have repeatedly
been found inert because only their passing branch was ever exercised, so a new
guard ships with the mutation that proves it can fail. The antecedent's reach is
the corpus's own function boundary — arguments in, values out; state handed
straight to a production builder without crossing it is not covered, and that
residue is a known, accepted limitation rather than claimed away here.

Where a case input already lives in a committed JSON fixture
(`sync_pipeline_emitter_parity_cases.json`, the `rich_control/` and
`error_handling/` documents, the `recipe_parity/` baseline spec dumps,
`examples/m11/`), the JSON is the definition and the corpus is its sole Python
binding; a test module's in-Python copy is a WITNESS, pinned by that module's
own fixture-equality test. There is therefore exactly one definition of every
case input in the tree. Invocation arguments — component name, folder, which
variant — are pinned byte-for-byte by the committed golden. The bare corpus
import costs 0.02–0.03 s (all production imports are lazy inside the
factories); rendering every case active at the time (60 of them) in one child cost 0.43 s (measured —
`docs/architecture/evidence/issue-165/measurements/`).

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
mutations(case) -> list[str]          # must include semantic, envelope, policy, revision
fingerprint(case, *, account, environment, mutation=None) -> ("sha256:<hex>", canonical_material: bytes)
```

`fingerprint` returns the digest **and the exact canonical bytes it was derived
from**, because a digest alone proves nothing: a constant is perfectly stable.
The gate asserts, per case:

* the canonical MATERIAL is identical under two different account/environment
  identities — the bytes must carry no tenant identity — and so is the digest;
* all four mutation kinds are declared (proving reaction to a `semantic` change
  says nothing about `envelope`, `policy` or `revision`);
* every mutation changes the material, changes the digest, and is itself
  relocatable under both identities;
* the four kinds produce **distinct** plans from each other — otherwise a
  provider can declare all four names, ignore which was requested, and return
  one identical "changed" plan every time;
* the digest is **recomputed**: it must equal `sha256:` + the SHA-256 of the
  returned material. Accepting any non-empty string would let the digest and the
  material drift apart, leaving "the exact canonical byte material used to derive
  it" unverified.

`cases()` and `mutations()` are materialised and type-checked INSIDE the guarded
call, so a generator that raises mid-iteration — or a bare string, which would
otherwise become a list of characters — is a coded refusal rather than a
traceback.

A provider that raises is reported as `PLAN_FINGERPRINT_MISMATCH`, not an
unhandled traceback: every refusal carries a stable code. Do not change the
command names or the check order when activating it.

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

> **Regeneration is APPEND-ONLY, and this is the step that gets it wrong.**
> Nobody hand-edits a 9,000-row ledger; they regenerate it. A regeneration that
> sorts every collected node id and renumbers from `pytest-000001` **repoints
> every row whose alphabetical position shifted** — measured here: adding 7
> tests moved **321** existing ids onto different tests, which the gate refuses
> as `MANIFEST_TRANSITION_ILLEGAL`.
>
> Correct procedure: read the previous manifest, keep every existing row's `id`,
> `node_id`, `state` **and position** verbatim, then append only the genuinely
> new node ids at the end with the next sequential ids — regardless of where
> they sort. Raise `minimum_active` by exactly the number appended. Sorting
> applies **once**, at the bootstrap, to fix the initial order.
>
> **Before the bootstrap has landed, the rules above do not yet bind.** Until the
> manifests exist on `dev`, the whole file is the bootstrap being authored: it is
> regenerated wholesale, sorted, all-active, numbered from `pytest-000001`, and
> the branch's intermediate commits form no ledger at all — the landing push is
> validated as `dev`'s tip → the branch tip, which takes the bootstrap path
> because `dev` carries no manifests. Reviewers have twice read an intermediate
> commit as a baseline and reported a legal bootstrap as an illegal transition;
> the question to ask is always **what does `dev` actually contain**. The instant
> the bootstrap lands, append-only binds forever and this paragraph stops
> applying.
>
> A node that no longer collects is **not** a regeneration outcome: it is an
> explicit `active → tombstone` edit by the owning slice, in the change that
> deletes the test. A generator that silently drops it must fail instead.
>
> Verify before committing, with the transition arm and not just the bootstrap
> arm — the bootstrap arm skips transition checking entirely and will not catch
> this:
>
> ```bash
> python scripts/wave_gate.py manifests --base <the previous commit>
> ```

**Appending** a test or golden: add rows at the END with the next ids and state
`active`, and raise `minimum_active` by exactly the number appended.
**Retiring** one: flip `state` to `tombstone` in place (never delete the row),
delete the artifact, and lower `minimum_active` by exactly the number tombstoned.

## 9a. The CI environment

The workflow sets `PYTHONPATH=src`, `PYTHONDONTWRITEBYTECODE=1`,
`PYTEST_DISABLE_PLUGIN_AUTOLOAD=1` and the three `BOOMI_*` variables the design plan
specified. Both decisions reversed an earlier position, and the reasons are recorded here
rather than left as a silent divergence:

* **`PYTEST_DISABLE_PLUGIN_AUTOLOAD=1`** — SET, after an earlier refutation of it
  turned out to be wrong. That refutation reasoned that `anyio` registers a
  `pytest11` entry point and two modules drive async code through `anyio.run`,
  so disabling autoload would change the validated configuration. It was an
  inference, not a measurement, and the inference was false: `anyio.run()` is a
  plain function, unrelated to the plugin's marker and fixtures, which this repo
  does not use. Measured with the flag set: all 9,712 nodes still collect and
  both modules pass 50/50. The flag costs nothing and stops a plugin arriving
  through a transitive dependency from changing what this job runs.
* **`BOOMI_LOCAL`, `BOOMI_DOCS_ENABLED`, `BOOMI_GOTCHAS_ENABLED`** — SET, as the plan
  specified. An earlier revision dropped them on the grounds that the suite was green
  without them, which was true but proved the wrong thing: with `BOOMI_LOCAL` unset a
  direct `import server` selects production initialization and fails for a missing
  `GCP_PROJECT_ID`, and the suite survived only because some test module's import side
  effect happened to set local mode first. That makes the required check
  ORDER-DEPENDENT — green for a reason nobody chose and nothing pins. They are set,
  and the run that validates this tree sets them.

Two diagnostic codes the plan named are also absent — `GOLDEN_INPUT_MISSING` and
`GOLDEN_FLOOR`. Neither adds a check: a missing case input surfaces as
`GOLDEN_RENDER_FAILED` (the render child fails), and the golden floor is enforced
by `MANIFEST_FLOOR_INVALID` at parse time. Minting codes that alias existing ones
would make the roster longer without making the gate stricter, and
`test_every_diagnostic_code_the_gate_can_raise_is_documented` keeps the roster and
the code in exact agreement in both directions.

## 10. What enforces the check, and what does not

This repository integrates by fast-forward **push** to `dev`; it does not use pull
requests, and `dev` reaches `main` only at milestone end.

**Measured configuration**, on both surfaces that can enforce a check — they are
configured separately and a rulesets query alone does not settle the question:

```
gh api repos/.../rulesets                  -> one ruleset, "dev requires the non-KB suite"
gh api repos/.../rules/branches/dev        -> [required_status_checks: Python 3.11 non-KB]
gh api repos/.../branches/dev/protection   -> 404 Branch not protected (no CLASSIC rule)
gh api repos/.../branches/main/protection  -> 404 Branch not protected
default branch                             -> main
origin/main  .github/workflows             -> absent entirely
origin/dev   .github/workflows             -> tests.yml
```

`origin/main` carrying no workflows is the measurement that rules out
`workflow_dispatch` as #171's mechanism: GitHub offers a dispatch trigger only
for a workflow present on the **default branch**, and `dev` reaches `main` only
at milestone end. A `push:` trigger resolves the workflow from the *pushed ref*
instead, which is why `scratch/**` works today with the file only on `dev`.
*(Provenance: the default-branch condition for `workflow_dispatch` and the
pushed-ref resolution for `push` are documented GitHub behaviour; the branch
inventory above is measured here. #152's landing run is the local evidence that a
push loads the workflow from the tree that arrives with it.)*

Repository **rulesets** and **classic branch protection** are distinct mechanisms;
an earlier revision of this section inferred "no protection" from the rulesets
query alone, which does not follow. Both are measured. Note the third line: `dev`
is still not protected by a CLASSIC rule and never has been — the enforcement
added by #172 is a RULESET, which that endpoint does not report, so reading only
`/protection` would now conclude "unprotected" and be wrong in the opposite
direction. §10 states nothing about GitHub's behaviour that was not either
measured here or is not load-bearing for a claim.

**What the gate delivers since #172: PREVENTION on `dev`, plus detection on the
tip that lands.** Every push to `dev` that triggers the workflow runs
the full gate on the pushed commit, and a failure is visible on it. Since #171,
every push to `scratch/**` **that starts a run** does the same on the candidate
*before* it is fast-forwarded, so a bad tip can be found without landing it. The
qualifier is not pedantry: gap 1 applies to the preflight exactly as it applies to
`dev`, so a scratch push carrying a skip directive is preflighted by nothing. Neither is
compulsory. **That changed with #172:** the `dev` ruleset makes the preflight the
only way to obtain the check a landing needs, so something outside the pushed tree
does now get a vote on `dev` — though nowhere else.

The gaps below are the ones currently known. They are **not claimed to be an
exhaustive bound** — gaps 1 and 3 were each found by review *after* an earlier
revision of this section asserted the then-current list was complete, and a claim of
completeness about a platform this document cannot test is exactly the kind of
assertion §10 exists to avoid. What united them was stated plainly: with no
required check and no ruleset, nothing outside the pushed tree got a vote, so
anything that stopped the workflow from starting also stopped the failure from being
seen. **Since #172 that holds everywhere EXCEPT `dev`**, where a rule outside the
pushed tree now votes: whatever suppresses the run also withholds the required check,
and the push is refused rather than landing silently.

1. **A push can suppress its own run — CLOSED on `dev` since #172.** GitHub honours
   skip directives (`[skip ci]`, `[ci skip]`, and siblings) in a pushed head
   commit's message and does not start the workflow. Such a commit therefore
   acquires no check at all. A skipped workflow cannot repair its own absence —
   nothing inside the pushed tree can observe a run that never started — so the only
   mechanism that could close this was a repository rule, and #171 correctly declined
   to predict whether one was viable. **#172 measured it: the required check REFUSES
   such a tip** (`GH013 … Required status check "Python 3.11 non-KB" is expected.`,
   push declined, branch unmoved — captured in
   [`evidence/issue-172/rulesets/negative-control-push.txt`](evidence/issue-172/rulesets/negative-control-push.txt)).
   *(Provenance: measured here. The skip behaviour itself is GitHub-documented; what
   is measured is that the rule refuses the resulting unchecked tip.)* The hole
   remains open anywhere the rule does not apply — every branch other than `dev`,
   including `scratch/**` preflights, where a skip directive still means no run and
   therefore no preflight.
2. **A bad tip is detected after it has landed** — narrowed by #172, not removed.
   The run still starts only once the push is accepted, so a candidate that is GREEN
   at preflight and would fail on the merged result still lands before anything says
   so; reverting remains the remedy for that divergence. What is no longer possible
   on `dev` is landing with NO verdict at all: a tip without a successful required
   check is refused outright.
3. **A push can disable the gate itself.** For a `push` event GitHub loads the
   workflow from the **pushed tree** — which is why the landing run executed the
   `tests.yml` that arrived with it. So a push that removes the workflow from
   discovery starts no run, and that tip lands silently. Precisely: deleting the
   file, moving it out of `.github/workflows/`, giving it an extension other than
   `.yml`/`.yaml`, or changing its trigger so `dev` no longer matches. A rename to
   another valid path *within* `.github/workflows/` does **not** disable it —
   discovery is by directory, not by filename, so `tests.yml` is not a magic name.
   This is gap 1's mechanism at tree level rather than commit-message level, and it
   is the sharper of the two: the gate's own definition sits inside the thing it is
   meant to check. *(Provenance: the tree-loading half is measured — the landing
   run demonstrates it; the consequences for a removing push follow from it and are
   not separately measured here.)*
4. **The gate could not be run on a branch by any convention-compliant means —
   addressed by #171's configuration; see its ledger for the run evidence.** A push
   to `scratch/**` that starts a run now triggers the same gate on the candidate
   itself, baselined on the exact fetched `origin/dev` and required to descend from it — no pull request
   involved. *(Provenance: the trigger and the gate's local behaviour are measured
   here; whether a real Actions run has been observed RED and GREEN on that route is
   recorded in [`ISSUE_171_AUDIT_LEDGER.md`](ISSUE_171_AUDIT_LEDGER.md), which is the
   single place that claim lives.)* The `pull_request` trigger has been
   REMOVED (criterion 7a): it validated the synthetic **merge** tree rather than
   the branch tip, so even setting the no-PR convention aside it never told you
   that the commit you were about to fast-forward was green, and keeping a
   never-observed-green arm is a second path without evidence. The
   `pull_request` resolver itself remains in the gate with full unit coverage; it
   is simply unreachable from CI.

   What the preflight means since #172: it is no longer optional for `dev`. The
   check it produces is exactly what the `dev` rule requires, so a candidate that
   skips its preflight has no check to present and the push is refused.

**On making it a required status check — MEASURED, 2026-08-16 (#172).** This paragraph
was wrong in both directions before it was measured — first calling a preflight
impossible, then calling it available-but-disallowed — and each error was the same
mechanism: asserting platform behaviour instead of running it. The experiment specified
here has now been performed, and what follows is its result rather than its prediction.
Raw captures (pre-state, the created rule, both push transcripts with exit codes, and the
after-state) are archived under
[`evidence/issue-172/rulesets/`](evidence/issue-172/rulesets/); the audit record is
[`ISSUE_172_AUDIT_LEDGER.md`](ISSUE_172_AUDIT_LEDGER.md).

Method: a ruleset with one `required_status_checks` rule naming the context
`Python 3.11 non-KB` and `integration_id` 15368, `enforcement: active`, **no bypass
actors**, targeting a disposable `scratch/ruleset-experiment` branch created at
`0df53ff` — the PREVIOUS `dev` tip, deliberately, so that advancing it to the green
`ffae2a1` would be a real fast-forward rather than a no-op. (Side effect, recorded because
it is a measurement too: that branch's own preflight run went RED with
`BASELINE_UNAVAILABLE … not an ancestor`, since `0df53ff` no longer descended from the
moved `origin/dev`. The gate refusing to preflight a stale base is correct behaviour, and
it is a real cost of the new requirement — see the note on rebasing below.) The context is named explicitly and deliberately: a
`google-cloud-build` app creates a permanently QUEUED check suite with zero check runs on
every push here (*measured here*), so an all-checks or suite-level rule would never be
satisfiable.

* **Negative control — the rule REFUSES an unchecked tip** (*measured here*). A commit
  built with `git commit-tree` on the branch's own tree, carrying `[skip ci]` in its
  message and therefore starting no workflow run at all, was pushed to that branch. The
  push was declined:

  ```
  remote: error: GH013: Repository rule violations found for refs/heads/scratch/ruleset-experiment.
  remote: - Required status check "Python 3.11 non-KB" is expected.
   ! [remote rejected] ... (push declined due to repository rule violations)
  ```

  exit code 1, branch unmoved. **What this measures, precisely:** that a tip carrying no
  successful required check is refused. It does NOT re-measure gap 1's premise — that a
  skip directive suppresses the run — because the push was refused before any run could
  start; the commit was check-less by virtue of never having reached GitHub at all. The
  skip behaviour itself remains GitHub-documented rather than measured here. The
  consequence for gap 1 is nonetheless direct: whatever leaves a tip without the required
  check, including a suppressed run, now cannot land it on `dev`.

* **Positive control — the rule ACCEPTS a check already attached to the SHA**
  (*measured here*). The exact commit `ffae2a1`, pushed from a different ref, advanced the
  protected branch: exit code 0, `0df53ff..ffae2a1`. Both of its check runs were settled
  `success` first, so the result cannot be attributed to an in-flight run. A check earned
  on one ref is usable by a later fast-forward of another — the property the whole
  question turned on. **Attribution limit, stated because the distinction matters
  operationally:** at push time `ffae2a1` carried TWO green runs of the required context —
  one from the `scratch/issue-173-preflight` route and one from its own `dev` push — so
  this control establishes SHA-attachment in general, not specifically that a
  `scratch/**`-produced check is what satisfies the rule. A SHA whose only check came from
  a scratch push isolates that, and criterion 5's `dev` fast-forward is exactly such a
  case.

**Both controls passed, so the rule was enabled on `dev`** (owner decision, recorded in
the ledger before the experiment ran): ruleset "dev requires the non-KB suite",
`enforcement: active`, no bypass actors, the same pinned context. It differs from the
experiment rule in one parameter — `do_not_enforce_on_create: false` rather than `true`,
which only affects branch creation and makes the `dev` rule the stricter of the two. The
temporary ruleset and the disposable branch were removed (`measured here`, four captures
archived: `rulesets` lists one ruleset, `rules/branches/dev` reports the required check,
`branches/dev` now reads `protected: true` while `/protection` still 404s, and the
`scratch/**` ref listing is empty). One piece of experiment litter was
handled, and the handling is worth recording because the first account of it was wrong:
the positive-control push started a run on the since-deleted experiment branch, which was
cancelled. Cancelling did NOT remove a verdict — it CREATED one (`measured here`: a
`Python 3.11 non-KB` check run with `conclusion: cancelled`, now the newest result for the
required context on `ffae2a1`). Consequence, stated because it is a real if narrow cost:
re-pushing `dev` at that exact tip would present a non-success newest result for the
required context. `dev` has since moved past it, so nothing is blocked today; a rollback
TO that tip would need the rule bypassed or a fresh run.

**What is measured on `dev` itself, and what is not — stated precisely because this
section's history is one of claiming more than was run.** The controls above were measured
on `scratch/ruleset-experiment`; the `dev` rule is byte-identical in its required context
and was verified live via `rules/branches/dev`. When that paragraph was first written no push to
`dev` had yet occurred under the rule, and it said so. **That measurement has since been
taken** (`measured here`, transcript archived as
[`criterion5-dev-fastforward.txt`](evidence/issue-172/rulesets/criterion5-dev-fastforward.txt)):
this slice's own landing commit `290b646` earned a green `Python 3.11 non-KB` check on a
`scratch/**` preflight and was then fast-forwarded onto `dev` — `ffae2a1..290b646`, exit
code 0.

That landing is also the clean isolation the positive control could not provide: `290b646`
carried **exactly one** check run for the required context, and its only source was the
scratch preflight. So the operational claim this whole regime rests on — *a check produced
by a `scratch/**` preflight is what satisfies the `dev` rule* — is now measured directly
rather than inferred. A second fact falls out of the archived positive control, which
pushed three commits at once: its two intermediates carried zero checks and it was still
accepted, so the rule evaluates the pushed HEAD only.

**What this changes.** The gate is no longer detection-only on `dev`: a push whose head
commit lacks a successful `Python 3.11 non-KB` check is refused, including one that
suppresses its own run. The cost is a workflow requirement rather than a workflow option —
**every `dev` landing must now first earn a green check on a `scratch/**` preflight push**,
the route #171 built. Gaps 2 and 3 above are unchanged in kind: a rule evaluates the
commit being pushed, so a tree that removes the workflow from discovery still starts no
run — and now simply cannot be pushed to `dev`, because the check it would need never
appears. Reverting the rule is one `DELETE` call.

The job name **`Python 3.11 non-KB`** remains the stable contract that #153 and
#154 cite as their "full non-KB Python 3.11 suite in CI" gate item. **Since #172 it
is also the string the `dev` ruleset requires**, so renaming the job is no longer a
documentation concern: the required context would never appear again and every push
to `dev` would be refused with `GH013` until an admin edited or deleted the rule.
Rename the job and the ruleset's `context` in the same change, or not at all. (An
earlier revision of this paragraph said "no ruleset depends on it" — true when
written, falsified by this very slice, and exactly the kind of stale authority that
invites the one edit that deadlocks landing.)

## 11. Interaction with the #149 reachability freeze

`tests/_m12_12_legacy_inventory.py::python_sources` scans repo-root `*.py`,
`src/boomi_mcp/**` and **`scripts/**`** — so adding `scripts/wave_gate.py` moves
`scan_contract.python_source_count`, and the #149 freeze fixture
(`tests/fixtures/m12_12/legacy_reachability_inventory.json`) records the new
value (205 → 206). That is the freeze working as designed: a change to the scan
universe requires a deliberate, reviewed baseline edit. `tests/` is **not**
scanned, so the other files this slice adds do not affect it.

Any future file added under `scripts/` will need the same one-scalar update.
