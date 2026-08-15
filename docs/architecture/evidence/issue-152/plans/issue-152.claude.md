# Implementation Plan — Issue #152 (M12.13): Endgame verification gate automation and Python 3.11 CI

Branch `codex/issue-152`, baseline `9080e3c2d0fcc82b01f781b2352d60995ba58ad8`.

---

## Summary

The architect plan is sound on the gate algebra (baseline resolution, immutable-row transitions, bootstrap exception, floor arithmetic) and I adopt it essentially unchanged. What I change is **scope on one item and sequencing on two**: I do *not* refactor thirteen existing golden-producing test modules onto a shared corpus — that is optional de-duplication whose absence is self-policing (both copies compare against the same committed golden bytes), and it is the single largest schedule risk in a slice whose deliverable is a fail-closed gate. I also correct a real coupling defect the architect plan misses: parametrizing the golden test over *active* rows only makes every golden tombstone silently break the test-node manifest. Finally I flag that this issue is **not S/low** — `scripts/wave_gate.py` plus its ~40-row negative matrix is the bulk of the work, and the "clean Python 3.11 env from `requirements-dev.txt`" probe can force `requirements-dev.txt` to grow before anything else can be validated.

---

## Verification of the architect's measured claims (what I actually found)

Read-only (no Bash); counts requiring `ls`/`git` are marked as such.

| Claim | Verdict | Evidence |
|---|---|---|
| `requirements.txt` omits pytest | **CONFIRMED.** 11 lines, runtime only (`fastmcp==3.1.1`, `pydantic`, `pyjwt`, `cryptography`, `httpx`, `python-dotenv`, `requests`, `itsdangerous`, `py-key-value-aio[mongodb]`, `motor`, `boomi>=3.0.1`). No test deps at all. | `requirements.txt` |
| `.venv` is 3.12 / pytest 9.0.2 | **NOT VERIFIABLE by planner** (no shell). Must be re-measured at step 0. | |
| `tests/test_m12_migration_matrix_evidence.py` runs a nested collection without `--ignore=tests/kb` | **CONFIRMED, and worse than stated.** `_collected_ids()` (lines 48–82) runs `pytest --collect-only -q -p no:randomly <root>/tests` with `env={"PYTHONPATH": "src", "PATH": "/usr/bin:/bin:/usr/sbin:/sbin"}` — no KB exclusion, a **hardcoded PATH** that will not match a GitHub runner, no `PYTHONDONTWRITEBYTECODE`, and no `-p no:cacheprovider`. It ignores the child's exit code and only parses stdout, so a KB import error in CI silently shrinks `collected` and fails `test_every_cited_regression_test_resolves`. **This is a hard CI blocker, not a nicety.** | `tests/test_m12_migration_matrix_evidence.py:48-82` |
| `.gitignore` ignores `CLAUDE.md` and `/AGENTS.md`; `docs/*` ignored except `docs/architecture/` | **CONFIRMED** at `.gitignore:91-92` and `:105-106`. Also `__pycache__/`, `.pytest_cache/`, `.venv/`, `venv/`, `.codex/` ignored — but **`.venv311/` is not**, so a 3.11 venv must live outside the repo. | `.gitignore:1-109` |
| CLAUDE.md/AGENTS.md already carry the checkpoint policy; the issue's "3-round hard cap" is superseded | **CONFIRMED for CLAUDE.md** (amended 2026-08-12, checkpoint-based, "every active golden-manifest entry"). **Do not restore hard-cap wording.** | project CLAUDE.md |
| No root `conftest.py` | **CONFIRMED.** Cross-module test imports (`from test_build_integration_wrapper import …` in `test_sync_pipeline_adapter_cutover.py:333`) rely on pytest's rootdir `sys.path` insertion, and `PYTHONPATH=src` is genuinely mandatory. | |
| `TYPED_RECIPE_CONTRIBUTIONS_V1.md:526-539` already contains the #149 caller-reachability rule | **PARTIALLY CONFIRMED — flag.** Lines 526–539 do contain a caller-vs-TCB reachability criterion, but framed as #145's threat-model boundary, with no `#149` attribution visible. **Confirm with `grep -n '#149'` before recording "already satisfied".** | `docs/architecture/TYPED_RECIPE_CONTRIBUTIONS_V1.md:515-554` |
| 60 tracked XML in `tests/fixtures/golden_xml/`, 21 `sync_pipeline_*.xml` | **NOT DIRECTLY COUNTABLE**, but arithmetic is internally consistent. `test_process_emitter_parity.py:1106-1145` asserts `sync_pipeline_*.xml` = `{anchors claimed by SYNC_CASES}` ∪ exactly four named `sync_pipeline_listener_*.xml`. 17 corpus cases + 4 = **21**. The 22nd `SyncPipelineBuilder` golden (`listener_wss_start`) lacks the prefix. 27 + 22 + 1 + 1 = 51 component docs; 6 IR + 3 recipe = 9 bare process docs; total 60. | |

### Producer mapping — spot-checked

Every producer the architect asserts is real, and the category counts hold. But the producers are **eight structurally different call paths**, not four:

1. `ProcessFlowBuilder.build(cfg, name=, folder_name=)` on literal dict configs — `tests/test_process_flow_builder_trycatch_dlq.py:44-120`.
2. `SyncPipelineBuilder.build(raw, name=case["name"], folder_name=case["folder_name"])` from `tests/fixtures/process_ir/sync_pipeline_emitter_parity_cases.json` — `tests/test_process_emitter_parity.py:1210-1233`.
3. `SyncPipelineBuilder.build(cfg, name=<derived string>, folder_name="Golden/Fixtures")` for the four listener goldens — `tests/test_sync_pipeline_adapter_cutover.py:172-187`.
4. `WrapperSubprocessBuilder.build(...)`.
5. `ProcessPropertyBuilder().build(**pp["config"])` where `pp["config"]` is read from **`examples/m11/process_property_map_function.integration.json`** — `tests/test_m11_composed_examples.py:87-96`. A golden whose input lives outside `tests/`.
6. Archetype path: `build_from_archetype_action(...)` → `_resolve_dependency_tokens(cfg, registry)` → `ProcessFlowBuilder.build(resolved, name="Archetype DLQ Golden", folder_name="Golden/Fixtures")` — `tests/patterns/test_database_to_api_sync_dlq.py:425-445`.
7. IR path: fixture JSON → `parse_process_ir_v1` → `lowering`/`compile_process_ir_v1` → `emit_process` → `artifact.process_xml`.
8. Recipe path: `compose_archetypes_action` / `build_from_archetype_action` → `IntegrationSpecV1.model_validate` → `run_fanout_recipe` / `run_sync_preset_recipe` → `result.artifact_for(process.key).process_xml` — `tests/patterns/test_recipe_preset_parity.py:255-285`.

**No golden traced is unmappable.** The schedule risk is that a single `CASE_REGISTRY` must reproduce eight heterogeneous paths — three through archetype/recipe machinery, two reading inputs from `examples/` — from a bare child process outside pytest.

---

## Scope call: the thirteen-module refactor

**Recommendation: build the registry, DEFER the thirteen-module refactor, reduce duplication by construction. File a follow-up issue.**

- **The registry itself is required.** Without a callable renderer per manifest row, `wave` cannot render every active golden twice in isolated processes — and the `transitional_oracle` (#159) / `deletion_only` (#160) dispositions have no meaning. Their whole point is that the gate keeps executing those goldens *after* the legacy tests that own them are removed. Any design binding a manifest row to a pytest node ID dies exactly when those dispositions become load-bearing.
- **The refactor is not required by any acceptance criterion.**
- **The "two sources of truth" objection does not apply here.** The duplicated artifact is a *case input*, and both copies are compared against **the same committed golden bytes**. Drift makes the registry's render stop equalling `expected_file` → `GOLDEN_MISMATCH`. Loud, immediate, caught by the very gate this issue ships. Contrast the genuinely dangerous duplication this repo already fixed (two hand-maintained "canonical chains" lists, `sync_pipeline_emitter_parity_cases.json:3-10`) which had **no shared fixed point**.
- **The refactor's risk is asymmetric.** Rewiring `test_process_emitter_parity.py` (1,234 lines, AST grammar extractor + six-figure probe sweep) or `test_sync_pipeline_adapter_cutover.py` (882 lines of fail-closed guards) puts thirteen high-value regression suites on the operating table to save duplication the gate already detects.

**How the registry gets case definitions instead:** cases with a shared importable non-test artifact are read from it (zero duplication): `sync_pipeline_emitter_parity_cases.json`, `process_ir_v1.json`, `rich_control/`, `error_handling/`, `examples/m11/*.json`, `Archetype.examples[i].parameters`. Cases defined as literal dicts inside a test module (~27 `ProcessFlowBuilder` rows, four listener chains, wrapper subprocess) get a second copy in the registry — pinned by identical golden bytes on both sides. **Do not** import test modules from the registry (`test_process_emitter_parity` executes `_sync_derive_enrichments()` at import, ~4.9 s, paid per child process).

**Deferral cost:** ~27 literal case configs in two places; a builder-default change regenerated through the old test alone turns the wave gate red with `GOLDEN_MISMATCH` and both must be updated. An annoyance, not a correctness hole. File follow-up issue "consolidate golden case definitions into `tests/_wave_gate_golden_corpus.py`" and reference it from `docs/architecture/ENDGAME_VERIFICATION_GATE.md`.

---

## Ordered implementation steps

`$B = 9080e3c2d0fcc82b01f781b2352d60995ba58ad8`.

### Step 0 — record the baseline and re-measure the three unverifiable facts
```bash
git rev-parse HEAD
.venv/bin/python -V && .venv/bin/python -m pytest --version
ls -1 tests/fixtures/golden_xml/*.xml | wc -l
ls -1 tests/fixtures/golden_xml/sync_pipeline_*.xml | wc -l
git ls-files tests/fixtures/golden_xml | wc -l
grep -n '#149' docs/architecture/TYPED_RECIPE_CONTRIBUTIONS_V1.md
grep -rn 'asyncio\|anyio\|importorskip\|version_info' tests --include=*.py | grep -v tests/kb | head -40
for p in python3.11 /opt/homebrew/bin/python3.11 "$HOME/.pyenv/versions/3.11"*/bin/python; do
  command -v "$p" >/dev/null 2>&1 && "$p" -V; done
```
The last two are schedule-critical: a version-gated or plugin-dependent test changes what the node manifest can promise; the 3.11 interpreter's availability decides Step 8's path.

### Step 1 — clean-environment probe (BEFORE writing the gate)
New: `requirements-dev.txt` = `-r requirements.txt` + `pytest==<measured>`.
Build a venv **outside the repo** (`/tmp/wg311`), install only that file, run the full non-KB suite. If anything fails on missing plugins/deps, **grow `requirements-dev.txt` here**. Record wall-clock; it sets the CI timeout.

> **Deviation:** drop `PYTEST_DISABLE_PLUGIN_AUTOLOAD: "1"` from the workflow env. Installing *only* pytest already means nothing to autoload, and the flag would silently break the suite if the probe shows an async plugin is needed. If a plugin is required, pin it and leave autoload on.

### Step 2 — fix the nested collection (CI blocker)
Modified: `tests/test_m12_migration_matrix_evidence.py`
- `_collected_ids()` argv → `[sys.executable, "-m", "pytest", "--collect-only", "-q", "-p", "no:randomly", "-p", "no:cacheprovider", "--ignore", str(_ROOT/"tests"/"kb"), str(_ROOT/"tests")]`.
- `env=` inherits the parent environment with `PYTHONPATH="src"` and `PYTHONDONTWRITEBYTECODE="1"` overlaid — **stop hardcoding `PATH`**.
- Add `test_nested_collection_excludes_kb_and_writes_no_bytecode`: extract `_collect_argv()`/`_collect_env()` and assert `--ignore` names `tests/kb`, `-p no:cacheprovider` present, `PYTHONPATH == "src"`, `PYTHONDONTWRITEBYTECODE == "1"`, `PATH` inherited.

### Step 3 — the golden case registry
New: `tests/_wave_gate_golden_corpus.py` (leading underscore ⇒ not collected).
- `RENDERERS = ("process-component-v1", "process-xml-v1", "component-xml-v1")`.
- `CASE_REGISTRY: Mapping[str, _Case]` — immutable `input_case` → `(declared_renderer, callable)`. Keys opaque, stable, never reused.
- `render_golden_case(input_case, renderer) -> bytes` — raises on unknown case; raises on renderer disagreement; returns **raw UTF-8 bytes**, no normalisation, no reparse, no read of `expected_file`.
- Case bodies grouped by the eight producer paths; JSON/example-backed ones read shared artifacts.
- `_child_main()` — private entry point runnable outside pytest; emits **one JSON envelope per line**: `{"id":…,"len":…,"sha256":…,"b64":…}`. Must `sys.path.insert` repo root, `src`, and `tests`.
- Deterministic: sorted iteration, no set ordering leaking into output.

### Step 4 — the goldens manifest
New: `tests/fixtures/wave_gate/goldens.jsonl`
- Header: `{"kind":"manifest","schema_version":1,"manifest":"goldens","minimum_active":60,"bootstrap_base":"9080e3c…"}`
- 60 rows, `golden-000001`… by lexicographic `expected_file`; fields exactly `id,input_case,renderer,expected_file,owner,disposition,state`; compact separators; LF; final newline.
- Dispositions: `try_catch_dlq_document_cache_archetype.xml` + `try_catch_notify_dlq_document_cache_archetype.xml` → `transitional_oracle`/`#159`; `try_catch_dlq_error_subprocess.xml` → `deletion_only`/`#160`; other 57 → `survivor`/`repository`.
- Generate with a **throwaway `/tmp` script**, not a committed `--update` mode. Record the generator snippet in the ENDGAME doc.

### Step 5 — the golden execution test
New: `tests/test_wave_gate_goldens.py`
- Parse the manifest with the same strict parser (import it from `scripts/wave_gate.py` via `importlib.util.spec_from_file_location` — one parser, not two).
- **Parametrize over ALL rows — active and tombstoned — with `ids=` set to the immutable manifest `id`.**
  - **Deliberate correction to the architect plan.** Active-only parametrization means every future tombstone deletes a pytest node ID; if that node is `active` in `test_nodes.jsonl` the gate fires `PYTEST_NODE_MISSING`, forcing #159/#160 into a confusing paired-tombstone edit. Keying on the immutable manifest ID makes the node set **independent of golden state**.
- Also: active `expected_file` set == on-disk XML set; executed-ID count == header `minimum_active` and ≥ floor; `verify_process_graph` clean for process renderers; ProcessProperty root/type for the one `component-xml-v1` row.
- Every `@pytest.mark.parametrize` here and in `test_wave_gate.py` carries **explicit `ids=`**.

### Step 6 — the gate script
New: `scripts/wave_gate.py`. Implement the architect's §2/§3/§4 contract: CLI (`ci --github-event PATH` / `wave --base COMMIT [--bootstrap] [--require-plan-fingerprint]`), exit codes 0/1/2, the diagnostic codes, ordered `ci`/`wave` check lists, strict JSONL parsing, base→current row algebra, single bootstrap exception, `_run_relocatable_plan_fingerprint_checks()` returning `pending:#153`.

- Stdlib only. Baseline manifests via `git show <base>:<path>`; current from the worktree.
- **All child processes get `PYTHONDONTWRITEBYTECODE=1`**, temp dirs under `tempfile.mkdtemp()` outside the repo, `PYTHONPATH=src`. Final `WORKTREE_DIRTY` check is `git status --porcelain --untracked-files=normal`.
- Determinism: two child invocations with `PYTHONHASHSEED=1` and `=2`, separate temp dirs; compare pass-1 vs pass-2 bytes per ID *before* comparing either to `expected_file`.
- Structural graph verification imports `verify_process_graph` in the child, not the parent.

> **Deviation (necessary):** the architect's local-bootstrap rule requires "uncommitted introduction with `HEAD == bootstrap_base`, **or** a single introduction commit whose parent is `bootstrap_base`". Unsatisfiable on this branch — `codex/issue-152` carries several commits before the manifests land, so neither arm holds and the gate is unrunnable locally on the very change it bootstraps. **Replace with:** local bootstrap requires `--bootstrap`, an explicit `--base` equal to both headers' `bootstrap_base`, *neither* manifest present at that base, and `git log <base> -- <both paths>` empty. The ancestry check is what actually prevents re-bootstrap.

### Step 7 — the gate unit tests
New: `tests/test_wave_gate.py`. Load the script via `importlib.util.spec_from_file_location`. Every row of the architect's §5 matrix, using `tempfile` git repos and synthetic event JSON. Never touch the real worktree. Explicit `ids=` on every parametrize. The row-and-file deletion cases are parametrized over all three dispositions — that parametrized test is the *permanent* proof; the scratch-branch runs are only the demonstration.

### Step 8 — the test-node manifest (LAST, only after Steps 2, 5, 7 are final)
New: `tests/fixtures/wave_gate/test_nodes.jsonl`, generated from a 3.11 collection. Rows sorted lexicographically by node ID, `pytest-000001` upward; header `minimum_active` = active row count; `bootstrap_base` identical to the goldens header.

**Chicken-and-egg resolution (3.11 authority vs a 3.12 dev box):**
- *If Step 0 found a usable 3.11:* generate from `/tmp/wg311`; then cross-check under `.venv` 3.12 that every manifest node is present (extras allowed).
- *If no 3.11 exists locally:* generate from `.venv` 3.12 and land it. The first CI run is then the authority: a 3.11 difference fails **closed** with `PYTEST_NODE_MISSING` listing the missing IDs; correct and re-push. Costs one round-trip, no new mechanism. Safe only because Step 0's `version_info`/`importorskip` grep confirms no version-gated collection.

### Step 9 — workflow and dev requirements
New: `.github/workflows/tests.yml` — the architect's shape: name `Python tests`; triggers `push.branches: [dev]`, `pull_request.branches: [dev]`; `permissions: contents: read`; concurrency by workflow+ref, cancel-in-progress for PRs only; one job `non-kb-python311` / display name **`Python 3.11 non-KB`**; `ubuntu-24.04`; timeout 60 min; env `PYTHONPATH: src`, `PYTHONDONTWRITEBYTECODE: "1"`, `BOOMI_LOCAL: "true"`, `BOOMI_DOCS_ENABLED: "false"`, `BOOMI_GOTCHAS_ENABLED: "false"`; steps: `actions/checkout@v7` (`fetch-depth: 0`, `persist-credentials: false`) → `actions/setup-python@v7` (`3.11`, pip cache keyed on both requirement files) → assert `sys.version_info[:2] == (3,11)` → `pip install -r requirements-dev.txt` → `python scripts/wave_gate.py ci --github-event "$GITHUB_EVENT_PATH"`. No `continue-on-error`, no `|| true`, no conditional skip.

### Step 10 — docs
- New `docs/architecture/ENDGAME_VERIFICATION_GATE.md`: both JSONL schemas, baseline selection per run context, the bootstrap exception (with the Step 6 deviation), legal/illegal transitions, floor arithmetic, CLI + exit codes + diagnostic-code table, deterministic-render protocol, the **#153 fingerprint seam** and its activation contract, branch-protection requirement, append/tombstone procedure, manifest **generation** commands, and the note that `test_wave_gate_goldens.py` parametrizes over tombstones so node IDs survive them.
- Modified `README.md`: "Verification and CI" section + repo-tree update.
- Modified `docs/architecture/M12_COMPATIBILITY_INVENTORY.md`: golden-count authority moves to the manifest; historical lists relabelled. **Careful:** `tests/test_m12_migration_matrix_evidence.py` parses this doc's §7.2 ledger and citation regex — do not disturb those sections; re-run that test after editing.
- Unchanged: `docs/architecture/TYPED_RECIPE_CONTRIBUTIONS_V1.md` (record the Step 0 grep as evidence), `docs/plans/m12-legacy-removal/`, `CLAUDE.md`, `/AGENTS.md`.

### Step 11 — rollout evidence (needs real GitHub Actions)
Land on `dev` (repo convention: FF-push-to-dev, no PR). Then from **scratch branches opened as PRs against `dev`** (which also exercises the PR/merge-base arm a FF push never touches): mutate one golden byte → `GOLDEN_MISMATCH`; add a collection-killing import → `PYTEST_COLLECTION_FAILED`; rename a manifested test while above the floor → `PYTEST_NODE_MISSING`; raise the floor above actual collection → `PYTEST_COLLECTION_FLOOR`; delete a golden **and** its manifest row → `MANIFEST_TRANSITION_ILLEGAL`; repeat the last locally once per disposition; revert everything and record green. Record every Actions URL in issue #152. Configure the `dev` ruleset to require `Python 3.11 non-KB`.

---

## File-by-file

**New**

| Path | Contents |
|---|---|
| `.github/workflows/tests.yml` | Python 3.11 job `Python 3.11 non-KB`; sole command `python scripts/wave_gate.py ci --github-event "$GITHUB_EVENT_PATH"`. |
| `requirements-dev.txt` | `-r requirements.txt` + pinned pytest (+ any plugin Step 1 proves necessary). No KB/cloud deps. |
| `scripts/wave_gate.py` | The fail-closed gate. Stdlib only; writes nothing into the repo. |
| `tests/fixtures/wave_gate/goldens.jsonl` | Header (`minimum_active: 60`, `bootstrap_base: 9080e3c…`) + 60 immutable rows. |
| `tests/fixtures/wave_gate/test_nodes.jsonl` | Header (final count, same `bootstrap_base`) + one row per required non-KB node ID. |
| `tests/_wave_gate_golden_corpus.py` | Closed `CASE_REGISTRY`, `render_golden_case()`, private child entry point. Not collected. |
| `tests/test_wave_gate_goldens.py` | Parametrized over **all** manifest rows keyed by immutable ID. |
| `tests/test_wave_gate.py` | The negative matrix against temp git repos and synthetic events. |
| `docs/architecture/ENDGAME_VERIFICATION_GATE.md` | The durable gate specification. |

**Modified**

| Path | Change |
|---|---|
| `tests/test_m12_migration_matrix_evidence.py` | Nested collection gains `--ignore tests/kb`, `-p no:cacheprovider`, inherited `PATH`, `PYTHONDONTWRITEBYTECODE=1`; new test pins the generated argv/env. |
| `README.md` | "Verification and CI" section + repo-tree update. |
| `docs/architecture/M12_COMPATIBILITY_INVENTORY.md` | Golden-count authority moves to the manifest; historical lists relabelled. |

**Explicitly unchanged:** everything under `src/`, every byte under `tests/fixtures/golden_xml/`, `examples/`, `TYPED_RECIPE_CONTRIBUTIONS_V1.md`, `CLAUDE.md`, `/AGENTS.md`, and the thirteen golden-producing test modules.

---

## Test plan

**Locally validatable:** full non-KB suite; collection reconciliation against the manifest floor; 3.12 contains every active 3.11 node; the gate's bootstrap arm; the pending `#153` seam refusing under `--require-plan-fingerprint`; `git status --porcelain` clean. Plus the committed negative matrix.

**Requires a real GitHub Actions run (repo policy fails closed on an unrunnable required gate):** the workflow triggering on push to `dev` and completing green at HEAD; the `push` baseline arm end-to-end; the `pull_request` merge-base arm; bootstrap under a real event payload; the five seeded-defect red runs and green reverts; the `dev` ruleset requiring `Python 3.11 non-KB`.

**Live Boomi QA:** none. Dark slice — the darkness proof *is* the QA artifact, per CLAUDE.md.

---

## Darkness proof (exact commands)

```bash
B=9080e3c2d0fcc82b01f781b2352d60995ba58ad8
git diff --exit-code "$B" -- src                          && echo "src: DARK"
git diff --exit-code "$B" -- tests/fixtures/golden_xml    && echo "goldens: DARK"
git diff --exit-code "$B" -- examples                     && echo "examples: DARK"
git diff --stat "$B" -- .
git status --porcelain --untracked-files=normal

git stash push --include-untracked -m wg152
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest tests \
  --ignore=tests/kb -q -p no:cacheprovider | tail -3
git stash pop
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest tests \
  --ignore=tests/kb -q -p no:cacheprovider | tail -3
```

---

## Effort honesty

The issue sizes this **S / low**. That is wrong:

- workflow + `requirements-dev.txt` + docs — genuinely S.
- `scripts/wave_gate.py` — **M/L**, realistically 700–1,100 lines.
- `tests/test_wave_gate.py` — **L**, probably the largest single file in the slice.
- `tests/_wave_gate_golden_corpus.py` — **M**: eight producer paths, two reading from `examples/`, three through archetype/recipe machinery, all runnable outside pytest.
- Step 1's clean-3.11 probe — **unbounded until measured.**

Even with the thirteen-module refactor deferred, this is **M–L**.

---

## Deviations from the architect plan

1. **Do not refactor the thirteen golden-producing test modules.** Not required by any acceptance criterion; drift between registry copy and test copy is loud (shared committed golden bytes), unlike the `sync_pipeline_emitter_parity_cases.json` case which had no shared fixed point. File a follow-up issue.
2. **Parametrize `tests/test_wave_gate_goldens.py` over ALL manifest rows (active *and* tombstoned), keyed by the immutable manifest ID.** Active-only couples every future golden tombstone to a matching test-node tombstone, biting #159/#160 exactly when the dispositions become load-bearing.
3. **Relax the local bootstrap arm** to `--bootstrap` + explicit `--base` == both headers' `bootstrap_base` + neither manifest at that base + empty `git log <base> -- <paths>`.
4. **Drop `PYTEST_DISABLE_PLUGIN_AUTOLOAD: "1"`** from the workflow env; install only pytest instead.
5. **The `test_m12_migration_matrix_evidence.py` fix goes further than `--ignore=tests/kb`:** stop hardcoding `PATH`, add `PYTHONDONTWRITEBYTECODE=1`.
6. **`bootstrap_base` pinned to `9080e3c…` with an operational precondition:** because this repo finishes with a fast-forward push to `dev`, `github.event.before` equals that SHA **only if nothing else lands on `dev` first**. If `dev` advances independently, bootstrap fails closed and the headers must be regenerated. Record in the ENDGAME doc.
7. **Manifest generation lives in `/tmp` plus a documented procedure**, not a committed `--update` mode.
8. **`TYPED_RECIPE_CONTRIBUTIONS_V1.md` "already satisfied" is downgraded from a finding to a one-command check** (`grep -n '#149'`).
