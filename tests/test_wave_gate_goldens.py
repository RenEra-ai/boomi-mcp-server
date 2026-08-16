"""Every ACTIVE golden-manifest entry is executed by the ordinary non-KB suite.

The wave gate (``scripts/wave_gate.py wave``) renders each active entry TWICE in
isolated child processes to prove determinism.  That is the per-wave obligation
and it is expensive.  This module is the per-COMMIT half: it renders each entry
once, inside the suite CI already runs, so a golden cannot rot between waves.

Two design points that are load-bearing:

1. **Parametrized over ALL rows, active and tombstoned, keyed by the immutable
   manifest id.**  Parametrizing over active rows only would couple the two
   manifests: tombstoning a golden would delete a pytest node id, and if that
   node is ``active`` in ``test_nodes.jsonl`` the gate fires
   ``PYTEST_NODE_MISSING`` — forcing #159/#160 into a paired tombstone edit for
   no reason.  Keying the parameter on ``golden-NNNNNN`` makes the node set
   independent of golden state: an active row renders and byte-compares, a
   tombstoned row asserts its file is gone, and the node id survives either way.

2. **One parser.**  The manifest is read through ``scripts/wave_gate.py``'s own
   strict parser rather than ``json.loads`` per line.  A second, laxer reader
   here would let this suite pass on a manifest the gate rejects.
"""

from __future__ import annotations

import importlib.util
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
_SRC = str(_ROOT / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
sys.path.insert(0, str(_ROOT / "tests" / "patterns"))
sys.path.insert(0, str(_ROOT / "tests"))

import _wave_gate_golden_corpus as corpus  # noqa: E402


def _load_gate():
    spec = importlib.util.spec_from_file_location(
        "_wave_gate_module", _ROOT / "scripts" / "wave_gate.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gate = _load_gate()

_MANIFEST_PATH = _ROOT / gate.GOLDENS_MANIFEST
MANIFEST = gate.parse_manifest(_MANIFEST_PATH.read_bytes(), "goldens")
ROWS = MANIFEST.rows
_IDS = [row["id"] for row in ROWS]

_BNS = {"bns": "http://api.platform.boomi.com/"}


# ---------------------------------------------------------------------------
# Per-row execution
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("row", ROWS, ids=_IDS)
def test_golden_manifest_row(row):
    """An active row renders to its committed bytes; a tombstoned row is gone."""
    target = _ROOT / row["expected_file"]

    if row["state"] == "tombstone":
        assert not target.exists(), (
            "{0} is tombstoned but {1} is still present; a tombstone means the "
            "artifact is deleted, not merely unreferenced".format(
                row["id"], row["expected_file"]
            )
        )
        assert row["input_case"] not in corpus.CASE_REGISTRY, (
            "{0} is tombstoned but the corpus still defines a renderer for "
            "{1}".format(row["id"], row["input_case"])
        )
        return

    emitted = corpus.render_golden_case(row["input_case"], row["renderer"])
    assert emitted == target.read_bytes(), (
        "{0} ({1}) does not match its committed golden {2}".format(
            row["id"], row["input_case"], row["expected_file"]
        )
    )

    if row["renderer"] in ("process-component-v1", "process-xml-v1"):
        from boomi_mcp.categories.components.process_graph_verifier import (
            verify_process_graph,
        )

        report = verify_process_graph(emitted.decode("utf-8"))
        assert report["errors"] == [], (row["id"], report["errors"])
    else:
        # The one non-process component: a ProcessProperty. Check it really is
        # one, so a renderer swap cannot quietly turn this row into a process.
        root = ET.fromstring(emitted.decode("utf-8"))
        assert root.get("type") == "processproperty", (row["id"], root.get("type"))
        declared = root.find(
            "bns:object/DefinedProcessProperties/definedProcessProperty", _BNS
        )
        assert declared is not None, row["id"]


# ---------------------------------------------------------------------------
# Corpus-level invariants
# ---------------------------------------------------------------------------

def test_active_rows_and_the_golden_directory_agree_exactly():
    """Set EQUALITY, not containment.

    Containment in one direction lets a golden be added with no manifest row —
    it would then never be rendered by the wave gate, which is the failure this
    whole manifest exists to prevent.
    """
    declared = {row["expected_file"] for row in MANIFEST.active}
    on_disk = {
        "tests/fixtures/golden_xml/{0}".format(path.name)
        for path in (_ROOT / gate.GOLDEN_DIR).glob("*.xml")
    }
    assert declared == on_disk, {
        "declared_but_missing": sorted(declared - on_disk),
        "present_but_undeclared": sorted(on_disk - declared),
    }


def test_the_registry_and_the_active_manifest_are_a_bijection():
    """Neither side may carry a case the other does not.

    An unreferenced registry case is dead code the gate never runs; a manifest
    row with no case cannot be rendered at all.
    """
    declared = {row["input_case"] for row in MANIFEST.active}
    assert declared == set(corpus.CASE_REGISTRY), {
        "manifest_only": sorted(declared - set(corpus.CASE_REGISTRY)),
        "registry_only": sorted(set(corpus.CASE_REGISTRY) - declared),
    }


def test_every_row_declares_the_renderer_its_case_declares():
    for row in MANIFEST.active:
        assert corpus.declared_renderer(row["input_case"]) == row["renderer"], row["id"]


def test_the_active_count_meets_the_committed_floor():
    """Assert a FLOOR against the committed header, never a count of what was found."""
    floor = MANIFEST.header["minimum_active"]
    assert floor >= 1
    assert len(MANIFEST.active) >= floor, (len(MANIFEST.active), floor)


def test_a_renderer_never_reads_the_file_it_is_compared_against():
    """The renderers must produce their bytes, not echo them.

    Without this the whole corpus could be a tautology: a case that returned
    ``expected_file.read_bytes()`` would match its golden forever, including
    after a regression. Rendering with the golden temporarily unreadable would
    require mutating the tree, so this asserts the weaker but still decisive
    property: no renderer source mentions the golden directory at all.
    """
    source = (_ROOT / gate.GOLDEN_CORPUS).read_text(encoding="utf-8")
    body = source.split('CASE_REGISTRY = _build_registry()')[0]
    assert "golden_xml" not in body, (
        "a renderer references the golden directory; renderers must emit, not read"
    )


def test_unknown_cases_and_renderer_mismatches_are_refused():
    """The registry is CLOSED: a manifest cannot name arbitrary Python."""
    with pytest.raises(corpus.UnknownCase):
        corpus.render_golden_case("no:such:case", "process-component-v1")
    sample = MANIFEST.active[0]
    wrong = next(r for r in corpus.RENDERERS if r != sample["renderer"])
    with pytest.raises(corpus.RendererMismatch):
        corpus.render_golden_case(sample["input_case"], wrong)


def _watched_corpus_containers():
    """Every module-level container in the corpus, by name.

    No exclusions: an earlier revision skipped ``CASE_REGISTRY`` for no stated
    reason, which left the registry itself an unwatched blind spot while the
    docstring claimed "every module-level container".
    """
    return {
        name: getattr(corpus, name)
        for name in dir(corpus)
        if name.isupper()
        and isinstance(getattr(corpus, name), (dict, list, tuple, set))
    }


def test_rendering_every_case_mutates_no_corpus_module_state():
    """The CONSEQUENCE half of the corpus CONTRACT: one case cannot perturb
    another through module-level state.

    Snapshot every module-level container, render all 60 active cases, require
    the snapshot to survive. Compared against the CURRENT attribute, not the
    object captured at snapshot time, so a rebind (``LISTENER_CHAINS = dict(...)``
    inside a renderer) is drift too — every later case would see the new table.

    The ANTECEDENT the contract states — that renderers COPY rather than share —
    is a separate, stronger property; see
    ``test_no_case_factory_hands_module_state_to_a_helper_by_reference``. This
    test alone is satisfied by builders that happen not to mutate their input,
    which is why it cannot stand in for that one.
    """
    import copy as _copy

    watched = _watched_corpus_containers()
    assert watched, "no corpus module state to watch — this check would be vacuous"
    before = {name: _copy.deepcopy(value) for name, value in watched.items()}

    for row in MANIFEST.active:
        corpus.render_golden_case(row["input_case"], row["renderer"])

    drifted = sorted(
        name for name in watched if getattr(corpus, name) != before[name]
    )
    assert drifted == [], (
        "rendering mutated corpus module-level state, so one case can perturb "
        "another through it: {0}".format(drifted)
    )


def test_no_case_factory_hands_module_state_to_a_helper_by_reference():
    """The ANTECEDENT of the CONTRACT bullet: renderers COPY shared inputs.

    The consequence test above is green even with every ``copy.deepcopy`` in the
    corpus reverted — measured, not assumed — because today's builders happen not
    to mutate what they are given. That makes it a fine regression test and a
    useless pin: a maintainer could delete the copies as redundant and nothing
    would object, re-opening the defect verbatim. So the antecedent is measured
    directly here.

    Method: wrap every PUBLIC corpus helper (the config builders the case
    factories call as module globals) so it records the arguments it receives,
    render every active case, then walk those arguments for any object that IS —
    by identity, not equality — a module-level container. One hit means a
    renderer handed shared state to production code, which is exactly what the
    contract forbids.
    """
    watched = _watched_corpus_containers()
    assert watched, "no corpus module state to watch — this check would be vacuous"
    watched_by_id = {id(obj): name for name, obj in watched.items()}

    recorded = []

    def _wrap(func):
        def _recording(*args, **kwargs):
            recorded.append((func.__name__, args, kwargs))
            return func(*args, **kwargs)
        return _recording

    helpers = {
        name: getattr(corpus, name)
        for name in dir(corpus)
        if not name.startswith("_")
        and callable(getattr(corpus, name))
        and getattr(getattr(corpus, name), "__module__", None) == corpus.__name__
    }
    assert helpers, "no corpus helpers to wrap — this check would be vacuous"

    def _leaks(obj, path, depth=0):
        """Identity hits for watched containers reachable from a recorded arg."""
        if depth > 6:
            return []
        found = []
        if id(obj) in watched_by_id:
            found.append("{0} is {1}".format(path, watched_by_id[id(obj)]))
        if isinstance(obj, dict):
            for key, value in obj.items():
                found += _leaks(value, "{0}[{1!r}]".format(path, key), depth + 1)
        elif isinstance(obj, (list, tuple, set)):
            for index, value in enumerate(obj):
                found += _leaks(value, "{0}[{1}]".format(path, index), depth + 1)
        return found

    try:
        for name, func in helpers.items():
            setattr(corpus, name, _wrap(func))
        for row in MANIFEST.active:
            corpus.render_golden_case(row["input_case"], row["renderer"])
    finally:
        for name, func in helpers.items():
            setattr(corpus, name, func)

    assert recorded, "no corpus helper was called — this check would be vacuous"
    leaks = []
    for func_name, args, kwargs in recorded:
        for index, value in enumerate(args):
            leaks += _leaks(value, "{0}(arg {1})".format(func_name, index))
        for key, value in kwargs.items():
            leaks += _leaks(value, "{0}({1}=)".format(func_name, key))
    assert leaks == [], (
        "these renderer arguments ARE module-level corpus state rather than "
        "copies of it, so a helper that mutated one would perturb every later "
        "case: {0}".format(sorted(set(leaks)))
    )


_UNIMPORTABLE_CHILD = r"""
import hashlib
import json
import os
import sys


class _Blocked(ImportError):
    '''Raised ONLY by the blocker below.

    A dedicated subclass, because `ModuleNotFoundError` is itself an
    `ImportError`: the first version of the witness caught bare `ImportError`
    and so could not tell "the blocker refused this" from "this was never on
    sys.path in the first place" — which made the witness pass with the blocker
    entirely absent.
    '''


class _BlockTestModules:
    '''Refuse to import ANY test module — the simulated #159/#160 deletion.

    Blocking every ``test_*`` name is deliberately STRONGER than deleting the
    two modules those issues own: it proves no golden depends on any test
    module at all.
    '''

    def find_spec(self, name, path=None, target=None):
        if name.rsplit(".", 1)[-1].startswith("test_"):
            raise _Blocked("test modules are unimportable in this child: " + name)
        return None


sys.meta_path.insert(0, _BlockTestModules())

tests_dir = sys.argv[1]
repo_root = os.path.dirname(tests_dir)
# The FULL path the probe needs, established BEFORE the witness runs. Inserting
# only `tests/` was not enough: the probe module imports `boomi_mcp`, so with
# the blocker disarmed it failed on that transitive import instead of loading —
# which made the "blocker is NOT armed" branch unreachable and the diagnostic
# wrong. The probe must be genuinely importable for its refusal to mean
# anything.
for _entry in (os.path.join(repo_root, "src"), repo_root, tests_dir):
    if _entry not in sys.path:
        sys.path.insert(0, _entry)

# Non-vacuity witness. Assert the probe's file exists, then require the
# blocker's OWN exception type: a `ModuleNotFoundError` here would mean the
# probe was unreachable and the witness vacuous, and a clean import would mean
# the blocker is not armed. Both are explicit failures.
probe = "test_process_flow_builder"
if not os.path.isfile(os.path.join(tests_dir, probe + ".py")):
    print(json.dumps({"error": "witness probe module is missing: " + probe}))
    sys.exit(3)
try:
    __import__(probe)
except _Blocked:
    pass
except ModuleNotFoundError as exc:
    print(json.dumps({"error": "witness probe was unreachable, not blocked: %s" % exc}))
    sys.exit(3)
else:
    print(json.dumps({"error": "the import blocker is NOT armed"}))
    sys.exit(3)

import _wave_gate_golden_corpus as corpus  # noqa: E402

rows = json.loads(open(sys.argv[2], "rb").read().decode("utf-8"))
shas = {}
for row in rows:
    payload = corpus.render_golden_case(row["input_case"], row["renderer"])
    shas[row["id"]] = hashlib.sha256(payload).hexdigest()

leaked = sorted(
    name for name in sys.modules if name.rsplit(".", 1)[-1].startswith("test_")
)
# ...and by FILE, not only by name: a factory could load a test module's file
# under a non-`test_` name via importlib and the name scan above would miss it,
# while #159/#160 deleting that file would still break the golden.
tests_real = os.path.realpath(tests_dir)
from_tests = sorted(
    name for name, mod in list(sys.modules.items())
    if getattr(mod, "__file__", None)
    and os.path.realpath(mod.__file__).startswith(tests_real + os.sep)
)
print(json.dumps({
    "shas": shas,
    "leaked_test_modules": leaked,
    "modules_loaded_from_tests": from_tests,
}))
"""


def test_every_active_golden_renders_with_all_test_modules_unimportable(tmp_path):
    """#165 acceptance criterion 3: deleting an owning test module leaves every
    golden renderable — proven for the strictly stronger condition that EVERY
    ``test_*`` module is unimportable.

    This is the property the ``transitional_oracle`` (#159) and
    ``deletion_only`` (#160) dispositions depend on: the gate must keep
    rendering those goldens AFTER the legacy tests that used to own their case
    definitions are removed.
    """
    import json as _json
    import os
    import subprocess

    request = [
        {"id": row["id"], "input_case": row["input_case"], "renderer": row["renderer"]}
        for row in MANIFEST.active
    ]
    request_path = tmp_path / "request.json"
    request_path.write_text(_json.dumps(request), encoding="utf-8")
    child_path = tmp_path / "render_without_test_modules.py"
    child_path.write_text(_UNIMPORTABLE_CHILD, encoding="utf-8")

    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    proc = subprocess.run(
        [sys.executable, str(child_path), str(_ROOT / "tests"), str(request_path)],
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
        env=env,
    )
    assert proc.returncode == 0, (proc.returncode, proc.stdout[-2000:], proc.stderr[-2000:])
    report = _json.loads(proc.stdout)
    assert "error" not in report, report

    # Every active golden rendered to its exact committed bytes.
    import hashlib

    expected = {
        row["id"]: hashlib.sha256((_ROOT / row["expected_file"]).read_bytes()).hexdigest()
        for row in MANIFEST.active
    }
    assert report["shas"] == expected, {
        "missing": sorted(set(expected) - set(report["shas"])),
        "mismatched": sorted(
            k for k in set(expected) & set(report["shas"])
            if expected[k] != report["shas"][k]
        ),
    }

    # The #159/#160 rows are really in the proven set, named so their eventual
    # retirement cannot silently drop this property.
    special = {
        row["id"]: row["disposition"]
        for row in MANIFEST.active
        if row["disposition"] in ("transitional_oracle", "deletion_only")
    }
    assert set(special) == {"golden-000056", "golden-000057", "golden-000060"}, special
    assert set(special) <= set(report["shas"])

    # And the child really finished with no test module loaded — by name, and
    # by resolved FILE, so a file-path import under a non-`test_` name cannot
    # hide a dependency on a module #159/#160 will delete. The corpus itself is
    # the one legitimate resident of tests/.
    assert report["leaked_test_modules"] == [], report["leaked_test_modules"]
    assert report["modules_loaded_from_tests"] == ["_wave_gate_golden_corpus"], (
        report["modules_loaded_from_tests"]
    )
