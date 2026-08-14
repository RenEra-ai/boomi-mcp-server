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
