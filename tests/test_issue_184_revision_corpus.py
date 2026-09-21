"""#184 correction batch 21b (CDX-184-r20-02): the compiler revision replays the tests' own inputs.

`revision-oracle-omits-changed-behaviour` recurred after three structural fixes, each of
which added hand-picked cases: batch 20's ride-on carry across a stream replacement moved
what the server accepts, tests pinned it, and `compiler_revision` stood still. So did three
more carries inside `_walk_lineage` that tests kill. The revision now folds in the verdicts
the compiler's entry points give every input the covered tests exercise, replayed from a
packaged corpus (`boomi_mcp.authoring.revision_corpus`).

THE GUARD is not a test here: `tests/conftest.py` registers the `_revision_corpus` plugin,
so in every run under `tests/`, each COVERED module (`_revision_corpus.covered_targets`)
fails — while it is imported, and in each of its tests — when it hands an entry point an
input the packaged corpus does not hold, an input the corpus cannot record exactly, or
calls one of the four internal funnels in `_revision_corpus._PROBES` with no entry point on
the stack. Those four are the funnels the recorded entry kinds pass through, not every
compiler internal: a covered test calling the emitter or the lowering directly is outside
the served bound, which speaks of an entry point's verdict. This module holds the witnesses
that the guard and the row are load-bearing.

THE COVERAGE CLAIM. Every entry-point call a covered test makes is in the packaged corpus,
so a behaviour change any covered test can observe through an entry point's verdict — a
finding with every field it carries, a refusal, a derived child-entry row, a lineage walk's
state sets, an emission plan — moves the served compiler revision. THE BOUND: a change no
covered input exhibits; an observation made other than through those verdicts; and, inside
a verdict, only what `revision_corpus.project_outcome` projects (a raise that is not a
compile refusal is recorded by type, so its message moves nothing).
"""

from __future__ import annotations

import ast
import copy
import functools
import gzip
import inspect
import json
import os
import subprocess
import sys
import textwrap
import types
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT / "src"), str(_ROOT / "tests")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.authoring import contract as authoring_contract  # noqa: E402
from boomi_mcp.authoring import revision_corpus  # noqa: E402
from boomi_mcp.authoring.revisions import canonical_json_bytes  # noqa: E402
from boomi_mcp.compiler.process_ir.semantic_validation import lineage  # noqa: E402

#: The guard is registered by `tests/conftest.py`, by the same name the producer's
#: `-p _revision_corpus` uses, so every run under `tests/` has it on.


def _producer():
    import _revision_corpus

    return _revision_corpus


def _payload():
    payload = authoring_contract._compiler_revision_payload()
    assert sorted(row for row, value in payload.items() if value == "unavailable") == []
    return payload


# ---------------------------------------------------------------------------
# the packaged corpus and its row
# ---------------------------------------------------------------------------


def test_the_guard_is_registered_over_existing_covered_modules(pytestconfig):
    producer = _producer()
    assert pytestconfig.pluginmanager.has_plugin("_revision_corpus")
    covered = producer.covered_targets()
    assert producer.GUARD_MODULE not in covered
    assert all((producer.ROOT / module).is_file() for module in covered)
    assert "tests/test_issue_184_child_state_transfer.py" in covered
    # Declared in the rootdir conftest, so a single-module run has the guard on too: with
    # the declaration in this module it loaded only when this module was collected, and a
    # developer adding a covered test locally saw green until CI ran the whole suite
    # (CDX-184-r5-CORPUS-06).
    # Every conftest pytest can load FOR THIS SUITE, taken from pytest's own authority rather
    # than from a list of directories to skip: a conftest is loaded from the rootdir and from
    # each directory on the collection path, and this suite's collection path is `tests/`
    # (`scripts/wave_gate.py` runs `pytest tests --ignore=tests/kb`). Anything else in the
    # working tree — a virtualenv, an agent worktree, a nested checkout — is neither, so it is
    # excluded by the rule instead of by name. Measured at merge time: a bare rglob over the
    # repo root picked up `.venv/**/conftest.py` from numpy, networkx and beartype, and a
    # transient `.claude/worktrees/**` copy of this very file
    # (CDX-184-r7-G3-CONFTEST-PIN-01, corrected at the merge).
    collection_path = [producer.ROOT / "conftest.py"]
    collection_path += sorted((producer.ROOT / "tests").rglob("conftest.py"))
    loadable = sorted(path.relative_to(producer.ROOT).as_posix()
                      for path in collection_path if path.is_file())
    assert loadable == [producer.PLUGIN_DECLARATION], loadable

    # …and every conftest THIS run actually loaded is that same one file. The static half
    # above is what pytest could load; this is what it did.
    loaded = sorted(
        str(Path(getattr(plugin, "__file__", "")))
        for plugin in pytestconfig.pluginmanager.get_plugins()
        if Path(getattr(plugin, "__file__", "") or "x").name == "conftest.py")
    assert loaded == [str(producer.ROOT / producer.PLUGIN_DECLARATION)], loaded
    declaration = (producer.ROOT / producer.PLUGIN_DECLARATION).read_text(encoding="utf-8")
    assert not hasattr(sys.modules[__name__], "pytest_plugins"), "declared here as well"
    # Its SHAPE, not just the line: a conftest is imported at preparse, before any wrapper
    # exists, so an entry-point call added there would be invisible to the guard it
    # registers (CDX-184-r6-GUARD-R2-04). A docstring and the declaration, nothing else.
    body = ast.parse(declaration).body
    assert [type(node).__name__ for node in body] == ["Expr", "Assign"], ast.dump(body[-1])
    assert isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str)
    assert [target.id for target in body[1].targets] == ["pytest_plugins"]
    assert [element.value for element in body[1].value.elts] == ["_revision_corpus"]


def test_the_packaged_corpus_ships_with_the_package():
    """The asset is a package RESOURCE, and losing it is silent: the row degrades to
    "unavailable" like a decorative one and the served revision then stands still whatever
    the compiler does. `assert_packaged` is the distinguishable form of that failure — it
    names the resource and the producer command — and the image build calls it beside the
    tool-import gate, because an image has no suite to run this in
    (CDX-184-r5-DOC-21B-06)."""
    from importlib import resources

    resource = resources.files("boomi_mcp.authoring").joinpath(revision_corpus.CORPUS_RESOURCE)
    assert resource.is_file()
    assert revision_corpus.assert_packaged() == len(revision_corpus.load_corpus()) > 2000
    gate = [line for line in (_ROOT / "Dockerfile").read_text(encoding="utf-8").splitlines()
            if "assert_packaged" in line]
    assert len(gate) == 1 and "boomi_mcp.authoring.revision_corpus" in gate[0], gate

    # The failure is distinguishable, and never silent: a missing resource raises with the
    # producer command in it, while the served row would only read "unavailable".
    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(revision_corpus, "CORPUS_RESOURCE", "revision_corpus_absent.json.gz")
        with pytest.raises(revision_corpus.CorpusNotPackaged) as refusal:
            revision_corpus.assert_packaged()
        assert revision_corpus.PRODUCER_COMMAND in str(refusal.value)
        assert authoring_contract._row_or_unavailable(
            authoring_contract._behaviour_corpus_payload) == "unavailable"


def _degraded(verdict):
    """Whether a verdict carries a degradation token instead of a measurement: an input that
    could not be rebuilt, a projection that raised, or a SLOT whose value could not be read
    (CDX-184-r7-ACC3-COUPLING-01 — a slot degrades on its own now, so the corpus has to be
    refused on the slot, not only on the whole verdict)."""
    if not isinstance(verdict, dict):
        return False
    if {"unreplayable", "unprojectable"} & set(verdict):
        return True
    return any(isinstance(value, dict) and "unencodable" in value for value in verdict.values())


def test_every_packaged_input_replays_to_a_verdict():
    """The cheap reverse check: no packaged input has gone stale in a way that stops it
    measuring anything. A record the current code cannot rebuild would contribute one fixed
    token to the revision whatever the compiler does. (Inputs no test makes any more are
    NOT refused: they are still behaviour the compiler has, and the producer drops them the
    next time it rewrites the corpus from a full harvest.)"""
    records = revision_corpus.load_corpus()
    verdicts = revision_corpus.replayed_verdicts(records)
    broken = [[key, verdict] for key, verdict in verdicts if _degraded(verdict)]
    assert broken == []
    # Non-vacuity: acceptances beside refusals, and more than one entry kind.
    assert len({record["entry"] for record in records}) > 3
    assert any(isinstance(verdict, dict) and "compiled" in verdict for _key, verdict in verdicts)
    assert any(isinstance(verdict, dict) and verdict.get("refused") for _key, verdict in verdicts)
    assert any(isinstance(verdict, dict) and isinstance(verdict.get("returned"), str)
               for _key, verdict in verdicts)


def test_the_corpus_row_is_in_the_compiler_revision():
    payload = _payload()
    assert payload["behaviour_corpus"] == revision_corpus.corpus_verdict_row()
    assert authoring_contract.sha256_fingerprint(payload) == authoring_contract._compiler_revision()


def test_a_fresh_interpreter_replays_the_same_row():
    """Determinism across processes: a fresh interpreter (its own hash seed, nothing the
    suite has warmed or patched) computes the row this process computes."""
    script = ("import json; from boomi_mcp.authoring import revision_corpus as c; "
              "print(json.dumps(c.corpus_verdict_row()))")
    process = subprocess.run([sys.executable, "-c", script], cwd=str(_ROOT), capture_output=True, text=True,
                             env=_producer().child_env(), check=True)
    assert json.loads(process.stdout) == revision_corpus.corpus_verdict_row()


def test_the_packaged_corpus_is_the_producer_s_own_serialization():
    """Byte for byte on the canonical body: the packaged file is what the producer writes for
    its own records, so it carries no hand edit and one record set has one body. (The gzip
    container is compared decompressed: another zlib build may compress the same body into
    other bytes, and nothing reads the compressed bytes.)"""
    raw = _producer().CORPUS_PATH.read_bytes()
    rewritten = revision_corpus.serialize_corpus(revision_corpus.parse_corpus(raw))
    assert gzip.decompress(rewritten) == gzip.decompress(raw)


_DOC = {"version": "1", "body": {"kind": "sequence", "steps": [
    {"kind": "source", "connection_ref": "$ref:conn", "operation_ref": "$ref:op"},
    {"kind": "set_dpp", "name": "OUT", "source_values": [{"value_type": "static", "value": "x"}]},
    {"kind": "target", "connection_ref": "$ref:tconn", "operation_ref": "$ref:top"},
    {"kind": "stop"}]}}


def test_an_input_is_rebuilt_exactly_even_behind_the_parser():
    """The encoding records what an object IS: a model mutated past its validator replays as
    that model, a tuple as a tuple and a list as a list — and refuses, without reading it, a
    value it cannot rebuild (a one-shot iterable here), so the call it belongs to runs
    untouched."""
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    ir = parse_process_ir_v1(_DOC)
    ir.body.steps = list(ir.body.steps)[1:]  # the parser refuses a root that does not start at a source
    rebuilt = revision_corpus.decode_value(revision_corpus.encode_value(ir))
    assert rebuilt == ir and type(rebuilt.body.steps) is list
    assert type(revision_corpus.decode_value(revision_corpus.encode_value((1, [2])))) is tuple
    one_shot = (step for step in ())
    ir.body.steps = one_shot
    with pytest.raises(revision_corpus.Unrecordable):
        revision_corpus.encode_value(ir)
    assert inspect.getgeneratorstate(one_shot) == inspect.GEN_CREATED


def _tagged_values():
    """One value per encoder tag, so no branch is carried by argument alone. Five of these
    (`$set`, `$frozenset`, `$enum`, `$bytes`, `$policy`) are exercised by no packaged input
    today — a defect in one would surface only as a fixed `unreplayable` token on the day an
    input needs it, which is exactly when a revision must still be measuring something
    (CDX-184-r5-CORPUS-07)."""
    from boomi_mcp.authoring.vetted_scripts import VettedScriptContractV1
    from boomi_mcp.compiler.process_ir.semantic_validation.validation_policy import (
        LegacyValidationPolicyV1,
        _EXEMPT_CODE,
    )
    from boomi_mcp.connector_replay.models import SideEffectV1
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    return {
        "$model": parse_process_ir_v1(_DOC),
        "$tuple": (1, ("a",), None),
        "$set": {"b", "a"},
        "$frozenset": frozenset({3, 1, 2}),
        "$map": {1: "one", "2": "two"},
        "$proxy": types.MappingProxyType({"a": [1, 2]}),
        "$enum": next(iter(SideEffectV1)),
        "$bytes": b"\x00\xff binary",
        "$policy": LegacyValidationPolicyV1("an-adapter", tuple(sorted(_EXEMPT_CODE))[:2]),
        "$script": VettedScriptContractV1(
            "groovy", "// source", reads=(("dpp", "A"),), replay_safe=True, rationale="witness"),
    }


@pytest.mark.parametrize("tag", sorted(_tagged_values()))
def test_every_encoder_tag_rebuilds_the_value_it_recorded(tag):
    """Each tag round-trips to an equal value of the same type, and is written under the tag
    it claims — so an unused branch cannot rot into one that silently rebuilds something
    else."""
    value = _tagged_values()[tag]
    encoded = revision_corpus.encode_value(value)
    assert isinstance(encoded, dict) and list(encoded) == [tag], encoded
    rebuilt = revision_corpus.decode_value(encoded)
    assert type(rebuilt) is type(value)
    if tag == "$policy":
        assert (rebuilt.adapter, rebuilt._exemptions) == (value.adapter, value._exemptions)
    elif tag == "$script":
        assert all(getattr(rebuilt, slot) == getattr(value, slot) for slot in type(value).__slots__)
    else:
        assert rebuilt == value
    # And the same value encodes the same way twice: a set is ordered by its canonical bytes.
    assert revision_corpus.canonical_json(encoded) == revision_corpus.canonical_json(
        revision_corpus.encode_value(rebuilt))


# ---------------------------------------------------------------------------
# the verdict projection: every field of a finding, not just its code and path
# ---------------------------------------------------------------------------

def _finding_authorities():
    """The finding classes the projection meets. Named here only so the test below can pin
    them against a real replay; the FIELDS come from each class itself."""
    from boomi_mcp.authoring.process_ir_effects import EffectAuthorityFindingV1
    from boomi_mcp.compiler.process_ir.diagnostics import CompilerDiagnostic
    from boomi_mcp.compiler.process_ir.semantic_validation.contracts import ValidationDiagnosticV1

    return (CompilerDiagnostic, EffectAuthorityFindingV1, ValidationDiagnosticV1)


def _carried_fields():
    """Every field the projection carries, derived from the authorities, never listed."""
    names = set()
    for finding_class in _finding_authorities():
        names.update(revision_corpus.declared_fields(finding_class))
    return sorted(names)


def _phase_other_than(item):
    from boomi_mcp.compiler.process_ir.semantic_validation.contracts import ValidationPhaseV1

    phases = [phase for phase in ValidationPhaseV1.__args__ if phase != item.phase]
    return {"phase": phases[0]}


#: One perturbation per CARRIED field. Keyed by field, looked up by the parametrization the
#: authorities generate, so a field added to any finding class arrives with no witness and
#: fails by name rather than being carried untested (CDX-184-r6-R2-PROJ-02: five of the nine
#: fields had a witness, and dropping phase, reason or internal_node_id back out left every
#: covered module green).
_DIAGNOSTIC_PERTURBATIONS = {
    "code": lambda item: {"code": item.code + "_PERTURBED"},
    "evidence": lambda item: {"evidence": ()},
    "internal_node_id": lambda item: {"internal_node_id": None},
    "message": lambda item: {"message": item.message + " (perturbed)"},
    "node_identity": lambda item: {"node_identity": "/"},
    "path": lambda item: {"path": item.path + "/perturbed"},
    "phase": _phase_other_than,
    "remediation": lambda item: {"remediation": item.remediation + " (perturbed)"},
    "severity": lambda item: {"severity": "advisory" if item.severity != "advisory" else "warning"},
}


def _perturb_every_diagnostic(field):
    """Rebuild every finding the lineage walk raises with one field changed — what a compiler
    edit at a report site does."""
    update = _DIAGNOSTIC_PERTURBATIONS[field]

    def apply(patched):
        real = lineage.finding

        def perturbed_finding(*args, **kwargs):
            item = real(*args, **kwargs)
            return item.model_copy(update=update(item))

        patched.setattr(lineage, "finding", perturbed_finding)

    return apply


def _perturb_every_resolver_finding(patched):
    """`reason` lives only on the effect resolver's value-free finding (18 of the projected
    rows), which the lineage factory cannot reach."""
    from boomi_mcp.authoring import process_ir_effects

    real = process_ir_effects.EffectAuthorityFindingV1
    patched.setattr(process_ir_effects, "EffectAuthorityFindingV1",
                    lambda code, path, reason: real(code, path, reason + "-perturbed"))


def _field_perturbation(field):
    if field == "reason":
        return _perturb_every_resolver_finding
    assert field in _DIAGNOSTIC_PERTURBATIONS, (
        "{0} is projected by no witness: add one here, or stop carrying it".format(field))
    return _perturb_every_diagnostic(field)


#: The fields NO other row can see: they change only what a lineage finding says, and every
#: other row is either a static spec table or an oracle keyed by code and pointer. Measured
#: exclusions: `severity`, `code` and `path` also decide bucketing, dedup and the oracles'
#: own keys, and `reason` is perturbed at the effect resolver, whose verdicts the
#: child-entry rows record — so those four witnesses assert the move without exclusivity.
_ONLY_THE_CORPUS_SEES = frozenset({"evidence", "internal_node_id", "message", "node_identity",
                                   "phase", "remediation"})


def _widened(kind, extra):
    """``kind`` plus one declared field, keeping a class path the projection can read."""
    if hasattr(kind, "model_fields"):
        widened = type(kind.__name__ + "Widened", (kind,), {
            "__annotations__": {extra: str}, extra: "carried"})
    elif getattr(kind, "_fields", None) is not None:
        widened = type(kind.__name__ + "Widened", (kind,), {"__slots__": (), "_fields": kind._fields + (extra,)})
    else:
        widened = type(kind.__name__ + "Widened", (kind,), {"__slots__": (extra,)})
    # The class PATH stays identical: `_projected` tags every object with it, so a renamed
    # subclass moved the row on the rename alone and three of these witnesses asserted nothing
    # about the field (CDX-184-r8-R4-PROJ-CONTAINERWITNESS-02).
    widened.__module__, widened.__qualname__ = kind.__module__, kind.__qualname__
    return widened


def _report_with_a_new_field(patched):
    from boomi_mcp.compiler.process_ir.semantic_validation import pipeline as validation_pipeline

    real = validation_pipeline.validate_process_ir

    def widened(*args, **kwargs):
        report = real(*args, **kwargs)
        kind = _widened(type(report), "blocked_by")
        return kind.model_construct(_fields_set=set(report.__pydantic_fields_set__),
                                    **{name: getattr(report, name) for name in type(report).model_fields})

    patched.setattr(validation_pipeline, "validate_process_ir", widened)


def _report_with_an_inverted_is_valid(patched):
    from boomi_mcp.compiler.process_ir.semantic_validation.contracts import ValidationReportV1

    patched.setattr(ValidationReportV1, "is_valid", property(lambda self: bool(self.errors)))


def _resolution_with_a_new_slot(patched):
    from boomi_mcp.authoring import process_ir_effects

    real = process_ir_effects.resolve_process_ir_effect_declarations

    def widened(*args, **kwargs):
        resolution = real(*args, **kwargs)
        kind = _widened(type(resolution), "rejected_refs")
        rebuilt = kind(resolution.capabilities_by_root, resolution.findings, resolution.inert)
        object.__setattr__(rebuilt, "rejected_refs", ("carried",))
        return rebuilt

    patched.setattr(process_ir_effects, "resolve_process_ir_effect_declarations", widened)


def _walk_with_a_new_field(patched):
    from boomi_mcp.compiler.process_ir.semantic_validation import lineage as lineage_module

    real = lineage_module.walk_lineage

    def widened(*args, **kwargs):
        walk = real(*args, **kwargs)
        kind = _widened(type(walk), "carried_extra")
        rebuilt = kind(*walk)
        return rebuilt

    patched.setattr(lineage_module, "walk_lineage", widened)


def _resolution_with_lists_for_tuples(patched):
    """The measured one-line source change: `EffectResolutionV1.__init__` normalises its two
    members with `tuple(...)`, three covered tests kill its removal, and the projection used
    to read tuple and list the same (CDX-184-r8-R4-PROJ-CONTAINERTYPE-01)."""
    from boomi_mcp.authoring import process_ir_effects

    real = process_ir_effects.resolve_process_ir_effect_declarations

    def as_lists(*args, **kwargs):
        resolution = real(*args, **kwargs)
        object.__setattr__(resolution, "findings", list(resolution.findings))
        object.__setattr__(resolution, "inert", list(resolution.inert))
        return resolution

    patched.setattr(process_ir_effects, "resolve_process_ir_effect_declarations", as_lists)


def _resolution_with_an_inverted_property(patched):
    """`EffectResolutionV1.ok` is a plain `@property`, served and asserted by 25 lines of a
    covered test, and inverting it moved no corpus row (CDX-184-r8-R4-PROJ-PLAINPROPERTY-03)."""
    from boomi_mcp.authoring.process_ir_effects import EffectResolutionV1

    patched.setattr(EffectResolutionV1, "ok", property(lambda self: bool(self.findings)))


#: One perturbation per VERDICT CONTAINER class, each changing what the entry point returns
#: without touching a finding field — and without renaming anything: the shapes rounds 7 and 8
#: measured leaving every served revision byte-identical.
_CONTAINER_PERTURBATIONS = {
    "the report's computed is_valid": _report_with_an_inverted_is_valid,
    "a field added to the report": _report_with_a_new_field,
    "a field added to the walk": _walk_with_a_new_field,
    "a slot added to the resolution": _resolution_with_a_new_slot,
    "the resolution's tuple normalisation": _resolution_with_lists_for_tuples,
    "the resolution's plain property": _resolution_with_an_inverted_property,
}


@pytest.mark.parametrize("case", sorted(_CONTAINER_PERTURBATIONS))
def test_a_verdict_container_field_moves_both_served_revisions(case):
    """CDX-184-r7-PROJ3-CONTAINER-01 / R3-REPORTFIELDS-01. Round 6 derived a FINDING's fields
    from its class and left the CONTAINERS hand-listed, so `project_outcome` carried three of
    the report's five served keys: inverting `is_valid` — which two covered tests assert
    through the served `validation_report` — left the row and all four revisions
    byte-identical, as did a new field on the report and a new slot on the resolution.

    A verdict is now the whole returned object, projected by the authority of each class it
    is made of, so each of these moves the row and both served revisions with the packaged
    corpus untouched."""
    baseline_row, baseline_served = _projection_baseline()
    with pytest.MonkeyPatch.context() as patched:
        _CONTAINER_PERTURBATIONS[case](patched)
        served = _served_pair()
        row = revision_corpus.corpus_verdict_row()
    assert row != baseline_row, case
    assert served[0] != baseline_served[0] and served[1] != baseline_served[1], case
    assert revision_corpus.corpus_verdict_row() == baseline_row


def test_a_verdict_container_subclass_alone_moves_nothing():
    """The control for the `_widened` cases: a subclass that adds NO field, carrying the same
    class path, must leave the row where it is. Without it those cases would pass on the
    rename alone, which is what CDX-184-r8-R4-PROJ-CONTAINERWITNESS-02 measured."""
    from boomi_mcp.compiler.process_ir.semantic_validation import pipeline as validation_pipeline

    baseline_row, _served = _projection_baseline()
    real = validation_pipeline.validate_process_ir

    def rebuilt(*args, **kwargs):
        report = real(*args, **kwargs)
        kind = type(type(report).__name__, (type(report),), {})
        kind.__module__, kind.__qualname__ = type(report).__module__, type(report).__qualname__
        return kind.model_construct(
            _fields_set=set(report.__pydantic_fields_set__),
            **{name: getattr(report, name) for name in type(report).model_fields})

    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(validation_pipeline, "validate_process_ir", rebuilt)
        assert revision_corpus.fresh_corpus_verdict_row() == baseline_row


def _first_stub(stub_class):
    stub = stub_class()
    stub.code = "X"
    return stub


def test_the_projection_reads_every_verdict_class_from_its_own_authority():
    """The classes the projection MEETS, collected from a real replay, each measured against
    its own authority: a model's fields AND computed fields, a named tuple's `_fields`, a
    slotted class's `__slots__`. Nothing here is a list of names — a class that gains a field
    is carried by the same rule the day it exists, which is what the four cases above
    measure end to end."""
    met = {}
    real = revision_corpus._projected

    def spy(value, depth=0):
        kind = type(value)
        if revision_corpus.declared_fields(kind):
            met.setdefault(kind, 0)
            met[kind] += 1
        return real(value, depth)

    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(revision_corpus, "_projected", spy)
        revision_corpus.fresh_corpus_verdict_row()
    names = {kind.__name__ for kind in met}
    assert {"ValidationReportV1", "ValidationDiagnosticV1", "CompilerDiagnostic",
            "EffectResolutionV1", "EffectAuthorityFindingV1", "LineageWalkV1"} <= names, sorted(names)
    for kind in met:
        declared = set(revision_corpus.declared_fields(kind))
        authority = set(getattr(kind, "model_fields", None) or ())
        authority |= set(getattr(kind, "model_computed_fields", None) or ())
        authority |= set(getattr(kind, "_fields", None) or ())
        for ancestor in kind.__mro__:
            authority |= set(getattr(ancestor, "__slots__", ()) or ())
            # A readable property declared in the package is served like any other member
            # (CDX-184-r8-R4-PROJ-PLAINPROPERTY-03).
            authority |= {name for name, member in vars(ancestor).items()
                          if isinstance(member, property) and member.fget is not None
                          and (member.fget.__module__ or "").startswith(("boomi_mcp", "src.boomi_mcp"))}
        assert declared == {name for name in authority if not name.startswith("__")}, kind
    # The measured instance: a plain property on the resolution, asserted by 25 lines of a
    # covered test and projected 438 times per replay.
    from boomi_mcp.authoring.process_ir_effects import EffectResolutionV1

    assert "ok" in revision_corpus.declared_fields(EffectResolutionV1)
    # The report is the measured case: five served keys, five projected.
    from boomi_mcp.compiler.process_ir.semantic_validation.contracts import ValidationReportV1

    assert set(revision_corpus.declared_fields(ValidationReportV1)) == {
        "advisories", "errors", "warnings", "version", "is_valid"}

    # A field a class declares but never assigned is OUT OF BAND: it projects as the tagged
    # object, which no string value can forge, so the three states are three verdicts
    # (CDX-184-r7-UNSET3-COLLIDE-01 measured the in-band spelling colliding, and
    # R3-UNSET-06 that the old witness compared the constant with itself).
    class _Stub:
        __slots__ = ("code", "reason")

    _Stub.__module__ = "boomi_mcp.authoring.process_ir_effects"
    _Stub.__qualname__ = "SlottedStub"
    states = []
    for value in (None, None, "$unset"):
        stub = _Stub()
        stub.code = "X"
        if value is not None or states:
            stub.reason = value
        states.append(revision_corpus.canonical_json(revision_corpus._projected(stub)))
    unassigned, none_valued, spelled = states
    assert len({unassigned, none_valued, spelled}) == 3, states
    projected = dict(revision_corpus._projected(_first_stub(_Stub))["$declared"][1])
    assert projected["reason"] == {"$unset": True}  # the literal, not the symbol
    assert revision_corpus._projected("$unset") == "$unset"

    # A declared name whose ACCESSOR raises is a degradation, not an absent field: `getattr`'s
    # default swallowed AttributeError, so a broken property read exactly like a field the
    # object never assigned (CDX-184-r9-R8V5-ATTRERR-PROPERTY-04). It reaches `_slot` now.
    def _raising(self):
        raise AttributeError("the accessor itself failed")

    _raising.__module__ = "boomi_mcp.authoring.process_ir_effects"  # a package-declared property

    class _Broken:
        __slots__ = ()
        reason = property(_raising)

    _Broken.__module__, _Broken.__qualname__ = "boomi_mcp.authoring.process_ir_effects", "BrokenStub"
    with pytest.raises(AttributeError):
        revision_corpus._projected(_Broken())
    degraded = revision_corpus._slot(_Broken)
    assert degraded == {"unencodable": "AttributeError"}, degraded
    assert _degraded({"returned": degraded}), "a raising accessor must fail the packaged replay"


def _one_finding_of(finding_class):
    """One real finding of ``finding_class``, built the way the compiler builds it."""
    from boomi_mcp.authoring.process_ir_effects import EffectAuthorityFindingV1
    from boomi_mcp.compiler.process_ir.diagnostics import CompilerDiagnostic, diagnostic

    if finding_class is EffectAuthorityFindingV1:
        return EffectAuthorityFindingV1("PROCESS_IR_EFFECT_DECLARATION_UNTRUSTED",
                                        "/effect_declarations/map_effects/0", "content-mismatch")
    if finding_class is CompilerDiagnostic:
        return diagnostic("PROCESS_IR_COMPILE_INTERNAL", "semantic_lowering", "/body/steps/0",
                          internal_node_id="n1")
    return lineage.finding("PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN", "warning", "lineage",
                           "/body/steps/0", evidence=(("effect_kind", "map"),), internal_node_id="n1")


def _projected_names(item):
    return [name for name, _value in revision_corpus._projected(item)["$declared"][1]]


def _assert_the_projection_carries(field):
    """The field is CARRIED, not merely order-affecting.

    CDX-184-r7-R3-PROJWITNESS-03, reconciled by measurement: round 6's per-field cases asserted
    only that the ROW moves. For `phase`, `code` and `path` that happens through
    `ProcessIRCompileError.__init__`, which sorts diagnostics by (phase rank, path, code), and
    for `severity` through report bucketing — so deleting any of those four from the projection
    alone left the whole guard module green, while deleting `reason` or `internal_node_id` was
    caught. (Round 2's mutation dropped three fields at once, two of them caught, which is why
    the two lenses disagreed.) This asserts the projection itself: the field's NAME is in the
    projected finding, and changing ONLY that field's value changes that finding's projection —
    a property no reordering can satisfy.
    """
    for finding_class in _finding_authorities():
        if field not in revision_corpus.declared_fields(finding_class):
            continue
        item = _one_finding_of(finding_class)
        assert field in _projected_names(item), (field, finding_class, _projected_names(item))
        before = revision_corpus._projected(item)
        if field == "reason":
            changed = type(item)(item.code, item.path, item.reason + "-perturbed")
        else:
            changed = item.model_copy(update=_DIAGNOSTIC_PERTURBATIONS[field](item))
        assert revision_corpus._projected(changed) != before, (field, finding_class)
        return
    raise AssertionError("no finding class declares {0}".format(field))


def _projection_baseline():
    """The row and the served pair with nothing patched, built ONCE for this module: every
    perturbation below is measured against it, so the module pays one manifest build for the
    baseline rather than one per field."""
    if _projection_baseline.value is None:
        served = _served_pair()
        _projection_baseline.value = (revision_corpus.corpus_verdict_row(), served)
        payload = authoring_contract._compiler_revision_payload()
        _projection_baseline.others = authoring_contract.sha256_fingerprint(
            {name: value for name, value in payload.items() if name != _CORPUS_ROW})
    return _projection_baseline.value


_projection_baseline.value = None


@pytest.mark.parametrize("field", _carried_fields())
def test_a_projected_finding_field_moves_both_served_revisions(field):
    """Every carried field, derived from the authorities: perturbing it at the site that
    builds the finding must move the corpus row AND the two revisions a caller compares.
    One manifest build per field, against one baseline built once for the module."""
    _assert_the_projection_carries(field)
    baseline_row, baseline_served = _projection_baseline()
    with pytest.MonkeyPatch.context() as patched:
        _field_perturbation(field)(patched)
        served = _served_pair()
        row = revision_corpus.corpus_verdict_row()
        payload = authoring_contract._compiler_revision_payload()
    assert row != baseline_row, field
    assert served[0] != baseline_served[0] and served[1] != baseline_served[1], field
    if field in _ONLY_THE_CORPUS_SEES:
        assert authoring_contract.sha256_fingerprint(
            {row_name: value for row_name, value in payload.items() if row_name != _CORPUS_ROW}
        ) == _projection_baseline.others, field
    assert revision_corpus.corpus_verdict_row() == baseline_row


def test_what_a_compile_returns_beside_its_plan_moves_both_served_revisions():
    """CDX-184-r6-V2-CORPUS-ACCEPT-01: an acceptance was projected as its emission plan
    alone, so trimming the CFG the entry point also RETURNS — a change 41 covered tests kill,
    and one the served `ProcessCfgSummaryV1.terminal_kinds` is derived from — left the row
    and both revisions byte-identical."""
    from boomi_mcp.compiler.process_ir import pipeline as compile_pipeline

    baseline_row, baseline_served = _projection_baseline()
    real = compile_pipeline._compile_parsed_process_ir_v1

    def without_the_first_exit(*args, **kwargs):
        cfg, plan = real(*args, **kwargs)
        return cfg.model_copy(update={"exit_node_ids": tuple(cfg.exit_node_ids[1:])}), plan

    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(compile_pipeline, "_compile_parsed_process_ir_v1", without_the_first_exit)
        served = _served_pair()
        row = revision_corpus.corpus_verdict_row()
    assert row != baseline_row
    assert served[0] != baseline_served[0] and served[1] != baseline_served[1]
    assert revision_corpus.corpus_verdict_row() == baseline_row


def test_the_projection_reads_an_enum_by_value_and_a_raise_by_type():
    """The two branches no packaged record reaches (CDX-184-r6-R2-PROJ-04): an enum member
    projects as its value, so two processes agree whatever `repr` does; a raise that is not a
    compile refusal projects as its TYPE, so a message that can carry an address moves
    nothing and a different type does."""
    from boomi_mcp.connector_replay.models import SideEffectV1

    member, other = list(SideEffectV1)[:2]
    projected = revision_corpus._projected(member)
    assert projected == {"$enum": ["boomi_mcp.connector_replay.models:SideEffectV1", member.value]}
    assert projected != revision_corpus._projected(other)
    # By VALUE and class, never by a repr: nothing in the projected bytes depends on how this
    # interpreter renders an enum member.
    assert str(member) not in revision_corpus.canonical_json(projected) or str(member) == member.value
    assert revision_corpus.project_outcome("validate", exc=ValueError("secret")) == {
        "raised": "builtins:ValueError"}
    assert (revision_corpus.project_outcome("validate", exc=ValueError("other text"))
            == revision_corpus.project_outcome("validate", exc=ValueError("secret")))
    assert (revision_corpus.project_outcome("validate", exc=TypeError("secret"))
            != revision_corpus.project_outcome("validate", exc=ValueError("secret")))

    # …and the TYPE is the class, not a bare name two classes can share
    # (CDX-184-r8-R4-PROJ-RAISENAME-05).
    class _Twin(Exception):
        pass

    _Twin.__module__, _Twin.__qualname__ = "boomi_mcp.other", "ValueError"
    assert (revision_corpus.project_outcome("validate", exc=_Twin())
            != revision_corpus.project_outcome("validate", exc=ValueError()))


# ---------------------------------------------------------------------------
# non-vacuity: the carries the tests kill and the revision used to miss
# ---------------------------------------------------------------------------


def _served_pair():
    """The two revisions the SERVER hands a caller, rebuilt from the manifest."""
    authoring_contract.reset_manifest_cache()
    revisions = authoring_contract.get_authoring_revisions()
    return revisions["compiler_revision"], revisions["capability_revision"]


def test_the_ride_on_carry_past_a_stream_replacement_moves_the_compiler_revision(monkeypatch):
    """(a) Batch 20's carry, neutralised with the wrapper
    `test_issue_184_child_state_transfer.py::test_the_ride_on_cache_surviving_a_stream_replacement_is_load_bearing`
    uses. Measured before this row: the compiler revision was byte-identical with and
    without it.

    The SERVED pair is pinned here, not inferred (CDX-184-r5-DOC-21B-04): amendment 3 §10
    asks that a behaviour change move the revisions a caller compares, and
    `capability_revision` covers `compiler_revision` only because the manifest is
    fingerprinted after the compiler row is folded in. This is the one witness that would
    fail if that order changed."""
    baseline = _payload()
    served = _served_pair()
    assert served[0] == authoring_contract.sha256_fingerprint(baseline)
    real_stream = lineage._Stream

    def without_the_ride_on_marker(*args, **kwargs):
        if kwargs.get("origin") == "opaque":
            kwargs.pop("retrieved_from", None)
        return real_stream(*args, **kwargs)

    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_Stream", without_the_ride_on_marker)
        perturbed = _payload()
        perturbed_served = _served_pair()
    assert perturbed["behaviour_corpus"] != baseline["behaviour_corpus"]
    assert authoring_contract.sha256_fingerprint(perturbed) != authoring_contract.sha256_fingerprint(baseline)
    assert perturbed_served[0] != served[0], "the served compiler_revision stood still"
    assert perturbed_served[1] != served[1], "the served capability_revision stood still"
    assert _payload() == baseline
    assert _served_pair() == served


def _walk_stream_calls(tree):
    walk = next(node for node in tree.body
                if isinstance(node, ast.FunctionDef) and node.name == "_walk_lineage")
    return [node for node in ast.walk(walk)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "_Stream"]


def _keywords(call):
    return {keyword.arg: keyword.value for keyword in call.keywords}


def _is_constant(node, value):
    return isinstance(node, ast.Constant) and node.value == value


def _is_name(node, name):
    return isinstance(node, ast.Name) and node.id == name


def _null_positional(index):
    def mutate(call):
        call.args[index] = ast.Constant(None)
    return mutate


def _drop_keyword(name):
    def mutate(call):
        call.keywords = [keyword for keyword in call.keywords if keyword.arg != name]
    return mutate


def _unchanged(call):
    return None


def _a_map_s_stream(call):
    # A map hands on the known stream it established: `_Stream(STREAM_KNOWN, target, "map", node)`.
    return len(call.args) == 4 and _is_constant(call.args[2], "map")


def _a_caller_entry_taking_an_identity(call):
    # `_Stream(STREAM_CALLER_ENTRY, identity, provably_nonempty=...)`
    return (len(call.args) == 2 and _is_name(call.args[0], "STREAM_CALLER_ENTRY")
            and _is_name(call.args[1], "identity"))


def _a_retrieve_of_unknown_content(call):
    """`_Stream(STREAM_UNKNOWN, origin="cache", …)`: the retrieve whose content no write proves.

    Selected by what the call MEANS — one positional stream kind and a cache origin — not by
    the exact keyword set it had when this witness was written. Batch 21a added
    `retrieved_origins` (and `caller_cache`) to this very call, and an exact-set selector
    matched nothing on the merged tree: six witnesses failed with an empty site list, which is
    the harness working and the selector being stale (CDX-184-r10, merge).
    """
    keywords = _keywords(call)
    return (len(call.args) == 1 and _is_name(call.args[0], "STREAM_UNKNOWN")
            and _is_constant(keywords.get("origin"), "cache"))


#: (b) The carries found killed by tests yet revision-silent, each identified by its CODE:
#: ``(which _Stream construction in _walk_lineage, how it is neutralised)``.
_TEST_KILLED_CARRIES = {
    "the_map_stream_origin": (_a_map_s_stream, _null_positional(2)),
    "the_map_stream_origin_node": (_a_map_s_stream, _null_positional(3)),
    "the_caller_entry_identity": (_a_caller_entry_taking_an_identity, _null_positional(1)),
    "the_retrieve_attribution": (_a_retrieve_of_unknown_content, _drop_keyword("retrieved_from")),
    # Batch 21a's cohort-origin carrier on the same retrieve — the merged tree's equivalent of
    # the attribution carry, and 21a kills it with its own source mutant
    # (`_lineage_with_source(patched, "retrieved_origins=inherited", "retrieved_origins=()")`).
    # Measured here: neutralising it moves the replayed row.
    "the_retrieve_cohort_origins": (_a_retrieve_of_unknown_content, _drop_keyword("retrieved_origins")),
}


@functools.lru_cache(maxsize=None)
def _mutated_walk(select, mutate):
    source = Path(lineage.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    sites = [call for call in _walk_stream_calls(tree) if select(call)]
    assert len(sites) == 1, [ast.get_source_segment(source, call) for call in sites]
    mutated = copy.deepcopy(tree)
    target = next(call for call in _walk_stream_calls(mutated)
                  if (call.lineno, call.col_offset) == (sites[0].lineno, sites[0].col_offset))
    mutate(target)
    code = compile(ast.fix_missing_locations(mutated), lineage.__file__, "exec")
    return next(const for const in code.co_consts
                if isinstance(const, types.CodeType) and const.co_name == "_walk_lineage")


def _payload_with_walk(code):
    # `inspect.unwrap`: the guard may have wrapped the walk, and the wrapper calls through.
    walk = inspect.unwrap(lineage._walk_lineage)
    real = walk.__code__
    walk.__code__ = code
    try:
        return _payload()
    finally:
        walk.__code__ = real


@pytest.mark.parametrize("carry", sorted(_TEST_KILLED_CARRIES))
def test_a_carry_the_tests_kill_moves_the_compiler_revision(carry):
    """(b) Each neutralised in memory by giving `_walk_lineage` the same code minus that one
    argument. Measured before this row: the compiler revision did not move for any of them,
    while a test failed for each."""
    baseline = _payload()
    perturbed = _payload_with_walk(_mutated_walk(*_TEST_KILLED_CARRIES[carry]))
    assert perturbed["behaviour_corpus"] != baseline["behaviour_corpus"], carry
    assert authoring_contract.sha256_fingerprint(perturbed) != authoring_contract.sha256_fingerprint(baseline)


def test_an_unchanged_walk_recompiled_moves_nothing():
    """The control: the same harness with no mutation leaves the row where it is, so a moved
    row above is a changed verdict, never a replaced code object."""
    baseline = _payload()
    perturbed = _payload_with_walk(_mutated_walk(_a_retrieve_of_unknown_content, _unchanged))
    assert perturbed["behaviour_corpus"] == baseline["behaviour_corpus"]


# ---------------------------------------------------------------------------
# the memo: keyed by the identity of the live code, never by a flag
# ---------------------------------------------------------------------------


def _primed():
    """The row, with the memo holding it. A cold process's first replay imports modules the
    payload's other rows use, so that computation is not stored and the second is."""
    row = revision_corpus.corpus_verdict_row()
    if not revision_corpus.memo_would_hit():
        row = revision_corpus.corpus_verdict_row()
    assert revision_corpus.memo_would_hit()
    return row


def _coverage():
    import test_issue_184_revision_coverage as coverage

    return coverage


_TUPLES_FOR_LISTS = "tuples for lists"


def _nested_tuples(value):
    if isinstance(value, list):
        return tuple(_nested_tuples(item) for item in value)
    if isinstance(value, dict):
        return {key: _nested_tuples(item) for key, item in value.items()}
    return value


def _rows_as_tuples(patched):
    """A row rebuilt with tuples where it held lists: Python-unequal, byte-identical, so the
    served revision does NOT move. It is in the perturbation set (CDX-184-r5-B21B-T2) because
    it is the only case that refuses a comparison of rows by Python equality, and the
    unsampled short-circuit witness must not depend on a sibling test for that arm."""
    real_rows = authoring_contract._document_emission_rows
    patched.setattr(authoring_contract, "_document_emission_rows", lambda: _nested_tuples(real_rows()))


def _every_perturbation():
    """``[(name, apply(patched))]``: every perturbation `test_issue_184_revision_coverage.py`
    applies — derived from its own maps, plus the two it applies inline — and this module's
    ride-on wrapper, code-level carries and the tuples-for-lists rebuild."""
    from boomi_mcp.compiler.process_ir.semantic_validation import pipeline as validation_pipeline

    coverage = _coverage()
    applied = [
        ("decision " + ".".join(key),
         lambda patched, key=key, replacement=replacement: coverage._patch(patched, key, replacement))
        for key, replacement in sorted(coverage._perturbations().items())
    ]
    applied += [
        ("call-state rule " + rule,
         lambda patched, target=target, replacement=replacement: coverage._patch(
             patched, ("lineage", target), replacement))
        for rule, (_row, target, replacement) in sorted(coverage._CALL_STATE_RULES.items())
    ]
    applied += [
        ("a retrieve dropping " + component,
         lambda patched, component=component: patched.setattr(
             lineage, "_overlay_cache_read", coverage._dropping(component)))
        for component in sorted(lineage._State.__slots__)
    ]

    def equivalent_decisions(patched):
        real_overlay, real_merge = lineage._overlay_cache_read, lineage._State.merged_with
        patched.setattr(lineage, "_overlay_cache_read", lambda *args: real_overlay(*args))
        patched.setattr(lineage._State, "merged_with", lambda self, other: real_merge(self, other))

    def no_cache_canonicalization(patched):
        for module in (validation_pipeline, lineage):
            patched.setattr(module, "canonical_cache_capabilities", lambda capabilities, canonical: capabilities)

    real_stream = lineage._Stream

    def ride_on_wrapper(patched):
        def without_the_ride_on_marker(*args, **kwargs):
            if kwargs.get("origin") == "opaque":
                kwargs.pop("retrieved_from", None)
            return real_stream(*args, **kwargs)

        patched.setattr(lineage, "_Stream", without_the_ride_on_marker)

    applied += [("equivalent decisions (inline)", equivalent_decisions),
                ("no cache canonicalization (inline)", no_cache_canonicalization),
                ("the ride-on wrapper", ride_on_wrapper),
                (_TUPLES_FOR_LISTS, _rows_as_tuples)]
    applied += [
        ("the carry " + carry,
         lambda patched, select=select, mutate=mutate: patched.setattr(
             inspect.unwrap(lineage._walk_lineage), "__code__", _mutated_walk(select, mutate)))
        for carry, (select, mutate) in sorted(_TEST_KILLED_CARRIES.items())
    ]
    return applied


def test_the_memo_misses_under_every_perturbation_the_revision_tests_apply(monkeypatch):
    """(a) Soundness, over EVERY perturbation the revision tests apply, derived from their own
    maps. With the memo holding only the unperturbed row, each perturbation must MISS — and a
    miss is a replay, which is exactly the forced-fresh computation — and once it is undone the
    memo must HIT again, with the row a fresh replay computes. So the memoized row equals a
    fresh one both ways, without replaying a hundred times to show it."""
    fresh = revision_corpus.fresh_corpus_verdict_row()
    assert _primed() == fresh
    # Only the unperturbed entry: an entry an earlier test computed for one of these very
    # perturbations (the same cached code object, say) would be a CORRECT hit, and would hide
    # whether the key sees the perturbation at all.
    del revision_corpus._MEMO.entries[:-1]
    perturbations = _every_perturbation()
    coverage = _coverage()
    assert len(perturbations) == (len(coverage._perturbations()) + len(coverage._CALL_STATE_RULES)
                                  + len(lineage._State.__slots__) + 4 + len(_TEST_KILLED_CARRIES))
    stale, unrestored = [], []
    for name, apply in perturbations:
        with monkeypatch.context() as patched:
            apply(patched)
            if revision_corpus.memo_would_hit():
                stale.append(name)
        if not revision_corpus.memo_would_hit():
            unrestored.append(name)
    assert {"answered from the memo while perturbed": stale, "missed once undone": unrestored} == {
        "answered from the memo while perturbed": [], "missed once undone": []}
    hits = revision_corpus._MEMO.hits
    assert revision_corpus.corpus_verdict_row() == fresh
    assert revision_corpus._MEMO.hits == hits + 1


def test_a_perturbed_state_the_memo_holds_is_answered_with_its_own_row(monkeypatch):
    """The other half of (a), for the multi-entry memo: a hit under a perturbation is legal
    only when the memo computed THAT state. Replaying a perturbation twice with the same objects
    (the cached code object of a carry) hits the second time, with the row a fresh replay
    computes under it — and not the unperturbed row."""
    baseline = _primed()
    select, mutate = _TEST_KILLED_CARRIES["the_retrieve_attribution"]
    code = _mutated_walk(select, mutate)
    with monkeypatch.context() as patched:
        patched.setattr(inspect.unwrap(lineage._walk_lineage), "__code__", code)
        first = revision_corpus.corpus_verdict_row()
        assert revision_corpus.memo_would_hit()
        hits = revision_corpus._MEMO.hits
        second = revision_corpus.corpus_verdict_row()
        assert revision_corpus._MEMO.hits == hits + 1
        assert first == second == revision_corpus.fresh_corpus_verdict_row() != baseline
    assert revision_corpus.corpus_verdict_row() == baseline


#: Non-package bindings the key holds, each with a perturbation that CHANGES what the replay
#: answers, so the witness below measures staleness rather than asserting a policy. The first
#: two are the round-5 stale hits; the last three are the ones NAMING callables still missed
#: (CDX-184-r6-MEMO-R2-01) and the module walk now covers — `json.dumps` was pinned while the
#: encoder it delegates to was not.
def _tagged_dump(real):
    def dump(self, *args, **kwargs):
        answer = real(self, *args, **kwargs)
        return dict(answer, __perturbed__=1) if isinstance(answer, dict) else answer

    return dump


def _tagged_init(real):
    def init(self, *args, **kwargs):
        real(self, *args, **kwargs)
        object.__setattr__(self, "__pydantic_fields_set__",
                           set(self.__pydantic_fields_set__) | {"__perturbed__"})

    return init


def _tagged_iterencode(real):
    def iterencode(self, o, _one_shot=False):
        for chunk in real(self, o, _one_shot):
            yield chunk
        yield " "

    return iterencode


_NON_PACKAGE_PERTURBATIONS = {
    "pydantic.BaseModel.model_dump": ("pydantic.main", "BaseModel", "model_dump", _tagged_dump),
    "pydantic.BaseModel.__init__": ("pydantic.main", "BaseModel", "__init__", _tagged_init),
    "pydantic.BaseModel.__eq__": (
        "pydantic.main", "BaseModel", "__eq__", lambda real: (lambda self, other: False)),
    "json.dumps": ("json", None, "dumps", lambda real: (lambda *a, **k: real(*a, **k) + " ")),
    "json.encoder.JSONEncoder.iterencode": (
        "json.encoder", "JSONEncoder", "iterencode", _tagged_iterencode),
}


@pytest.mark.parametrize("binding", sorted(_NON_PACKAGE_PERTURBATIONS))
def test_a_patched_non_package_binding_the_replay_calls_misses_the_memo(binding):
    """(b, outside the package) CDX-184-r5-STALE-01: the key walked only `boomi_mcp` modules,
    so patching a third-party or stdlib callable the replay CALLS left the memo hitting and
    the SERVED row stale. Naming the callables then left three more serving stale rows
    (CDX-184-r6-MEMO-R2-01); the key holds the callable surface of every MODULE the replay
    was measured to call into, so each of these is a miss, and the served row equals a fresh
    replay under the perturbation.

    Bound by hand, not with monkeypatch: `pydantic.BaseModel.model_dump` is read off the
    class, and restoring it must put back exactly the object the key held."""
    import importlib

    module_name, owner_name, attribute, make = _NON_PACKAGE_PERTURBATIONS[binding]
    baseline = _primed()
    owner = importlib.import_module(module_name)
    if owner_name is not None:
        owner = getattr(owner, owner_name)
    real = vars(owner)[attribute]
    setattr(owner, attribute, make(real))
    try:
        assert not revision_corpus.memo_would_hit(), binding
        fresh = revision_corpus.fresh_corpus_verdict_row()
        assert fresh != baseline, binding
        assert revision_corpus.corpus_verdict_row() == fresh, binding
    finally:
        setattr(owner, attribute, real)
    assert revision_corpus.memo_would_hit()
    assert revision_corpus.corpus_verdict_row() == baseline


def test_the_non_package_modules_the_key_holds_are_re_measured_here():
    """CDX-184-r6-MEMO-R2-01: the key named seven non-package CALLABLES and called that "the
    set the replay calls"; profiling one replay found 155, three of which served stale rows.
    The measurement is the authority now, and this re-runs it: every module a replay calls
    into must be one whose callable surface the key holds. A module the replay starts using
    fails here by name instead of quietly leaving the key.

    Package modules are held by the identity walk; a module that is not in `sys.modules` is a
    generated namedtuple namespace whose functions live in the package class that owns them.
    """
    import sys as _sys

    def owner(function):
        for candidate in (getattr(function, "__module__", None),
                          getattr(getattr(function, "__objclass__", None), "__module__", None),
                          getattr(type(getattr(function, "__self__", None)), "__module__", None)):
            if candidate:
                return candidate
        return None

    called = set()

    def profile(frame, event, arg):
        if event == "call":
            called.add(frame.f_globals.get("__name__"))
        elif event == "c_call":
            called.add(owner(arg))
        return None

    revision_corpus.fresh_corpus_verdict_row()  # warm: the replay's own imports have happened
    _sys.setprofile(profile)
    try:
        revision_corpus.fresh_corpus_verdict_row()
    finally:
        _sys.setprofile(None)
    package = ("boomi_mcp.", "src.boomi_mcp")
    # The GUARD's own frames are not compiler behaviour and are not key material: when a
    # covered module is collected, the replay runs through the plugin's entry wrapper and the
    # profiler sees it, so a src-side list could never satisfy this witness in a combined run
    # (CDX-184-r7-SUITE3-PROFILE-01). The same argument `_EXCLUDED` already makes for
    # recording, made here for measuring; the case below runs it alongside a covered module.
    guard = {_producer().__name__, __name__, __name__.rpartition(".")[2]}
    outside = {name for name in called
               if name and name in _sys.modules and name not in guard
               and name != "boomi_mcp" and not name.startswith(package)}
    assert outside, "the profile measured nothing: the witness would be vacuous"
    assert outside <= set(revision_corpus._NON_PACKAGE_MODULES), sorted(
        outside - set(revision_corpus._NON_PACKAGE_MODULES))
    # Non-vacuity of the holding itself: the listed modules do contribute a real surface.
    held = revision_corpus._non_package_identity()
    assert len(held) > 1000 and len(set(map(id, held))) > 500


def test_the_profile_witness_holds_when_a_covered_module_is_collected_too():
    """The combined shape, in a child process: with a covered module in the same run the
    guard's wrappers are installed at collection, so the replay the witness profiles runs
    through them. Measured before the fix: the witness failed with `['_revision_corpus']`
    whenever any covered module was collected — i.e. in every full-suite run
    (CDX-184-r7-SUITE3-PROFILE-01)."""
    producer = _producer()
    node = "{0}::{1}".format(producer.GUARD_MODULE,
                             "test_the_non_package_modules_the_key_holds_are_re_measured_here")
    process = producer.run_pytest([node, producer.covered_targets()[0]])
    assert process.returncode == 0, (process.stdout + process.stderr)[-3000:]


def test_a_keyword_default_set_in_place_misses_the_memo():
    """(b, one level down again) CDX-184-r6-MEMO-R2-02: a function's default tables were held
    by identity, so setting a keyword default IN PLACE — the one live instance is the
    provenance the lowering stamps on a cfg edge, which the emission plan a compile
    acceptance is digested over carries — served a stale row. The tables are walked now, like
    any other module-level container."""
    from boomi_mcp.compiler.process_ir import lowering

    baseline = _primed()
    defaults = lowering._transition.__kwdefaults__
    assert defaults["provenance"] == "cfg_edge"
    defaults["provenance"] = "synthetic"
    try:
        assert not revision_corpus.memo_would_hit()
        fresh = revision_corpus.fresh_corpus_verdict_row()
        assert fresh != baseline
        assert revision_corpus.corpus_verdict_row() == fresh
    finally:
        defaults["provenance"] = "cfg_edge"
    assert revision_corpus.memo_would_hit()
    assert revision_corpus.corpus_verdict_row() == baseline


def test_an_unreadable_returned_value_costs_only_its_own_slot():
    """CDX-184-r7-ACC3-COUPLING-01. The acceptance computed its plan hash and the digest of
    everything else in one expression, so one unreadable field on the returned CFG turned all
    367 acceptances into a single constant token and took the plan hash — the served bytes a
    compile hash is taken over — with it. Each half is its own slot now: the unreadable one
    degrades, names why, and the packaged-replay test refuses a corpus that produces one,
    while the plan hash still discriminates."""
    import datetime

    accepted = None
    for record in revision_corpus.load_corpus():
        if record["entry"] != "compile":
            continue
        function, arguments = revision_corpus._rebuild(record)
        try:
            accepted = function(*arguments)
        except Exception:  # noqa: BLE001 - most packaged compile inputs are refusals
            continue
        break
    assert accepted is not None, "no packaged compile input is accepted"
    cfg, plan = accepted
    readable = revision_corpus.project_outcome("compile", (cfg, plan))
    unreadable = revision_corpus.project_outcome("compile", (datetime.datetime(2020, 1, 1), plan))
    assert unreadable["compiled"] == readable["compiled"]
    assert set(unreadable["returned"]) == {"unencodable"}, unreadable["returned"]
    assert "datetime" in unreadable["returned"]["unencodable"]
    assert isinstance(readable["returned"], str)

    # …and the surviving half still measures the plan: a different plan is a different hash.
    field = next(name for name in revision_corpus.declared_fields(type(plan))
                 if isinstance(getattr(plan, name), tuple) and getattr(plan, name))
    trimmed = plan.model_copy(update={field: getattr(plan, field)[1:]})
    other = revision_corpus.project_outcome("compile", (datetime.datetime(2020, 1, 1), trimmed))
    assert other["compiled"] != unreadable["compiled"]


def test_the_bound_names_the_container_item_the_key_cannot_see():
    """CDX-184-r7-R3-NONPKGCONTAINER-05, pinned rather than closed, with the measurement in
    the open: the key holds a listed non-package module's attributes and the namespaces of the
    classes defined there, never its containers' ITEMS. `copy._copy_dispatch[dict]` is the one
    live instance measured to move a replayed row while the memo still hits.

    Walking those containers was measured and rejected: `re._cache` and `posix.environ` churn
    while the process runs, so the key would differ between two calls in the same state and
    the memo would never hit. The bound says this in the module, and this test is what keeps
    the statement true — if the key ever grows to cover it, the first assertion fails and the
    bound must be rewritten."""
    import copy as copy_module

    baseline = _primed()
    dispatch = copy_module._copy_dispatch
    real = dispatch[dict]
    dispatch[dict] = lambda value: {}
    try:
        assert revision_corpus.memo_would_hit(), "the key now sees it: rewrite THE BOUND"
        assert revision_corpus.fresh_corpus_verdict_row() != baseline, "no longer a live instance"
    finally:
        dispatch[dict] = real
    assert revision_corpus.corpus_verdict_row() == baseline
    source = Path(revision_corpus.__file__).read_text(encoding="utf-8")
    assert "_copy_dispatch" in source, "THE BOUND must name the measured instance"


def test_a_lambda_inside_a_module_level_tuple_misses_the_memo(monkeypatch):
    """(b, one level down) CDX-184-r5-STALE-02: a tuple's members were held by the tuple's own
    identity alone, so swapping the `__code__` of a lambda a module-level rules table
    dispatches left the memo hitting. Both measured instances are here, each named by what it
    IS in the table rather than by an index the table can renumber."""
    from boomi_mcp.compiler.process_ir import pipeline as compile_pipeline
    from boomi_mcp.models import authoring_workflow

    tables = {
        "the bytes reducer in the compile pipeline's scalar slots":
            next(member for _kind, member in compile_pipeline._SCALAR_SLOTS
                 if getattr(member, "__name__", "") == "<lambda>"),
        "the map-effects binding key of the authoring workflow":
            dict(authoring_workflow._BINDING_KEYS_V1)["map_effects"],
    }

    def _other(value):  # a different code object with the same signature
        return value

    _primed()
    for label, lambda_ in tables.items():
        real = lambda_.__code__
        lambda_.__code__ = _other.__code__
        try:
            assert not revision_corpus.memo_would_hit(), label
        finally:
            lambda_.__code__ = real
        assert revision_corpus.memo_would_hit(), label


def test_the_manifest_is_built_once_under_concurrent_cold_callers():
    """CDX-184-r5-STALE-03: the manifest cache was an unguarded get/set, so cold callers on
    the server's worker threads each replayed the whole corpus — four threads took 13.3 s
    against 2.3 s for one. The miss path is serialized now, and the memo refuses to store a
    key it already holds, so concurrent stores cannot occupy the entries twice."""
    import threading

    authoring_contract.reset_manifest_cache()
    # Cold in BOTH caches, or four threads would answer from the memo and measure nothing.
    del revision_corpus._MEMO.entries[:]
    computations = revision_corpus._MEMO.computations
    answers, entries = [], len(revision_corpus._MEMO.entries)
    threads = [threading.Thread(target=lambda: answers.append(
        authoring_contract.build_authoring_contract_manifest()["compiler_revision"]))
        for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    # Counted before anything else asks for the row: one cold build, one replay.
    assert revision_corpus._MEMO.computations - computations == 1
    assert len(set(answers)) == 1 and answers[0] == authoring_contract._compiler_revision()
    keys = [key for key, _held, _row in revision_corpus._MEMO.entries]
    assert len(keys) == len({tuple(key) for key in keys}) <= revision_corpus.MEMO_ENTRIES
    assert len(revision_corpus._MEMO.entries) <= entries + 1


def test_an_in_place_setitem_on_a_module_level_table_misses_the_memo(monkeypatch):
    """(b) A table patched IN PLACE keeps its own identity; its item does not. The lineage
    module's own tables are read-only proxies (they can only be REPLACED, which the
    perturbation map above covers), so the table here is a mutable one the replay was
    measured to read: the map builders' dispatch table the effect derivation consults."""
    from boomi_mcp.categories.components.builders import map_builder

    with pytest.raises(TypeError):
        lineage.PROPERTY_SURVIVAL_V1[("message", None)] = "unmeasured"  # type: ignore[index]
    assert ("boomi_mcp.categories.components.builders.map_builder", "MAP_BUILDERS") not in (
        revision_corpus._UNREAD_BY_THE_REPLAY)
    _primed()
    kind = next(iter(map_builder.MAP_BUILDERS))
    with monkeypatch.context() as patched:
        patched.setitem(map_builder.MAP_BUILDERS, kind, map_builder.MAP_BUILDERS[kind])
        assert revision_corpus.memo_would_hit(), "re-setting an item to the same object is no change"
        patched.setitem(map_builder.MAP_BUILDERS, kind, type("Replaced", (), {}))
        assert not revision_corpus.memo_would_hit()
    assert revision_corpus.memo_would_hit()


class _Unreadable(BaseException):
    """Raised by any use of a sentinel. A BaseException, so no `except Exception` on the replay
    path can turn a read into an ordinary verdict."""


def _unusable(*_args, **_kwargs):
    raise _Unreadable()


class _Sentinel:
    __slots__ = ()

    def __getattribute__(self, name):
        raise _Unreadable(name)


for _dunder in ("__call__", "__getitem__", "__setitem__", "__delitem__", "__iter__", "__len__",
                "__contains__", "__bool__", "__eq__", "__ne__", "__lt__", "__gt__", "__le__", "__ge__",
                "__hash__", "__str__", "__format__", "__int__", "__index__", "__or__", "__ror__",
                "__and__", "__rand__", "__sub__", "__add__", "__radd__", "__enter__", "__exit__",
                "__reversed__", "__get__", "__instancecheck__", "__subclasscheck__", "__mro_entries__"):
    setattr(_Sentinel, _dunder, _unusable)


def _replay_with_sentinels(bindings):
    """The row replayed with every binding (in every loaded module copy) rebound to a sentinel.
    Bound and restored by hand: a sentinel cannot be inspected, and a patch helper would."""
    import importlib
    import sys as _sys

    saved = []
    try:
        for module_name, attribute in sorted(bindings):
            copies = [_sys.modules[name] for name in (module_name, "src." + module_name) if name in _sys.modules]
            if not copies:
                copies = [importlib.import_module(module_name)]
            assert any(attribute in vars(copy_) for copy_ in copies), (module_name, attribute)
            for copy_ in copies:
                if attribute in vars(copy_):
                    saved.append((copy_, attribute, vars(copy_)[attribute]))
                    setattr(copy_, attribute, _Sentinel())
        try:
            return revision_corpus.fresh_corpus_verdict_row()
        except _Unreadable:
            return "a sentinel was used"
    finally:
        for copy_, attribute, original in reversed(saved):
            setattr(copy_, attribute, original)


def test_the_bindings_the_memo_leaves_out_are_unread_by_the_replay():
    """(i) The measurement that licenses each exclusion, repeated on every run: with EVERY
    excluded binding rebound to a sentinel that raises on any use, the whole corpus replays to
    the same row. A corpus input that starts reading one of them fails here, naming none but
    moving the row — and the control shows the sentinel does catch a binding that IS read."""
    # Every excluded binding exists once the payload has been built (some are submodules the
    # manifest imports lazily); a binding that no longer exists fails, so the list stays true.
    authoring_contract._compiler_revision_payload()
    baseline = revision_corpus.fresh_corpus_verdict_row()
    assert _replay_with_sentinels(revision_corpus._UNREAD_BY_THE_REPLAY) == baseline
    control = {("boomi_mcp.compiler.process_ir.semantic_validation.lineage", "_overlay_cache_read")}
    assert _replay_with_sentinels(control) != baseline


def test_a_class_attribute_patch_misses_the_memo(monkeypatch):
    """(c) A method replaced on a class the replay uses, and a class-level table replaced on a
    builder the effect derivation consults: both keep every module attribute's identity."""
    from boomi_mcp.categories.components.builders import map_builder

    _primed()
    real_merge = lineage._State.merged_with
    with monkeypatch.context() as patched:
        patched.setattr(lineage._State, "merged_with", lambda self, other: real_merge(self, other))
        assert not revision_corpus.memo_would_hit()
    assert revision_corpus.memo_would_hit()
    with monkeypatch.context() as patched:
        patched.setattr(map_builder.MapFunctionBuilder, "SUPPORTED_MAP_TYPES",
                        frozenset(map_builder.MapFunctionBuilder.SUPPORTED_MAP_TYPES))
        assert not revision_corpus.memo_would_hit()
    assert revision_corpus.memo_would_hit()


def test_undoing_a_perturbation_returns_to_a_hit_that_equals_a_fresh_replay(monkeypatch):
    """(d) Measured end to end, with a real replay under the perturbation: the perturbed row is
    computed (and becomes the memo's entry), and once undone the baseline is answered again —
    equal to a replay that bypasses the memo."""
    baseline = _primed()
    with monkeypatch.context() as patched:
        patched.setattr(lineage, "_after_a_whole_cache_removal",
                        lambda state, cache_ref, proved_to_run: state.without_content(cache_ref))
        perturbed = revision_corpus.corpus_verdict_row()
    assert perturbed != baseline
    assert revision_corpus.corpus_verdict_row() == baseline == revision_corpus.fresh_corpus_verdict_row()
    assert revision_corpus.memo_would_hit()


_FIRST_REPLAY_SCRIPT = (
    "import json; from boomi_mcp.authoring import revision_corpus as c; "
    "rows = [c.corpus_verdict_row() for _ in range(3)]; "
    "print(json.dumps({'computations': c._MEMO.computations, 'hits': c._MEMO.hits, "
    "'entries': len(c._MEMO.entries), 'same': rows[0] == rows[-1] == c.fresh_corpus_verdict_row()}))"
)


def test_the_first_replay_in_a_process_is_the_only_one():
    """CDX-184-r7-MEMO3-FIRSTREPLAY-01. The store is gated on NOTHING having changed while the
    row was computed, and a first replay used to change the key — it imports the modules it
    needs — so every process replayed the whole corpus twice: measured 1.59 s / 1.10 s /
    0.008 s with computations=2. `_settle` empties that window before the key is taken (one
    record of every entry kind, every class path the corpus names, and the
    `boomi_mcp.categories`/`boomi_mcp.recipes` subtrees), so the first row is stored and the
    second caller hits.

    In a fresh interpreter, because that is the only place the property exists: nothing in
    this module's process is cold by the time a test runs."""
    process = subprocess.run([sys.executable, "-c", _FIRST_REPLAY_SCRIPT], cwd=str(_ROOT),
                             capture_output=True, text=True, env=_producer().child_env(), check=True)
    measured = json.loads(process.stdout)
    assert measured == {"computations": 1, "hits": 2, "entries": 1, "same": True}, measured


def test_a_row_computed_across_a_change_to_what_already_existed_is_not_memoized():
    """The other half: the store must refuse a row whose state moved under it.

    Two rounds of this branch accepted one. CDX-184-r7-R3-SUBSETSTORE-04: `set(key).issubset`
    over a key holding 3,412 ids more than once, which an ADDITIVE mid-replay change
    satisfied. CDX-184-r8-B21B-R4-STORE-01: a walk restricted to the modules already loaded,
    which could not see the 20 the replay imports. There is no branch now — the row is stored
    only when the key is unchanged, and `_settle` makes that the normal case — so a change to
    anything at all while the row is computed refuses it. Load-bearing only in pair with
    `test_the_first_replay_in_a_process_is_the_only_one`: never storing would satisfy this
    test alone, and fails that one."""
    baseline = _primed()
    real = revision_corpus.replay
    seen = []

    def mutating(record):
        seen.append(record)
        if len(seen) == 500:
            lineage.B21B_R7_MID_REPLAY = True  # an existing module's namespace, mid-row
        return real(record)

    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(revision_corpus, "replay", mutating)
        revision_corpus.corpus_verdict_row()
        assert len(seen) > 500, "the mutation never ran"
        assert not revision_corpus.memo_would_hit(), "a row computed across a change was stored"
    del lineage.B21B_R7_MID_REPLAY
    assert revision_corpus.corpus_verdict_row() == baseline
    assert revision_corpus.memo_would_hit()


def test_an_unperturbed_repeat_is_answered_from_the_memo():
    """(e) The speed is real: with nothing patched, a repeat replays nothing."""
    row = _primed()
    hits, computations = revision_corpus._MEMO.hits, revision_corpus._MEMO.computations
    for _ in range(3):
        assert revision_corpus.corpus_verdict_row() == row
    assert (revision_corpus._MEMO.hits, revision_corpus._MEMO.computations) == (hits + 3, computations)
    # Every entry holds every object its key was taken over, so no identity in any stored key
    # can be reused by another object while the entry exists; and there are never more entries
    # than the memo keeps.
    entries = revision_corpus._MEMO.entries
    assert 0 < len(entries) <= revision_corpus.MEMO_ENTRIES
    assert all(len(held) == len(key) - 1 and tuple(map(id, held)) == key[1:] for key, held, _row in entries)


# ---------------------------------------------------------------------------
# the short-circuit: the served revision's answer, replaying only when it must
# ---------------------------------------------------------------------------


_CORPUS_ROW = "behaviour_corpus"


def _short_circuit_disagreements(helper, perturbations, baseline_payload):
    """Run ``helper`` under each perturbation and check its answer against the FULL comparison,
    ``sha256_fingerprint(payload) != sha256_fingerprint(baseline_payload)``.

    Which entries rest on the replay is decided HERE, from the rows' own canonical bytes, never
    from what the helper returned: a helper that skipped the corpus row would otherwise vouch
    for itself. The rows are the helper's (they are `_compiler_revision_payload`'s own, which
    the served revision uses verbatim); the comparison logic is what is under test.

    * Every other row byte-equal to the baseline: the answer rests on the corpus row, so the
      full payload gets that row REPLAYED — the helper's own replay when the memo's counter
      proves it ran under this perturbation, otherwise a fresh one bypassing the memo.
    * Some other row differs: the helper answers without the corpus row (it says so, with
      `NOT_COMPARED` in its place), and the full payload gets the BASELINE'S OWN corpus row. The
      fingerprint is SHA-256 over canonical JSON with sorted keys, so equal bytes mean every
      row serializes identically; with another row already differing, no corpus value can make
      the payload's bytes equal the baseline's, and the baseline's own row is the one value that
      could. If the payload differs even with it, it differs with every value — so this entry is
      checked exactly, with no replay.
    """
    baseline = authoring_contract.sha256_fingerprint(baseline_payload)
    other_rows = set(baseline_payload) - {_CORPUS_ROW}
    disagreements, unavailable, rested_on_the_replay, checked = [], [], [], 0
    for name, apply in perturbations:
        with pytest.MonkeyPatch.context() as patched:
            apply(patched)
            computed_before = revision_corpus._MEMO.computations
            moved, rows = helper(baseline_payload)
            full = {key: value for key, value in rows.items() if key != _CORPUS_ROW}
            assert set(full) == other_rows, (name, sorted(set(full) ^ other_rows))
            if all(canonical_json_bytes(full[key]) == canonical_json_bytes(baseline_payload[key])
                   for key in other_rows):
                rested_on_the_replay.append(name)
                if (rows.get(_CORPUS_ROW, authoring_contract.NOT_COMPARED) != authoring_contract.NOT_COMPARED
                        and revision_corpus._MEMO.computations > computed_before):
                    full[_CORPUS_ROW] = rows[_CORPUS_ROW]
                else:
                    full[_CORPUS_ROW] = revision_corpus.fresh_corpus_verdict_row()
            else:
                full[_CORPUS_ROW] = baseline_payload[_CORPUS_ROW]
        checked += 1
        unavailable += [(name, row) for row, value in full.items() if value == "unavailable"]
        if (authoring_contract.sha256_fingerprint(full) != baseline) != moved:
            disagreements.append(name)
    return {"checked": checked, "disagree": disagreements, "unavailable": unavailable,
            "rested_on_the_replay": rested_on_the_replay}


def test_the_short_circuit_answers_exactly_as_the_full_comparison():
    """`_compiler_revision_moved` against the FULL comparison, over EVERY perturbation the
    revision tests apply — derived from their own maps, unsampled (every entry
    `_every_perturbation()` returns).

    Each entry is checked exactly, by the argument `_short_circuit_disagreements` states: when
    another row already differs, the full payload is built with the baseline's own corpus row —
    the only corpus value that could make the payload's canonical bytes equal the baseline's —
    and if it still differs it differs with any value; when every other row is byte-equal, the
    corpus row is replayed and compared. No full payload may read "unavailable"."""
    baseline_payload = authoring_contract._compiler_revision_payload()
    perturbations = _every_perturbation()
    outcome = _short_circuit_disagreements(
        authoring_contract._compiler_revision_moved, perturbations, baseline_payload)
    assert (outcome["checked"], outcome["disagree"], outcome["unavailable"]) == (len(perturbations), [], [])
    # Non-vacuity: both arms were reached — some perturbation rests on the replay and some
    # does not. WHICH ones rest is a measurement, not a list: a perturbation rests only while
    # no hand-picked row happens to sample it, and batch 21a's new child-entry and call-state
    # rows took two of the code-level carries out of that set (measured at the merge: three of
    # the five still rest). What must stay true is that a carry the hand-picked rows do NOT
    # sample still exists — that is the case only the corpus can answer.
    rested = set(outcome["rested_on_the_replay"])
    carries = {"the carry " + carry for carry in _TEST_KILLED_CARRIES}
    assert rested & carries, sorted(rested)
    assert "equivalent decisions (inline)" in rested
    assert len(rested) < len(perturbations)


def test_the_short_circuit_names_the_row_it_did_not_compute():
    """CDX-184-r5-DOC-21B-05: the returned payload used to be 27 rows when the answer did not
    rest on the corpus, and the converted tests scan it for a row that degraded to
    "unavailable" — a scan that cannot see a row nobody computed. The row is now present and
    reads `NOT_COMPARED`, which is distinguishable from both a real row and a degraded one."""
    baseline_payload = authoring_contract._compiler_revision_payload()
    with pytest.MonkeyPatch.context() as patched:
        _rows_as_tuples(patched)  # byte-identical rows: the answer rests on the replay
        moved, rested = authoring_contract._compiler_revision_moved(baseline_payload)
    assert not moved and rested[_CORPUS_ROW] == baseline_payload[_CORPUS_ROW]
    with pytest.MonkeyPatch.context() as patched:
        # Any row but the corpus moving is enough: the helper then never asks for the replay.
        patched.setattr(authoring_contract, "_document_emission_rows", lambda: [])
        moved, short = authoring_contract._compiler_revision_moved(baseline_payload)
    assert moved and short[_CORPUS_ROW] == authoring_contract.NOT_COMPARED
    assert set(short) == set(baseline_payload) == set(rested)
    assert authoring_contract.NOT_COMPARED != "unavailable"
    assert sorted(row for row, value in short.items() if value == "unavailable") == []


def _broken_by_python_inequality(baseline_payload):
    """A helper comparing the other rows with Python `!=` instead of their canonical bytes."""
    payload = authoring_contract._compiler_revision_payload(_without_corpus_row=True)
    if set(payload) | {_CORPUS_ROW} != set(baseline_payload) or any(
            payload[key] != baseline_payload[key] for key in payload):
        return True, payload
    payload[_CORPUS_ROW] = authoring_contract._row_or_unavailable(authoring_contract._behaviour_corpus_payload)
    return payload[_CORPUS_ROW] != baseline_payload[_CORPUS_ROW], payload


def _broken_by_skipping_the_corpus_row(baseline_payload):
    """A helper that answers "not moved" whenever the other rows agree, never replaying."""
    payload = authoring_contract._compiler_revision_payload(_without_corpus_row=True)
    return any(canonical_json_bytes(payload[key]) != canonical_json_bytes(baseline_payload[key])
               for key in payload), payload


def _a_carry_only_the_corpus_row_sees(baseline_payload):
    """``(name, apply)`` for a code-level carry that moves NO other row, chosen by measuring.

    The broken twin below has to be handed a perturbation whose answer can only come from the
    corpus row. Which carry that is changes with the hand-picked rows around it: batch 21a's
    child-entry and call-state rows now sample the two retrieve carries, so naming one here
    made this witness vacuous on the merged tree (it reported no disagreement because the
    other rows had already moved). Measured per run instead, and if NONE is left the assertion
    below says so rather than passing quietly.
    """
    for carry, (select, mutate) in sorted(_TEST_KILLED_CARRIES.items()):
        def apply(patched, select=select, mutate=mutate):
            patched.setattr(inspect.unwrap(lineage._walk_lineage), "__code__",
                            _mutated_walk(select, mutate))

        with pytest.MonkeyPatch.context() as patched:
            apply(patched)
            rows = authoring_contract._compiler_revision_payload(_without_corpus_row=True)
        if all(canonical_json_bytes(rows[key]) == canonical_json_bytes(baseline_payload[key])
               for key in rows):
            return "the carry " + carry, apply
    raise AssertionError("every code-level carry is now sampled by a hand-picked row: this "
                         "witness needs a perturbation only the corpus row can answer")


def test_the_short_circuit_witness_refuses_a_broken_helper():
    """Non-vacuity of the witness above, on cases built for it:

    * a row rebuilt with tuples where it held lists — Python-unequal, byte-identical, so the
      served revision does NOT move — refuses a helper comparing rows with `!=`;
    * a carry only the corpus row sees refuses a helper that skips the corpus comparison.

    Both cases are in `_every_perturbation` now (CDX-184-r5-B21B-T2), so the unsampled witness
    above runs the real helper on each of them and neither arm depends on this test; what this
    one adds is the two broken twins, which must be refused."""
    real_rows = authoring_contract._document_emission_rows
    as_tuples = _nested_tuples(real_rows())
    assert as_tuples != real_rows() and canonical_json_bytes(as_tuples) == canonical_json_bytes(real_rows())

    baseline_payload = authoring_contract._compiler_revision_payload()
    name, only_the_corpus_row_moves = _a_carry_only_the_corpus_row_sees(baseline_payload)
    # The tuples case is the one `_every_perturbation` carries, so the unsampled witness above
    # runs the REAL helper on it too; here it is handed to the broken twin.
    tuples = [(_TUPLES_FOR_LISTS, _rows_as_tuples)]
    assert _short_circuit_disagreements(
        _broken_by_python_inequality, tuples, baseline_payload)["disagree"] == [_TUPLES_FOR_LISTS]
    assert _short_circuit_disagreements(
        _broken_by_skipping_the_corpus_row, [(name, only_the_corpus_row_moves)],
        baseline_payload)["disagree"] == [name]


# ---------------------------------------------------------------------------
# non-vacuity: the guard refuses what it must, and only that
# ---------------------------------------------------------------------------

_MODULE = textwrap.dedent('''
    from boomi_mcp.compiler.process_ir.contracts import SymbolTableV1
    from boomi_mcp.compiler.process_ir.semantic_validation.pipeline import validate_process_ir
    from boomi_mcp.models.process_ir import parse_process_ir_v1
    from boomi_mcp.authoring import revision_corpus

    DOC = {doc!r}

    def test_a_graph_no_covered_test_exercises():
        doc = dict(DOC, body=dict(DOC["body"], steps=list(DOC["body"]["steps"])))
        doc["body"]["steps"][1] = dict(doc["body"]["steps"][1], name="BATCH_21B_ABSENT")
        validate_process_ir(parse_process_ir_v1(doc), SymbolTableV1(symbols=()))

    def test_an_input_the_corpus_cannot_record():
        class Symbols(SymbolTableV1):
            pass
        validate_process_ir(parse_process_ir_v1(DOC), Symbols(symbols=()))

    def test_an_input_the_corpus_already_holds():
        record = next(r for r in revision_corpus.load_corpus() if r["entry"] == "validate")
        function, args = revision_corpus._rebuild(record)
        function(*args)
''')


def test_a_covered_test_whose_input_the_corpus_lacks_fails_and_only_that_test(tmp_path):
    """(c) The guard itself, in a fresh run of a module made covered for this purpose: the test
    handing the validator a graph no corpus input holds FAILS and names the producer command,
    so does the one whose input cannot be recorded exactly, and the one repeating a packaged
    input PASSES — so the refusal is the missing input, not the harness."""
    producer = _producer()
    module = tmp_path / "test_guard_witness.py"
    module.write_text(_MODULE.format(doc=_DOC), encoding="utf-8")
    process = producer.run_pytest([str(module)], env={producer.EXTRA_COVERED_ENV: str(module)})
    output = process.stdout + process.stderr
    failed = sorted(line.split("::", 1)[1].split(" ")[0]
                    for line in output.splitlines() if line.startswith("FAILED "))
    assert failed == ["test_a_graph_no_covered_test_exercises", "test_an_input_the_corpus_cannot_record"], output
    assert "absent from the packaged corpus" in output and "cannot record exactly" in output
    assert producer.producer_command() in output
    assert "1 passed" in output and process.returncode == 1

    # Without the module in the covered set the same run is green: only covered tests answer.
    assert producer.run_pytest([str(module)]).returncode == 0


_IMPORT_MODULE = textwrap.dedent('''
    from boomi_mcp.compiler.process_ir.contracts import SymbolTableV1
    from boomi_mcp.compiler.process_ir.semantic_validation.pipeline import validate_process_ir
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    DOC = {doc!r}

    # The point of this module: an entry-point call in the module BODY, run while the module
    # is imported, with an input no covered test makes.
    _doc = dict(DOC, body=dict(DOC["body"], steps=list(DOC["body"]["steps"])))
    _doc["body"]["steps"][1] = dict(_doc["body"]["steps"][1], name="BATCH_21B_AT_IMPORT")
    validate_process_ir(parse_process_ir_v1(_doc), SymbolTableV1(symbols=()))

    def test_the_module_was_imported():
        assert _doc["body"]["steps"][1]["name"] == "BATCH_21B_AT_IMPORT"
''')

_IMPORT_PACKAGED = textwrap.dedent('''
    from boomi_mcp.authoring import revision_corpus

    # The same shape, with an input the corpus HOLDS: the same import-time recording, and
    # nothing to refuse.
    _record = next(r for r in revision_corpus.load_corpus() if r["entry"] == "validate")
    _function, _args = revision_corpus._rebuild(_record)
    _function(*_args)

    def test_the_module_was_imported():
        assert _record["entry"] == "validate"
''')


_BOOTSTRAP_PROBE = textwrap.dedent('''
    import os

    def test_the_bootstrap_flag_is_set_for_the_harvest_pass_only():
        print("BOOTSTRAP=" + repr(os.environ.get("REVISION_CORPUS_BOOTSTRAP")))
''')


def test_the_producer_sets_the_bootstrap_flag_for_its_harvest_and_nowhere_else(tmp_path):
    """The merge with batch 21a turns on this flag.

    21a's revision invariant asserts that each of ITS source mutants moves the served
    revision, which cannot hold while the corpus being regenerated is the stale one — and
    this producer refuses to write until the covered tests are clean. The two deadlock, so the
    harvest pass carries `REVISION_CORPUS_BOOTSTRAP=1` and 21a stands that one assertion
    branch down for it. Everywhere the corpus is real — `--check`, the suite, CI — the flag is
    absent and the invariant is asserted.
    """
    producer = _producer()
    module = tmp_path / "test_bootstrap_probe.py"
    module.write_text(_BOOTSTRAP_PROBE, encoding="utf-8")
    covered = {producer.EXTRA_COVERED_ENV: str(module)}

    # What the PRODUCER's own harvest pass hands its child, not what this test passes.
    process, harvested = producer.harvest([str(module), "-s"], env=covered)
    assert harvested["stats"] is not None, "the harvest pass did not run"
    assert "BOOTSTRAP='1'" in process.stdout, process.stdout[-1500:]

    # …and a check pass, which is what the suite and CI run, does not carry it.
    check_run = producer.run_pytest([str(module), "-s"], env=covered)
    assert "BOOTSTRAP=None" in check_run.stdout, check_run.stdout[-1500:]


def _standalone_sample():
    """The covered modules this witness runs on their own.

    Derived from `covered_targets()`, never from which two modules once failed
    (CDX-184-r7-G3-STANDALONE-WITNESS-01). The full set is 17 standalone child runs, measured
    at ~90 s — 37 s of it one module, the revision-coverage suite — so this takes a
    deterministic slice of the sorted set: every eighth module, plus the two the defect was
    measured on. It tracks the authority, rotates with the covered set rather than with which
    modules once failed, and stays near 20 s. `REVISION_CORPUS_STANDALONE_ALL=1` runs all 17.
    """
    covered = _producer().covered_targets()
    if os.environ.get("REVISION_CORPUS_STANDALONE_ALL"):
        return covered
    measured = ["tests/test_issue_184_child_state_transfer.py", "tests/test_issue_184_child_entries.py"]
    return sorted(set(covered[::8]) | {module for module in measured if module in covered})


@pytest.mark.parametrize("module", _standalone_sample())
def test_a_covered_module_passes_when_it_is_the_only_one_run(module):
    """CDX-184-r6-GUARD-R2-01. The wrappers can only be installed on modules already
    imported, and the EXCLUSIONS — `_compiler_revision_payload`, the corpus replay — were not
    among the modules the session pre-imports. So when a covered test was the first code to
    import `boomi_mcp.authoring.contract`, every entry-point call the served revision makes
    inside it was recorded as that test's own input and refused: these two modules FAILED run
    on their own (36 and 621 absent inputs), and no regeneration could clear it, because the
    producer never records those calls. Run in a child process, one module at a time, which
    is the only shape that reproduces it."""
    producer = _producer()
    process = producer.run_pytest([module])
    assert process.returncode == 0, (process.stdout + process.stderr)[-4000:]
    assert "absent from the packaged corpus" not in process.stdout + process.stderr


def test_an_import_time_refusal_survives_a_run_that_selects_no_test_from_the_module(tmp_path):
    """CDX-184-r6-GUARD-R2-03. An import-time refusal is carried to the module's own tests,
    so a run that selects none of them — `-k`, a marker skip, `--collect-only` — used to exit
    0 with the refusal swallowed. It is delivered at session finish now, whatever the
    selection was."""
    producer = _producer()
    module = tmp_path / "test_import_deselected.py"
    module.write_text(_IMPORT_MODULE.format(doc=_DOC), encoding="utf-8")
    covered = {producer.EXTRA_COVERED_ENV: str(module)}
    for selection in (["-k", "no_such_test"], ["--collect-only"]):
        process = producer.run_pytest([str(module), *selection], env=covered)
        output = process.stdout + process.stderr
        assert process.returncode != 0, (selection, output[-2000:])
        assert "absent from the packaged corpus" in output, (selection, output[-2000:])
        assert producer.producer_command() in output, selection
    # ...and a selection that DOES run the module's test still reports it there, once.
    process = producer.run_pytest([str(module)], env=covered)
    assert process.returncode == 1 and "1 failed" in process.stdout + process.stderr

    # A more severe status is RAISED to, never replaced: a run interrupted by an unrelated
    # collection error still exits 2, with the refusal printed (CDX-184-r7-G3-EXITSTATUS-01).
    broken = tmp_path / "test_unrelated_collection_error.py"
    broken.write_text("raise RuntimeError('unrelated')\n", encoding="utf-8")
    process = producer.run_pytest([str(module), str(broken)], env=covered)
    output = process.stdout + process.stderr
    assert process.returncode == 2, output[-2000:]
    assert "absent from the packaged corpus" in output, output[-2000:]


_SPELLED_MODULE = textwrap.dedent('''
    import sys
    from pathlib import Path

    _ROOT = Path(__file__).resolve().parent
    for _p in ({roots!r}):
        if _p not in sys.path:
            sys.path.insert(0, _p)

    from {spelling}compiler.process_ir.contracts import SymbolTableV1
    from {spelling}compiler.process_ir.semantic_validation.pipeline import validate_process_ir
    from {spelling}models.process_ir import parse_process_ir_v1

    DOC = {doc!r}

    def _absent(name):
        doc = dict(DOC, body=dict(DOC["body"], steps=list(DOC["body"]["steps"])))
        doc["body"]["steps"][1] = dict(doc["body"]["steps"][1], name=name)
        return doc

    validate_process_ir(parse_process_ir_v1(_absent("B21B_R7_AT_IMPORT")), SymbolTableV1(symbols=()))

    def test_an_absent_input_from_a_test_body():
        validate_process_ir(parse_process_ir_v1(_absent("B21B_R7_IN_TEST")), SymbolTableV1(symbols=()))
''')


@pytest.mark.parametrize("spelling", sorted(_producer()._SPELLINGS))
def test_a_covered_module_is_watched_through_every_spelling_of_the_package(spelling, tmp_path):
    """CDX-184-r7-G3-GUARD-SRC-01. The wrappers can only be installed on modules already
    imported, and the pre-import covered the bare spelling only: a covered module reaching the
    compiler through `src.boomi_mcp.` was INVISIBLE — silently green alone, refused only if
    some earlier module happened to have warmed that copy, which is the import-order-dependent
    answer this batch exists to remove. Parametrized over `_SPELLINGS`, the plugin's own
    authority, so a third spelling would arrive with a witness.

    Both halves in one run: the call in the module BODY and the call in a test body."""
    producer = _producer()
    module = tmp_path / "test_spelling_{0}.py".format(spelling.replace(".", "_"))
    module.write_text(_SPELLED_MODULE.format(
        spelling=spelling, doc=_DOC, roots=(str(_ROOT), str(_ROOT / "src"))), encoding="utf-8")
    process = producer.run_pytest([str(module)], env={producer.EXTRA_COVERED_ENV: str(module)})
    output = process.stdout + process.stderr
    assert process.returncode == 1, output[-3000:]
    assert output.count("absent from the packaged corpus") >= 2, output[-3000:]
    assert "B21B_R7" not in output, "the refusal must stay value-free"
    assert producer.producer_command() in output


_REIMPORT_MODULE = textwrap.dedent('''
    import importlib
    import sys
    from pathlib import Path

    for _p in ({roots!r}):
        if _p not in sys.path:
            sys.path.insert(0, _p)

    from boomi_mcp.compiler.process_ir.contracts import SymbolTableV1

    TARGET = "boomi_mcp.compiler.process_ir.pipeline"

    def _novel(tag):
        """A payload the compiler refuses at the parse step: the guard records the INPUT
        before the call, so a wrapped copy refuses it and an unwrapped copy is silent — no
        nested entry point runs either way."""
        return {{"version": "1", "probe": tag, "body": {{"kind": "sequence", "steps": []}}}}

    def _call(module, tag):
        try:
            module.parse_and_compile_process_ir_v1(_novel(tag), SymbolTableV1(symbols=()))
        except Exception:
            pass

    def test_control_through_the_wrapped_copy():
        _call(sys.modules[TARGET], "B21B_R8_CONTROL")

    def test_escape_reimport():
        saved = sys.modules.pop(TARGET)
        try:
            _call(importlib.import_module(TARGET), "B21B_R8_REIMPORT")
        finally:
            sys.modules[TARGET] = saved

    def test_escape_reload():
        _call(importlib.reload(sys.modules[TARGET]), "B21B_R8_RELOAD")

    def test_escape_src_spelling():
        _call(importlib.import_module("src." + TARGET), "B21B_R8_SRC")
''')


def test_the_projection_keeps_the_container_type_a_caller_sees():
    """CDX-184-r8-R4-PROJ-CONTAINERTYPE-01: tuple projected as list and a read-only mapping as
    a plain one, so dropping the `tuple(...)` normalisation in `EffectResolutionV1.__init__` —
    which three covered tests kill — left every revision byte-identical. The encoder has always
    distinguished them; the projection does now too."""
    from types import MappingProxyType

    assert revision_corpus._projected(("a",)) != revision_corpus._projected(["a"])
    assert revision_corpus._projected({"a": 1}) != revision_corpus._projected(MappingProxyType({"a": 1}))
    assert revision_corpus._projected([("a",)]) != revision_corpus._projected([["a"]])
    assert revision_corpus._projected(frozenset("a")) != revision_corpus._projected({"a"})


def test_a_mapping_cannot_forge_the_unassigned_marker():
    """CDX-184-r8-R4-PROJ-TAGSET-04: the projection's own tags were not in `_TAGS`, so past the
    depth limit — where the exact encoder takes over — a real mapping spelling `$unset` was
    stored as itself and read as the marker. Every projection tag is a tag now."""
    assert {"$declared", "$items", "$unset"} <= revision_corpus._TAGS
    forged = revision_corpus.encode_value({"$unset": True})
    assert list(forged) == ["$map"], forged
    assert forged != {"$unset": True}


def test_the_hook_wraps_a_module_that_is_imported_a_second_time(tmp_path):
    """CDX-184-r8-HOOK4-REIMPORT-01. `install()` recorded its work by module NAME, so the
    import hook fired for exactly the import it could not act on: after an eviction or a
    `reload` the fresh module object kept the REAL entry points and every call through it
    escaped the guard. Work is keyed on the wrapper's identity now.

    Three escapes in one covered module — evict-and-reimport, reload, and a first import of
    the `src.` spelling — each handing the validator an input the corpus lacks, plus a control
    through the normally wrapped copy. All four must be refused."""
    producer = _producer()
    module = tmp_path / "test_reimport_escape.py"
    module.write_text(_REIMPORT_MODULE.format(doc=_DOC, roots=(str(_ROOT), str(_ROOT / "src"))),
                      encoding="utf-8")
    process = producer.run_pytest([str(module)], env={producer.EXTRA_COVERED_ENV: str(module)})
    output = process.stdout + process.stderr
    failed = sorted(line.split("::", 1)[1].split(" ")[0]
                    for line in output.splitlines() if line.startswith("FAILED "))
    assert failed == ["test_control_through_the_wrapped_copy", "test_escape_reimport",
                      "test_escape_reload", "test_escape_src_spelling"], output[-3000:]
    assert output.count("absent from the packaged corpus") >= 4


def test_a_second_session_in_one_interpreter_starts_clean(tmp_path):
    """CDX-184-r8-HOOK4-SESSIONEND-03: nothing removed the import hook or reset the session,
    so a second `pytest.main()` in the same interpreter reprinted the first run's refusal and
    failed a module that has nothing to do with it. The session is torn down at
    `pytest_sessionfinish` now: hook removed, wrappers restored, state dropped."""
    producer = _producer()
    covered = tmp_path / "test_second_session_covered.py"
    covered.write_text(_IMPORT_MODULE.format(doc=_DOC), encoding="utf-8")
    clean = tmp_path / "test_second_session_clean.py"
    clean.write_text("def test_nothing_to_do_with_the_corpus():\n    assert True\n", encoding="utf-8")
    driver = tmp_path / "driver.py"
    driver.write_text(
        "import json, sys, pytest\n"
        "argv = ['-p', '_revision_corpus', '-p', 'no:cacheprovider', '-q', '--no-header']\n"
        "first = pytest.main(argv + ['-k', 'nothing_matches', {0!r}])\n"
        "second = pytest.main(argv + [{1!r}])\n"
        "import _revision_corpus as guard\n"
        "print(json.dumps({{'first': int(first), 'second': int(second),\n"
        "                  'session': guard._SESSION is not None,\n"
        "                  'covered': guard._COVERED is not None,\n"
        "                  'finders': [type(f).__name__ for f in sys.meta_path\n"
        "                              if type(f).__name__ == '_InstallOnImport']}}))\n".format(
            str(covered), str(clean)), encoding="utf-8")
    process = subprocess.run([sys.executable, str(driver)], cwd=str(_ROOT), capture_output=True,
                             text=True, env=producer.child_env({producer.EXTRA_COVERED_ENV: str(covered)}))
    measured = json.loads(process.stdout.strip().splitlines()[-1])
    assert measured["first"] != 0, process.stdout[-2000:]
    assert measured["second"] == 0, process.stdout[-3000:]
    assert measured["finders"] == [], measured
    # …and nothing of either run is left behind: the second builds no session, and used to
    # leave the covered-path set cached for a third
    # (CDX-184-r9-R5-SESSION-COVERED-LEAK-04).
    assert measured["session"] is False and measured["covered"] is False, measured


def test_a_change_inside_a_module_the_replay_imports_is_not_memoized():
    """CDX-184-r8-B21B-R4-STORE-01. The store used to ask "did anything ALREADY LOADED change",
    and a replay imports 20 in-package modules, so a perturbation inside one of them
    (`map_builder.MAP_BUILDERS`, measured in a cold process) produced a row no state ever made
    — stored, then served. `_settle` imports them before the key is taken, so the question is
    "did anything change" again; in a cold child process, the perturbed row must not be served.
    """
    producer = _producer()
    script = (
        "import json\n"
        "from boomi_mcp.authoring import revision_corpus as c\n"
        "records = c.load_corpus()\n"
        "real = c.replay\n"
        "seen = []\n"
        "def mutating(record):\n"
        "    seen.append(record)\n"
        "    if len(seen) == 900:\n"
        "        from boomi_mcp.categories.components.builders import map_builder\n"
        "        kind = next(iter(map_builder.MAP_BUILDERS))\n"
        "        map_builder.MAP_BUILDERS[kind] = type('Replaced', (), {})\n"
        "    return real(record)\n"
        "c.replay = mutating\n"
        "stored = c.corpus_verdict_row()\n"
        "c.replay = real\n"
        "print(json.dumps({'hit': c.memo_would_hit(), 'entries': len(c._MEMO.entries),\n"
        "                  'served_is_stored': c.corpus_verdict_row() == stored,\n"
        "                  'fresh_is_stored': c.fresh_corpus_verdict_row() == stored}))\n")
    process = subprocess.run([sys.executable, "-c", script], cwd=str(_ROOT), capture_output=True,
                             text=True, env=producer.child_env(), check=True)
    measured = json.loads(process.stdout.strip().splitlines()[-1])
    assert measured["entries"] == 0, measured
    assert not measured["hit"] and not measured["served_is_stored"], measured


def test_a_field_added_to_an_existing_verdict_class_is_not_answered_from_the_cache():
    """CDX-184-r8-B21B-R4-FIELDCACHE-01: the projection's field cache was per process and never
    invalidated, so widening a class IN PLACE moved the memo key but not the row — two states
    with the same key gave two rows, decided by cache history. The cache is cleared at the
    start of every row now, so a row always reads the authority as it is."""
    from boomi_mcp.compiler.process_ir.semantic_validation.contracts import ValidationReportV1

    baseline = revision_corpus.fresh_corpus_verdict_row()
    assert revision_corpus._DECLARED_FIELDS, "the cache is not being used at all"
    fields = ValidationReportV1.model_fields
    fields["blocked_by"] = fields["version"]
    try:
        # The cache still holds the pre-widening tuple for this class — and a ROW must not
        # answer from it: it clears the cache first, so the widened authority is read.
        assert "blocked_by" not in revision_corpus._DECLARED_FIELDS[ValidationReportV1]
        assert revision_corpus.fresh_corpus_verdict_row() != baseline
        assert "blocked_by" in revision_corpus._DECLARED_FIELDS[ValidationReportV1]
    finally:
        del fields["blocked_by"]
    assert revision_corpus.fresh_corpus_verdict_row() == baseline


def test_an_entry_point_call_made_while_a_covered_module_is_imported_is_checked(tmp_path):
    """CDX-184-r5-CORPUS-02. The instrumentation used to be installed after collection, so a
    call a covered module made in its BODY was invisible to both the harvest and the guard —
    measured on `tests/test_issue_184_passthrough_entry.py`, whose import compiles two graphs
    that were absent from the packaged corpus while everything ran green.

    Both halves, in one fresh run: the module whose import hands the validator an absent
    input FAILS (through its own test, because failing the collection would interrupt the
    whole session), and the module whose import replays a PACKAGED input passes. Then the
    same absent module in harvest mode, to show the producer now records it — under the
    module's own node, not a test's."""
    producer = _producer()
    absent = tmp_path / "test_import_absent.py"
    absent.write_text(_IMPORT_MODULE.format(doc=_DOC), encoding="utf-8")
    packaged = tmp_path / "test_import_packaged.py"
    packaged.write_text(_IMPORT_PACKAGED, encoding="utf-8")
    covered = {producer.EXTRA_COVERED_ENV: ":".join([str(absent), str(packaged)])}
    process = producer.run_pytest([str(absent), str(packaged)], env=covered)
    output = process.stdout + process.stderr
    failed = sorted(line.split("::", 1)[0].split("/")[-1]
                    for line in output.splitlines() if line.startswith("FAILED "))
    assert failed == ["test_import_absent.py"], output
    assert "absent from the packaged corpus" in output and producer.producer_command() in output
    assert "1 failed, 1 passed" in output and process.returncode == 1

    _run, harvested = producer.harvest([str(absent)], env={producer.EXTRA_COVERED_ENV: str(absent)})
    at_import = [row for row in harvested["rows"] if "::" not in row["node"]]
    assert [row["entry"] for row in at_import] == ["validate"], harvested["rows"]
    assert at_import[0]["digest"] not in {revision_corpus.digest(record)
                                          for record in revision_corpus.load_corpus()}
    assert "record" in at_import[0], "the producer must package the input, not just name it"
