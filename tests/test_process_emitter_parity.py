"""Byte-parity tests for the ProcessIR process-emitter registry.

TWO independent corpora live here; they share no machinery on purpose.

1. DIRECT-IR corpus (#138 M12.3). Three frozen ProcessIRV1 documents
   (``fixtures/process_ir/process_ir_v1.json``) driven through ``lowering`` +
   ``emit_process``, with ``ir_to_legacy_flow_sequence`` over the UNCHANGED
   legacy builder as the oracle and a committed pre-extraction fixture per
   document as an independent byte anchor, so "registry equals the current
   builder" is never the ONLY oracle. The registry verifier result must also
   match ``verify_process_graph`` on the legacy XML.

   They exercise 17 of the registry's 18 emitter kinds (start, both connector
   roles, message, map, flowcontrol, dataprocess, doccache load/retrieve/remove,
   set-properties, processcall, branch, decision, exception, stop,
   returndocuments) — every kind EXCEPT ``catcherrors``, which #142 added and
   which ``tests/test_process_ir_error_handling.py`` covers. Do NOT restate this
   as "all kinds": the count moves whenever the registry gains a key, and the
   claim then goes stale silently (it already did once, between #138 and #142).

2. ADAPTER-DIALECT corpus (#139E M12.4). Every currently-canonical NON-listener
   ``sync_pipeline`` chain, from
   ``fixtures/process_ir/sync_pipeline_emitter_parity_cases.json``.

   Heeding the warning above, this corpus states no case COUNT anywhere in prose:
   its size is a measurement, and ``test_sync_corpus_covers_every_canonical_chain``
   recomputes it from the builder's and adapter's own stage tables on every run.
   A literal here could only go stale — and #139E shipped a first draft that did
   exactly that, inheriting #139C's stage-kind-level chain list while claiming
   primitive-level completeness. Live QA caught it by measuring the universe.

   The direct-IR oracle above is structurally unreachable for this dialect and
   #140/#146 did not change that: it enters through ``ir_to_legacy_flow_sequence``,
   whose frozen #136 codec requires a non-empty ``flow_sequence``, while the
   map-less sync chains have none. The only way in is the dialect's own
   normalizer, so each case calls ``SyncPipelineBuilder.lower_config`` exactly
   once. #139E deliberately does NOT invent a ProcessIR-to-``PipelineSpec``
   inverse — no such direction exists anywhere in the tree, and building one
   would be the second semantic compiler ADR-001 §6 forbids.

   The oracle is ``ProcessFlowBuilder`` on that same lowered core, which is
   genuinely independent because ``SyncPipelineBuilder.build`` intercepts BEFORE
   it would delegate. The cases carrying an ``anchor`` are complemented by the
   committed raw-byte ``golden_xml`` components — the complement matters, because
   a uniform drift moves both sides of a differential together and the
   differential alone would never see it.

The WSS listener chains are in NEITHER corpus: their fused ``start_listen`` entry
has no registry key at all, so they stay on the legacy renderer and keep their own
goldens. They ARE generated as candidates by the coverage test, which asserts the
gate routes them to the legacy arm — so when #140 lands ``start_listen`` and they
become canonical, that test fails until this corpus grows to cover them.
"""

from __future__ import annotations

import copy
import itertools
import json
import re
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from boomi_mcp.categories.components.builders.process_flow_builder import (
    _SYNC_PIPELINE_STAGE_ALT_PRIMITIVE,
    _SYNC_PIPELINE_STAGE_PRIMITIVE,
    ProcessFlowBuilder,
    SyncPipelineBuilder,
    WrapperSubprocessBuilder,
    _sync_pipeline_is_canonical,
)
from boomi_mcp.categories.components.process_graph_verifier import verify_process_graph
from boomi_mcp.compiler.process_ir import lowering
from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1, SymbolTableV1
from boomi_mcp.compiler.process_ir.emitter_registry import emit_process
from boomi_mcp.compiler.process_ir.legacy_adapters.emission import emit_legacy_result
from boomi_mcp.compiler.process_ir.legacy_adapters.sync_pipeline import (
    _SOURCE_STAGE_KINDS,
    _TARGET_STAGE_KINDS,
    adapt_sync_pipeline,
)
from boomi_mcp.models._process_ir_compat import (
    ConnectorBindingV1,
    ConnectorResolutionContextV1,
    ir_to_legacy_flow_sequence,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1

_FIXTURES = _ROOT / "tests" / "fixtures" / "process_ir"
_PARITY_DIR = _FIXTURES / "emitter_parity"
_GOLDEN_XML = _ROOT / "tests" / "fixtures" / "golden_xml"
GOLDEN_DOCS = json.loads((_FIXTURES / "process_ir_v1.json").read_text())
SYNC_CASES = json.loads(
    (_FIXTURES / "sync_pipeline_emitter_parity_cases.json").read_text()
)["cases"]
_SHARED = json.loads((_FIXTURES / "flow_sequence_compat_cases.json").read_text())["shared"]
_BINDINGS = _SHARED["bindings"]

# A Data Process step declares its profile KIND (json/xml); the registry requires
# the matching Boomi component type on the resolved symbol.
_DP_PROFILE_COMPONENT_TYPE = {"json": "profile.json", "xml": "profile.xml"}


def _sentinel_symbols(cfg) -> SymbolTableV1:
    """A symbol per authored ref, typed only enough for lowering (which resolves
    ref -> component_id and ignores the type). Component id == the ref token."""
    refs = set()
    for node in cfg.nodes:
        s = node.semantic
        for field in ("connection_ref", "operation_ref", "map_ref", "cache_ref", "process_ref"):
            if getattr(s, field, None):
                refs.add(getattr(s, field))
        for step in getattr(s, "steps", ()):
            if getattr(step, "profile_ref", None):
                refs.add(step.profile_ref)
        for src in getattr(s, "source_values", ()):
            if getattr(src, "profile_ref", None):
                refs.add(src.profile_ref)
    symbols = []
    for ref in sorted(refs):
        b = _BINDINGS.get(ref)
        symbols.append(
            ComponentSymbolV1(
                ref=ref,
                component_id=ref,
                component_type="sentinel",
                connector_type=b["connector_type"] if b else None,
                action_type=b["action_type"] if b else None,
            )
        )
    return SymbolTableV1(symbols=tuple(symbols))


def _symbols_from_plan(plan) -> SymbolTableV1:
    """The registry-canonical symbol table, typed from the plan's RESOLVED emitter
    inputs — exactly the component types the registry requirement check validates
    (component id == the resolved id, which equals the authored ref token)."""
    id_type = {}
    for node in plan.nodes:
        e = node.emitter_input
        k = e.emitter_kind
        if k in ("connectoraction_source", "connectoraction_target"):
            id_type[e.connection_id] = "connector-settings"
            id_type[e.operation_id] = "connector-action"
        elif k == "map":
            id_type[e.map_id] = "transform.map"
        elif k in ("doccacheload", "doccacheretrieve", "doccacheremove"):
            id_type[e.document_cache_id] = "documentcache"
        elif k == "processcall":
            id_type[e.process_id] = "process"
        elif k == "dataprocess":
            for st in e.steps:
                pid = getattr(st, "profile_id", "")
                if pid:
                    kind = str(getattr(st, "profile_type", "")).strip().lower()
                    id_type[pid] = _DP_PROFILE_COMPONENT_TYPE.get(kind, "profile.json")
        elif k == "setproperties_step":
            for src in e.source_values:
                if src.value_type == "profile":
                    id_type[src.profile_id] = src.profile_type
    symbols = []
    for cid in sorted(id_type):
        b = _BINDINGS.get(cid)
        symbols.append(
            ComponentSymbolV1(
                ref=cid,
                component_id=cid,
                component_type=id_type[cid],
                connector_type=b["connector_type"] if b else None,
                action_type=b["action_type"] if b else None,
            )
        )
    return SymbolTableV1(symbols=tuple(symbols))


def _context():
    return ConnectorResolutionContextV1(
        operation_bindings={ref: ConnectorBindingV1(**b) for ref, b in _BINDINGS.items()},
        fallback_target=_SHARED["target"],
    )


def _legacy_shapes_inner(process_xml: str) -> str:
    return re.search(r"<shapes>(.*)</shapes>", process_xml, re.DOTALL).group(1)


def _build_legacy(config, name="ParityProcess"):
    builder = (
        WrapperSubprocessBuilder
        if config.get("process_kind") == "wrapper_subprocess"
        else ProcessFlowBuilder
    )
    return builder.build(config, name=name, folder_name="ParityFolder")


def _emit(doc_name):
    ir = parse_process_ir_v1(GOLDEN_DOCS[doc_name])
    cfg = lowering.lower_process_ir_to_cfg(ir)
    plan = lowering.lower_cfg_to_emission_plan(cfg, _sentinel_symbols(cfg))
    symbols = _symbols_from_plan(plan)
    return ir, emit_process(plan, symbols)


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_registry_shapes_match_legacy_builder_bytes(doc_name):
    """Registry shape XML == the legacy builder's <shapes>, byte-for-byte."""
    ir, artifact = _emit(doc_name)
    legacy_config = ir_to_legacy_flow_sequence(ir, _context())
    legacy_inner = _legacy_shapes_inner(_build_legacy(legacy_config))
    assert "".join(artifact.shape_xml_parts) == legacy_inner


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_registry_process_xml_matches_committed_fixture(doc_name):
    """Independent byte anchor: not just "registry == current builder"."""
    _ir, artifact = _emit(doc_name)
    fixture = (_PARITY_DIR / f"{doc_name}.process.xml").read_text()
    assert artifact.process_xml == fixture


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_registry_emission_is_deterministic(doc_name):
    _ir, a = _emit(doc_name)
    _ir2, b = _emit(doc_name)
    assert a.process_xml == b.process_xml
    assert a.shape_xml_parts == b.shape_xml_parts


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_registry_verifier_matches_legacy_verifier(doc_name):
    """The registry's verifier summary equals verify_process_graph on legacy XML."""
    ir, artifact = _emit(doc_name)
    legacy_config = ir_to_legacy_flow_sequence(ir, _context())
    legacy = verify_process_graph(_build_legacy(legacy_config))
    assert artifact.verifier.errors == ()
    assert [dict(code=e["code"], shape=e["shape"]) for e in legacy["errors"]] == []
    assert artifact.verifier.shapes_checked == legacy["shapes_checked"]
    assert len(artifact.verifier.warnings) == len(legacy["warnings"])


# ---------------------------------------------------------------------------
# #139E — the sync_pipeline adapter-dialect corpus
# ---------------------------------------------------------------------------
# Everything below is the SECOND corpus described in the module docstring. It
# shares no helper with the direct-IR tests above except ``_legacy_shapes_inner``
# (a `<shapes>` extractor, dialect-agnostic by construction).

#: The stage kind each source/target primitive is declared on. Only what the
#: primitive vocabulary itself does not say; the vocabulary is read from the
#: builder's own constants below, never restated here.
_SYNC_STAGE_IDS = {
    "db_read": {"connection_id": "$ref:db_conn", "operation_id": "$ref:db_op"},
    "rest_fetch": {
        "connection_id": "$ref:rest_src_conn",
        "operation_id": "$ref:rest_src_op",
    },
    "soap_fetch": {
        "connection_id": "$ref:soap_src_conn",
        "operation_id": "$ref:soap_src_op",
    },
    "rest_send": {
        "action_type": "POST",
        "connection_id": "$ref:rest_conn",
        "operation_id": "$ref:rest_op",
    },
    "soap_send": {"connection_id": "$ref:soap_conn", "operation_id": "$ref:soap_op"},
    "db_write": {"connection_id": "$ref:db_w_conn", "operation_id": "$ref:db_w_op"},
    # Present so listener chains can be BUILT as candidates and land in the
    # legacy-arm bucket by the gate's own verdict. A lowered listener binding
    # carries no connection_id at all, which is half of why the canonical chain
    # cannot represent it.
    "wss_listen": {"operation_id": "$ref:wss_op"},
}

_ANCHORED_SYNC_CASES = sorted(n for n, c in SYNC_CASES.items() if c["anchor"])


def _sync_fingerprint(config):
    """``<source_primitive>[_map]_<target_primitive>`` for a raw sync config.

    Derived from the config, never from the case NAME -- so a case whose name
    disagrees with its own stages cannot smuggle a duplicate past the coverage
    test below.
    """
    return "_".join(s["config"]["primitive"] for s in config["pipeline"]["stages"])


def _sync_candidate_universe():
    """Recompute the candidate universe from the BUILDER's and ADAPTER's own tables.

    Every (source primitive, target primitive, +/-map) triple the stage tables
    admit. Nothing here is hand-listed: the KINDS come from the primitive tables'
    own keys and are assigned to a slot by the adapter's ``_SOURCE_STAGE_KINDS`` /
    ``_TARGET_STAGE_KINDS``, and the PRIMITIVES come from the tables' values.

    Listener chains are deliberately INCLUDED as candidates rather than skipped by
    name. This function must not decide canonicality itself -- that is the gate's
    job, and the caller partitions on it. A ``kind == "listener"`` shortcut here
    would buy no coverage today (measured: the canonical set is 16 either way) and
    would silently cost the fail-closed property on the day #140 promotes
    ``start_listen``, which is precisely when this test needs to fire.

    Returns ``{fingerprint: raw_config}``.
    """
    tables = (dict(_SYNC_PIPELINE_STAGE_PRIMITIVE), dict(_SYNC_PIPELINE_STAGE_ALT_PRIMITIVE))
    kinds = {k for table in tables for k in table}

    # The transform slot is exempted from the classification below and generated
    # with a fixed ``map`` primitive, which rests on an INVARIANT rather than on
    # the gate: ``lower_config`` collapses a map stage to
    # ``{"mode": "map_ref", "map_ref": ...}``, so the map primitive is a pure
    # discriminator that never reaches emission -- an alt map primitive would emit
    # byte-identical XML and generating cases for it would test nothing.
    #
    # That invariant is exactly the kind of unasserted assumption that produced the
    # two coverage holes before it, so it is asserted rather than trusted: an alt
    # map primitive would be added BECAUSE someone wants different emission, which
    # is precisely when the exemption stops being safe.
    assert "map" not in _SYNC_PIPELINE_STAGE_ALT_PRIMITIVE, (
        "the sync_pipeline transform slot gained an alt primitive. Decide whether it "
        "reaches emission: if it does, it must enter the candidate universe and this "
        "corpus; if it is still a pure discriminator, relax this assert and say why."
    )

    # ``map`` is the transform slot, not an endpoint; every OTHER kind must be
    # classifiable, or a new stage kind would be silently unreachable -- the exact
    # shape of the bug this derivation exists to prevent, one level up from the
    # primitive it already catches.
    unclassified = sorted(
        k for k in kinds if k != "map" and k not in _SOURCE_STAGE_KINDS | _TARGET_STAGE_KINDS
    )
    assert not unclassified, (
        f"sync_pipeline stage kind(s) {unclassified} are admitted by the builder's "
        f"primitive tables but fill neither slot in the adapter's "
        f"_SOURCE_STAGE_KINDS/_TARGET_STAGE_KINDS. Classify them there, add ids to "
        f"_SYNC_STAGE_IDS, and add a case per +/-map variant to "
        f"sync_pipeline_emitter_parity_cases.json."
    )

    sources, targets = [], []
    for table in tables:
        for kind, p in table.items():
            if kind in _SOURCE_STAGE_KINDS:
                sources.append((kind, p))
            elif kind in _TARGET_STAGE_KINDS:
                targets.append((kind, p))
    # A primitive the tables admit but this file has no ids for would otherwise
    # blow up with a bare KeyError deep in the loop. Say what to do instead: the
    # whole point of deriving the universe is that a NEW primitive must land here
    # as work-to-do, not as a puzzle.
    missing = sorted(p for _kind, p in sources + targets if p not in _SYNC_STAGE_IDS)
    assert not missing, (
        f"sync_pipeline primitive(s) {missing} are admitted by the builder's stage "
        f"tables but have no id vocabulary in _SYNC_STAGE_IDS. Add ids here AND a "
        f"case per +/-map variant to sync_pipeline_emitter_parity_cases.json."
    )

    out = {}
    for (skind, sp), (tkind, tp), with_map in itertools.product(
        sources, targets, (False, True)
    ):
        stages = [{"key": "s", "kind": skind, "config": {"primitive": sp, **_SYNC_STAGE_IDS[sp]}}]
        if with_map:
            stages.append(
                {"key": "m", "kind": "map", "config": {"primitive": "map", "map_ref": "$ref:field_map"}}
            )
        stages.append({"key": "t", "kind": tkind, "config": {"primitive": tp, **_SYNC_STAGE_IDS[tp]}})
        keys = [s["key"] for s in stages]
        out[f"{sp}_{'map_' if with_map else ''}{tp}"] = {
            "process_kind": "sync_pipeline",
            "pipeline": {
                "stages": stages,
                "dependencies": [
                    {"from_stage": a, "to_stage": b} for a, b in zip(keys, keys[1:])
                ],
            },
        }
    return out


def _sync_case(case_name):
    """Resolve one case. ``lower_config`` is the SOLE normalizer for this dialect
    and is called exactly once per case -- there is no inverse direction, and
    #139E does not invent one."""
    case = SYNC_CASES[case_name]
    raw = copy.deepcopy(case["config"])
    lowered = SyncPipelineBuilder.lower_config(copy.deepcopy(case["config"]))
    return case, raw, lowered


def _emit_sync(lowered):
    return emit_legacy_result(
        adapt_sync_pipeline(copy.deepcopy(lowered)), dialect="sync_pipeline"
    )


def test_sync_corpus_covers_every_canonical_chain():
    """Fail-closed on the SUPERSET direction, by derivation rather than by list.

    #139C's inline chain set was hand-written at the STAGE-KIND level (6 chains),
    which silently omitted the map-ful SOAP chain and every mixed-connector-family
    combination -- exactly the surface the #139A cross-role connector guard and
    #139C's family-conditional canonicalizer defect live on. Freezing that list
    verbatim, as this corpus first did, would have inherited the gap and added
    prose claiming it was complete.

    So the universe is recomputed here from the builder's own primitive tables and
    each candidate is put through the REAL gate; the corpus must cover exactly the
    ones that come out canonical. Add a primitive to
    ``_SYNC_PIPELINE_STAGE_ALT_PRIMITIVE`` and this fails until the corpus grows --
    which a hand-maintained literal could never do.
    """
    canonical, legacy_arm, rejected = {}, {}, {}
    for fingerprint, raw in _sync_candidate_universe().items():
        try:
            lowered = SyncPipelineBuilder.lower_config(copy.deepcopy(raw))
        except BuilderValidationError as exc:  # rejected upstream, by design
            rejected[fingerprint] = exc.error_code
            continue
        bucket = canonical if _sync_pipeline_is_canonical(lowered) else legacy_arm
        bucket[fingerprint] = raw

    # Every candidate lands in exactly one of three buckets, and all three are
    # pinned. A silent fourth outcome -- or a chain drifting between buckets --
    # is what an unpinned partition would hide.
    assert rejected == {
        "db_read_db_write": "SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED",
        "db_read_map_db_write": "SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED",
    }
    # The legacy arm is exactly the WSS listener chains (#140 owns the fused
    # start_listen entry). When that lands, these move into `canonical` and the
    # corpus assertion below fails until the corpus grows -- which is the whole
    # reason listener candidates are generated rather than skipped by name.
    assert set(legacy_arm) == {
        "wss_listen_rest_send",
        "wss_listen_map_rest_send",
        "wss_listen_soap_send",
        "wss_listen_map_soap_send",
        "wss_listen_db_write",
        "wss_listen_map_db_write",
    }
    # KEEP this literal. It looks like the redundant total that was removed from
    # test_sync_corpus_has_one_case_per_fingerprint..., but it is a different shape:
    # that one was IMPLIED by its neighbours, whereas this is an independent floor
    # against UNIFORM drift. If the corpus and the derivation grew together,
    # `corpus == set(canonical)` below would still hold and only this literal would
    # fire -- the same reason the golden anchors sit beside the differential oracle.
    assert len(canonical) == 16

    corpus = {_sync_fingerprint(c["config"]) for c in SYNC_CASES.values()}
    assert corpus == set(canonical)


def test_sync_corpus_has_one_case_per_fingerprint_plus_the_verb_duplicate():
    """The deletion direction, and the only intentional duplicate.

    ``soap_lowercase_execute`` deliberately repeats the ``soap_fetch_soap_send``
    fingerprint with the non-uppercase verb the pre-#139C canonicalizer corrupted.
    Any OTHER duplicate is a case that silently tests nothing new.

    No total is asserted here on purpose: the coverage test pins the fingerprint
    set and this one pins the duplicates, which together determine the count
    exactly. A third assertion spelling it out would be a hard-coded number that
    constrains nothing and goes stale the day the corpus legitimately grows.
    """
    seen = {}
    for name, case in SYNC_CASES.items():
        seen.setdefault(_sync_fingerprint(case["config"]), []).append(name)
    dupes = {f: sorted(n) for f, n in seen.items() if len(n) > 1}
    assert dupes == {
        "soap_fetch_soap_send": ["soap_fetch_soap_send", "soap_lowercase_execute"]
    }


def test_sync_corpus_anchors_every_committed_sync_golden():
    """Fail-closed floor on the ADDITION direction: a new ``sync_pipeline_*``
    golden cannot arrive without a parity case claiming it, and a case cannot
    claim an anchor that does not exist."""
    claimed = {c["anchor"] for c in SYNC_CASES.values() if c["anchor"]}
    committed = {p.name for p in _GOLDEN_XML.glob("sync_pipeline_*.xml")}
    assert claimed == committed
    for name in claimed:
        assert (_GOLDEN_XML / name).is_file()


@pytest.mark.parametrize("case_name", sorted(SYNC_CASES))
def test_sync_case_routes_to_the_canonical_chain(case_name):
    """Pins that every case in the corpus really is canonical (the MEMBER
    direction; ``test_sync_corpus_covers_every_canonical_chain`` owns the superset
    direction). A listener case added here would take the legacy arm, and every
    parity assertion below would then be comparing the legacy renderer with
    itself."""
    _case, _raw, lowered = _sync_case(case_name)
    assert _sync_pipeline_is_canonical(lowered) is True


@pytest.mark.parametrize("case_name", sorted(SYNC_CASES))
def test_sync_canonical_shape_bytes_match_the_legacy_renderer(case_name):
    """Registry shape XML == the legacy renderer's <shapes>, byte-for-byte."""
    case, _raw, lowered = _sync_case(case_name)
    artifact = _emit_sync(lowered)
    legacy = ProcessFlowBuilder.build(
        copy.deepcopy(lowered), name=case["name"], folder_name=case["folder_name"]
    )
    assert "".join(artifact.shape_xml_parts) == _legacy_shapes_inner(legacy)


@pytest.mark.parametrize("case_name", sorted(SYNC_CASES))
def test_sync_canonical_component_matches_the_legacy_component(case_name):
    """The full component envelope, not just the shapes: the differential across
    the whole emitted document."""
    case, raw, lowered = _sync_case(case_name)
    emitted = SyncPipelineBuilder.build(
        raw, name=case["name"], folder_name=case["folder_name"]
    )
    assert emitted == ProcessFlowBuilder.build(
        copy.deepcopy(lowered), name=case["name"], folder_name=case["folder_name"]
    )


@pytest.mark.parametrize("case_name", sorted(SYNC_CASES))
def test_sync_verifier_results_are_clean_and_equivalent(case_name):
    case, _raw, lowered = _sync_case(case_name)
    artifact = _emit_sync(lowered)
    legacy = verify_process_graph(
        ProcessFlowBuilder.build(
            copy.deepcopy(lowered), name=case["name"], folder_name=case["folder_name"]
        )
    )
    assert artifact.verifier.errors == ()
    assert [dict(code=e["code"], shape=e["shape"]) for e in legacy["errors"]] == []
    assert artifact.verifier.shapes_checked == legacy["shapes_checked"]
    assert len(artifact.verifier.warnings) == len(legacy["warnings"])


@pytest.mark.parametrize("case_name", sorted(SYNC_CASES))
def test_sync_canonical_emission_is_deterministic(case_name):
    """Emitting twice from the SAME lowered core isolates emission determinism
    from lowering determinism (the latter is pinned separately, by
    ``test_sync_pipeline_builder.py::test_all_stage_and_dependency_permutations_lower_identically``)."""
    _case, _raw, lowered = _sync_case(case_name)
    a = _emit_sync(lowered)
    b = _emit_sync(lowered)
    assert a.process_xml == b.process_xml
    assert a.shape_xml_parts == b.shape_xml_parts


@pytest.mark.parametrize("case_name", _ANCHORED_SYNC_CASES)
def test_sync_anchored_case_matches_the_committed_component_bytes(case_name):
    """The independent byte anchor, asserted on BOTH renderers.

    The canonical half duplicates test_sync_pipeline_builder.py on purpose (it is
    what makes this corpus self-contained). The LEGACY half is the non-redundant
    one: nothing else in the tree pins ``ProcessFlowBuilder``'s own output against
    these bytes, which is exactly the uniform-drift hole the differential oracle
    is blind to.
    """
    case, raw, lowered = _sync_case(case_name)
    golden = (_GOLDEN_XML / case["anchor"]).read_text()
    assert (
        SyncPipelineBuilder.build(
            raw, name=case["name"], folder_name=case["folder_name"]
        )
        == golden
    )
    assert (
        ProcessFlowBuilder.build(
            copy.deepcopy(lowered), name=case["name"], folder_name=case["folder_name"]
        )
        == golden
    )
