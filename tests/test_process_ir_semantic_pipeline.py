"""``validate_process_ir`` orchestration (issue #143, M12.8) — slice 6.

Still DARK: callable and tested, invoked from no production path. The darkness
itself is asserted at the bottom, by grep rather than by assertion — "nothing
calls it" is a claim about the whole tree, and only a search can support it.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1, SymbolTableV1
from boomi_mcp.compiler.process_ir.semantic_validation import (
    DEFAULT_VALIDATION_CAPABILITIES,
    MapEffectContractV1,
    ProcessIRValidationCapabilitiesV1,
    ScriptEffectContractV1,
    StateEffectV1,
    SubprocessSummaryV1,
    ValidationReportV1,
    canonical_report_json,
    validate_process_ir,
)
from boomi_mcp.errors import (
    PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN,
    PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
    PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1

_FIXTURES = _ROOT / "tests" / "fixtures" / "process_ir"
GOLDEN_DOCS = json.loads((_FIXTURES / "process_ir_v1.json").read_text())

_SOURCE = {"kind": "source", "connection_ref": "$ref:conn", "operation_ref": "$ref:op"}
_TARGET = {"kind": "target", "connection_ref": "$ref:tconn", "operation_ref": "$ref:top"}


def _doc(steps):
    return {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [_SOURCE] + list(steps) + [_TARGET, {"kind": "stop"}],
        },
    }


def _read_dpp(name):
    return {
        "kind": "set_dpp",
        "name": "OUT",
        "source_values": [{"value_type": "dpp", "property_name": name}],
    }


#: Only the profile refs need a per-ref type, because a Data Process step
#: declares its profile KIND and the symbol must carry the matching type.
_PROFILE_TYPE_BY_REF = {"$ref:profile2": "profile.xml"}


def _symbols_for(ir):
    """A symbol table covering every ref the document mentions.

    Supplying real symbols matters: with an EMPTY table every component
    reference legitimately fails, and the resulting wall of reference errors
    would mask whatever the test was actually about.
    """
    from boomi_mcp.compiler.process_ir.lowering import lower_process_ir_to_cfg

    cfg = lower_process_ir_to_cfg(ir)
    # Type by ROLE, not by name: a process_call may reference a literal
    # component id rather than a $ref token, and a name table would silently
    # mistype it as a profile and manufacture a type-mismatch error.
    by_role = {
        "connection_ref": "connector-settings",
        "operation_ref": "connector-action",
        "map_ref": "transform.map",
        "cache_ref": "documentcache",
        "process_ref": "process",
    }
    refs = {}
    for node in cfg.nodes:
        sem = node.semantic
        for role, component_type in by_role.items():
            value = getattr(sem, role, None)
            if isinstance(value, str) and value:
                refs[value] = component_type
        for container in ("steps", "source_values"):
            for item in getattr(sem, container, ()) or ():
                nested = getattr(item, "profile_ref", None)
                if isinstance(nested, str) and nested:
                    refs[nested] = _PROFILE_TYPE_BY_REF.get(nested, "profile.json")
    return SymbolTableV1(
        symbols=tuple(
            ComponentSymbolV1(
                ref=ref,
                component_id="id{0}".format(index),
                component_type=refs[ref],
            )
            for index, ref in enumerate(sorted(refs))
        )
    )


def _validate(doc, capabilities=DEFAULT_VALIDATION_CAPABILITIES, symbols=None):
    ir = parse_process_ir_v1(doc)
    return validate_process_ir(ir, symbols or _symbols_for(ir), capabilities)


# ---------------------------------------------------------------------------
# contract
# ---------------------------------------------------------------------------


def test_the_entry_point_returns_a_report_not_an_exception_for_bad_input():
    """Expected invalidity is the normal case this function describes."""
    report = _validate(_doc([_read_dpp("MISSING")]))
    assert isinstance(report, ValidationReportV1)
    assert report.is_valid is False


def test_a_clean_document_is_valid():
    report = _validate(_doc([]))
    assert report.is_valid is True
    assert report.errors == ()


def test_warnings_do_not_make_a_report_invalid():
    """Only errors block. A warning that blocked would make every non-waiting
    subprocess call unbuildable."""
    ir = parse_process_ir_v1(GOLDEN_DOCS["wrapper_flow"])
    report = validate_process_ir(ir, _symbols_for(ir))
    assert report.warnings
    assert report.errors == ()
    assert report.is_valid is True


def test_validation_is_pure_and_repeatable():
    doc = _doc([_read_dpp("MISSING")])
    assert canonical_report_json(_validate(doc)) == canonical_report_json(_validate(doc))


def test_the_default_capability_set_is_the_strict_one():
    """No contracts means everything opaque stays opaque — the strict case is
    the DEFAULT, not something a caller has to opt into."""
    assert DEFAULT_VALIDATION_CAPABILITIES.map_effects == ()
    assert DEFAULT_VALIDATION_CAPABILITIES.script_effects == ()
    assert DEFAULT_VALIDATION_CAPABILITIES.subprocess_summaries == ()


# ---------------------------------------------------------------------------
# typed contracts change the outcome
# ---------------------------------------------------------------------------


def test_an_untrusted_map_leaves_a_read_unproven():
    doc = _doc([{"kind": "map_ref", "map_ref": "$ref:m"}, _read_dpp("A")])
    codes = {f.code for f in _validate(doc).errors + _validate(doc).warnings}
    assert PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN in codes
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE in codes


def test_a_typed_map_contract_establishes_the_state_and_clears_both_findings():
    doc = _doc([{"kind": "map_ref", "map_ref": "$ref:m"}, _read_dpp("A")])
    capabilities = ProcessIRValidationCapabilitiesV1(
        map_effects=(
            MapEffectContractV1(
                map_ref="$ref:m", effect=StateEffectV1(writes=(("dpp", "A"),))
            ),
        )
    )
    report = _validate(doc, capabilities)
    codes = {f.code for f in report.errors + report.warnings}
    assert PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN not in codes
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE not in codes
    assert report.is_valid is True


def test_a_script_contract_is_bound_to_the_digest_of_its_exact_source():
    """Editing the script must invalidate the contract automatically. A contract
    bound to a position would keep vouching for code that no longer exists."""
    script = "return 1"
    doc = _doc(
        [
            {
                "kind": "data_process",
                "steps": [
                    {
                        "operation": "custom_scripting",
                        "script": script,
                        "language": "groovy2",
                        "use_cache": True,
                    }
                ],
            }
        ]
    )
    matching = ProcessIRValidationCapabilitiesV1(
        script_effects=(
            ScriptEffectContractV1(
                language="groovy2",
                source_sha256=hashlib.sha256(script.encode()).hexdigest(),
                effect=StateEffectV1(),
            ),
        )
    )
    stale = ProcessIRValidationCapabilitiesV1(
        script_effects=(
            ScriptEffectContractV1(
                language="groovy2",
                source_sha256=hashlib.sha256(b"return 2").hexdigest(),
                effect=StateEffectV1(),
            ),
        )
    )
    matched = {f.code for f in _validate(doc, matching).warnings}
    unmatched = {f.code for f in _validate(doc, stale).warnings}
    assert PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN not in matched
    assert PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN in unmatched


def test_a_subprocess_summary_replaces_the_unknown_ordering_warning():
    capabilities = ProcessIRValidationCapabilitiesV1(
        subprocess_summaries=(
            SubprocessSummaryV1(process_ref="$ref:child", effect=StateEffectV1()),
        )
    )
    ir = parse_process_ir_v1(GOLDEN_DOCS["wrapper_flow"])
    report = validate_process_ir(ir, _symbols_for(ir), capabilities)
    codes = {f.code for f in report.warnings}
    # the golden's OTHER non-waiting call uses a literal process id, so one
    # unknown-ordering warning legitimately remains; the declared one is gone.
    unknown = [
        f
        for f in report.warnings
        if f.code == PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN
    ]
    assert len(unknown) < 2


def test_there_is_no_trust_flag_anywhere_on_the_capability_contract():
    """The issue forbids a free-form 'trust me' escape hatch: presence in the
    typed set IS the verification boundary."""
    fields = set(ProcessIRValidationCapabilitiesV1.model_fields)
    assert not {f for f in fields if "trust" in f or "allow" in f or "skip" in f}
    with pytest.raises(Exception):
        ProcessIRValidationCapabilitiesV1(trusted=True)


# ---------------------------------------------------------------------------
# report shape
# ---------------------------------------------------------------------------


def test_report_buckets_are_ordered_deterministically_across_runs():
    doc = _doc([_read_dpp("A"), _read_dpp("B"), {"kind": "map_ref", "map_ref": "$ref:m"}])
    first = canonical_report_json(_validate(doc))
    second = canonical_report_json(_validate(doc))
    assert first == second


def test_no_finding_in_any_bucket_carries_a_compile_family_code():
    """ADR-001 §7: the report cannot carry a code that blames the compiler."""
    doc = _doc([_read_dpp("A"), {"kind": "map_ref", "map_ref": "$ref:m"}])
    report = _validate(doc)
    for item in report.errors + report.warnings + report.advisories:
        assert not item.code.startswith("PROCESS_IR_COMPILE_"), item.code


def test_the_control_and_wrapper_goldens_validate_without_errors():
    for name in ("control_flow", "wrapper_flow"):
        ir = parse_process_ir_v1(GOLDEN_DOCS[name])
        report = validate_process_ir(ir, _symbols_for(ir))
        assert report.errors == (), (name, [f.code for f in report.errors])


def test_the_linear_golden_has_one_genuine_read_before_write():
    """``linear_flow`` is a CODEC fixture, not a lineage-valid document: it
    exercises every node kind, and its ``set_ddp`` reads a DPP nothing writes.

    That is a real finding, not a false positive — the legacy walker reaches the
    identical verdict for the identical shape
    (``cache_property_lineage`` returns ``PROCESS_LINEAGE_PROPERTY_READ_BEFORE_WRITE``
    for a strict property read with no writer, with the same "declare a
    default_value to accept absence" remediation). Asserting "every golden is
    clean" would have forced the rule to be weakened to fit a fixture that was
    never lineage-checked.
    """
    ir = parse_process_ir_v1(GOLDEN_DOCS["linear_flow"])
    report = validate_process_ir(ir, _symbols_for(ir))
    assert [f.code for f in report.errors] == [
        PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE
    ]


def test_a_sentinel_value_never_reaches_the_serialized_report():
    doc = _doc([_read_dpp("SENTINEL_PROPERTY")])
    payload = canonical_report_json(_validate(doc))
    assert "SENTINEL_PROPERTY" not in payload
    assert "$ref:" not in payload


# ---------------------------------------------------------------------------
# darkness
# ---------------------------------------------------------------------------


def test_the_package_is_still_wired_to_nothing_in_production():
    """Slice 6 must remain dark. Proven by searching the tree, because
    'nothing imports it' is a claim no single assertion can support."""
    result = subprocess.run(
        [
            "grep",
            "-rl",
            "semantic_validation",
            "--include=*.py",
            str(_ROOT / "src" / "boomi_mcp" / "categories"),
            str(_ROOT / "src" / "boomi_mcp" / "patterns"),
            str(_ROOT / "src" / "boomi_mcp" / "compiler" / "process_ir" / "legacy_adapters"),
            str(_ROOT / "src" / "boomi_mcp" / "compiler" / "process_ir" / "pipeline.py"),
        ],
        capture_output=True,
        text=True,
    )
    assert result.stdout.strip() == "", result.stdout
