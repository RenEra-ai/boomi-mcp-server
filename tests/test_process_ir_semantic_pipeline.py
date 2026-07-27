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
from boomi_mcp.models.process_ir import ProcessIRValidationError, parse_process_ir_v1

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


def test_the_package_is_wired_at_exactly_the_two_intended_sites():
    """Slices 8 and 9 end the darkness — deliberately, at TWO named places.

    Kept as a search rather than deleted: the value was never "it is dark", it
    was "we know every place it is reached from". A THIRD call site appearing
    silently is exactly what this catches.

    * ``integration_builder.py`` — slice 8, the plan/apply mutation gate.
    * ``emission.py``            — slice 9, the canonical emission boundary,
      which is where the adapter identity (hence the exemption policy) is known.
    """
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
    reached = sorted(
        Path(line).name for line in result.stdout.split() if line.strip()
    )
    assert reached == ["emission.py", "integration_builder.py"], reached


# ---------------------------------------------------------------------------
# Repo Codex commit-review findings (6 x P2) — regressions
#
# All six concerned the TYPED CAPABILITY path, where declared contract fields
# were accepted and then not consumed. That is the same defect class this issue
# has hit repeatedly: a field that exists, validates, and means nothing.
# ---------------------------------------------------------------------------


def test_a_trusted_contract_read_of_unestablished_state_is_reported():
    """F1. Applying only a contract's writes let a map that DECLARES it reads an
    unwritten key produce a valid report."""
    doc = _doc([{"kind": "map_ref", "map_ref": "$ref:m"}])
    capabilities = ProcessIRValidationCapabilitiesV1(
        map_effects=(
            MapEffectContractV1(
                map_ref="$ref:m", effect=StateEffectV1(reads=(("dpp", "NEVER_SET"),))
            ),
        )
    )
    codes = {f.code for f in _validate(doc, capabilities).errors}
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE in codes


def test_a_trusted_contract_read_that_is_established_is_clean():
    """The discriminator for F1 — the rule must not fire on a satisfied read."""
    doc = _doc(
        [
            {
                "kind": "set_dpp",
                "name": "SET_FIRST",
                "source_values": [{"value_type": "static", "value": "v"}],
            },
            {"kind": "map_ref", "map_ref": "$ref:m"},
        ]
    )
    capabilities = ProcessIRValidationCapabilitiesV1(
        map_effects=(
            MapEffectContractV1(
                map_ref="$ref:m", effect=StateEffectV1(reads=(("dpp", "SET_FIRST"),))
            ),
        )
    )
    assert _validate(doc, capabilities).errors == ()


def test_duplicate_effect_contract_bindings_are_rejected():
    """F4. First-match-wins made the report depend on tuple ORDER."""
    effect = StateEffectV1(writes=(("dpp", "A"),))
    with pytest.raises(Exception):
        ProcessIRValidationCapabilitiesV1(
            map_effects=(
                MapEffectContractV1(map_ref="$ref:m", effect=effect),
                MapEffectContractV1(map_ref="$ref:m", effect=StateEffectV1()),
            )
        )


def test_duplicate_subprocess_and_script_bindings_are_rejected():
    import hashlib

    digest = hashlib.sha256(b"s").hexdigest()
    with pytest.raises(Exception):
        ProcessIRValidationCapabilitiesV1(
            subprocess_summaries=(
                SubprocessSummaryV1(process_ref="$ref:c", effect=StateEffectV1()),
                SubprocessSummaryV1(process_ref="$ref:c", effect=StateEffectV1()),
            )
        )
    with pytest.raises(Exception):
        ProcessIRValidationCapabilitiesV1(
            script_effects=(
                ScriptEffectContractV1(
                    language="groovy2", source_sha256=digest, effect=StateEffectV1()
                ),
                ScriptEffectContractV1(
                    language="groovy2", source_sha256=digest, effect=StateEffectV1()
                ),
            )
        )


def test_distinct_bindings_are_still_accepted():
    """Guard the F4 guard: rejecting duplicates must not reject distinct rows."""
    capabilities = ProcessIRValidationCapabilitiesV1(
        map_effects=(
            MapEffectContractV1(map_ref="$ref:m1", effect=StateEffectV1()),
            MapEffectContractV1(map_ref="$ref:m2", effect=StateEffectV1()),
        )
    )
    assert len(capabilities.map_effects) == 2


def test_a_profile_ref_must_match_the_kind_its_step_declares():
    """F3. A json-declared split bound to profile.xml was accepted here and only
    caught at emission — as a COMPILER-defect code for an authored mistake."""
    doc = _doc(
        [
            {
                "kind": "data_process",
                "steps": [
                    {
                        "operation": "split_documents",
                        "profile_type": "json",
                        "profile_ref": "$ref:profileX",
                        "link_element_key": "k",
                        "link_element_name": "n",
                    }
                ],
            }
        ]
    )
    ir = parse_process_ir_v1(doc)
    symbols = _symbols_for(ir)
    # rebind that one profile symbol to the WRONG kind
    rebound = SymbolTableV1(
        symbols=tuple(
            ComponentSymbolV1(
                ref=s.ref,
                component_id=s.component_id,
                component_type="profile.xml" if s.ref == "$ref:profileX" else s.component_type,
            )
            for s in symbols.symbols
        )
    )
    codes = {f.code for f in validate_process_ir(ir, rebound).errors}
    assert "PROCESS_IR_REFERENCE_COMPONENT_TYPE_MISMATCH" in codes


def test_two_unresolved_nested_profile_refs_are_two_findings():
    """F5. Both reported at the node path with identical evidence, so dedup
    collapsed them into one and identified neither step."""
    doc = _doc(
        [
            {
                "kind": "data_process",
                "steps": [
                    {
                        "operation": "split_documents",
                        "profile_type": "json",
                        "profile_ref": "$ref:missingA",
                        "link_element_key": "k",
                        "link_element_name": "n",
                    },
                    {
                        "operation": "combine_documents",
                        "profile_type": "json",
                        "profile_ref": "$ref:missingB",
                        "combine_into_link_element_key": "p",
                        "link_element_key": "k2",
                        "link_element_name": "n2",
                    },
                ],
            }
        ]
    )
    ir = parse_process_ir_v1(doc)
    # deliberately supply NO symbols for either nested profile
    bare = SymbolTableV1(
        symbols=tuple(
            s for s in _symbols_for(ir).symbols if "missing" not in s.ref
        )
    )
    notfound = [
        f
        for f in validate_process_ir(ir, bare).errors
        if f.code == "PROCESS_IR_REFERENCE_COMPONENT_NOT_FOUND"
    ]
    assert len(notfound) == 2, [f.path for f in notfound]
    assert len({f.path for f in notfound}) == 2


# ---------------------------------------------------------------------------
# QA #198 (round 14): F3's narrowing was applied with the DATA PROCESS
# vocabulary to BOTH nested containers. The two do not share one:
#
#   data_process step      -> a bare KIND        ("json" / "xml")
#   set_property source    -> the FULL type      ("profile.json" / … / "profile.db")
#
# so on `source_values` the lookup could never hit, the narrowing was inert,
# and `profile.db` had no representation at all. Both vocabularies now come
# from the emitter that enforces them.
# ---------------------------------------------------------------------------


def _profile_source_doc(declared, ref="$ref:profileP"):
    return _doc(
        [
            {
                "kind": "set_dpp",
                "name": "OUT",
                "source_values": [
                    {
                        "value_type": "profile",
                        "element_id": "e1",
                        "element_name": "en",
                        "profile_ref": ref,
                        "profile_type": declared,
                    }
                ],
            }
        ]
    )


def _rebound(ir, ref, component_type):
    return SymbolTableV1(
        symbols=tuple(
            ComponentSymbolV1(
                ref=s.ref,
                component_id=s.component_id,
                component_type=component_type if s.ref == ref else s.component_type,
            )
            for s in _symbols_for(ir).symbols
        )
    )


def _codes_for(declared, bound_to):
    doc = _profile_source_doc(declared)
    ir = parse_process_ir_v1(doc)
    symbols = _rebound(ir, "$ref:profileP", bound_to)
    return {f.code for f in validate_process_ir(ir, symbols).errors}


_MISMATCH = "PROCESS_IR_REFERENCE_COMPONENT_TYPE_MISMATCH"


def test_a_set_property_profile_source_is_narrowed_to_its_declared_type():
    """The half F3 missed: declared `profile.json`, bound to `profile.xml`.
    Previously clean here and surfaced only as a compiler-defect code from the
    emitter."""
    assert _MISMATCH in _codes_for("profile.json", "profile.xml")


def test_a_matching_set_property_profile_source_stays_clean():
    """The discriminator — narrowing that rejected the correct binding too
    would satisfy the case above and break every real payload."""
    assert _MISMATCH not in _codes_for("profile.json", "profile.json")


def test_the_set_property_surface_supports_profile_db():
    """`profile.db` is legitimate for a Set-Properties source and is absent
    from the data-process map, so the inherited vocabulary could not express
    it at all — the correct binding would have been narrowed to nothing."""
    assert _MISMATCH not in _codes_for("profile.db", "profile.db")
    assert _MISMATCH in _codes_for("profile.db", "profile.json")


def test_an_uninterpretable_declaration_does_not_invent_a_mismatch():
    """Fail-open is deliberate: a declaration this phase cannot interpret is
    the emitter's to reject. Narrowing on a vocabulary it does not understand
    would manufacture a reference error out of an unread value.

    Reachable specifically HERE: the Set-Properties source types `profile_type`
    as a free non-blank string (`models/process_ir.py:346`), whereas the
    data_process container closes it to `Literal["json", "xml"]` at parse time.
    """
    assert _MISMATCH not in _codes_for("profile.nonsense", "profile.json")


def test_a_blank_declaration_never_reaches_this_phase():
    """The other half of the fail-open story: blank is rejected by the IR model
    itself, so the empty-string branch of the resolver is unreachable through
    the parser rather than merely untested."""
    with pytest.raises(ProcessIRValidationError):
        parse_process_ir_v1(_profile_source_doc(""))


def test_the_data_process_container_keeps_the_bare_kind_vocabulary():
    """The container that WAS correct must stay correct: a bare kind still
    narrows there, and the full type is not its vocabulary."""
    from boomi_mcp.compiler.process_ir.semantic_validation.references import (
        _declared_allowed,
    )

    assert _declared_allowed("steps", "json") == frozenset({"profile.json"})
    assert _declared_allowed("steps", "xml") == frozenset({"profile.xml"})
    assert _declared_allowed("source_values", "profile.db") == frozenset({"profile.db"})
    # each vocabulary is inert on the other container, so neither narrows to a
    # type that surface cannot declare
    assert len(_declared_allowed("steps", "profile.json")) > 1
    assert len(_declared_allowed("source_values", "json")) > 1


def test_the_validator_narrows_with_the_emitters_own_vocabularies():
    """One definition. A private second copy of the asymmetry is what shipped
    it inverted, so the identity — not merely the value — is asserted."""
    from boomi_mcp.compiler.process_ir import emitter_registry
    from boomi_mcp.compiler.process_ir.semantic_validation import references

    assert references.DP_PROFILE_COMPONENT_TYPE is emitter_registry._DP_PROFILE_COMPONENT_TYPE
    assert references.SETPROP_PROFILE_TYPES == frozenset(
        emitter_registry._SETPROP_PROFILE_TYPES
    )


# ---------------------------------------------------------------------------
# Codex review round 2: several contracted scripts in ONE data_process node run
# in sequence, so the walk over their effects must be sequential too. Checking
# every declared read against the state from before the whole node reported a
# script's read of what the PREVIOUS script in the same node had just written.
# ---------------------------------------------------------------------------

_S1 = "script one"
_S2 = "script two"


def _two_script_doc():
    return _doc(
        [
            {
                "kind": "data_process",
                "steps": [
                    {"operation": "custom_scripting", "script": _S1, "language": "groovy2"},
                    {"operation": "custom_scripting", "script": _S2, "language": "groovy2"},
                ],
            }
        ]
    )


def _script_caps(first, second):
    return ProcessIRValidationCapabilitiesV1(
        script_effects=(
            ScriptEffectContractV1(
                language="groovy2",
                source_sha256=hashlib.sha256(_S1.encode()).hexdigest(),
                effect=StateEffectV1(replay_safe=True, **first),
            ),
            ScriptEffectContractV1(
                language="groovy2",
                source_sha256=hashlib.sha256(_S2.encode()).hexdigest(),
                effect=StateEffectV1(replay_safe=True, **second),
            ),
        )
    )


def _script_codes(first, second):
    doc = _two_script_doc()
    ir = parse_process_ir_v1(doc)
    return {
        f.code for f in validate_process_ir(ir, _symbols_for(ir), _script_caps(first, second)).errors
    }


def test_a_script_may_read_what_an_earlier_script_in_the_same_node_wrote():
    """The valid dependency: step order is execution order."""
    codes = _script_codes({"writes": (("dpp", "A"),)}, {"reads": (("dpp", "A"),)})
    assert "PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE" not in codes


def test_a_script_may_not_read_what_a_LATER_script_in_the_same_node_writes():
    """The discriminator, and the reason this cannot be fixed by simply
    applying all writes before all reads: reversing the order must still be a
    defect, or the sequencing is not being modelled at all."""
    codes = _script_codes({"reads": (("dpp", "A"),)}, {"writes": (("dpp", "A"),)})
    assert "PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE" in codes


def test_a_profile_declaration_is_matched_exactly_not_case_folded():
    """Codex review round 2. A Set-Properties `profile_type` is an
    unconstrained non-blank string that lowering passes through verbatim, and
    the emitter matches it EXACTLY. Case-folding here made this phase accept
    `PROFILE.JSON` as supported — which emission then rejects — and, against a
    differently-typed symbol, manufacture a type mismatch for a declaration
    that is not supported at all and is meant to fall open."""
    from boomi_mcp.compiler.process_ir import emitter_registry
    from boomi_mcp.compiler.process_ir.semantic_validation.references import (
        _declared_allowed,
    )

    for variant in ("PROFILE.JSON", "Profile.Json", " profile.json "):
        narrowed = _declared_allowed("source_values", variant)
        assert variant not in emitter_registry._SETPROP_PROFILE_TYPES, variant
        # unsupported by the emitter -> must fall OPEN here, never narrow
        assert len(narrowed) > 1, (variant, sorted(narrowed))

    # and the exact form still narrows, so this is not "never narrow anything"
    assert _declared_allowed("source_values", "profile.json") == frozenset(
        {"profile.json"}
    )


def test_an_exactly_declared_profile_still_reports_a_real_mismatch():
    """The discriminator for the above: falling open on unsupported spellings
    must not disarm the check for the supported ones."""
    assert _MISMATCH in _codes_for("profile.json", "profile.xml")
    assert _MISMATCH not in _codes_for("PROFILE.JSON", "profile.xml")


# ---------------------------------------------------------------------------
# §6 architect review: the `capability` phase existed in the order with no
# collector, and PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID was registered,
# given message and remediation text, and emitted by nobody.
# ---------------------------------------------------------------------------

_CONTRACT_INVALID = "PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID"
_EFFECT = StateEffectV1(writes=(("dpp", "A"),), replay_safe=True)


def _map_doc():
    return _doc([{"kind": "map_ref", "map_ref": "$ref:m"}])


def _caps_codes(capabilities):
    doc = _map_doc()
    ir = parse_process_ir_v1(doc)
    return {f.code for f in validate_process_ir(ir, _symbols_for(ir), capabilities).errors}


def test_a_contract_bound_to_a_map_that_is_not_in_the_document_is_reported():
    """A contract naming a map the document does not contain is a CALLER error.
    Ignoring it let a caller believe the map was vouched for while the node
    stayed opaque, and the only symptom was a lineage warning pointing at the
    NODE rather than at the contract that failed to match it."""
    caps = ProcessIRValidationCapabilitiesV1(
        map_effects=(MapEffectContractV1(map_ref="$ref:ghost", effect=_EFFECT),))
    assert _CONTRACT_INVALID in _caps_codes(caps)


def test_an_unbound_script_or_subprocess_contract_is_reported():
    script = ProcessIRValidationCapabilitiesV1(script_effects=(
        ScriptEffectContractV1(language="groovy2",
                               source_sha256=hashlib.sha256(b"absent").hexdigest(),
                               effect=_EFFECT),))
    subprocess_ = ProcessIRValidationCapabilitiesV1(subprocess_summaries=(
        SubprocessSummaryV1(process_ref="$ref:ghost", effect=_EFFECT),))
    assert _CONTRACT_INVALID in _caps_codes(script)
    assert _CONTRACT_INVALID in _caps_codes(subprocess_)


def test_a_contract_that_does_bind_is_not_reported():
    """The discriminator: a collector that flagged every contract would satisfy
    the cases above and make the whole capability surface unusable."""
    caps = ProcessIRValidationCapabilitiesV1(
        map_effects=(MapEffectContractV1(map_ref="$ref:m", effect=_EFFECT),))
    assert _CONTRACT_INVALID not in _caps_codes(caps)


def test_the_default_capability_set_reports_nothing():
    """Empty is the strict default and must stay silent — otherwise every
    existing caller acquires an error."""
    assert _CONTRACT_INVALID not in _caps_codes(DEFAULT_VALIDATION_CAPABILITIES)


def test_the_capability_finding_carries_no_contract_content():
    """Binding keys are caller-supplied strings. Only the container and index
    reach the report, so no map ref, script text or digest can leak."""
    caps = ProcessIRValidationCapabilitiesV1(
        map_effects=(MapEffectContractV1(map_ref="$ref:secret-map", effect=_EFFECT),))
    doc = _map_doc()
    ir = parse_process_ir_v1(doc)
    report = validate_process_ir(ir, _symbols_for(ir), caps)
    blob = report.model_dump_json()
    assert "secret-map" not in blob
    assert "/capabilities/map_effects/0" in blob
