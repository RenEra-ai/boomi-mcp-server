"""#177 invariant 1: every emittable ProcessIR code carries complete served text.

DC-175-D is "served prose describing a capability the enforcement no longer grants".
Its diagnostic half is a code that is raised but whose served description is missing,
wrong, or half-present. `L3-04` is the instance that motivated this file: a capability
code was raised by the compiler while `compiler_diagnostic_specs()` omitted it entirely,
so callers received a code they could not look up.

The class never got its invariant, which is why it came back — twice on the same files.
The invariant is here, and it is derived from the runtime authorities on BOTH sides:

* the SUPPLY side is the three served spec tables;
* the DEMAND side is `tests/_process_ir_diagnostic_emissions.py`, which reads the
  emitting modules and reports what they actually raise.

Neither side is a hand-list, so neither can go stale silently. The whole point is that
adding a code without registering its text fails HERE, at the seam, rather than four
slices later in a review.
"""

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _process_ir_diagnostic_emissions import (  # noqa: E402
    PINNED_SINKS,
    collect_emissions,
    pinned_sink_definitions,
    verifier_issue_sites,
)
from boomi_mcp.compiler.process_ir.diagnostics import (  # noqa: E402
    compiler_diagnostic_specs,
)
from boomi_mcp.compiler.process_ir.semantic_validation.findings import (  # noqa: E402
    finding_specs,
    non_emittable_registered_codes,
    registered_codes,
)
from boomi_mcp.compiler.process_ir.semantic_validation.validation_policy import (  # noqa: E402
    lookup_policy,
    registered_adapters,
)
from boomi_mcp.models.process_ir import (  # noqa: E402
    process_ir_v1_parse_diagnostic_specs,
)

#: The ONLY sink calls whose code cannot be read from the source, each with the reason it
#: cannot and the authority that supplies its codes instead. This is a closed table
#: compared with `==`, not a filter: a NEW dynamic emission path fails this file rather
#: than disappearing from its coverage. Pinning SITES (file + line + sink) rather than
#: exempting CODES keeps the code-set derivation total — an exempted code would be a hole
#: in the invariant; an exempted site is a hole in the *reader*, and the site's own
#: authority closes it below.
PINNED_DELEGATION_SITES = {
    (
        "src/boomi_mcp/compiler/process_ir/semantic_validation/flow.py",
        "finding",
    ): "re-serves `item.code` from a finding the semantic report already produced, so it "
    "introduces no code of its own",
    (
        "src/boomi_mcp/compiler/process_ir/semantic_validation/validation_policy.py",
        "finding",
    ): "raises the exemption code chosen by `LegacyValidationPolicyV1.exemption_for`; the "
    "family is derived below from the policy registry, which owns it",
}


def _policy_exemption_codes():
    """Every exemption code any registered policy can raise — from the registry itself.

    This is the authority behind the `validation_policy` pinned site. Deriving it (rather
    than listing four codes here) is what keeps the coverage claim total when a policy is
    added or an exemption retired.
    """
    codes = set()
    for adapter in registered_adapters():
        policy = lookup_policy(adapter)
        assert policy is not None, adapter
        codes.update(policy.exemptions)
    return codes


def _by_layer():
    """Each layer's OWN served table, kept separate — the whole point of the guard.

    Merging the three tables is what hides `L3-04`: the capability code it names is
    registered by the parser as well, so a union-based check stays green while the
    COMPILER — which is what raises it on the compile path — serves nothing. Measured:
    with both compiler entries removed the union still contains the code. So the tables
    are compared per producer.
    """
    return {
        "parser": {spec["code"]: spec for spec in process_ir_v1_parse_diagnostic_specs()},
        "semantic": {spec["code"]: spec for spec in finding_specs()},
        "compiler": {spec["code"]: spec for spec in compiler_diagnostic_specs()},
    }


#: Which served table(s) may satisfy each emitting layer. A semantic-validation module may
#: legitimately raise a COMPILER-owned code — `_restore` re-serves the compiler's own
#: diagnostic verbatim rather than inventing a semantic twin — so its emissions are
#: satisfied by either table. The parser and compiler answer only for their own.
_SATISFYING_TABLES = {
    "parser": ("parser",),
    "compiler": ("compiler",),
    "semantic": ("semantic", "compiler"),
}


def _served():
    merged = {}
    for table in _by_layer().values():
        merged.update(table)
    return merged


def test_every_pinned_diagnostic_sink_still_exists():
    """The anti-vacuity anchor: a renamed sink must FAIL, not empty the scan.

    A source scan that finds nothing looks exactly like a source scan whose sink was
    renamed. This repo has shipped a guard that enumerated nothing and passed everything
    five separate times, so the sinks are resolved by DEFINITION before anything else in
    this file is believed.
    """
    found = pinned_sink_definitions()
    assert found, "no sinks pinned at all"
    missing = sorted(key for key, present in found.items() if not present)
    assert missing == [], missing
    assert len(found) == len(PINNED_SINKS)


def test_the_emission_scan_finds_every_producer():
    """Each layer must contribute codes, or its arm of the invariant is vacuous."""
    by_producer, _unresolved = collect_emissions()
    assert set(by_producer) == {"parser", "compiler", "semantic", "verifier"}, sorted(
        by_producer
    )
    empty = sorted(name for name, codes in by_producer.items() if not codes)
    assert empty == [], empty


def test_the_only_unreadable_emission_sites_are_the_pinned_delegations():
    """A new dynamic emission path fails here rather than silently escaping coverage.

    The reader deliberately does NOT model Python control flow — #175's four-round prose
    scanner is the recorded cost of a checker that tries to cover an open-ended space. The
    price of that decision is paid here: every call it cannot read is named, and the set is
    compared whole.
    """
    _by_producer, unresolved = collect_emissions()
    observed = {(path, sink) for path, _lineno, sink, _dump in unresolved}
    assert observed == set(PINNED_DELEGATION_SITES), {
        "unpinned (a new dynamic emission path)": sorted(
            observed - set(PINNED_DELEGATION_SITES)
        ),
        "pinned but gone (retire the entry)": sorted(
            set(PINNED_DELEGATION_SITES) - observed
        ),
    }


def test_every_emittable_process_ir_code_has_complete_served_text():
    """#177 invariant 1, and the guard the `L3-04` mutant must break.

    Every code the parser, compiler or semantic validator can raise is served with a
    message AND a remediation, both non-blank. The verifier is excluded here because it
    serves its own result dict rather than one of these registries — its own structural
    check is `test_every_graph_verifier_issue_carries_its_own_text`.
    """
    by_producer, _unresolved = collect_emissions()
    layers = _by_layer()

    unserved = []
    blank = []
    checked = 0
    for producer, tables in _SATISFYING_TABLES.items():
        emitted = by_producer[producer]
        assert emitted, "{0} emits nothing — its arm would be vacuous".format(producer)
        for code in sorted(emitted):
            specs = [layers[table][code] for table in tables if code in layers[table]]
            if not specs:
                unserved.append((producer, code))
                continue
            checked += 1
            for spec in specs:
                for field in ("message", "remediation"):
                    if not (spec.get(field) or "").strip():
                        blank.append((producer, code, field))

    assert unserved == [], unserved
    assert blank == [], blank
    # Coverage is the authority's own size, not a floor: every emitted code of every
    # producer was looked up and found.
    assert checked == sum(len(by_producer[p]) for p in _SATISFYING_TABLES), checked


def test_the_served_code_set_is_exactly_what_the_authorities_account_for():
    """The other direction: no served row for a code nothing can raise.

    A registry that may grow rows nothing reaches is how a served table drifts from the
    enforcement behind it — DC-175-D read from the supply side. The served union is
    therefore partitioned into three authority-derived parts and compared whole:

    * codes the source scan reads directly;
    * the exemption family the policy registry owns (its emission site is dynamic);
    * the codes production declares non-emittable, proven below.
    """
    by_producer, _unresolved = collect_emissions()
    served = set(_served())

    statically_emitted = set().union(
        by_producer["parser"], by_producer["compiler"], by_producer["semantic"]
    )
    accounted = (
        statically_emitted
        | _policy_exemption_codes()
        | set(non_emittable_registered_codes())
    )

    assert served == accounted, {
        "served but unaccounted": sorted(served - accounted),
        "accounted but not served": sorted(accounted - served),
    }


def test_the_non_emittable_declaration_is_proven_not_merely_declared():
    """A declaration nobody checks is exactly how a served fact goes stale.

    `non_emittable_registered_codes()` is the one place this slice lets production say
    "registered, but unreachable". That claim is checked from both sides: the codes are
    really registered, and the source scan really finds nothing raising them. If one
    becomes reachable, this fails and the declaration must be retired.
    """
    declared = set(non_emittable_registered_codes())
    assert declared, "nothing declared — this test would be vacuous"
    assert declared <= set(registered_codes()), sorted(declared - set(registered_codes()))

    by_producer, _unresolved = collect_emissions()
    everything = set().union(*by_producer.values())
    reachable = sorted(declared & everything)
    assert reachable == [], reachable


def test_every_graph_verifier_issue_carries_its_own_text():
    """The native verifier's codes are a separate namespace, checked structurally.

    `verify_process_graph` serves `(code, message, remediation)` straight out of `_issue`
    rather than through a registry, so "is it registered" is the wrong question. The
    checkable property is that every call supplies a literal code and message/remediation
    that carry literal text of their own — an f-string interpolating a shape id still has
    a literal skeleton; a bare forwarded variable does not.
    """
    sites = verifier_issue_sites()
    assert sites, "no _issue calls found — the scan would be vacuous"

    defective = [
        (path, lineno, code, has_message, has_remediation)
        for path, lineno, code, has_message, has_remediation in sites
        if not code or not has_message or not has_remediation
    ]
    assert defective == [], defective

    # ...and those codes stay OUT of the ProcessIR registries: they are collapsed to
    # PROCESS_IR_COMPILE_INTERNAL / PROCESS_IR_COMPILE_VERIFIER_FAILED when the verifier
    # runs inside compilation, so registering them would advertise a compile-time code a
    # caller never receives.
    served = set(_served())
    leaked = sorted({code for _p, _l, code, _m, _r in sites} & served)
    assert leaked == [], leaked


def test_the_guard_fails_when_a_registration_is_removed(monkeypatch):
    """In-memory replay of `L3-04`, alongside the hand-run source mutant.

    The real defect: a capability code raised by the compiler with NO compiler
    registration. Removing it from BOTH compiler tables keeps them symmetric, so the
    fail-closed accessor stays green and the parser still registers the code — which means
    a union-only guard passes. Only the producer-aware direction sees it.
    """
    from boomi_mcp.compiler.process_ir import diagnostics

    code = "PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED"
    assert code in {spec["code"] for spec in compiler_diagnostic_specs()}

    stripped_messages = {k: v for k, v in diagnostics._MESSAGES.items() if k != code}
    stripped_remediation = {
        k: v for k, v in diagnostics._REMEDIATION.items() if k != code
    }
    monkeypatch.setattr(diagnostics, "_MESSAGES", stripped_messages)
    monkeypatch.setattr(diagnostics, "_REMEDIATION", stripped_remediation)

    # The accessor is still HAPPY — symmetric and non-blank — which is what makes this a
    # faithful replay rather than a test of the accessor.
    assert code not in {spec["code"] for spec in compiler_diagnostic_specs()}

    with pytest.raises(AssertionError) as caught:
        test_every_emittable_process_ir_code_has_complete_served_text()
    assert code in str(caught.value)
