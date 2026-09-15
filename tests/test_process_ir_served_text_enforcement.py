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

import re
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

#: Every sink call whose code the reader does not attempt to read, keyed by FULL site
#: identity — `(path, sink, code-expression)` — mapped to how many times that exact site
#: occurs and WHY it cannot be read.
#:
#: Two earlier shapes of this table were fail-open and both were caught in review. Keying
#: on `(path, sink)` alone let a SECOND unresolved call in the same file collapse onto the
#: existing entry; and pinning a hand-listed tuple of "the codes this site emits" let that
#: list go stale when the code behind it changed. The count closes the first. The second is
#: closed by `test_every_code_named_in_the_emitting_modules_is_served` below, which needs no
#: per-site list at all: a code the modules can raise has to be NAMED in them, so requiring
#: every named code to be served catches a changed default without anyone tracing a value.
#:
#: The table is longer than it was because the reader no longer skips a sink's own
#: definition body. Skipping it meant deciding whether its first parameter had been
#: rebound, and deciding that means enumerating Python's binding forms — the open-ended
#: space that produced a finding in four consecutive rounds. A longer table is a cost paid
#: in review; a reader that guesses is a cost paid in silent coverage loss.
PINNED_DELEGATION_SITES = {
    (
        "src/boomi_mcp/compiler/process_ir/body_capabilities.py",
        "raise_compile_error",
        "Name(id='code', ctx=Load())",
    ): (
        1,
        "#156 `_as_compile_error`, re-raising a SHARED model rule's refusal as a "
        "compile diagnostic. The chain grammar is deliberately one rule rendered by "
        "both entry points, so the compiler side has a `PydanticCustomError` in hand "
        "and reads its canonical code out of the model's own `_CUSTOM_ERROR_CODES`. "
        "The code is therefore a variable by construction: hand-listing the three "
        "codes here would be a second copy of that table, which is the exact "
        "duplicate-authority defect the shared rule exists to avoid. Every code it "
        "can serve is a member of that table and is served by the PARSER's message "
        "and remediation registries, which `test_every_emittable_process_ir_code_"
        "has_complete_served_text` covers on the parser arm.",
    ),
    (
        "src/boomi_mcp/compiler/process_ir/diagnostics.py",
        "CompilerDiagnostic",
        "Name(id='code', ctx=Load())",
    ): (1, "the body of the `diagnostic` factory, constructing the model from its own `code` parameter"),
    (
        "src/boomi_mcp/compiler/process_ir/diagnostics.py",
        "diagnostic",
        "Name(id='code', ctx=Load())",
    ): (1, "the body of `raise_compile_error`, forwarding into the `diagnostic` factory"),
    (
        "src/boomi_mcp/compiler/process_ir/invariants.py",
        "_fail",
        "Name(id='code', ctx=Load())",
    ): (1, "`_check_region_containment` forwards its `code` parameter, which sits in sixth position and carries a default; two of its three call sites omit it, and the default is read at RUNTIME by `runtime_forward_defaults()`"),
    (
        "src/boomi_mcp/compiler/process_ir/invariants.py",
        "raise_compile_error",
        "Name(id='code', ctx=Load())",
    ): (1, "the body of the `_fail` wrapper, forwarding into `raise_compile_error`"),
    (
        "src/boomi_mcp/compiler/process_ir/pipeline.py",
        "diagnostic",
        "Attribute(value=Name(id='item', ctx=Load()), attr='code', ctx=Load())",
    ): (
        1,
        "#184 `_enforce_semantic_report._restore`. It re-serves a report finding's "
        "code through the `diagnostic` factory only when the code has no "
        "validation-table text and no delegated original. Since QA-184-s1-r17-02 every "
        "code a report can carry has validation-table text, so the set reaching this "
        "call is derived by `test_every_raised_code_serves_its_own_table_text` as the "
        "semantic codes the semantic tables lack. That set is empty, and any member "
        "would have to be in the compiler tables.",
    ),
    (
        "src/boomi_mcp/compiler/process_ir/pipeline.py",
        "CompilerDiagnostic",
        "Attribute(value=Name(id='item', ctx=Load()), attr='code', ctx=Load())",
    ): (1, "`_compile_error_from_validation` re-serves a code carried by an already-validated finding. Both sites are checkable rather than trusted: the one at `:274` REFUSES any code absent from the parser's own served set (`code not in authored`, `pipeline.py:267`) before constructing, and the one at `:124` forwards `item.code` from a report the parser produced"),
    (
        "src/boomi_mcp/compiler/process_ir/pipeline.py",
        "CompilerDiagnostic",
        "Name(id='code', ctx=Load())",
    ): (1, "`_compile_error_from_validation` re-serves a code carried by an already-validated finding. Both sites are checkable rather than trusted: the one at `:274` REFUSES any code absent from the parser's own served set (`code not in authored`, `pipeline.py:267`) before constructing, and the one at `:124` forwards `item.code` from a report the parser produced"),
    (
        "src/boomi_mcp/compiler/process_ir/semantic_validation/findings.py",
        "ValidationDiagnosticV1",
        "Name(id='code', ctx=Load())",
    ): (1, "the body of the `finding` factory, constructing the model from its own `code` parameter"),
    (
        "src/boomi_mcp/compiler/process_ir/semantic_validation/flow.py",
        "finding",
        "Attribute(value=Name(id='item', ctx=Load()), attr='code', ctx=Load())",
    ): (
        1,
        "`collect_connector_flow_findings` re-serves each diagnostic of the "
        "`ProcessIRCompileError` that `connector_resolution.validate_connector_calls` "
        "raises, code verbatim, through `finding()` and therefore through the SEMANTIC "
        "tables. QA-184-s1-r17-02: this reason used to say the site introduced no code of "
        "its own, and every code it carries fell back to generic text on the typed plan "
        "route. The codes are derived by `translation_sites()`, and each must be in the "
        "semantic tables (`test_every_raised_code_serves_its_own_table_text`).",
    ),
    (
        "src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py",
        "finding",
        "Name(id='code', ctx=Load())",
    ): (1, "the body of `_report`, the local one-hop wrapper the lineage rules call; its `code` parameter has NO default, checked from the AST"),
    (
        "src/boomi_mcp/compiler/process_ir/semantic_validation/validation_policy.py",
        "finding",
        "Name(id='exemption', ctx=Load())",
    ): (1, "raises the exemption code chosen by `LegacyValidationPolicyV1.exemption_for`; the family is derived from the policy registry, which owns it"),
    (
        "src/boomi_mcp/models/process_ir.py",
        "ProcessIRDiagnostic",
        "Name(id='code', ctx=Load())",
    ): (1, "the body of the `_diagnostic` factory, constructing the model from its own `code` parameter"),
    (
        "src/boomi_mcp/models/process_ir.py",
        "_diagnostic",
        "Name(id='code', ctx=Load())",
    ): (1, "the parse-error translator forwards a `code` it resolved from the routing map `_CUSTOM_ERROR_CODES`, which the reader collects as an authority in its own right"),
}

#: Code-shaped names in the scanned modules that are deliberately NOT ProcessIR served
#: diagnostics. Two real boundaries, not exemptions for something the reader cannot read —
#: and the test below checks this set in BOTH directions, so an entry that stops being named
#: or starts being served must be retired rather than left standing:
#:
#: * `legacy_adapters/**` is a separate error namespace with its own served surface;
#: * the last two are ordinary module constants that merely LOOK like codes, which is the
#:   cost of matching on shape — and matching on shape is what caught a diagnostic default
#:   changed to a brand-new literal, which matching on known constants alone did not.
#: Each entry is `code -> the path prefix it is allowed to appear under`. Scoping to the
#: SOURCE BOUNDARY is the point: keyed by code alone, a compiler forward that started using
#: `LEGACY_ADAPTER_SEMANTIC_LOSS` would be skipped here and the compiler would emit an
#: unregistered code with every guard green.
UNSERVED_BY_DESIGN = {
    "LEGACY_ADAPTER_OUTPUT_PARITY_FAILED": "src/boomi_mcp/compiler/process_ir/legacy_adapters/",
    "LEGACY_ADAPTER_SEMANTIC_LOSS": "src/boomi_mcp/compiler/process_ir/legacy_adapters/",
    "LEGACY_ADAPTER_UNSUPPORTED_KIND": "src/boomi_mcp/compiler/process_ir/legacy_adapters/",
    "LEGACY_ADAPTER_ALIAS_PREFIX": "src/boomi_mcp/compiler/process_ir/legacy_adapters/",
    "PROCESS_COMPONENT_TYPE": (
        "src/boomi_mcp/compiler/process_ir/semantic_validation/references.py"
    ),
}


#: Forwarding owners Python cannot introspect, keyed by EXACT
#: `(path, function, parameter)` and mapped to the reason its default is accounted for.
#: A nested function is not reachable through `getattr`, so `inspect.signature` cannot read
#: its default — the one case where neither the source census nor the runtime check can
#: speak, and therefore the one case a human must.
#: The fourth element is `has_default`, read from the AST — and it must be FALSE. A pinned
#: disposition that merely SAID "there is no default to read" was fail-open: the owner could
#: gain a constructed default, the tuple identity would not move, and the source census
#: cannot see an assembled value. The claim is now a checked fact.
UNREADABLE_DEFAULTS = {
    (
        "src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py",
        "_report",
        "code",
        False,
    ): (
        "a closure inside `collect_lineage_findings`; it takes `code` as a required "
        "parameter with NO default (verified in source), so there is no default value to "
        "read and every code it serves arrives from one of its call sites, each of which "
        "passes a literal the census already collects"
    ),
}


def _allowed_unserved(code, path):
    """True only where this exact code is allowed to be named unserved."""
    allowed = UNSERVED_BY_DESIGN.get(code)
    return allowed is not None and path.startswith(allowed)


#: Parse-layer codes the COMPILER table registers on purpose, because public compile
#: re-parses through the parser (#178) and these two are what a caller receives from the
#: compile path.
#:
#: Admitting EVERY parser code was too blanket: a parser-only code such as
#: `PROCESS_IR_REFERENCE_INVALID_FORMAT` inserted into the compiler registry was then
#: "accounted for" while the projection advertised bogus compiler attribution for it. There
#: is no rule separating the two legitimate rows from the rest — it is a per-code design
#: decision — so they are named, and the test checks the set in BOTH directions so a row
#: that stops being registered has to be retired here rather than left standing.
COMPILER_REGISTERED_PARSE_CODES = frozenset(
    {
        "PROCESS_IR_SCHEMA_BRANCH_CARDINALITY",
        "PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED",
        # #156. The third, and it arrives the same way: the serialized-chain
        # grammar is ONE rule shared by the parser and `body_capabilities`, and
        # the compiler renders it through `_as_compile_error`. Its map-separator
        # rule raises the parser's cardinality code, so a caller handing a mutated
        # model straight to `compile_process_ir_v1` receives that code FROM the
        # compile path and needs the compiler's text for it.
        #
        # Registered per-code, not by admitting the parser family: the two
        # reference-format codes are also in `_CUSTOM_ERROR_CODES` and are NOT
        # reachable this way, which is why `_as_compile_error` translates a closed
        # set (`body_capabilities._CHAIN_RULE_CODES`) rather than doing a blanket
        # lookup.
        #
        # RETIRED by #158, as the stale-allowance check below requires: the
        # compiler now EMITS `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` itself —
        # `body_capabilities._check_listener_placement` raises it for a listener
        # that is not the first root step — so the source scan accounts for it
        # directly and an allowance for it would be a second, stale account.
    }
)


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


#: Which served table(s) may satisfy the codes an emitting MODULE names. A
#: semantic-validation module calls both factories: `finding()` for its findings, and
#: `diagnostic()` for the compiler-family refusal `flow` raises on a resolver crash. So
#: at this module granularity either table may answer.
#:
#: That is not the question of which text a call SERVES. `finding()` reads only the
#: semantic tables, so a compiler-owned code raised through it needs a semantic entry as
#: well, and QA-184-s1-r17-02 was exactly that gap: the typed plan route served the
#: generic fallback for every compiler-owned code the validator raised while this mapping
#: kept every guard below green. The per-factory check is
#: `test_every_raised_code_serves_its_own_table_text`. The parser and compiler answer only
#: for their own.
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
    import collections

    _by_producer, unresolved = collect_emissions()
    # FULL identity, plus how many times each site occurs. Line numbers are deliberately
    # excluded from the key — they churn on any edit above — but the expression is not,
    # so a genuinely NEW dynamic call cannot hide behind an existing entry, and a second
    # copy of an existing one shows up as a count change.
    observed = collections.Counter(
        (path, sink, dump) for path, _lineno, sink, dump in unresolved
    )
    expected = collections.Counter(
        {site: count for site, (count, _reason) in PINNED_DELEGATION_SITES.items()}
    )
    assert observed == expected, {
        "unpinned (a new dynamic emission path)": sorted(
            site for site in observed if site not in expected
        ),
        "pinned but gone (retire the entry)": sorted(
            site for site in expected if site not in observed
        ),
        "count changed (a second call at a pinned site)": sorted(
            (site, expected[site], observed[site])
            for site in set(observed) & set(expected)
            if observed[site] != expected[site]
        ),
    }
    # Every pinned site states WHY it cannot be read. A blank reason is an unexplained
    # hole in the reader, which is what this table exists to prevent.
    blank = sorted(
        site for site, (_count, reason) in PINNED_DELEGATION_SITES.items()
        if not reason.strip()
    )
    assert blank == [], blank


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
    layers = _by_layer()

    # PER PRODUCER, not over the merged union. Merging was the same cross-layer masking the
    # forward direction already avoids: a compiler-owned delegated code added to the
    # SEMANTIC registry was accounted for by the union, and the projection then advertised
    # bogus semantic-validator attribution for it. Each table must be justified by the layer
    # that owns it.
    # Both of these are SEMANTIC-registry facts: the policy exemptions are raised through
    # `finding()`, and `non_emittable_registered_codes()` is declared beside `registered_codes()`.
    # Applying them to every producer let a compiler-owned code sit in the semantic registry,
    # and the semantic non-emittable code sit in the compiler registry, both "accounted for".
    semantic_only = _policy_exemption_codes() | set(non_emittable_registered_codes())
    exemptions = set()
    non_emittable = set()

    unaccounted = {}
    for producer, tables in _SATISFYING_TABLES.items():
        # A layer's OWN table is the one it must account for; `_SATISFYING_TABLES` lists the
        # tables its emissions may draw on, and the first entry is always its own.
        own = layers[tables[0]]
        emitted = set(by_producer[producer])
        accounted = emitted | (semantic_only if producer == "semantic" else set())
        if producer == "compiler":
            accounted |= COMPILER_REGISTERED_PARSE_CODES
        if producer == "semantic":
            # Accounted for by what `finding()` can actually be HANDED, per factory rather
            # than per module: the codes its raise sites resolve, the policy exemptions,
            # and the codes its translation site re-serves from a caught compile refusal
            # (`_derive_raised`). QA-184-s1-r17-02 put the compiler's words in this table
            # for every compiler-owned code among those, so a code the validator cannot
            # raise still has no business here, and one it can raise must be here.
            accounted = set(_derive_raised()["semantic"]) | semantic_only
        extra = sorted(set(own) - accounted)
        if extra:
            unaccounted[producer] = extra
    assert unaccounted == {}, unaccounted

    # Both directions on the named allowance: an entry that stops being registered in the
    # compiler table, or that the compiler starts emitting itself, must be retired here.
    stale_allowance = sorted(
        code
        for code in COMPILER_REGISTERED_PARSE_CODES
        if code not in layers["compiler"] or code in by_producer["compiler"]
    )
    assert stale_allowance == [], stale_allowance

    # ...and nothing served anywhere is unaccounted for across all three.
    served = set(_served())
    statically_emitted = set().union(
        by_producer["parser"], by_producer["compiler"], by_producer["semantic"]
    )
    accounted_all = statically_emitted | semantic_only
    assert served == accounted_all, {
        "served but unaccounted": sorted(served - accounted_all),
        "accounted but not served": sorted(accounted_all - served),
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
    # The FULL derived case set, not a floor. Requiring only non-emptiness made this
    # sampleable: aliasing `_issue` and moving all but one call to the alias left the scan
    # reporting a single site and the guard green, which fails both the bidirectional-pin
    # and full-case-set criteria. The count is derived from the module, never typed.
    from _process_ir_diagnostic_emissions import verifier_issue_call_count

    assert len(sites) == verifier_issue_call_count(), (len(sites), verifier_issue_call_count())

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


def test_a_forwarded_code_parameter_is_reported_rather_than_resolved():
    """The convergent property, after four rounds of trying to resolve instead.

    A revision of the reader tried to work out which codes could reach a forwarded
    parameter, by reading its default and the owner's call sites. Every round of review
    found another Python form it read wrongly — unpacked arguments, a rebound parameter, an
    unreachable default, an aliased call, and bindings (`case x`, `import ... as x`, a
    nested `def x`) that carry no `Name(Store)` node at all. Each fix was right and the
    next round found another form: Python's binding and call syntax has no closed case set,
    so a reader over it cannot make the coverage claim the structural-fix rule requires.

    So the reader no longer tries. It reports the SHAPE, and the two outcomes below are the
    whole contract:

    * a forward into a registered sink's OWN first parameter is the sink's definition body
      — skipped, because that sink's call sites are scanned instead;
    * every other forward is UNRESOLVED, and must be pinned with a human-stated authority.

    Asserted on real parsed source, in both directions, so a reader that resolved
    everything and a reader that skipped everything both fail.
    """
    import ast as _ast

    from _process_ir_diagnostic_emissions import _ModuleScan, _called_name

    def sites(source):
        scan = _ModuleScan("<synthetic>", source)
        out = []
        for node in _ast.walk(scan.tree):
            if not isinstance(node, _ast.Call) or _called_name(node) != "finding":
                continue
            if not node.args:
                continue
            forward = scan.forwarded_parameter(node, node.args[0])
            skipped = (
                forward is not None
                and forward[0].name in scan.sinks
                and forward[1] == 0
            )
            out.append((forward is not None, skipped))
        return out

    # A first-parameter wrapper: recognised as a forward AND skipped.
    wrapper = """
def finding(code, severity, phase, path):
    return code

def report(code, node):
    return finding(code, "error", "p", "/body")
"""
    assert sites(wrapper) == [(True, True)], sites(wrapper)

    # Any OTHER parameter position: a forward, but NOT skipped — it must reach the
    # unresolved table. This is the case the deleted resolver used to swallow.
    sixth = """
def helper(edge, prefix, by_id, outbound, node, code="X"):
    return finding(code, "error", "p", "/body")
"""
    assert sites(sixth) == [(True, False)], sites(sixth)

    # A literal is not a forward at all, and must resolve normally rather than being
    # reported — otherwise the pinned table would fill with ordinary emissions.
    literal = """
def emit():
    return finding("PROCESS_IR_SEMANTIC_JOIN_UNSUPPORTED", "error", "p", "/body")
"""
    assert sites(literal) == [(False, False)], sites(literal)


def test_every_code_named_in_the_emitting_modules_is_served():
    """The claim that makes a pinned site safe without reading data flow.

    A pinned site records that a human, not the reader, accounts for the codes reaching it.
    That account can go stale: change the default behind
    `_check_region_containment` to a fresh unregistered code and the site's identity does
    not move, so nothing that keys on the site would notice.

    This closes that without tracing a single value. A code the scanned modules can raise
    must be NAMED in them — as a `boomi_mcp.errors` constant or as a literal — so requiring
    every named code to carry complete served text catches the changed default directly. It
    over-approximates on purpose: a code named for some other reason is still required to be
    served, which can demand a registration that was not strictly needed but can never miss
    one that was.
    """
    from _process_ir_diagnostic_emissions import referenced_codes

    referenced = referenced_codes()
    assert referenced, "no codes named at all — this test would be vacuous"

    from _process_ir_diagnostic_emissions import producer_of, runtime_forward_defaults

    layers = _by_layer()

    def _complete_for(code, producer):
        """Is `code` served by a table that PRODUCER's own emissions may draw on?

        Per producer, never against the merged union. Checking the union here would
        recreate exactly the cross-layer masking the producer-aware equality above exists
        to stop: a compiler module naming a parser-only code would look served while
        `compiler_diagnostic_specs()` omits it and the compiler falls back to generic prose.
        """
        for table in _SATISFYING_TABLES.get(producer, ()):
            spec = layers[table].get(code)
            if (
                spec
                and (spec.get("message") or "").strip()
                and (spec.get("remediation") or "").strip()
            ):
                return True
        return False

    unserved = sorted(
        (code, path)
        for code, paths in referenced.items()
        for path in sorted(paths)
        if not _allowed_unserved(code, path)
        and producer_of(path) in _SATISFYING_TABLES
        and not _complete_for(code, producer_of(path))
    )
    assert unserved == [], unserved

    # The by-design set is checked in BOTH directions and PER PATH. Two ways an entry goes
    # stale, and both must fail: it stops being NAMED where it is allowed, or it starts
    # being SERVED there. The served half was briefly dropped when this check gained path
    # scoping — while the comment above it still claimed both directions — which would have
    # let a later registration hide behind a standing exemption. A reference from anywhere
    # ELSE is not covered by this set at all; those are caught by `unserved` above.
    stale = []
    for code, allowed in UNSERVED_BY_DESIGN.items():
        at_allowed = [path for path in referenced.get(code, ()) if path.startswith(allowed)]
        if not at_allowed:
            stale.append((code, "no longer named under " + allowed))
            continue
        now_served = sorted(
            path for path in at_allowed if _complete_for(code, producer_of(path))
        )
        if now_served:
            stale.append((code, "now served for its own producer at " + str(now_served)))
    assert sorted(stale) == [], sorted(stale)


def test_every_runtime_forward_default_is_served():
    """The half a source reader cannot cover: the default's evaluated VALUE.

    The census above reads source, so it recognises a code written as a whole literal or a
    known constant. A default written `"PROCESS_IR_" + "SEMANTIC_TOTALLY_NEW_UNREGISTERED"`
    is neither — and the architect review demonstrated exactly that: a genuinely emittable
    unregistered code with every source-reading guard green.

    Reading such expressions means modelling concatenation, f-strings, `.format`, `.join`
    and whatever comes next — the open-ended space that produced a finding in four
    consecutive Stage-2 rounds. This asks PYTHON for the value instead. However the author
    wrote it, the value is the value, and a value has no syntax to enumerate.
    """
    from _process_ir_diagnostic_emissions import producer_of, runtime_forward_defaults

    defaults, unreadable = runtime_forward_defaults()
    assert defaults, "no forwarded defaults found — this test would be vacuous"

    layers = _by_layer()

    def _complete_for(code, producer):
        for table in _SATISFYING_TABLES.get(producer, ()):
            spec = layers[table].get(code)
            if (
                spec
                and (spec.get("message") or "").strip()
                and (spec.get("remediation") or "").strip()
            ):
                return True
        return False

    unserved = sorted(
        (path, function, param, default)
        for (path, function, param), default in defaults.items()
        if not _allowed_unserved(default, path)
        and producer_of(path) in _SATISFYING_TABLES
        and not _complete_for(default, producer_of(path))
    )
    assert unserved == [], unserved

    # An owner Python cannot introspect (a nested function is not reachable through
    # `getattr`) is REPORTED, never assumed empty, and is dispositioned by EXACT
    # `(path, function, parameter)`. Accepting it because some delegation in the same FILE
    # was pinned was fail-open: `_report` could gain a concatenated default while the
    # unrelated pin in its file kept this green and the source census, which cannot see an
    # assembled value, stayed silent.
    unaccounted = sorted(row for row in unreadable if row not in UNREADABLE_DEFAULTS)
    assert unaccounted == [], unaccounted
    stale = sorted(set(UNREADABLE_DEFAULTS) - set(unreadable))
    assert stale == [], stale
    # No pinned unreadable owner may carry a default. If one gains a default its
    # `has_default` flips True, the tuple stops matching, and it lands in `unaccounted`
    # above — but this states the invariant directly so the reason cannot be misread.
    with_defaults = sorted(row for row in unreadable if row[3])
    assert with_defaults == [], with_defaults


def test_no_forwarding_call_site_builds_its_code_at_runtime():
    """A pinned forwarding owner must be handed a plain constant.

    Its code arrives either as a DEFAULT — read as an evaluated value by
    `runtime_forward_defaults()` — or EXPLICITLY at a call site. An explicit argument built
    at runtime can be read by neither the source census nor the runtime default check, and
    the architect review demonstrated exactly that: replacing the real
    `PROCESS_IR_COMPILE_ERROR_REGION_INVALID` at a `_check_region_containment` call site with
    a concatenated unregistered value changed the emitted diagnostic while every guard stayed
    green.

    Evaluating such expressions means modelling concatenation, f-strings and `.format` — the
    space that produced a finding in four consecutive rounds. Banning them at these few
    sites is closed instead, and costs nothing: every real site already passes a constant.
    """
    from _process_ir_diagnostic_emissions import unresolvable_forward_arguments

    # A site already carried by `PINNED_DELEGATION_SITES` is not banned twice: that table
    # states the authority its codes come from, and the pinned-site test above compares it
    # whole, so such a site is accounted for rather than unread. Anything ELSE that hands a
    # forwarding owner an unreadable code fails here.
    pinned = {(path, sink, dump) for path, sink, dump in PINNED_DELEGATION_SITES}
    offenders = sorted(
        row for row in unresolvable_forward_arguments()
        if (row[0], row[2], row[3]) not in pinned
    )
    assert offenders == [], offenders


#: The legacy-adapter subtree serves its own error namespace (`LEGACY_ADAPTER_*`) and must
#: not raise a CANONICAL ProcessIR diagnostic. The design plan called for this boundary
#: assertion and it was dropped when the reader stopped excluding the subtree — the reader
#: scanning it is not the same as the boundary being enforced.
_LEGACY_ADAPTER_ROOT = "src/boomi_mcp/compiler/process_ir/legacy_adapters/"

#: `emission.py` imports `CompilerDiagnostic` to TYPE-CHECK diagnostics it re-raises from the
#: canonical compiler, not to construct one. Pinned by exact module so a new importer has to
#: be justified here rather than joining a blanket allowance.
_LEGACY_ADAPTER_TYPE_ONLY_IMPORTERS = {
    "src/boomi_mcp/compiler/process_ir/legacy_adapters/emission.py": ("CompilerDiagnostic",),
}


def test_the_legacy_adapter_boundary_raises_no_canonical_diagnostic():
    """`legacy_adapters/**` may not emit a canonical ProcessIR diagnostic.

    Its codes are a separate served namespace, which is why they sit in
    `UNSERVED_BY_DESIGN`. If a module there started raising a canonical code, that code would
    be served by the ProcessIR registries while its emitter lived outside every producer this
    file accounts for.
    """
    import ast as _ast

    from _process_ir_diagnostic_emissions import (
        _ModuleScan,
        _called_name,
        _iter_files,
        _ROOT,
    )

    offenders = []
    for path in _iter_files():
        relative = str(path.relative_to(_ROOT))
        if not relative.startswith(_LEGACY_ADAPTER_ROOT):
            continue
        scan = _ModuleScan(path, path.read_text())
        for node in _ast.walk(scan.tree):
            if isinstance(node, _ast.Call) and _called_name(node) in scan.sinks:
                offenders.append((relative, node.lineno, _called_name(node)))
            elif isinstance(node, (_ast.Import, _ast.ImportFrom)):
                for alias in node.names:
                    if alias.name not in scan.sinks:
                        continue
                    allowed = _LEGACY_ADAPTER_TYPE_ONLY_IMPORTERS.get(relative, ())
                    # A RENAMED import does not inherit the allowance: `import X as CD`
                    # keeps `alias.name == "X"` while every call reads `CD`, so the module
                    # could construct a canonical diagnostic under a name the scan and this
                    # allowance both miss.
                    if alias.asname is not None:
                        offenders.append(
                            (relative, node.lineno, "aliased import " + alias.name))
                    elif alias.name not in allowed:
                        offenders.append((relative, node.lineno, "import " + alias.name))
    assert offenders == [], offenders

    # Both directions on the type-only allowance: an importer that stops importing must be
    # retired here rather than left standing as a blanket permission.
    stale = []
    for relative, names in _LEGACY_ADAPTER_TYPE_ONLY_IMPORTERS.items():
        source = (_ROOT / relative).read_text()
        for name in names:
            if name not in source:
                stale.append((relative, name))
    assert stale == [], stale


# ---------------------------------------------------------------------------
# SELF-184-37: a served remediation names every gate its code's raisers cite
# ---------------------------------------------------------------------------

#: Capability-citing literals in the scanned modules that are NOT message text, keyed by
#: `(path, literal)` (line numbers churn) and mapped to why. `capability_citations` reports
#: every citing literal it cannot place at a raise site, and this table is compared with that
#: set whole: a new citation the reader cannot read fails, and a stale entry must be retired.
CAPABILITY_CITATIONS_NOT_MESSAGE_TEXT = {
    (
        "src/boomi_mcp/compiler/process_ir/error_handling.py",
        "{0}/retry/source_replay_policy",
    ): (
        "a JSON-pointer template naming the authored `source_replay_policy` field. It is "
        "the PATH argument of its raise sites, never their message text"
    ),
}

_MIXING_GATE = "process_call_connector_mixing"
_BODY_PLACEMENT_CODE = "PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY"

#: The two texts SELF-184-37 replaced, verbatim, for the faithful revert mutant.
_SLOT_ONLY_REMEDIATION = {
    "parser": (
        "Use a node kind this body slot admits. The admitted set for each slot is "
        "published at "
        "get_schema_template(schema_name='process_ir_authoring', category='placement'); "
        "a kind absent from a slot is rejected, so absence is the rule, not an omission."
    ),
    "compiler": (
        "Use a node kind this body slot admits. The admitted set for each slot is "
        "published at get_schema_template(schema_name='process_ir_authoring', "
        "category='placement'); a kind absent from a slot is rejected outright."
    ),
}


def _names(capability, text):
    """Whole-token match, the same rule `capability_citations` uses for a citation."""
    return re.search(
        r"(?<![A-Za-z0-9_])" + re.escape(capability) + r"(?![A-Za-z0-9_])", text or ""
    ) is not None


def _gate_citations():
    """`(pairs, capabilities)` read from source, with the reader's fail-closed half asserted."""
    from _process_ir_diagnostic_emissions import capability_citations
    from boomi_mcp.models.process_ir import PROCESS_IR_V1_CAPABILITIES

    capabilities = frozenset(PROCESS_IR_V1_CAPABILITIES)
    assert capabilities, "no capability table — the census would be vacuous"
    pairs, unassociated = capability_citations(capabilities)
    observed = {(path, text) for path, _lineno, text in unassociated}
    pinned = set(CAPABILITY_CITATIONS_NOT_MESSAGE_TEXT)
    assert observed == pinned, {
        "unplaced (a citation the reader cannot place at a raise site)": sorted(
            observed - pinned),
        "pinned but gone (retire the entry)": sorted(pinned - observed),
    }
    return pairs, capabilities


def _check_every_remediation_names_its_gates(pairs, capabilities):
    """`[(code, layer, capability)]` omissions, asserted empty; returns the pairs it checked."""
    from _process_ir_diagnostic_emissions import producer_of

    # Floor, and the anti-vacuity anchor: the SELF-184-37 pair itself, raised at BOTH
    # layers, and served by both layers' tables.
    sites = pairs.get(_BODY_PLACEMENT_CODE, {}).get(_MIXING_GATE, frozenset())
    assert {producer_of(path) for path, _lineno in sites} >= {"parser", "compiler"}, sorted(sites)

    layers = _by_layer()
    checked = set()
    missing = []
    for code, cited in pairs.items():
        tables = [layer for layer in ("parser", "compiler", "semantic") if code in layers[layer]]
        assert tables, ("a gate-citing code no table serves", code)
        for capability in cited:
            assert capability in capabilities, capability
            for layer in tables:
                checked.add((code, layer, capability))
                if not _names(capability, layers[layer][code]["remediation"]):
                    missing.append((code, layer, capability))
    assert {(_BODY_PLACEMENT_CODE, "parser", _MIXING_GATE),
            (_BODY_PLACEMENT_CODE, "compiler", _MIXING_GATE)} <= checked, sorted(checked)
    assert sorted(missing) == [], sorted(missing)
    return checked


def test_every_code_remediation_names_every_gate_its_raisers_cite():
    """SELF-184-37: every served remediation names each capability gate its code's raisers cite.

    `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` has two raising rules: slot admission,
    and the `process_call_connector_mixing` gate, which keeps the code by recorded #141/#175
    design. Its one remediation described only slot admission, so a mixing refusal told its
    author to "use a node kind this body slot admits" about a kind the slot does admit.

    Both sides are derived. `capability_citations` scans the model and compiler modules for
    raise sites and verdict renderers, and the gate names come from
    `PROCESS_IR_V1_CAPABILITIES`. Every served table the code appears in is checked
    separately, parser and compiler, the way `_by_layer` keeps them. The floor is the
    SELF-184-37 pair itself, raised at both layers. The reader's fail-closed half is in
    `_gate_citations`: a citing literal it cannot place is reported, not dropped.
    """
    pairs, capabilities = _gate_citations()
    checked = _check_every_remediation_names_its_gates(pairs, capabilities)
    # Every derived pair was looked up in every table serving its code: the authority's
    # own size, not a floor.
    layers = _by_layer()
    expected = {
        (code, layer, capability)
        for code, cited in pairs.items() for capability in cited
        for layer in ("parser", "compiler", "semantic") if code in layers[layer]
    }
    assert checked == expected, sorted(expected ^ checked)


def test_a_reverted_remediation_fails_the_gate_citation_invariant(monkeypatch):
    """Mutants against `test_every_code_remediation_names_every_gate_its_raisers_cite`.

    1. Faithful revert: each table's SELF-184-37 remediation restored to its slot-only text,
       one table at a time. The invariant names exactly that `(code, layer, gate)`.
    2. Derived: for every `(code, gate)` pair the scan finds and every table serving the
       code, the gate's name erased from that one remediation. Each fails naming its triple.

    Patched at `_REMEDIATION` in `boomi_mcp.models.process_ir` and
    `boomi_mcp.compiler.process_ir.diagnostics`, which the served spec accessors read at
    call time. The unmutated control passes before and after each.
    """
    from boomi_mcp.compiler.process_ir import diagnostics
    from boomi_mcp.compiler.process_ir.semantic_validation import findings
    from boomi_mcp.models import process_ir as parser_module

    modules = {"parser": parser_module, "compiler": diagnostics, "semantic": findings}
    pairs, capabilities = _gate_citations()
    _check_every_remediation_names_its_gates(pairs, capabilities)

    def expect(layer, code, text, capability):
        table = dict(modules[layer]._REMEDIATION)
        table[code] = text
        monkeypatch.setattr(modules[layer], "_REMEDIATION", table)
        with pytest.raises(AssertionError) as caught:
            _check_every_remediation_names_its_gates(pairs, capabilities)
        assert repr((code, layer, capability)) in str(caught.value), str(caught.value)[:1500]
        monkeypatch.undo()
        _check_every_remediation_names_its_gates(pairs, capabilities)

    for layer, text in _SLOT_ONLY_REMEDIATION.items():
        expect(layer, _BODY_PLACEMENT_CODE, text, _MIXING_GATE)

    derived = 0
    for code, cited in pairs.items():
        for capability in cited:
            for layer, module in modules.items():
                if code not in module._REMEDIATION:
                    continue
                erased = re.sub(
                    r"(?<![A-Za-z0-9_])" + re.escape(capability) + r"(?![A-Za-z0-9_])",
                    "this capability", module._REMEDIATION[code])
                expect(layer, code, erased, capability)
                derived += 1
    assert derived >= 2, derived


# ---------------------------------------------------------------------------
# SELF-184-37, second instance: a code the compiler serves by translating a
# shared model rule serves the parser's words
# ---------------------------------------------------------------------------

#: The derivation's size when the test was written. A floor, never the authority: the
#: authority is `compiler_translated_codes()`, and a new translated code simply joins it.
_TRANSLATED_CODE_FLOOR = 10


def _translated_codes():
    """The derived set, with the reader's own non-vacuity asserted."""
    from _process_ir_diagnostic_emissions import compiler_translated_codes

    derived = compiler_translated_codes()
    assert derived["unreadable"] == (), derived["unreadable"]
    translated = derived["translated"]
    assert len(translated) >= _TRANSLATED_CODE_FLOOR, sorted(translated)
    by_mechanism = {}
    for code, mechanisms in translated.items():
        for mechanism in mechanisms:
            by_mechanism.setdefault(mechanism.split(" ", 1)[0], set()).add(code)
    # Every mechanism contributes, so none of the three arms is vacuous...
    assert set(by_mechanism) == {"renders", "translates", "registered"}, sorted(by_mechanism)
    # ...and the registered arm IS the allowance this file already pins in both directions.
    assert by_mechanism["registered"] == set(COMPILER_REGISTERED_PARSE_CODES), sorted(
        by_mechanism["registered"])
    return derived


def test_a_translated_code_serves_the_parsers_remediation(monkeypatch):
    """SELF-184-37, second instance: a code the compiler serves for a MODEL rule serves the
    parser's remediation, word for word.

    The compiler served `PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED` for the
    model's own orphan-`continue` rule, run through `_as_compile_error`, under a remediation
    that described only continuation after a branch or decision. Measured at `73b9d3d` and
    at the batch baseline `8048836`. The rule is the parser's, so the text is too, which is
    the layer rule `diagnostics._MESSAGES` states: where the fact is genuinely identical at
    both layers, the wording is identical too.

    The set is derived, not listed. `compiler_translated_codes()` reads three mechanisms
    from source: a compiler function rendering a model verdict, a translator re-raising a
    model rule's refusal, and a code the compiler table registers that no compiler module
    raises. The registered arm is checked against `COMPILER_REGISTERED_PARSE_CODES`, and
    each arm must contribute. A translated code that a compiler-only rule also raises
    (`native`) keeps one text as well, worded to cover those rules.

    FOLDED by QA-184-s1-r17-01. The equality itself is now decided by the broader rule,
    `test_every_code_two_tables_serve_serves_one_remediation`, with one exemption table,
    `CODES_WITH_LAYER_TEXT`. Checking only translated codes is how the two codes the compiler
    raises natively, from raisers that restate a model rule, kept two texts. This test keeps
    what the derivation alone guarantees: every arm contributes, every translated code is in
    both tables and therefore inside the broader check, and no translated code may be
    exempted, because its compiler raiser IS a model rule.

    Mutant, in-file: each translated code's compiler text changed alone must fail the check.

    COVERAGE BOUND. Two properties are decided mechanically: a remediation names every
    capability gate its raisers cite
    (`test_every_code_remediation_names_every_gate_its_raisers_cite`), and a code two tables
    serve serves one text (`test_every_code_two_tables_serve_serves_one_remediation`).
    Neither decides whether a remediation's WORDING fits every rule that raises its code.
    That is a claim about English, and a reader over English cannot make it, as the
    projection says of its own guard over `_REVIEWED_PLACEMENT_PROSE`. The unified texts were
    written against the raising rules this derivation lists; that comparison is review, not
    a guard.
    """
    from boomi_mcp.compiler.process_ir import diagnostics

    derived = _translated_codes()
    shared = _check_shared_codes_serve_one_remediation()
    translated = set(derived["translated"])
    both = shared[("parser", "compiler")]
    assert translated <= both, sorted(translated - both)
    exempted = sorted(translated & set(CODES_WITH_LAYER_TEXT))
    assert exempted == [], exempted
    # The compiler-only rules the one text must also cover are found, not assumed away.
    assert {
        "PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY",
        "PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED",
    } <= set(derived["native"]), sorted(derived["native"])

    for code in derived["translated"]:
        table = dict(diagnostics._REMEDIATION)
        table[code] = table[code] + " Edited on this layer only."
        monkeypatch.setattr(diagnostics, "_REMEDIATION", table)
        with pytest.raises(AssertionError) as caught:
            _check_shared_codes_serve_one_remediation()
        assert repr(code) in str(caught.value), str(caught.value)[:1500]
        monkeypatch.undo()
        _check_shared_codes_serve_one_remediation()


# ---------------------------------------------------------------------------
# QA-184-s1-r17-01: every code two served tables carry serves one remediation
# ---------------------------------------------------------------------------

#: Codes whose remediation may differ between two tables that both serve it, each mapped to
#: the broader fact its layer's raiser decides, which one text cannot state. Compared whole
#: with the codes measured unequal, so it fails when that set grows and when an entry goes
#: stale. Empty: every shared code serves one text. A translated code may never be here
#: (`test_a_translated_code_serves_the_parsers_remediation`).
CODES_WITH_LAYER_TEXT = {}

_TABLE_PAIRS = (("parser", "compiler"), ("parser", "semantic"), ("compiler", "semantic"))

#: The remediations QA-184-s1-r17-01 replaced, verbatim, for the faithful revert mutants.
_R17_01_REMEDIATION = {
    ("parser", "PROCESS_IR_SEMANTIC_NESTING_LIMIT"): (
        "Reduce Branch/Decision nesting to at most "
        "PROCESS_IR_V1_MAX_CONTROL_DEPTH levels, or move the deeper routing into a "
        "subprocess. This is a ProcessIR v1 compiler bound, not a Boomi platform limit."
    ),
    ("compiler", "PROCESS_IR_SEMANTIC_NESTING_LIMIT"): (
        "Reduce Branch/Decision nesting to at most the documented control depth, or "
        "move the deeper routing into a subprocess. This is a ProcessIR v1 compiler "
        "bound, not a Boomi platform limit."
    ),
    ("compiler", "PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED"): (
        "End the catch body with a stop, an exception, a staging cache write, or a "
        "process_call that hands the caught document to a recovery process. "
        "Every caught document must reach a terminal."
    ),
}


def _table_modules():
    from boomi_mcp.compiler.process_ir import diagnostics
    from boomi_mcp.compiler.process_ir.semantic_validation import findings
    from boomi_mcp.models import process_ir as parser_module

    return {"parser": parser_module, "compiler": diagnostics, "semantic": findings}


def _check_shared_codes_serve_one_remediation():
    """`{pair: frozenset(shared codes)}`, after asserting each shared code serves one text."""
    layers = _by_layer()
    shared, unequal = {}, {}
    for pair in _TABLE_PAIRS:
        left, right = (layers[name] for name in pair)
        shared[pair] = frozenset(set(left) & set(right))
        for code in sorted(shared[pair]):
            if left[code]["remediation"] != right[code]["remediation"]:
                unequal.setdefault(code, []).append(pair)
    pinned = set(CODES_WITH_LAYER_TEXT)
    assert set(unequal) == pinned, {
        "unequal and not exempted": sorted(
            (code, unequal[code]) for code in set(unequal) - pinned),
        "exempted but equal or no longer shared (retire it)": sorted(pinned - set(unequal)),
    }
    blank = sorted(code for code, reason in CODES_WITH_LAYER_TEXT.items() if not reason.strip())
    assert blank == [], blank
    return shared


def test_every_code_two_tables_serve_serves_one_remediation():
    """QA-184-s1-r17-01: a code two served tables carry serves one remediation from both.

    `diagnostics._MESSAGES` states the layer rule: where the fact is genuinely identical at
    both layers, the wording is identical too. Batch 16 checked it only for the codes the
    compiler serves by TRANSLATING a model rule, so it missed two the compiler raises
    natively from raisers that restate a model rule. `PROCESS_IR_SEMANTIC_NESTING_LIMIT`
    (`body_capabilities._walk_control` and `invariants._check_control_depth` restate the
    depth bound) served "the documented control depth" at one layer and a constant's name at
    the other. `PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED` (`_walk_try_catch` restates the
    catch-terminal rule) served "staging cache write" against "staging cache_put".

    Checked for every PAIR of tables, not only parser and compiler: the validator's tables
    now carry the compiler's words for the compiler-owned codes it raises
    (`findings._COMPILER_WORDED_CODES`), and they must stay the compiler's.

    `CODES_WITH_LAYER_TEXT` would hold a code whose layer decides a genuinely broader fact,
    with that fact stated, and it is compared whole. It is empty. The floors are the shared
    sets' sizes when this was written and the four codes the two QA findings name.
    """
    shared = _check_shared_codes_serve_one_remediation()
    both = shared[("parser", "compiler")]
    assert {"PROCESS_IR_SEMANTIC_NESTING_LIMIT", "PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED"} <= both, (
        sorted(both))
    assert len(both) >= 12, sorted(both)
    worded = shared[("compiler", "semantic")]
    assert {"PROCESS_IR_SEMANTIC_PROFILE_MISMATCH",
            "PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED"} <= worded, sorted(worded)
    assert len(worded) >= 19, sorted(worded)


def test_a_divergent_shared_remediation_fails_the_parity_invariant(monkeypatch):
    """Mutants against `test_every_code_two_tables_serve_serves_one_remediation`.

    1. Faithful revert: each text QA-184-s1-r17-01 replaced, restored alone in its table.
    2. Derived: for every code two tables share, and each of the two tables, that one table's
       text edited alone.

    Each must fail naming its code, and the unmutated control passes after each.
    """
    modules = _table_modules()
    shared = _check_shared_codes_serve_one_remediation()

    def expect(layer, code, text):
        table = dict(modules[layer]._REMEDIATION)
        table[code] = text
        monkeypatch.setattr(modules[layer], "_REMEDIATION", table)
        with pytest.raises(AssertionError) as caught:
            _check_shared_codes_serve_one_remediation()
        assert repr(code) in str(caught.value), str(caught.value)[:1500]
        monkeypatch.undo()
        _check_shared_codes_serve_one_remediation()

    for (layer, code), text in _R17_01_REMEDIATION.items():
        expect(layer, code, text)

    derived = 0
    for pair, codes in shared.items():
        for code in sorted(codes):
            for layer in pair:
                expect(layer, code,
                       modules[layer]._REMEDIATION[code] + " Edited on this layer only.")
                derived += 1
    assert derived == 2 * sum(len(codes) for codes in shared.values()), derived
    assert derived >= 2 * (12 + 19), derived


# ---------------------------------------------------------------------------
# QA-184-s1-r17-02: every code a table factory can be handed serves its table's text
# ---------------------------------------------------------------------------

#: How the codes reaching each DYNAMIC call into a table-reading factory are derived. Keyed
#: like `PINNED_DELEGATION_SITES`, whose reasons state the same authorities in prose, and
#: compared whole with the `site` rows of `routed_emissions()`, so a new dynamic call into
#: a factory fails until its codes have a derivation here. A `definition` row needs none:
#: it is a wrapper's own body forwarding its first parameter, and the wrapper's call sites
#: are read as emissions.
ROUTED_DYNAMIC_SITES = {
    (
        "src/boomi_mcp/models/process_ir.py",
        "_diagnostic",
        "Name(id='code', ctx=Load())",
    ): "routing map",
    (
        "src/boomi_mcp/compiler/process_ir/body_capabilities.py",
        "raise_compile_error",
        "Name(id='code', ctx=Load())",
    ): "chain rule",
    (
        "src/boomi_mcp/compiler/process_ir/invariants.py",
        "_fail",
        "Name(id='code', ctx=Load())",
    ): "forward",
    (
        "src/boomi_mcp/compiler/process_ir/pipeline.py",
        "diagnostic",
        "Attribute(value=Name(id='item', ctx=Load()), attr='code', ctx=Load())",
    ): "restore",
    (
        "src/boomi_mcp/compiler/process_ir/semantic_validation/flow.py",
        "finding",
        "Attribute(value=Name(id='item', ctx=Load()), attr='code', ctx=Load())",
    ): "translation",
    (
        "src/boomi_mcp/compiler/process_ir/semantic_validation/validation_policy.py",
        "finding",
        "Name(id='exemption', ctx=Load())",
    ): "policy exemption",
}


def _derive_raised():
    """`{layer: {code: frozenset(sources)}}`: every code each table factory can be handed.

    Read from source (`routed_emissions`, `translation_sites`, `forward_owner_codes`) and,
    for a dynamic site whose codes a registry owns, from that registry. The `restore` site is
    the one whose codes depend on the tables themselves, since it re-serves a report code the
    validation tables lack, so `_check_raised_codes_are_served` resolves it against the
    tables as they are.
    """
    from _process_ir_diagnostic_emissions import (
        forward_owner_codes,
        raising_methods,
        routed_emissions,
        table_factory_reads,
        translation_sites,
    )
    from boomi_mcp.compiler.process_ir import body_capabilities
    from boomi_mcp.models import process_ir as parser_module

    reads = table_factory_reads()
    assert reads and all(
        found == {"_MESSAGES", "_REMEDIATION"} for found in reads.values()), dict(reads)
    # The call walk behind `translation_sites` does not follow a method; that limit is a
    # coverage gap only if a method raises, and none does.
    assert raising_methods() == (), raising_methods()

    codes, dynamic, ambiguous = routed_emissions()
    assert ambiguous == (), ambiguous
    raised = {layer: {} for layer in ("parser", "compiler", "semantic")}

    def add(layer, found, source):
        for code in found:
            raised[layer].setdefault(code, set()).add(source)

    for layer, found in codes.items():
        add(layer, found, "raise site")

    sites = {}
    for path, _lineno, sink, dump, kind, layer in dynamic:
        if kind == "site":
            sites.setdefault((path, sink, dump), set()).add(layer)
    assert set(sites) == set(ROUTED_DYNAMIC_SITES), {
        "a dynamic call into a factory with no derivation": sorted(
            set(sites) - set(ROUTED_DYNAMIC_SITES)),
        "a derivation for a call that is gone (retire it)": sorted(
            set(ROUTED_DYNAMIC_SITES) - set(sites)),
    }
    translations = translation_sites()
    forwards = forward_owner_codes()
    for key, disposition in ROUTED_DYNAMIC_SITES.items():
        (layer,) = sites[key]
        if disposition == "routing map":
            add(layer, parser_module._CUSTOM_ERROR_CODES.values(), disposition)
        elif disposition == "chain rule":
            add(layer, [
                code for code in (
                    body_capabilities._translatable_chain_rule_code(tag)
                    for tag in parser_module._CUSTOM_ERROR_CODES
                ) if code
            ], disposition)
        elif disposition == "forward":
            entry = forwards[key]
            assert entry["unreadable"] == () and entry["codes"], dict(entry)
            add(layer, entry["codes"], disposition)
        elif disposition == "translation":
            (entry,) = [
                value for (path, _lineno), value in translations.items()
                if path == key[0] and value["sink"] == key[1]
            ]
            assert entry["unreadable"] == () and entry["codes"], dict(entry)
            # The re-raised family is read from the handler and is not vacuous: the walk
            # found a code in it, which the handler re-raises instead of translating.
            assert entry["reraised_prefixes"] and set(entry["raisable"]) - set(entry["codes"]), (
                dict(entry))
            add(layer, entry["codes"], disposition)
        elif disposition == "policy exemption":
            add(layer, _policy_exemption_codes(), disposition)
        elif disposition == "restore":
            assert layer == "compiler", (key, layer)
        else:
            raise AssertionError(("an unknown disposition", key, disposition))
    # Every translation site the reader finds by SHAPE is a pinned one, so a second one
    # cannot join without a derivation of its own.
    shaped = {(path, value["sink"]) for (path, _lineno), value in translations.items()}
    pinned = {(key[0], key[1]) for key, disposition in ROUTED_DYNAMIC_SITES.items()
              if disposition == "translation"}
    assert shaped == pinned, (sorted(shaped), sorted(pinned))
    return {
        layer: {code: frozenset(sources) for code, sources in found.items()}
        for layer, found in raised.items()
    }


def _check_raised_codes_are_served(raised):
    """Every derived code is in the table its factory reads. Returns the number checked.

    The `restore` arm is resolved here: `_restore` re-serves through `diagnostic()` exactly
    the report codes the validation tables lack, so those join the compiler's arm.
    """
    layers = _by_layer()
    arms = {layer: dict(found) for layer, found in raised.items()}
    for code in raised["semantic"]:
        if code not in layers["semantic"]:
            arms["compiler"][code] = frozenset(arms["compiler"].get(code, frozenset()) | {"restore"})
    missing = sorted(
        (layer, code) for layer, found in arms.items() for code in found
        if code not in layers[layer]
    )
    if missing:
        # Raised rather than asserted: pytest truncates a long assertion message, and every
        # unserved row must reach the report whole.
        raise AssertionError("unserved {0!r}; sources {1!r}".format(
            missing, {repr(row): sorted(arms[row[0]][row[1]]) for row in missing}))
    return sum(len(found) for found in arms.values())


def test_every_raised_code_serves_its_own_table_text():
    """QA-184-s1-r17-02: every code a table factory can be handed is in the table it reads.

    The semantic validator raised `PROCESS_IR_SEMANTIC_PROFILE_MISMATCH` and
    `PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED` through `finding()`, which selects
    text from the SEMANTIC tables only, and those tables had no entry for either. So both
    typed routes served "semantic validation rejected the payload" and a fallback carrying
    a placeholder. Every guard above stayed green because `_SATISFYING_TABLES` lets the
    compiler's table answer for a semantic MODULE, and the flow translation site was pinned
    as introducing no code. Measured at `f1ed254`: every compiler-owned code the validator
    raises fell back, the two QA named and every code its translation site re-serves.

    The check is per FACTORY. `routed_emissions()` routes each raise site to the table its
    factory reads, derived from the sinks' own bodies. `ROUTED_DYNAMIC_SITES` derives the
    codes reaching each call whose code is not written at the call: the parser's routing
    map, the compiler's chain-rule translator (codes its own tables can render), the region
    forward (runtime default plus call-site constants), the validator's translation site
    (every code the caught compile refusal can carry), the policy registry's exemptions,
    and `_restore` (report codes the validation tables lack, which must then be in the
    compiler's). The parser's factory raises on a code it lacks, and the other two fall back,
    so a missing entry here is a crash or a fallback on a public route.

    Floors, and the anti-vacuity anchors: each table is handed codes; the parser is handed
    its whole routing map; `CONNECTOR_ACTION_UNSUPPORTED` reaches the validator only through
    the translation site, and `PROFILE_MISMATCH` through a raise site and that site both.
    Coverage is the derivation's own size, not a floor.
    """
    from boomi_mcp.models import process_ir as parser_module

    raised = _derive_raised()
    checked = _check_raised_codes_are_served(raised)
    assert checked == sum(len(found) for found in raised.values()), checked
    empty = sorted(layer for layer, found in raised.items() if not found)
    assert empty == [], empty
    assert set(parser_module._CUSTOM_ERROR_CODES.values()) <= set(raised["parser"])
    semantic = raised["semantic"]
    assert semantic["PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED"] == {"translation"}, (
        semantic.get("PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED"))
    assert {"raise site", "translation"} <= semantic["PROCESS_IR_SEMANTIC_PROFILE_MISMATCH"], (
        semantic.get("PROCESS_IR_SEMANTIC_PROFILE_MISMATCH"))


def test_a_missing_table_entry_fails_the_raised_code_invariant(monkeypatch):
    """Mutants against `test_every_raised_code_serves_its_own_table_text`.

    1. Faithful revert of QA-184-s1-r17-02: the validator's tables without the rows
       `findings._COMPILER_WORDED_CODES` adds. The invariant names both QA codes.
    2. Derived: every `(table, code)` the derivation lists, removed from that one table's
       two dicts. Each fails naming its row.

    The unmutated control passes after each.
    """
    modules = _table_modules()
    raised = _derive_raised()
    _check_raised_codes_are_served(raised)

    def expect(layer, codes, rows):
        for name in ("_MESSAGES", "_REMEDIATION"):
            table = {code: text for code, text in getattr(modules[layer], name).items()
                     if code not in codes}
            monkeypatch.setattr(modules[layer], name, table)
        with pytest.raises(AssertionError) as caught:
            _check_raised_codes_are_served(raised)
        for row in rows:
            assert repr(row) in str(caught.value), (row, str(caught.value)[:1500])
        monkeypatch.undo()
        _check_raised_codes_are_served(raised)

    expect("semantic", set(modules["semantic"]._COMPILER_WORDED_CODES), [
        ("semantic", "PROCESS_IR_SEMANTIC_PROFILE_MISMATCH"),
        ("semantic", "PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED"),
    ])

    derived = 0
    for layer, found in raised.items():
        for code in sorted(found):
            expect(layer, {code}, [(layer, code)])
            derived += 1
    assert derived == sum(len(found) for found in raised.values()), derived


# ---------------------------------------------------------------------------
# QA-184-s1-r17-01 / -02: served text names no internal constant and carries no
# placeholder, in every table and in each factory's fallback
# ---------------------------------------------------------------------------

#: A code no table registers, used to ask each factory what it serves for one.
_UNREGISTERED_PROBE = "PROCESS_IR_TEST_UNREGISTERED_PROBE"

#: The code slot of a factory's fallback row in `_served_texts()`.
_FALLBACK = "unregistered-code fallback"

#: The fallback QA-184-s1-r17-02 replaced, verbatim, for the faithful revert mutant.
_R17_02_FALLBACK = (
    "Fetch this code's authoring rule with "
    "get_schema_template(schema_name='process_ir_authoring', "
    "authoring_entry_id=<the id this diagnostic serves in "
    "authoring_contract_entry_ids>). The category sweep this used to name "
    "pages at twenty of sixty-five entries, so following it literally left "
    "most diagnostics off the first page of their own remediation's route; "
    "the entry id resolves in one call and the diagnostic already carries it."
)

#: The size of `internal_constant_names()` when this was written. A floor, never the
#: authority: the authority is the scanned modules, and a new constant simply joins it.
_INTERNAL_NAME_FLOOR = 200

#: Any angle-bracketed token, at any length: the #451 shape. The catalog guard's pattern
#: stops at forty characters, and the QA-184-s1-r17-02 placeholder is longer than that.
_PLACEHOLDER = re.compile(r"<[^<>\n]+>")


def _fallback_texts():
    """`{layer: {"message": ..., "remediation": ...}}`: what each table factory serves for a
    code no table registers, asked of the factory itself rather than read from its source."""
    modules = _table_modules()
    registered = sorted(layer for layer, table in _by_layer().items()
                        if _UNREGISTERED_PROBE in table)
    assert registered == [], registered
    # The parser's factory has no fallback: an unregistered code raises, so it serves nothing.
    with pytest.raises(KeyError):
        modules["parser"]._diagnostic(_UNREGISTERED_PROBE, ())
    compiled = modules["compiler"].diagnostic(_UNREGISTERED_PROBE, "schema", "")
    found = modules["semantic"].finding(_UNREGISTERED_PROBE, "error", "reachability", "")
    return {
        "compiler": {"message": compiled.message, "remediation": compiled.remediation},
        "semantic": {"message": found.message, "remediation": found.remediation},
    }


def _served_texts():
    """`[(layer, code, field, text)]`: every message and remediation of every served table,
    and both factories' fallbacks."""
    rows = [
        (layer, code, field, spec[field])
        for layer, table in _by_layer().items()
        for code, spec in table.items()
        for field in ("message", "remediation")
    ]
    for layer, texts in _fallback_texts().items():
        rows.extend((layer, _FALLBACK, field, text) for field, text in texts.items())
    return rows


def _check_no_internal_name(names, rows):
    token = re.compile(
        r"(?<![A-Za-z0-9_])("
        + "|".join(re.escape(name) for name in sorted(names, key=len, reverse=True))
        + r")(?![A-Za-z0-9_])"
    )
    hits = sorted(
        (layer, code, field, name)
        for layer, code, field, text in rows
        for name in set(token.findall(text))
    )
    assert hits == [], hits


def test_no_served_remediation_names_an_internal_constant():
    """QA-184-s1-r17-01: no served text names an internal constant where its value belongs.

    The nesting remediation told its reader to reduce nesting "to at most
    PROCESS_IR_V1_MAX_CONTROL_DEPTH levels", a module constant's name, which no served page
    resolves, while the message beside it stated the number. Both sides are derived: the
    names are every module-level UPPER_SNAKE binding in the scanned modules
    (`internal_constant_names`), and the texts are every message and remediation in the
    three served tables plus what the compiler's and the validator's factories serve for a
    code they do not register, asked of the factories. The floor is the QA name itself,
    inside the case set, and the case set's size when this was written.
    """
    from _process_ir_diagnostic_emissions import internal_constant_names

    names = internal_constant_names()
    assert "PROCESS_IR_V1_MAX_CONTROL_DEPTH" in names, "the QA-184-s1-r17-01 name is outside the case set"
    assert len(names) >= _INTERNAL_NAME_FLOOR, len(names)
    rows = _served_texts()
    assert len(rows) == 2 * sum(len(table) for table in _by_layer().values()) + 4, len(rows)
    _check_no_internal_name(names, rows)


def test_a_served_internal_name_fails_the_constant_name_invariant(monkeypatch):
    """Mutants against `test_no_served_remediation_names_an_internal_constant`.

    1. Faithful revert: the parser's nesting remediation restored to the constant's name.
    2. Derived: in each table, the first code's message and then its remediation with an
       internal name appended, a different name each time.
    3. Each factory's fallback with an internal name appended.

    Each must fail naming its row, and the unmutated control passes after each.
    """
    from _process_ir_diagnostic_emissions import internal_constant_names

    modules = _table_modules()
    names = internal_constant_names()
    _check_no_internal_name(names, _served_texts())

    def expect(module, attribute, value, row):
        monkeypatch.setattr(module, attribute, value)
        with pytest.raises(AssertionError) as caught:
            _check_no_internal_name(names, _served_texts())
        assert repr(row) in str(caught.value), (row, str(caught.value)[:1500])
        monkeypatch.undo()
        _check_no_internal_name(names, _served_texts())

    code = "PROCESS_IR_SEMANTIC_NESTING_LIMIT"
    table = dict(modules["parser"]._REMEDIATION)
    table[code] = _R17_01_REMEDIATION[("parser", code)]
    expect(modules["parser"], "_REMEDIATION", table,
           ("parser", code, "remediation", "PROCESS_IR_V1_MAX_CONTROL_DEPTH"))

    pool = sorted(names)
    slots = [(layer, field) for layer in ("parser", "compiler", "semantic")
             for field in ("message", "remediation")]
    for index, (layer, field) in enumerate(slots):
        attribute = "_MESSAGES" if field == "message" else "_REMEDIATION"
        table = dict(getattr(modules[layer], attribute))
        first = sorted(table)[0]
        name = pool[index * len(pool) // len(slots)]
        table[first] = table[first] + " See " + name + "."
        expect(modules[layer], attribute, table, (layer, first, field, name))

    for layer in ("compiler", "semantic"):
        text = modules[layer]._UNREGISTERED_CODE_REMEDIATION + " See PROCESS_IR_V1_MAX_CONTROL_DEPTH."
        expect(modules[layer], "_UNREGISTERED_CODE_REMEDIATION", text,
               (layer, _FALLBACK, "remediation", "PROCESS_IR_V1_MAX_CONTROL_DEPTH"))


def _check_pasteable(rows):
    hits = sorted(
        (layer, code, field, token)
        for layer, code, field, text in rows
        for token in _PLACEHOLDER.findall(text)
    )
    assert hits == [], hits


def test_every_served_remediation_is_pasteable():
    """QA-184-s1-r17-02 / #451: no served text carries an angle-bracket placeholder.

    #451's guard covers the catalog's entries, and the catalog is built from the three
    tables, so a placeholder there was caught. The factories' FALLBACK is not in any table,
    and it asked the caller to substitute "<the id this diagnostic serves in
    authoring_contract_entry_ids>" into a call. It was served for real, on both typed
    routes, for every compiler-owned code the validator raised. This covers every message
    and remediation of every table and both fallbacks, asked of the factories. The floor is
    the case set's size, and the fallback rows are proved to be the fallback rather than a
    table text reached by accident.
    """
    rows = _served_texts()
    assert len(rows) == 2 * sum(len(table) for table in _by_layer().values()) + 4, len(rows)
    fallbacks = _fallback_texts()
    assert set(fallbacks) == {"compiler", "semantic"}, sorted(fallbacks)
    table_texts = {spec["remediation"] for table in _by_layer().values() for spec in table.values()}
    reached = sorted({texts["remediation"] for texts in fallbacks.values()} & table_texts)
    assert reached == [], reached
    _check_pasteable(rows)


def test_a_placeholder_fails_the_pasteable_invariant(monkeypatch):
    """Mutants against `test_every_served_remediation_is_pasteable`.

    1. Faithful revert: each factory's fallback restored to the QA-184-s1-r17-02 text, one
       factory at a time.
    2. Derived: in each table, the first code's remediation with a `<kind>` placeholder.

    Each must fail naming its row, and the unmutated control passes after each.
    """
    modules = _table_modules()
    _check_pasteable(_served_texts())

    def expect(module, attribute, value, row):
        monkeypatch.setattr(module, attribute, value)
        with pytest.raises(AssertionError) as caught:
            _check_pasteable(_served_texts())
        assert repr(row) in str(caught.value), (row, str(caught.value)[:1500])
        monkeypatch.undo()
        _check_pasteable(_served_texts())

    placeholder = "<the id this diagnostic serves in authoring_contract_entry_ids>"
    for layer in ("compiler", "semantic"):
        expect(modules[layer], "_UNREGISTERED_CODE_REMEDIATION", _R17_02_FALLBACK,
               (layer, _FALLBACK, "remediation", placeholder))
    for layer in ("parser", "compiler", "semantic"):
        table = dict(modules[layer]._REMEDIATION)
        first = sorted(table)[0]
        table[first] = table[first] + " Pass node_kind='<kind>'."
        expect(modules[layer], "_REMEDIATION", table, (layer, first, "remediation", "<kind>"))
