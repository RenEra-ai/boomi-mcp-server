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
        "src/boomi_mcp/models/process_ir.py",
        "_diagnostic",
        "Name(id='code', ctx=Load())",
    ): (1, "the parse-error translator forwards a `code` it resolved from the routing map "
           "`_CUSTOM_ERROR_CODES`, which the reader collects as an authority in its own right"),
    (
        "src/boomi_mcp/compiler/process_ir/diagnostics.py",
        "diagnostic",
        "Name(id='code', ctx=Load())",
    ): (1, "the definition body of the `diagnostic` sink itself"),
    (
        "src/boomi_mcp/compiler/process_ir/invariants.py",
        "raise_compile_error",
        "Name(id='code', ctx=Load())",
    ): (1, "the definition body of the `_fail` wrapper, forwarding into `raise_compile_error`"),
    (
        "src/boomi_mcp/compiler/process_ir/invariants.py",
        "_fail",
        "Name(id='code', ctx=Load())",
    ): (1, "`_check_region_containment` forwards its `code` parameter, which sits in sixth "
           "position and carries a default; two of its three call sites omit it"),
    (
        "src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py",
        "finding",
        "Name(id='code', ctx=Load())",
    ): (1, "the definition body of `_report`, the local one-hop wrapper the lineage rules call"),
    (
        "src/boomi_mcp/compiler/process_ir/semantic_validation/flow.py",
        "finding",
        "Attribute(value=Name(id='item', ctx=Load()), attr='code', ctx=Load())",
    ): (1, "re-serves `item.code` from a finding the semantic report already produced, so it "
           "introduces no code of its own"),
    (
        "src/boomi_mcp/compiler/process_ir/semantic_validation/validation_policy.py",
        "finding",
        "Name(id='exemption', ctx=Load())",
    ): (1, "raises the exemption code chosen by `LegacyValidationPolicyV1.exemption_for`; the "
           "family is derived below from the policy registry, which owns it"),
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
UNREADABLE_DEFAULTS = {
    (
        "src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py",
        "_report",
        "code",
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
