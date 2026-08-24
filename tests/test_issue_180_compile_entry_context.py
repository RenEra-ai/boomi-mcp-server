"""Issue #180 — every ProcessIR compile entry gets the trusted context.

**The structural fix for defect class DC-154-A**, second instance.

The class is *(a server-built trusted context threaded to SOME compile sites,
runtime authority: the set of compile entries a root passes through)*. The first
instance was `build_materialization_plan`, fixed in #154 by adding the parameter
— an instance patch, and the ledger's note beside it read "this compile is the
THIRD one the authoring path runs for a root, and it was the only one still
strict". That claim was an ENUMERATION, and it was wrong: there was a fourth,
`materialize_canonical_process_xml`, which recompiles the plan at apply time.
Because it was strict, a root whose declaration turned a blocking finding into a
warning planned clean, compiled clean, and then failed at materialization — the
effect channel never reached apply at all.

So the enumeration is replaced here by an invariant derived from the authority:

- The set of ENTRIES is derived from the compiler's own signatures — every
  public function in the two pipeline modules that HAS a `capabilities`
  parameter. Nothing is hand-listed, so a new capability-aware entry joins the
  check by existing.
- The set of CALL SITES is derived by parsing `src/` — not grepped, not
  recalled — so a new call site joins the check by being written.
- A site that is deliberately strict must say so in `STRICT_BY_DESIGN` with a
  reason. An unlisted strict site fails.

`test_the_check_reports_a_strict_call_it_has_not_been_told_about` is the
non-vacuity witness: without it, a checker that silently found no call sites at
all would pass exactly like a clean tree.
"""

from __future__ import annotations

import ast
import inspect
import sys
from pathlib import Path

import pytest

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

_PIPELINE_MODULES = (
    "boomi_mcp.compiler.process_ir.pipeline",
    "boomi_mcp.compiler.process_ir.semantic_validation.pipeline",
)

#: Call sites that compile a root WITHOUT trusted context on purpose, with the
#: reason. Keyed by `(repo-relative path, entry name)`. Read by the test — not
#: documentation. A row here that no longer matches a strict call is also a
#: failure, so a fixed site cannot leave a stale exemption behind.
STRICT_BY_DESIGN = {
    (
        "src/boomi_mcp/compiler/process_ir/legacy_adapters/emission.py",
        "compile_process_ir_v1",
    ): (
        "the LEGACY dialect adapter. A legacy document has no effect "
        "declarations to resolve — the channel is part of the typed authoring "
        "surface — so strict is the correct question here, not an omission."
    ),
}


def _capability_aware_entries():
    """`{name: capabilities-parameter-index or None}` — from the SIGNATURES.

    The index is what makes a POSITIONAL argument countable: `validate_process_ir`
    takes `capabilities` positionally, and a check that only looked for the
    keyword would have reported a compliant call site as strict.
    """
    import importlib

    entries = {}
    for module_name in _PIPELINE_MODULES:
        module = importlib.import_module(module_name)
        for name in dir(module):
            if name.startswith("_"):
                continue
            obj = getattr(module, name)
            if not inspect.isfunction(obj) or obj.__module__ != module_name:
                continue
            parameters = list(inspect.signature(obj).parameters.items())
            for index, (parameter_name, parameter) in enumerate(parameters):
                if parameter_name != "capabilities":
                    continue
                entries[name] = (
                    index
                    if parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
                    else None
                )
    return entries


def _call_name(node):
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _supplies_context(node, entries):
    """Whether this call hands the entry a `capabilities` argument."""
    if any(keyword.arg == "capabilities" for keyword in node.keywords):
        return True
    # `**kwargs` forwarding could carry it; treat that as supplied rather than
    # guess.
    if any(keyword.arg is None for keyword in node.keywords):
        return True
    index = entries[_call_name(node)]
    return index is not None and len(node.args) > index


def _paired_strict_calls(tree, entries):
    """Strict calls that are the ELSE half of a fail-closed pair.

    Every capability-aware site in this repo is written as

        compile(...) if context is not None else compile(...)

    on purpose: passing `capabilities=None` would OVERRIDE the strict default
    rather than fall back to it. The `else` half is therefore not an omission —
    it is the same site's strict branch, and reporting it would make the
    invariant unsatisfiable by correct code.

    The pairing is read off the AST, not assumed: a ternary counts only when its
    OTHER branch calls the SAME entry WITH context. A ternary that supplies
    context in neither branch is still reported, which is what keeps this from
    being a blanket exemption for anything written as a conditional.
    """
    paired = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.IfExp):
            continue
        for supplying, strict in ((node.body, node.orelse), (node.orelse, node.body)):
            supplying_names = {
                _call_name(inner) for inner in ast.walk(supplying)
                if isinstance(inner, ast.Call)
                and _call_name(inner) in entries
                and _supplies_context(inner, entries)
            }
            for inner in ast.walk(strict):
                if (
                    isinstance(inner, ast.Call)
                    and _call_name(inner) in entries
                    and not _supplies_context(inner, entries)
                    and _call_name(inner) in supplying_names
                ):
                    paired.add(inner)
    return paired


def _strict_calls(source, path, entries):
    """Every call to a capability-aware entry that supplies no context."""
    tree = ast.parse(source)
    paired = _paired_strict_calls(tree, entries)
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _call_name(node)
        if name not in entries:
            continue
        if _supplies_context(node, entries):
            continue
        if node in paired:
            continue
        found.append((path, name, node.lineno))
    return found


def _sweep(entries):
    strict = []
    for file_path in sorted(_SRC.rglob("*.py")):
        relative = file_path.relative_to(_ROOT).as_posix()
        strict.extend(
            _strict_calls(file_path.read_text(encoding="utf-8"), relative, entries))
    return strict


def test_the_entry_set_is_derived_and_covers_the_known_entries():
    """The coverage claim, against the authority's full case set.

    The authority is the pipeline signature. Asserting the three entries the
    authoring path actually uses are present keeps the derivation from passing
    on an empty set — the failure mode that would make every other test here
    vacuous.
    """
    entries = _capability_aware_entries()
    assert entries, "no capability-aware compile entry was derived"
    assert {
        "compile_process_ir_v1",
        "compile_process_ir_model_v1",
        "validate_process_ir",
    } <= set(entries), sorted(entries)
    # The positional/keyword split is real and load-bearing.
    assert entries["compile_process_ir_v1"] is None
    assert entries["validate_process_ir"] == 2


def test_every_compile_entry_call_site_is_context_aware_or_declared_strict():
    entries = _capability_aware_entries()
    strict = _sweep(entries)
    undeclared = sorted(
        (path, name, line) for path, name, line in strict
        if (path, name) not in STRICT_BY_DESIGN
    )
    assert undeclared == [], (
        "these call sites compile a ProcessIR root with no trusted context. "
        "Thread the resolved capabilities through, or add the site to "
        "STRICT_BY_DESIGN with the reason it is correct: {0}".format(undeclared))


def test_no_strict_exemption_outlives_the_call_it_exempts():
    """A fixed site must not leave its exemption behind.

    An allowlist nobody prunes becomes a list of sites that used to be wrong,
    and the next real one hides among them.
    """
    entries = _capability_aware_entries()
    live = {(path, name) for path, name, _line in _sweep(entries)}
    stale = sorted(key for key in STRICT_BY_DESIGN if key not in live)
    assert stale == [], (
        "STRICT_BY_DESIGN names call sites that are no longer strict (or no "
        "longer exist); remove them: {0}".format(stale))


def test_the_apply_recompile_is_covered_by_this_sweep():
    """The site the enumeration missed, named explicitly.

    Without this, the sweep could be trivially satisfied by a refactor that
    moved the apply-time recompile somewhere the walk does not reach — and the
    class would recur exactly as it did the first time.
    """
    path = _ROOT / "src/boomi_mcp/categories/components/canonical_process_apply.py"
    source = path.read_text(encoding="utf-8")
    entries = _capability_aware_entries()

    calls = [
        node for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call) and _call_name(node) in entries
    ]
    assert calls, "the apply-time recompile is no longer in this module"
    assert any(
        any(keyword.arg == "capabilities" for keyword in node.keywords)
        for node in calls
    ), "the apply-time recompile supplies no trusted context"
    assert "plan.effect_capabilities" in source, (
        "the context must come off the PLAN — a parameter can be forgotten by "
        "the next call site, which is how this defect happened")


def test_the_check_reports_a_strict_call_it_has_not_been_told_about():
    """NON-VACUITY WITNESS.

    A checker that parsed nothing, or matched no call, would pass on a tree full
    of strict calls. Feed it a synthetic module containing exactly one, at a
    path no exemption covers, and require it to be reported — and require the
    keyword and positional spellings NOT to be reported, so the witness also
    proves the check is not simply reporting everything.
    """
    entries = _capability_aware_entries()

    strict_source = "compile_process_ir_v1(ir, symbols)\n"
    reported = _strict_calls(strict_source, "src/synthetic_witness.py", entries)
    assert [(name, line) for _path, name, line in reported] == [
        ("compile_process_ir_v1", 1)], reported

    # ...and the two compliant spellings are silent.
    assert _strict_calls(
        "compile_process_ir_v1(ir, symbols, capabilities=ctx)\n",
        "src/synthetic_witness.py", entries) == []
    assert _strict_calls(
        "validate_process_ir(ir, symbols, ctx)\n",
        "src/synthetic_witness.py", entries) == []

    # A call the entry set does not name is not this check's business.
    assert _strict_calls(
        "some_other_compile(ir, symbols)\n",
        "src/synthetic_witness.py", entries) == []

    # The fail-closed PAIR is silent — that is the idiom every real site uses.
    assert _strict_calls(
        "x = (compile_process_ir_v1(ir, s, capabilities=c)\n"
        "     if c is not None else compile_process_ir_v1(ir, s))\n",
        "src/synthetic_witness.py", entries) == []

    # ...but a ternary that supplies context in NEITHER branch still is not, so
    # the pairing rule cannot be used to launder a genuinely strict site.
    both_strict = _strict_calls(
        "x = (compile_process_ir_v1(ir, s)\n"
        "     if c is not None else compile_process_ir_v1(ir, s))\n",
        "src/synthetic_witness.py", entries)
    assert len(both_strict) == 2, both_strict

    # ...and a pair naming a DIFFERENT entry in the other branch is not a pair.
    assert len(_strict_calls(
        "x = (validate_process_ir(ir, s, c)\n"
        "     if c is not None else compile_process_ir_v1(ir, s))\n",
        "src/synthetic_witness.py", entries)) == 1


@pytest.mark.parametrize("key,reason", sorted(STRICT_BY_DESIGN.items()))
def test_every_strict_exemption_states_a_reason(key, reason):
    assert len(reason.split()) >= 8, (key, reason)
