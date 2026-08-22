"""Which ProcessIR diagnostic codes each layer can actually EMIT, read from source.

#177 invariant 1. The served diagnostic tables (`process_ir_v1_parse_diagnostic_specs`,
`compiler_diagnostic_specs`, `finding_specs`) are the *supply* side of the contract;
this module derives the *demand* side — the codes the emitting modules genuinely raise —
so the two can be compared. A guard that read only the served tables could never see the
defect this issue exists to close: a code that is raised and carries no served text, or a
served row for a code nothing raises.

WHY AST AND NOT A HAND-LIST
---------------------------
A hand-written list of "codes the compiler can raise" is precisely the mechanism of
DC-175-D — a hand-written record of a fact whose authority lives elsewhere. It would go
stale the first time a code was added, and its staleness would be invisible. So the case
set is read from the modules that do the raising.

WHY IT FAILS CLOSED
-------------------
This repo has shipped a guard that enumerated nothing and therefore passed everything five
separate times (#149, #151, #162, #175, and #175's own prose scanner). Three defences:

1. The sink names are PINNED and their definitions are resolved: if `raise_compile_error`
   is renamed, `pinned_sink_definitions()` reports the miss instead of the scan quietly
   finding zero calls to a name that no longer exists.
2. Every call to a sink whose code argument cannot be resolved is recorded as an
   UNRESOLVED SITE, never skipped. The caller asserts the site set equals a closed pinned
   table, so a new dynamic emission path fails the guard rather than vanishing from it.
3. The values of `_MESSAGES` / `_REMEDIATION` are excluded from resolution, so a registry
   can never prove its own reachability.

WHAT IS DELIBERATELY *NOT* MODELLED
-----------------------------------
Python control flow. An AST resolver that tries to follow arbitrary data flow is a second
implementation of the interpreter, and #175's four-round prose-scanner failure is the
recorded cost of a checker that models an open-ended space. Resolution covers exactly the
closed forms this tree uses — literal, module constant, `boomi_mcp.errors` constant,
routing-map value, conditional between two resolvable codes, a local helper whose every
return is a resolvable code, and one-hop parameter forwarding — and everything else is a
pinned site with a stated reason.
"""

from __future__ import annotations

import ast
import pathlib
import sys
from types import MappingProxyType

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_SRC = _ROOT / "src"
for _p in (str(_ROOT), str(_SRC)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp import errors as _errors  # noqa: E402

__all__ = [
    "EMISSION_ROOTS",
    "PINNED_SINKS",
    "collect_emissions",
    "pinned_sink_definitions",
    "verifier_issue_sites",
]

#: The modules that may raise a ProcessIR diagnostic. Directories are scanned whole —
#: there are NO module-level exclusions, deliberately: an exclusion is a claim ("nothing
#: under here emits") that would itself need a guard. Scanning everything makes that claim
#: a derived fact instead — a module that calls no sink simply contributes nothing.
EMISSION_ROOTS = (
    "src/boomi_mcp/models/process_ir.py",
    "src/boomi_mcp/compiler/process_ir",
    "src/boomi_mcp/categories/components/process_graph_verifier.py",
)

#: `(module path suffix, function name)` for every diagnostic-construction sink. Pinned by
#: DEFINITION, not just by call: `pinned_sink_definitions()` proves each one still exists,
#: so renaming a sink fails the guard instead of silently emptying the scan.
PINNED_SINKS = (
    ("src/boomi_mcp/models/process_ir.py", "_diagnostic"),
    ("src/boomi_mcp/compiler/process_ir/diagnostics.py", "diagnostic"),
    ("src/boomi_mcp/compiler/process_ir/diagnostics.py", "raise_compile_error"),
    ("src/boomi_mcp/compiler/process_ir/diagnostics.py", "internal_defect"),
    ("src/boomi_mcp/compiler/process_ir/invariants.py", "_fail"),
    ("src/boomi_mcp/compiler/process_ir/semantic_validation/findings.py", "finding"),
    ("src/boomi_mcp/categories/components/process_graph_verifier.py", "_issue"),
)

_SINK_NAMES = frozenset(name for _path, name in PINNED_SINKS)

#: Registry tables whose contents describe codes rather than raise them. Their VALUES are
#: excluded from resolution so a registry cannot prove its own reachability; their KEYS are
#: never collected at all, which is the same rule stated from the other side.
_REGISTRY_TABLES = frozenset({"_MESSAGES", "_REMEDIATION"})

#: Every public string constant in `boomi_mcp.errors`, which is where diagnostic codes live.
_ERROR_CONSTANTS = MappingProxyType(
    {
        name: value
        for name, value in vars(_errors).items()
        if name.isupper() and isinstance(value, str)
    }
)


def _producer(relative):
    text = str(relative)
    if text.endswith("models/process_ir.py"):
        return "parser"
    if "process_graph_verifier" in text:
        return "verifier"
    if "/semantic_validation/" in text:
        return "semantic"
    return "compiler"


def _iter_files():
    for entry in EMISSION_ROOTS:
        target = _ROOT / entry
        if target.is_dir():
            for path in sorted(target.rglob("*.py")):
                yield path
        else:
            yield target


def pinned_sink_definitions():
    """`{(path, name): found}` for every pinned sink — the anti-vacuity anchor.

    A scan that finds no calls is indistinguishable from a scan whose sink was renamed,
    unless the definitions are checked separately. This is that check.
    """
    found = {}
    for relative, name in PINNED_SINKS:
        source = (_ROOT / relative).read_text()
        tree = ast.parse(source)
        found[(relative, name)] = any(
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name
            for node in ast.walk(tree)
        )
    return found


class _ModuleScan:
    def __init__(self, path, source):
        self.path = path
        self.tree = ast.parse(source)
        self.constants = {}
        for node in ast.walk(self.tree):
            if (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)
            ):
                self.constants[node.targets[0].id] = node.value.value

        # Everything reachable from a registry table's value expression.
        self.excluded = set()
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id in _REGISTRY_TABLES
                for target in node.targets
            ):
                for sub in ast.walk(node.value):
                    self.excluded.add(id(sub))

        # Local helpers whose every `return` is a resolvable code — `_wiring_code` is the
        # real instance: it picks between two codes by node kind, and both are independently
        # raised at literal sites, but the CALL is what a sink sees.
        self.code_returning = {}
        for node in ast.walk(self.tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            returns = [
                child.value
                for child in ast.walk(node)
                if isinstance(child, ast.Return) and child.value is not None
            ]
            resolved = [self._simple(value) for value in returns]
            if returns and all(item is not None for item in resolved):
                self.code_returning[node.name] = tuple(resolved)

        # One-hop wrappers: `def w(code, ...)` forwarding its first parameter into a sink.
        # `lineage._report` is the real instance. The wrapper's own body is a FORWARD, not
        # an emission; its call sites carry the codes, so the wrapper joins the sink set.
        self.sinks = set(_SINK_NAMES)
        for _pass in range(3):
            for node in ast.walk(self.tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if not node.args.args:
                    continue
                first = node.args.args[0].arg
                for child in ast.walk(node):
                    if (
                        isinstance(child, ast.Call)
                        and _called_name(child) in self.sinks
                        and child.args
                        and isinstance(child.args[0], ast.Name)
                        and child.args[0].id == first
                    ):
                        self.sinks.add(node.name)

        # Which function ENCLOSES each call, innermost first. A module-wide set of every
        # parameter name was the first cut and it is fail-open: a local variable named
        # `code` inside a function that forwards nothing at all was skipped as though it
        # were a wrapper's forwarded parameter, so the call was neither resolved nor
        # reported. The skip is now scoped to the one function the call is actually in.
        self.enclosing = {}
        self._map_enclosing(self.tree, None)

    def _map_enclosing(self, node, current):
        """Record the innermost enclosing function for every Call in the tree."""
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self._map_enclosing(child, child)
                continue
            if isinstance(child, ast.Call):
                self.enclosing[id(child)] = current
            self._map_enclosing(child, current)

    def forwarded_parameter(self, call, argument):
        """`(owner, index, name)` when the code argument is the enclosing function's own parameter.

        The first version required the FIRST parameter, which silently mis-handled
        `_check_region_containment(..., code=..., message=...)`: its code parameter sits in
        sixth position and carries a DEFAULT, and two of its three call sites omit it — so
        the code that default names was emitted by a path the reader could not see.

        Any parameter position now counts, and `resolve_forward` below reads both the
        default and the actual arguments at that function's call sites, so the emission is
        accounted for instead of pinned as unreadable.
        """
        if not isinstance(argument, ast.Name):
            return None
        owner = self.enclosing.get(id(call))
        if owner is None:
            return None
        for index, arg in enumerate(owner.args.args):
            if arg.arg == argument.id:
                return (owner, index, arg.arg)
        return None

    def resolve_forward(self, forward):
        """Every code that can reach a forwarded parameter, or None if any path is opaque.

        Two sources, both closed: the parameter's DEFAULT, and the argument supplied at each
        call site of the owning function (positional or keyword). A call site this cannot
        read makes the whole forward unresolved, so an opaque path is reported rather than
        assumed empty.
        """
        owner, index, param = forward
        codes = set()

        defaults = owner.args.defaults
        offset = len(owner.args.args) - len(defaults)
        if defaults and index >= offset:
            resolved = self._simple(defaults[index - offset])
            if resolved is None:
                return None
            codes.add(resolved)

        seen_call = False
        for node in ast.walk(self.tree):
            if not isinstance(node, ast.Call) or _called_name(node) != owner.name:
                continue
            seen_call = True
            supplied = None
            if len(node.args) > index:
                supplied = node.args[index]
            else:
                for kw in node.keywords:
                    if kw.arg == param:
                        supplied = kw.value
                        break
            if supplied is None:
                continue  # omitted -> the default above covers it
            resolved = self._simple(supplied)
            if resolved is None:
                return None
            codes.add(resolved)

        if not seen_call and not codes:
            return None
        return tuple(sorted(codes))

    def _simple(self, node):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name):
            if node.id in _ERROR_CONSTANTS:
                return _ERROR_CONSTANTS[node.id]
            if node.id in self.constants:
                return self.constants[node.id]
        return None

    def resolve(self, node):
        """Every code this expression can supply, or `None` if it cannot be resolved."""
        simple = self._simple(node)
        if simple is not None:
            return (simple,)
        if isinstance(node, ast.IfExp):
            left = self.resolve(node.body)
            right = self.resolve(node.orelse)
            if left is not None and right is not None:
                return tuple(left) + tuple(right)
            return None
        if isinstance(node, ast.Call):
            name = _called_name(node)
            if name in self.code_returning:
                return self.code_returning[name]
        return None

    def routing_map_codes(self):
        """Values of dict literals that map something onto a diagnostic code.

        `_CUSTOM_ERROR_CODES` is the real instance: the parser routes a pydantic
        custom-error tag to a code, and those codes are emitted through pydantic rather
        than through a direct sink call. A map whose values are codes exists to SELECT a
        code to raise.
        """
        codes = set()
        for node in ast.walk(self.tree):
            if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Dict):
                continue
            if id(node.value) in self.excluded:
                continue
            values = node.value.values
            if values and all(
                isinstance(value, ast.Name) and value.id in _ERROR_CONSTANTS
                for value in values
            ):
                codes.update(_ERROR_CONSTANTS[value.id] for value in values)
        return codes


def verifier_issue_sites():
    """Every `_issue(...)` call in the graph verifier, with its literal shape.

    Returns `(relative_path, lineno, code, has_literal_message, has_literal_remediation)`.
    The verifier serves its own result dict directly rather than going through a
    registry, so "is this code registered" is the wrong question for it — the
    checkable property is that each call supplies a literal code and a message and
    remediation with a non-empty literal skeleton (an f-string interpolating a shape
    id still has literal text around the placeholder; a bare variable does not).
    """
    path = _ROOT / "src/boomi_mcp/categories/components/process_graph_verifier.py"
    tree = ast.parse(path.read_text())
    relative = str(path.relative_to(_ROOT))
    sites = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or _called_name(node) != "_issue":
            continue
        if len(node.args) < 5:
            sites.append((relative, node.lineno, None, False, False))
            continue
        code = node.args[0]
        code_value = (
            code.value
            if isinstance(code, ast.Constant) and isinstance(code.value, str)
            else (_ERROR_CONSTANTS.get(code.id) if isinstance(code, ast.Name) else None)
        )
        sites.append(
            (
                relative,
                node.lineno,
                code_value,
                _has_literal_text(node.args[3]),
                _has_literal_text(node.args[4]),
            )
        )
    return tuple(sorted(sites, key=lambda row: row[1]))


def _has_literal_text(node):
    """True when the expression carries non-empty literal text of its own."""
    if isinstance(node, ast.Constant):
        return isinstance(node.value, str) and bool(node.value.strip())
    if isinstance(node, ast.JoinedStr):
        return any(
            isinstance(part, ast.Constant)
            and isinstance(part.value, str)
            and part.value.strip()
            for part in node.values
        )
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return _has_literal_text(node.left) or _has_literal_text(node.right)
    if isinstance(node, ast.Call) and _called_name(node) == "format":
        return _has_literal_text(node.func.value) if isinstance(node.func, ast.Attribute) else False
    return False


def _called_name(call):
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    return getattr(func, "attr", None)


def collect_emissions():
    """`(by_producer, unresolved_sites)` read from `EMISSION_ROOTS`.

    `by_producer` maps `parser`/`compiler`/`semantic`/`verifier` to the frozen set of codes
    that layer can raise. `unresolved_sites` is a sorted tuple of
    `(relative_path, lineno, sink, reason)` for every sink call whose code could not be
    resolved — never dropped, so the caller can pin them as a closed table.
    """
    by_producer = {}
    unresolved = []

    for path in _iter_files():
        relative = str(path.relative_to(_ROOT))
        scan = _ModuleScan(path, path.read_text())
        producer = _producer(relative)
        bucket = by_producer.setdefault(producer, set())
        bucket.update(scan.routing_map_codes())

        for node in ast.walk(scan.tree):
            if not isinstance(node, ast.Call):
                continue
            name = _called_name(node)
            if name not in scan.sinks or not node.args:
                continue
            argument = node.args[0]
            if id(argument) in scan.excluded:
                continue

            resolved = scan.resolve(argument)
            if resolved is not None:
                bucket.update(resolved)
                continue

            # A call forwarding the ENCLOSING function's own parameter is a wrapper body,
            # not an emission of its own. Where the wrapper is itself a registered sink its
            # call sites are already scanned; otherwise the codes are resolved here from the
            # parameter's default and the owning function's call sites. Only a forward whose
            # sources cannot all be read falls through to the unresolved table.
            forward = scan.forwarded_parameter(node, argument)
            if forward is not None:
                if forward[0].name in scan.sinks and forward[1] == 0:
                    continue
                forwarded = scan.resolve_forward(forward)
                if forwarded is not None:
                    bucket.update(forwarded)
                    continue

            # The COMPLETE dump. Truncating it to 80 characters made two long
            # expressions sharing a prefix collide in the guard's site key, which is the
            # same fail-open shape — an identity coarser than the property it pins — that
            # the pinned-site table itself was introduced to fix.
            unresolved.append((relative, node.lineno, name, ast.dump(argument)))

    return (
        MappingProxyType(
            {key: frozenset(value) for key, value in sorted(by_producer.items())}
        ),
        tuple(sorted(unresolved)),
    )
