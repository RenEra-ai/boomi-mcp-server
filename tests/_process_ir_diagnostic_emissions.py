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
routing-map value, conditional between two resolvable codes, and a local helper whose every
return is a resolvable code — and everything else is a pinned site with a stated reason.

A revision of this module DID try to resolve codes reaching a forwarded parameter, by
reading that parameter's default and the owner's call sites. It was wrong in four
consecutive review rounds — unpacked arguments, a rebound parameter, an unreachable
default, an aliased call, and bindings (`case x`, `import ... as x`, a nested `def x`) that
carry no `Name(Store)` node at all. Each fix was correct and the next round found another
form, because Python's binding and call syntax has no closed case set — the same shape as
this repo's prose scanner, which was wrong four rounds running for the same reason. The
resolver is GONE rather than extended: such a forward is now reported unresolved, and the
one site that has one is pinned with the authority for its codes stated by a human and
asserted against the live registry.
"""

from __future__ import annotations

import ast
import pathlib
import re
import sys
from types import MappingProxyType

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_SRC = _ROOT / "src"
for _p in (str(_ROOT), str(_SRC)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp import errors as _errors  # noqa: E402

__all__ = [
    "DIAGNOSTIC_CONSTRUCTORS",
    "EMISSION_ROOTS",
    "PINNED_SINKS",
    "TABLE_FACTORIES",
    "capability_citations",
    "collect_emissions",
    "compiler_translated_codes",
    "forward_owner_codes",
    "internal_constant_names",
    "pinned_sink_definitions",
    "producer_of",
    "raising_methods",
    "referenced_codes",
    "routed_emissions",
    "table_factory_reads",
    "table_routes",
    "translation_sites",
    "unresolvable_forward_arguments",
    "runtime_forward_defaults",
    "verifier_issue_call_count",
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

#: Diagnostic MODELS that may be constructed directly. Their constructors are sinks too:
#: `pipeline._compile_error_from_validation` builds a `CompilerDiagnostic` itself rather than
#: going through `diagnostic()`, and that site was invisible while only the factory
#: FUNCTIONS were pinned.
DIAGNOSTIC_CONSTRUCTORS = (
    "CompilerDiagnostic",
    "ProcessIRDiagnostic",
    "ValidationDiagnosticV1",
)

_SINK_NAMES = frozenset(
    [name for _path, name in PINNED_SINKS] + list(DIAGNOSTIC_CONSTRUCTORS)
)

#: Registry tables whose contents describe codes rather than raise them. Their VALUES are
#: excluded from resolution so a registry cannot prove its own reachability; their KEYS are
#: never collected at all, which is the same rule stated from the other side.
_REGISTRY_TABLES = frozenset({"_MESSAGES", "_REMEDIATION"})

#: What a diagnostic code looks like: SCREAMING_SNAKE with at least two segments.
_CODE_SHAPE = re.compile(r"^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+){2,}$")

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


def producer_of(relative):
    """Which layer a scanned path belongs to — the census needs it per REFERENCE.

    Exported because a code named in a compiler module must be checked against the
    COMPILER's own table: checking it against the merged union recreates the cross-layer
    masking this whole file exists to prevent.
    """
    return _producer(relative)


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
        rebound = set()
        for node in ast.walk(self.tree):
            if not isinstance(node, ast.Assign) or len(node.targets) != 1:
                continue
            target = node.targets[0]
            if not isinstance(target, ast.Name):
                continue
            if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                if target.id in self.constants and self.constants[target.id] != node.value.value:
                    rebound.add(target.id)
                self.constants.setdefault(target.id, node.value.value)
            else:
                # Assigned to something that is NOT a plain string: the name is no longer a
                # constant anywhere the reader can trust. Resolving it from an earlier literal
                # would report a code the runtime never emits — and hide the one it does.
                rebound.add(target.id)
        for name in rebound:
            self.constants.pop(name, None)

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
        # ALIASES are sinks. `d = diagnostic` then `d(<assembled code>, ...)` reached the
        # canonical factory while the reader matched only the original name — a rename away
        # from the whole scan. Resolved to a fixpoint so an alias of an alias counts too.
        while True:
            before = len(self.sinks)
            for node in ast.walk(self.tree):
                if (
                    isinstance(node, ast.Assign)
                    and isinstance(node.value, ast.Name)
                    and node.value.id in self.sinks
                ):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            self.sinks.add(target.id)
                elif (
                    isinstance(node, ast.AnnAssign)
                    and isinstance(node.value, ast.Name)
                    and node.value.id in self.sinks
                    and isinstance(node.target, ast.Name)
                ):
                    self.sinks.add(node.target.id)
            if len(self.sinks) == before:
                break
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

        This reports the SHAPE only. It deliberately does not try to say which codes can
        reach that parameter — see the module docstring: an earlier revision did, and four
        consecutive review rounds each found a further Python form it read wrongly
        (unpacked arguments, a rebound parameter, an unreachable default, an aliased call,
        bindings that carry no `Name(Store)` node at all). Python's binding and call syntax
        is not a closed set, so a reader that resolves it cannot make the coverage claim
        the structural-fix rule requires. A forward whose owner is not itself a
        first-parameter sink is therefore reported UNRESOLVED, and lands in the caller's
        pinned table where a human states the authority.
        """
        if not isinstance(argument, ast.Name):
            return None
        owner = self.enclosing.get(id(call))
        if owner is None:
            return None
        # EVERY parameter kind. Reading only `owner.args.args` skipped keyword-only and
        # positional-only parameters, so refactoring a forwarding helper to
        # `def helper(*, code=...)` removed it from this reader without changing the pinned
        # inner call's identity. Python's parameter kinds are a CLOSED set of three —
        # unlike the call and binding syntax this module deliberately refuses to model —
        # so enumerating them converges.
        for index, arg in enumerate(
            list(owner.args.posonlyargs) + list(owner.args.args) + list(owner.args.kwonlyargs)
        ):
            if arg.arg == argument.id:
                return (owner, index, arg.arg)
        return None

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


def verifier_issue_call_count():
    """How many calls the verifier makes to its diagnostic sink, by ANY binding.

    `verifier_issue_sites()` matches the name `_issue`. Counting calls to whatever that name
    is currently bound to — including aliases — gives the case set an independent size, so
    moving calls onto an alias shrinks the sites list while this count stays put and the
    comparison fails.
    """
    path = _ROOT / "src/boomi_mcp/categories/components/process_graph_verifier.py"
    tree = ast.parse(path.read_text())
    aliases = {"_issue"}
    # TRUE fixpoint, not a fixed pass count. `ast.walk` yields assignments in tree order,
    # which need not be dependency order, so a long alias chain could be discovered one link
    # per pass and a fixed three passes would stop short — leaving the count and the sites
    # list BOTH at one and the equality quiet.
    while True:
        before = len(aliases)
        for node in ast.walk(tree):
            # Plain AND annotated bindings: `issue: Callable = _issue` is an `AnnAssign`, so
            # an alias declared with a type hint was invisible and the count collapsed to the
            # one remaining direct call while the sites list did the same — both sides moving
            # together is precisely how an equality goes quiet.
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Name):
                if node.value.id in aliases:
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            aliases.add(target.id)
            elif (
                isinstance(node, ast.AnnAssign)
                and isinstance(node.value, ast.Name)
                and node.value.id in aliases
                and isinstance(node.target, ast.Name)
            ):
                aliases.add(node.target.id)
        if len(aliases) == before:
            break
    return sum(
        1
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and _called_name(node) in aliases
    )


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


def _has_default(owner, param):
    """Does `param` carry a default in `owner`'s signature? Read from the AST.

    Positional defaults align to the TAIL of `posonlyargs + args`; keyword-only defaults
    align element-wise with `kwonlyargs` and may be `None` for a required one.
    """
    positional = list(owner.args.posonlyargs) + list(owner.args.args)
    defaults = list(owner.args.defaults)
    offset = len(positional) - len(defaults)
    for index, arg in enumerate(positional):
        if arg.arg == param:
            return index >= offset
    for arg, default in zip(owner.args.kwonlyargs, owner.args.kw_defaults):
        if arg.arg == param:
            return default is not None
    return False


def _called_name(call):
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    return getattr(func, "attr", None)


def referenced_codes():
    """Every diagnostic-code constant NAMED anywhere in the scanned modules.

    This is the claim that makes a pinned site safe without reading data flow. A pinned
    site says "a human states which codes reach here"; that statement can go stale when the
    code behind it changes. But a code the modules can emit has to be NAMED in them — as a
    `boomi_mcp.errors` constant or a literal — so requiring every named code to be served
    catches the stale case without anyone tracing a value.

    It is an over-approximation (a code named for another purpose is still required to be
    served), which is the safe direction: it can demand a registration that was not strictly
    needed, never miss one that was.
    """
    codes = set(_ERROR_CONSTANTS.values())
    # Code-SHAPED literals count too, not only strings that already match a known constant.
    # Measured: with only known constants, changing a diagnostic default to a brand-new
    # literal was invisible to this census — the very staleness it exists to catch. The
    # families are DERIVED from the real code set (the first two underscore-separated
    # tokens of each), never hand-typed, so a new family cannot appear without a code in it.
    families = {"_".join(code.split("_")[:2]) for code in codes}

    def _is_code_shaped(value):
        if not _CODE_SHAPE.match(value):
            return False
        return "_".join(value.split("_")[:2]) in families

    referenced = {}
    for path in _iter_files():
        relative = str(path.relative_to(_ROOT))
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and node.id in _ERROR_CONSTANTS:
                referenced.setdefault(_ERROR_CONSTANTS[node.id], set()).add(relative)
            elif (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and (node.value in codes or _is_code_shaped(node.value))
            ):
                referenced.setdefault(node.value, set()).add(relative)
    return MappingProxyType(
        {code: frozenset(paths) for code, paths in sorted(referenced.items())}
    )


def runtime_forward_defaults():
    """Runtime default VALUES of every parameter that a sink call forwards.

    The census reads source, so it sees a code only when it is written as a whole literal
    or a known constant. A default written as `"PROCESS_IR_" + "SEMANTIC_..."` is neither,
    and it produced a genuinely emittable unregistered code that every source-reading guard
    missed — demonstrated by the architect review.

    Chasing that in source means reading concatenation, f-strings, `.format`, `.join` and
    whatever comes next: the open-ended space that already cost four review rounds. So this
    does not read the expression at all. It IMPORTS the module and asks Python for the
    evaluated default. However the author wrote it, the value is the value.

    Returns `(defaults, unreadable)`: `defaults` maps `(module, function, parameter)` to the
    runtime string default; `unreadable` lists forwards whose owner cannot be introspected
    (a nested function is not reachable through `getattr`), so the caller can require them
    to be pinned rather than assume they are empty.
    """
    import importlib
    import inspect

    defaults = {}
    unreadable = []
    for path in _iter_files():
        relative = str(path.relative_to(_ROOT))
        scan = _ModuleScan(path, path.read_text())
        dotted = relative[len("src/") :].removesuffix(".py").replace("/", ".")
        module = None
        for node in ast.walk(scan.tree):
            if not isinstance(node, ast.Call) or _called_name(node) not in scan.sinks:
                continue
            # Positional OR `code=` keyword — the same two forms the emission scan reads. A
            # positional-only scan skipped keyword-form factories such as
            # `diagnostic` -> `CompilerDiagnostic(code=code)`, so an assembled default on one
            # of them would have been invisible to this check as well as to the source census.
            argument = node.args[0] if node.args else None
            if argument is None:
                for keyword in node.keywords:
                    if keyword.arg == "code":
                        argument = keyword.value
                        break
            if argument is None:
                continue
            forward = scan.forwarded_parameter(node, argument)
            if forward is None:
                continue
            owner, _index, param = forward
            if module is None:
                module = importlib.import_module(dotted)
            function = getattr(module, owner.name, None)
            if function is None or not callable(function):
                # Not introspectable (a closure is not reachable through `getattr`), so the
                # AST is asked the one question that still has a definite answer: does this
                # parameter carry a DEFAULT at all? A pinned disposition saying "there is no
                # default to read" is then a checked fact rather than prose — without it,
                # the owner could gain a constructed default and this tuple would not move.
                unreadable.append(
                    (relative, owner.name, param, _has_default(owner, param))
                )
                continue
            try:
                default = inspect.signature(function).parameters[param].default
            except (TypeError, ValueError, KeyError):
                unreadable.append(
                    (relative, owner.name, param, _has_default(owner, param))
                )
                continue
            if isinstance(default, str) and default:
                defaults[(relative, owner.name, param)] = default
    return MappingProxyType(defaults), tuple(sorted(unreadable))


def unresolvable_forward_arguments():
    """Call sites that hand a pinned forwarding owner a code it cannot read.

    A forwarding owner's code can arrive two ways: as its DEFAULT, which
    `runtime_forward_defaults()` reads as an evaluated value, or EXPLICITLY at a call site.
    An explicit argument built at runtime (`"PROCESS_IR_" + "..."`, an f-string, `.format`)
    cannot be read from source, and evaluating it would mean modelling the open-ended space
    this module refuses to model.

    So it is BANNED rather than evaluated: at these few sites the code must be a plain
    constant. That is a closed requirement — a constructed argument fails here and its
    author must either write a constant or justify a new pin — and it costs nothing, because
    every real site already passes one.
    """
    offenders = []
    for path in _iter_files():
        relative = str(path.relative_to(_ROOT))
        scan = _ModuleScan(path, path.read_text())
        owners = {}
        for node in ast.walk(scan.tree):
            if not isinstance(node, ast.Call) or _called_name(node) not in scan.sinks:
                continue
            argument = node.args[0] if node.args else None
            if argument is None:
                for keyword in node.keywords:
                    if keyword.arg == "code":
                        argument = keyword.value
                        break
            if argument is None:
                continue
            forward = scan.forwarded_parameter(node, argument)
            if forward is not None:
                owner, index, param = forward
                owners[owner.name] = (index, param)

        for node in ast.walk(scan.tree):
            if not isinstance(node, ast.Call):
                continue
            name = _called_name(node)
            if name not in owners:
                continue
            index, param = owners[name]
            # Unpacking makes position and keyword identity unknowable, so such a call is an
            # OFFENDER rather than an omission — treating it as omitted silently fell back to
            # the default while `**{"code": <assembled>}` supplied something else entirely.
            if any(isinstance(a, ast.Starred) for a in node.args) or any(
                k.arg is None for k in node.keywords
            ):
                offenders.append((relative, node.lineno, name, "<unpacked arguments>"))
                continue
            supplied = node.args[index] if len(node.args) > index else None
            if supplied is None:
                for keyword in node.keywords:
                    if keyword.arg == param:
                        supplied = keyword.value
                        break
            if supplied is None:
                continue  # omitted -> the default is read at runtime instead
            # A body that FORWARDS its own parameter onward is not a call site supplying a
            # code; it is another link in the same chain, and its own callers are checked.
            if scan.forwarded_parameter(node, supplied) is not None:
                continue
            # `resolve` — not `_simple` — so the closed forms this module already reads
            # (a conditional between two codes, a helper whose every return is a code)
            # stay legal. Only genuinely unreadable expressions are banned.
            if scan.resolve(supplied) is None:
                offenders.append((relative, node.lineno, name, ast.dump(supplied)))
    return tuple(sorted(offenders))


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
            if name not in scan.sinks:
                continue
            # The code may be the first POSITIONAL argument or the `code=` KEYWORD. Reading
            # only positionals made every keyword-form call invisible — including the direct
            # `CompilerDiagnostic(code=..., ...)` construction in `pipeline.py`, which is a
            # real emission the scan silently skipped.
            argument = node.args[0] if node.args else None
            if argument is None:
                for keyword in node.keywords:
                    if keyword.arg == "code":
                        argument = keyword.value
                        break
            if argument is None:
                continue
            if id(argument) in scan.excluded:
                continue

            resolved = scan.resolve(argument)
            if resolved is not None:
                bucket.update(resolved)
                continue

            # NOTHING is skipped. An earlier revision skipped a sink's own definition body
            # by matching the argument's NAME to the sink's first parameter, which is
            # fail-open the moment that parameter is rebound (`code = choose()`), and
            # deciding "is this name rebound" means enumerating Python's binding forms —
            # the same open-ended space that cost four review rounds. So every forward
            # falls through to the unresolved table, and the handful of sink definitions
            # are pinned there with a one-line reason. The reader models no Python
            # semantics at all; the price is a longer pinned table, which is a price paid
            # in review rather than in silent coverage loss.

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



# ---------------------------------------------------------------------------
# SELF-184-37: the gates each code's raisers cite, and the codes the compiler
# serves by translating a shared model rule
# ---------------------------------------------------------------------------

#: The producers whose raise sites take their remediation from a served table. The graph
#: verifier serves its own text inline (`verifier_issue_sites`), so it is not one of them.
_TABLE_PRODUCERS = ("parser", "compiler", "semantic")


def _docstring_ids(tree):
    ids = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                ids.add(id(body[0].value))
    return ids


def _keyword(call, name):
    for keyword in call.keywords:
        if keyword.arg == name:
            return keyword.value
    return None


def _argument(call, index, name):
    if index is not None and len(call.args) > index:
        return call.args[index]
    return _keyword(call, name)


def _positional_index(function, name):
    positional = list(function.args.posonlyargs) + list(function.args.args)
    for index, arg in enumerate(positional):
        if arg.arg == name:
            return index
    return None


def _reason_test(test, reason):
    """The constant NAME in `reason == NAME`, else None."""
    if (
        isinstance(test, ast.Compare)
        and isinstance(test.left, ast.Name)
        and test.left.id == reason
        and len(test.ops) == 1
        and isinstance(test.ops[0], ast.Eq)
        and isinstance(test.comparators[0], ast.Name)
    ):
        return test.comparators[0].id
    return None


def _statement_blocks(function):
    """Every statement list inside `function`, its own body included."""
    for node in ast.walk(function):
        for field in ("body", "orelse", "finalbody"):
            block = getattr(node, field, None)
            if isinstance(block, list) and block and isinstance(block[0], ast.stmt):
                yield block
        for handler in getattr(node, "handlers", ()) or ():
            yield handler.body


class _SourceIndex:
    """One read of the table-serving producers, shared by the two SELF-184-37 readers.

    `capability_citations` and `compiler_translated_codes` must agree on what a raise site,
    a verdict and a verdict's renderer are, so both read them here. Every form is a closed
    one, in the stance of `collect_emissions`, and a shape outside them is reported by the
    reader using it rather than guessed:

    * a RAISE SITE is a pinned sink or one-hop wrapper (code resolved as `collect_emissions`
      resolves it), a `PydanticCustomError` whose tag the parser's `_CUSTOM_ERROR_CODES`
      routes, or a local helper returning such an error built from one of its own
      parameters (`_body_kind_error`);
    * a VERDICT is a function returning `(REASON, at, message)` with `REASON` a module
      constant, or one returning another verdict's result unchanged;
    * a RENDERER unpacks a verdict into three names and answers with the statements that
      follow, up to its first plain `raise`: an `if reason == REASON: raise ...` arm, a
      conditional code `A if reason == REASON else B`, or that final `raise`. Reasons are
      compared by VALUE through `ALIAS = NAME` chains, because the compiler's listener
      renderer tests `LISTENER_PLACEMENT_POSITION`, an alias of the
      `ENTRY_PLACEMENT_POSITION` the verdict returns.
    """

    def __init__(self):
        import importlib

        self.routing = importlib.import_module("boomi_mcp.models.process_ir")._CUSTOM_ERROR_CODES
        self.known_codes = set(_ERROR_CONSTANTS.values())
        self.modules = []
        for path in _iter_files():
            relative = str(path.relative_to(_ROOT))
            if _producer(relative) in _TABLE_PRODUCERS:
                self.modules.append((relative, _ModuleScan(path, path.read_text())))
        self.parser = next(r for r, _scan in self.modules if _producer(r) == "parser")

        # Module-level string constants as NODES (a message may name one defined in another
        # scanned module, and the node is what gets marked), and each constant's VALUE.
        self.module_constants = {}
        self.global_constants = {}
        self.functions = {}
        literal_values = {}
        aliases = {}
        for relative, scan in self.modules:
            own = {}
            for statement in scan.tree.body:
                if not (
                    isinstance(statement, ast.Assign)
                    and len(statement.targets) == 1
                    and isinstance(statement.targets[0], ast.Name)
                ):
                    continue
                name, value = statement.targets[0].id, statement.value
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    own[name] = value
                    self.global_constants.setdefault(name, []).append(value)
                    literal_values.setdefault(name, value.value)
                elif isinstance(value, ast.Name):
                    aliases.setdefault(name, value.id)
            self.module_constants[relative] = own
            self.functions[relative] = {
                node.name: node
                for node in ast.walk(scan.tree)
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
        self.values = dict(literal_values)
        for name in aliases:
            target, seen = name, set()
            while target in aliases and target not in seen:
                seen.add(target)
                target = aliases[target]
            if target in literal_values:
                self.values[name] = literal_values[target]

        self.helpers = self._helpers()
        self.sink_message = self._sink_message()
        self.verdicts, self.delegates_to = self._verdicts()
        self.renderers = self._renderers()

    def _helpers(self):
        """`{name: (code, parameter index, parameter name)}` for error-building helpers."""
        helpers = {}
        for relative, _scan in self.modules:
            for name, function in self.functions[relative].items():
                returns = [
                    node.value for node in ast.walk(function)
                    if isinstance(node, ast.Return) and node.value is not None
                ]
                shapes = set()
                for value in returns:
                    if not (
                        isinstance(value, ast.Call)
                        and _called_name(value) == "PydanticCustomError"
                        and len(value.args) >= 2
                        and isinstance(value.args[0], ast.Constant)
                        and value.args[0].value in self.routing
                        and isinstance(value.args[1], ast.Name)
                    ):
                        shapes = None
                        break
                    shapes.add((self.routing[value.args[0].value], value.args[1].id))
                if shapes and len(shapes) == 1:
                    code, parameter = next(iter(shapes))
                    index = _positional_index(function, parameter)
                    if index is not None or parameter in {a.arg for a in function.args.kwonlyargs}:
                        helpers[name] = (code, index, parameter)
        return helpers

    def _sink_message(self):
        """Where each sink takes its message, read from the sink's own signature."""
        found = {}
        for relative, scan in self.modules:
            for name, function in self.functions[relative].items():
                if name not in scan.sinks:
                    continue
                every = (
                    list(function.args.posonlyargs)
                    + list(function.args.args)
                    + list(function.args.kwonlyargs)
                )
                if any(arg.arg == "message" for arg in every):
                    found[name] = _positional_index(function, "message")
        return found

    def site(self, call, scan):
        """`(kind, codes, code expression, message expression)` for a raise site, else None.

        `kind` is `pydantic`, `helper` or `sink`.
        """
        name = _called_name(call)
        if name == "PydanticCustomError":
            if (
                len(call.args) >= 2
                and isinstance(call.args[0], ast.Constant)
                and call.args[0].value in self.routing
            ):
                return "pydantic", (self.routing[call.args[0].value],), None, call.args[1]
            return None
        if name in self.helpers:
            code, index, parameter = self.helpers[name]
            return "helper", (code,), None, _argument(call, index, parameter)
        if name in scan.sinks:
            expression = call.args[0] if call.args else _keyword(call, "code")
            if expression is None or id(expression) in scan.excluded:
                return None
            resolved = scan.resolve(expression) or ()
            codes = tuple(code for code in resolved if code in self.known_codes)
            return "sink", codes, expression, _argument(call, self.sink_message.get(name), "message")
        return None

    def texts(self, expression, scope, relative, seen=frozenset()):
        """The literal nodes a message expression is made of, over the closed forms only."""
        if expression is None:
            return []
        if isinstance(expression, ast.Constant):
            return [expression] if isinstance(expression.value, str) else []
        if isinstance(expression, ast.JoinedStr):
            return [part for part in expression.values if isinstance(part, ast.Constant)]
        if isinstance(expression, ast.BinOp) and isinstance(expression.op, ast.Add):
            return (self.texts(expression.left, scope, relative, seen)
                    + self.texts(expression.right, scope, relative, seen))
        if isinstance(expression, ast.IfExp):
            return (self.texts(expression.body, scope, relative, seen)
                    + self.texts(expression.orelse, scope, relative, seen))
        if isinstance(expression, ast.Call):
            if isinstance(expression.func, ast.Attribute) and expression.func.attr == "format":
                return self.texts(expression.func.value, scope, relative, seen)
            name = _called_name(expression)
            helper = (
                self.functions[relative].get(name)
                if isinstance(expression.func, ast.Name) else None
            )
            if helper is not None and ("call", name) not in seen:
                out = []
                for node in ast.walk(helper):
                    if isinstance(node, ast.Return) and node.value is not None:
                        out += self.texts(node.value, helper, relative, seen | {("call", name)})
                return out
            return []
        if isinstance(expression, ast.Name):
            name = expression.id
            if name in seen:
                return []
            if scope is not None:
                bound = [
                    node.value for node in ast.walk(scope)
                    if isinstance(node, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id == name for t in node.targets)
                ]
                if bound:
                    out = []
                    for value in bound:
                        out += self.texts(value, scope, relative, seen | {name})
                    return out
            if name in self.module_constants[relative]:
                return [self.module_constants[relative][name]]
            return list(self.global_constants.get(name, ()))
        return []

    def _verdicts(self):
        """`({name: (path, function, [(reason, message)])}, {delegator: {verdict}})`."""
        verdicts = {}
        for relative, _scan in self.modules:
            own = self.module_constants[relative]
            for name, function in self.functions[relative].items():
                rows = [
                    (node.value.elts[0].id, node.value.elts[2])
                    for node in ast.walk(function)
                    if isinstance(node, ast.Return)
                    and isinstance(node.value, ast.Tuple)
                    and len(node.value.elts) == 3
                    and isinstance(node.value.elts[0], ast.Name)
                    and node.value.elts[0].id in own
                ]
                if rows:
                    verdicts[name] = (relative, function, rows)
        delegates_to = {}
        for relative, _scan in self.modules:
            for name, function in self.functions[relative].items():
                for node in ast.walk(function):
                    if not (isinstance(node, ast.Return) and isinstance(node.value, ast.Name)):
                        continue
                    for bound in ast.walk(function):
                        if (
                            isinstance(bound, ast.Assign)
                            and any(isinstance(t, ast.Name) and t.id == node.value.id
                                    for t in bound.targets)
                            and isinstance(bound.value, ast.Call)
                            and _called_name(bound.value) in verdicts
                        ):
                            delegates_to.setdefault(name, set()).add(_called_name(bound.value))
        return verdicts, delegates_to

    def verdict_of(self, value, function):
        """The verdict an unpacked value comes from, or None."""
        if isinstance(value, ast.Call) and _called_name(value) in self.verdicts:
            return _called_name(value)
        if isinstance(value, ast.Name):
            sources = {
                _called_name(call)
                for bound in ast.walk(function)
                if isinstance(bound, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == value.id for t in bound.targets)
                for call in ast.walk(bound.value)
                if isinstance(call, ast.Call) and _called_name(call) in self.verdicts
            }
            if len(sources) == 1:
                return next(iter(sources))
        return None

    def _renderers(self):
        """`{verdict: [(path, scan, function, reason name, following statements)]}`."""
        renderers = {}
        for relative, scan in self.modules:
            for name, function in self.functions[relative].items():
                for block in _statement_blocks(function):
                    for index, statement in enumerate(block):
                        if not (
                            isinstance(statement, ast.Assign)
                            and len(statement.targets) == 1
                            and isinstance(statement.targets[0], ast.Tuple)
                            and len(statement.targets[0].elts) == 3
                            and all(isinstance(e, ast.Name) for e in statement.targets[0].elts)
                        ):
                            continue
                        verdict = self.verdict_of(statement.value, function)
                        if verdict is None:
                            continue
                        row = (relative, scan, name, statement.targets[0].elts[0].id,
                               block[index + 1:])
                        if all(row[:4] != other[:4] for other in renderers.get(verdict, ())):
                            renderers.setdefault(verdict, []).append(row)
        return renderers

    def renderers_of(self, verdict, seen=frozenset()):
        """Every renderer of `verdict`, including those of a verdict that delegates to it."""
        found = list(self.renderers.get(verdict, ()))
        for delegator in sorted(n for n, targets in self.delegates_to.items() if verdict in targets):
            if delegator not in seen:
                found += self.renderers_of(delegator, seen | {verdict})
        return found

    def reasons_of(self, verdict, seen=frozenset()):
        """Every reason `verdict` can return, its delegates' reasons included."""
        reasons = {constant for constant, _message in self.verdicts.get(verdict, (0, 0, ()))[2]}
        for delegate in self.delegates_to.get(verdict, ()):
            if delegate not in seen:
                reasons |= self.reasons_of(delegate, seen | {verdict})
        return reasons

    def _same(self, left, right):
        return self.values.get(left, left) == self.values.get(right, right)

    @staticmethod
    def render_block(following):
        """The statements a renderer answers with: up to and including its first plain `raise`."""
        block = []
        for statement in following:
            block.append(statement)
            if isinstance(statement, ast.Raise):
                break
        return block

    def raised_codes(self, statement, scan, reason, constant):
        call = statement.exc if isinstance(statement, ast.Raise) else None
        if not isinstance(call, ast.Call):
            return ()
        found = self.site(call, scan)
        if found is None:
            return ()
        _kind, codes, expression, _message = found
        if isinstance(expression, ast.IfExp):
            guard = _reason_test(expression.test, reason)
            if guard is not None:
                branch = expression.body if self._same(guard, constant) else expression.orelse
                return tuple(c for c in (scan.resolve(branch) or ()) if c in self.known_codes)
        return codes

    def rendered_codes(self, following, scan, reason, constant):
        """The codes one renderer serves for `constant`, or () when it cannot be read."""
        explicit = []
        for statement in self.render_block(following):
            guard = _reason_test(statement.test, reason) if isinstance(statement, ast.If) else None
            if guard is not None:
                arm = statement.body if self._same(guard, constant) else statement.orelse
                for node in arm:
                    for raised in ast.walk(node):
                        if isinstance(raised, ast.Raise):
                            explicit += self.raised_codes(raised, scan, reason, constant)
                continue
            if isinstance(statement, ast.Raise):
                return tuple(explicit) or self.raised_codes(statement, scan, reason, constant)
        return tuple(explicit)

    def translators(self):
        """`{name: parameter index}` for functions outside the parser that call one of their own
        parameters inside a `try` and, on `PydanticCustomError`, raise through a sink
        (`body_capabilities._as_compile_error`). Found by shape, never by name."""
        found = {}
        for relative, scan in self.modules:
            if _producer(relative) == "parser":
                continue
            for name, function in self.functions[relative].items():
                parameters = [a.arg for a in list(function.args.posonlyargs) + list(function.args.args)]
                for node in ast.walk(function):
                    if not isinstance(node, ast.Try):
                        continue
                    handlers = [
                        h for h in node.handlers
                        if isinstance(h.type, ast.Name) and h.type.id == "PydanticCustomError"
                    ]
                    if not any(
                        isinstance(c, ast.Call) and _called_name(c) in scan.sinks
                        for h in handlers for c in ast.walk(h)
                    ):
                        continue
                    called = {
                        c.func.id
                        for statement in node.body for c in ast.walk(statement)
                        if isinstance(c, ast.Call) and isinstance(c.func, ast.Name)
                        and c.func.id in parameters
                    }
                    if len(called) == 1:
                        found[name] = parameters.index(next(iter(called)))
        return found

    def model_rule_codes(self, name):
        """Every code a parser function can raise as a `PydanticCustomError`, through the
        parser functions it calls by name. Over-approximate: a code it MAY raise counts."""
        functions = self.functions[self.parser]
        scan = dict(self.modules)[self.parser]
        codes, stack, seen = set(), [name], set()
        while stack:
            current = stack.pop()
            if current in seen or current not in functions:
                continue
            seen.add(current)
            for node in ast.walk(functions[current]):
                if not isinstance(node, ast.Call):
                    continue
                if isinstance(node.func, ast.Name) and node.func.id in functions:
                    stack.append(node.func.id)
                found = self.site(node, scan)
                if found is not None and found[0] in ("pydantic", "helper"):
                    codes.update(found[1])
        return codes


def capability_citations(capabilities):
    """`(pairs, unassociated)`: the capability names each code's raise sites cite, from source.

    SELF-184-37. `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` is raised by two rules, slot
    admission and the `process_call_connector_mixing` gate, and served one remediation that
    described only the first. A mixing refusal told its author to use a kind the slot admits,
    about a kind the slot does admit. The defect is a remediation that omits a gate its own
    raisers cite, and that is readable from source without running anything: a raise site's
    message names the gate, and the served remediation for its code does not.

    `capabilities` is the served capability table's key set. The caller passes it, so this
    module keeps no copy. A literal CITES a capability when the name appears in it as a whole
    token.

    Two closed forms place a literal at a raise site (`_SourceIndex` defines them):

    1. the literal is part of the MESSAGE argument of a raise site;
    2. the literal is the message of a VERDICT, rendered by each function that unpacks the
       verdict under the code its reason selects.

    Message text is read through literals, f-strings, `+`, `.format()`, conditionals, module
    constants (across the scanned modules), local assignments and one-hop local helpers that
    return text. A capability-citing literal the reader cannot place is returned in
    `unassociated` rather than dropped, and so is a renderer it cannot read. The caller pins
    that set whole.

    Returns:

    * `pairs`: `{code: {capability: frozenset({(path, lineno), ...})}}`;
    * `unassociated`: sorted `(path, lineno, text)` rows. Docstrings, the served text tables
      (`_MESSAGES`/`_REMEDIATION`), and a literal that IS a capability name (the table's own
      keys) are not message text and are never collected.
    """
    names = sorted(capabilities, key=len, reverse=True)
    assert names, "no capability names given; the census would be vacuous"
    token = re.compile(
        r"(?<![A-Za-z0-9_])(" + "|".join(re.escape(name) for name in names) + r")(?![A-Za-z0-9_])"
    )
    index = _SourceIndex()
    associated = {}

    def associate(nodes, codes, relative):
        for node in nodes:
            if token.search(node.value):
                associated.setdefault(id(node), (relative, node, set()))[2].update(codes)

    # Form 1: the message argument of a raise site.
    for relative, scan in index.modules:
        for node in ast.walk(scan.tree):
            if not isinstance(node, ast.Call):
                continue
            found = index.site(node, scan)
            if found is None:
                continue
            _kind, codes, _expression, message = found
            if codes and message is not None:
                associate(index.texts(message, scan.enclosing.get(id(node)), relative),
                          codes, relative)

    # Form 2: a verdict's message, rendered under the code its reason selects.
    unreadable = []
    for verdict, (relative, function, rows) in sorted(index.verdicts.items()):
        for constant, message in rows:
            nodes = [n for n in index.texts(message, function, relative) if token.search(n.value)]
            if not nodes:
                continue
            for r_relative, r_scan, r_name, reason, following in index.renderers_of(verdict):
                codes = index.rendered_codes(following, r_scan, reason, constant)
                if not codes:
                    unreadable.append((r_relative, 0, "renderer {0} of {1} for {2}".format(
                        r_name, verdict, constant)))
                    continue
                associate(nodes, codes, relative)

    pairs = {}
    for relative, node, codes in associated.values():
        for code in codes:
            for capability in token.findall(node.value):
                pairs.setdefault(code, {}).setdefault(capability, set()).add(
                    (relative, node.lineno))

    unassociated = list(unreadable)
    for relative, scan in index.modules:
        docstrings = _docstring_ids(scan.tree)
        for node in ast.walk(scan.tree):
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and id(node) not in docstrings
                and id(node) not in scan.excluded
                and node.value not in capabilities
                and token.search(node.value)
                and id(node) not in associated
            ):
                unassociated.append((relative, node.lineno, node.value))

    return (
        MappingProxyType({
            code: MappingProxyType({cap: frozenset(sites) for cap, sites in sorted(caps.items())})
            for code, caps in sorted(pairs.items())
        }),
        tuple(sorted(unassociated)),
    )


def compiler_translated_codes():
    """The codes the COMPILER serves by translating a shared model rule, read from source.

    SELF-184-37, second instance. The compiler served
    `PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED` for the model's own orphan-`continue`
    rule under its branch-only remediation. The rule is the parser's, so the text must be the
    parser's too. Three mechanisms put a model rule's refusal on a compiler diagnostic:

    1. a compiler function RENDERS a model verdict (`_SourceIndex` defines renderer): it
       raises the code the verdict's reason selects;
    2. a TRANSLATOR runs a model rule and re-raises its `PydanticCustomError` as a compile
       diagnostic (`_as_compile_error`, found by shape). Every code the rule can raise, through
       the parser functions it calls, is translated when the compiler table serves it. For any
       other code the translator re-raises the model's own error, reported under `raw`;
    3. a code the compiler table REGISTERS and no compiler module raises. It is registered for
       the compile path's re-served parse diagnostics, the set the served-text test pins as
       `COMPILER_REGISTERED_PARSE_CODES`.

    Returns a mapping with:

    * `translated`: `{code: (mechanism, ...)}`;
    * `native`: `{code: ((path, lineno), ...)}`, compiler- and semantic-layer raise sites of a
      translated code that render no model verdict. They are the code's compiler-only rules;
    * `raw`: `{code: (mechanism, ...)}`;
    * `unreadable`: rows the reader could not read. The caller asserts it is empty.
    """
    import importlib

    table = importlib.import_module("boomi_mcp.compiler.process_ir.diagnostics")
    served = {
        code for code in table._REMEDIATION
        if table._MESSAGES.get(code) and table._REMEDIATION.get(code)
    }
    index = _SourceIndex()
    translated, raw, unreadable, render_calls = {}, {}, [], set()

    for verdict in sorted(index.verdicts):
        reasons = sorted(index.reasons_of(verdict))
        for relative, scan, name, reason, following in index.renderers_of(verdict):
            if _producer(relative) == "parser":
                continue
            for statement in index.render_block(following):
                for node in ast.walk(statement):
                    if isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call):
                        render_calls.add(id(node.exc))
            for constant in reasons:
                codes = index.rendered_codes(following, scan, reason, constant)
                if not codes:
                    unreadable.append((relative, name, "{0} for {1}".format(verdict, constant)))
                for code in codes:
                    translated.setdefault(code, set()).add(
                        "renders {0} ({1}) in {2}".format(verdict, constant, name))

    translators = index.translators()
    if not translators:
        unreadable.append(("", "", "no translator found"))
    parser_functions = index.functions[index.parser]
    for relative, scan in index.modules:
        if _producer(relative) == "parser":
            continue
        for node in ast.walk(scan.tree):
            if not (isinstance(node, ast.Call) and _called_name(node) in translators):
                continue
            position = translators[_called_name(node)]
            rule = node.args[position] if len(node.args) > position else None
            if not (isinstance(rule, ast.Name) and rule.id in parser_functions):
                unreadable.append((relative, str(node.lineno), "translator argument"))
                continue
            for code in index.model_rule_codes(rule.id):
                (translated if code in served else raw).setdefault(code, set()).add(
                    "translates {0}".format(rule.id))

    emitted, _unresolved = collect_emissions()
    for code in set(table._REMEDIATION) - set(emitted["compiler"]):
        translated.setdefault(code, set()).add("registered for re-served parse diagnostics")

    native = {}
    for relative, scan in index.modules:
        if _producer(relative) == "parser":
            continue
        for node in ast.walk(scan.tree):
            if not isinstance(node, ast.Call) or id(node) in render_calls:
                continue
            found = index.site(node, scan)
            if found is None or found[0] != "sink":
                continue
            for code in found[1]:
                if code in translated:
                    native.setdefault(code, set()).add((relative, node.lineno))

    def freeze(mapping):
        return MappingProxyType({key: tuple(sorted(value)) for key, value in sorted(mapping.items())})

    return MappingProxyType({
        "translated": freeze(translated),
        "native": freeze(native),
        "raw": freeze(raw),
        "unreadable": tuple(sorted(unreadable, key=str)),
    })


# ---------------------------------------------------------------------------
# QA-184-s1-r17-02: the served TABLE each raise site reads, and the codes a
# translated compile refusal can carry
# ---------------------------------------------------------------------------

#: The factories that select a diagnostic's text BY CODE from a served table, and the
#: layer whose `_MESSAGES`/`_REMEDIATION` each reads. A served diagnostic's text is
#: decided by the factory it went through, not by the module that called it, so
#: `routed_emissions()` assigns each raise site to its factory's table.
#: `collect_emissions` buckets by MODULE instead, which is right for "is this code served
#: somewhere" and wrong for "which text does this call serve": a `finding()` and a
#: `diagnostic()` in one semantic module land in one bucket, the served-text guards let
#: the compiler's table satisfy both, and the validator served its generic fallback for
#: every compiler-owned code it raised while those guards stayed green.
#: Pinned by definition like `PINNED_SINKS`; `table_factory_reads()` proves each factory
#: still reads both of its module's tables.
TABLE_FACTORIES = (
    ("src/boomi_mcp/models/process_ir.py", "_diagnostic", "parser"),
    ("src/boomi_mcp/compiler/process_ir/diagnostics.py", "diagnostic", "compiler"),
    ("src/boomi_mcp/compiler/process_ir/semantic_validation/findings.py", "finding", "semantic"),
)


def table_factory_reads():
    """`{(path, factory): frozenset(table names)}`: the registry tables each factory loads.

    The anti-vacuity anchor for the routing below. A factory that stopped reading its
    tables would leave every route pointing at text nothing selects.
    """
    found = {}
    for relative, name, _layer in TABLE_FACTORIES:
        tree = ast.parse((_ROOT / relative).read_text())
        loads = set()
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
                loads.update(
                    sub.id for sub in ast.walk(node)
                    if isinstance(sub, ast.Name) and sub.id in _REGISTRY_TABLES
                )
        found[(relative, name)] = frozenset(loads)
    return MappingProxyType(found)


def _route(routes, relative, name):
    return routes.get((relative, name)) or routes.get((None, name)) or frozenset()


def table_routes():
    """`(scans, routes)`: every sink whose calls reach a table factory, and which table.

    `routes` maps `(None, name)` for a pinned sink, which is called across modules by name,
    and `(path, name)` for a module's own one-hop wrapper, to the frozen set of layers its
    calls reach. Derived to a fixpoint from each sink's definition body, never listed:
    `raise_compile_error` reaches `diagnostic`, `internal_defect` and `invariants._fail`
    reach `raise_compile_error`, lineage's `_report` reaches `finding`. A model constructor
    (`CompilerDiagnostic(...)`) reaches no table, because its caller supplies the text, and
    every such call is a pinned delegation site already.
    """
    scans = tuple(
        (str(path.relative_to(_ROOT)), _ModuleScan(path, path.read_text()))
        for path in _iter_files()
    )
    pinned = {name for _path, name in PINNED_SINKS}
    routes = {(None, name): {layer} for _path, name, layer in TABLE_FACTORIES}
    definitions = []
    for relative, scan in scans:
        for node in ast.walk(scan.tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in scan.sinks:
                key = (None, node.name) if node.name in pinned else (relative, node.name)
                definitions.append((relative, key, node))
    while True:
        changed = False
        for relative, key, node in definitions:
            reached = set()
            for call in ast.walk(node):
                if isinstance(call, ast.Call) and _called_name(call) != node.name:
                    reached |= set(_route(routes, relative, _called_name(call)))
            current = routes.setdefault(key, set())
            if not reached <= current:
                current |= reached
                changed = True
        if not changed:
            break
    return scans, MappingProxyType(
        {key: frozenset(value) for key, value in routes.items() if value}
    )


def routed_emissions():
    """`(codes, dynamic, ambiguous)`: what each table-reading factory is handed, from source.

    * `codes`: `{layer: {code: frozenset((path, lineno), ...)}}` for every call into a
      routed sink whose code resolves, by the closed forms `collect_emissions` reads;
    * `dynamic`: sorted `(path, lineno, sink, dump, kind, layer)` for every call whose code
      does not resolve. `kind` is `definition` when the call forwards the FIRST parameter
      of an enclosing routed sink: that is the wrapper's own body, and its call sites carry
      the codes. Every other such call is a `site`, whose codes the caller must derive;
    * `ambiguous`: sinks reaching more than one table. The caller asserts it is empty
      rather than guessing which text such a call serves.
    """
    scans, routes = table_routes()
    codes = {layer: {} for _path, _name, layer in TABLE_FACTORIES}
    dynamic = []
    for relative, scan in scans:
        for node in ast.walk(scan.tree):
            if not isinstance(node, ast.Call):
                continue
            name = _called_name(node)
            layers = _route(routes, relative, name)
            if not layers:
                continue
            argument = _argument(node, 0, "code")
            if argument is None or id(argument) in scan.excluded:
                continue
            resolved = scan.resolve(argument)
            if resolved is not None:
                for layer in layers:
                    for code in resolved:
                        codes[layer].setdefault(code, set()).add((relative, node.lineno))
                continue
            forward = scan.forwarded_parameter(node, argument)
            kind = "site"
            if forward is not None and forward[1] == 0 and _route(routes, relative, forward[0].name):
                kind = "definition"
            for layer in sorted(layers):
                dynamic.append((relative, node.lineno, name, ast.dump(argument), kind, layer))
    ambiguous = sorted(
        (str(key), tuple(sorted(value))) for key, value in routes.items() if len(value) > 1
    )
    return (
        MappingProxyType({
            layer: MappingProxyType(
                {code: frozenset(sites) for code, sites in sorted(found.items())})
            for layer, found in codes.items()
        }),
        tuple(sorted(dynamic)),
        tuple(ambiguous),
    )


def _module_file(relative, level, module):
    """The repository path of the module an import names, or None when there is no file."""
    if level:
        base = pathlib.PurePosixPath(relative).parent
        for _step in range(level - 1):
            base = base.parent
    else:
        base = pathlib.PurePosixPath("src")
    parts = module.split(".") if module else []
    candidates = [base.joinpath(*parts).with_suffix(".py")] if parts else []
    candidates.append(base.joinpath(*parts, "__init__.py"))
    for candidate in candidates:
        if (_ROOT / candidate).is_file():
            return str(candidate)
    return None


def _imports_of(relative, tree, scanned):
    """`{local name: (module path, name or None)}` for every import of a scanned module or of
    a name in one. `name` is None when the local name binds the module itself."""
    bound = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            target = _module_file(relative, node.level, node.module)
            for alias in node.names:
                local = alias.asname or alias.name
                if target in scanned:
                    bound[local] = (target, alias.name)
                    continue
                dotted = "{0}.{1}".format(node.module, alias.name) if node.module else alias.name
                submodule = _module_file(relative, node.level, dotted)
                if submodule in scanned:
                    bound[local] = (submodule, None)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                target = _module_file(relative, 0, alias.name)
                if alias.asname and target in scanned:
                    bound[alias.asname] = (target, None)
    return bound


class _CallIndex:
    """Functions and imports of the scanned modules, for a by-name walk of what a function
    can reach. The walk follows a function NAMED in a body (called or passed along), in
    its own module or imported from a scanned one, and a function reached through an
    imported module's attribute. It does not follow a method: `raising_methods()` reports
    every method holding a compile-table sink call, and the caller asserts there is none,
    so that limit is a checked fact rather than an assumption."""

    def __init__(self, scans, routes):
        self.scans = dict(scans)
        self.routes = routes
        self.functions = {relative: {} for relative in self.scans}
        for relative, scan in scans:
            for node in ast.walk(scan.tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    self.functions[relative].setdefault(node.name, []).append(node)
        scanned = set(self.scans)
        self.imports = {
            relative: _imports_of(relative, scan.tree, scanned) for relative, scan in scans
        }

    def target(self, relative, name):
        if name in self.functions[relative]:
            return (relative, name)
        bound = self.imports[relative].get(name)
        if bound and bound[1] is not None and bound[1] in self.functions.get(bound[0], {}):
            return bound
        return None

    def raising_walk(self, starts, layer):
        """`(codes, unreadable, reached)` for the functions reachable from `starts`.

        `codes` maps every code a `layer`-routed sink call resolves to onto its sites; a
        call the reader cannot resolve is `unreadable`, unless it is a routed wrapper's own
        body forwarding its first parameter.
        """
        codes, unreadable, seen = {}, [], set()
        stack = sorted(starts)
        while stack:
            relative, name = stack.pop()
            if (relative, name) in seen:
                continue
            seen.add((relative, name))
            scan = self.scans[relative]
            for function in self.functions[relative].get(name, ()):
                for node in ast.walk(function):
                    if isinstance(node, ast.Call):
                        sink = _called_name(node)
                        if layer in _route(self.routes, relative, sink):
                            argument = _argument(node, 0, "code")
                            resolved = scan.resolve(argument) if argument is not None else None
                            if resolved is not None:
                                for code in resolved:
                                    codes.setdefault(code, set()).add((relative, node.lineno))
                                continue
                            forward = (
                                scan.forwarded_parameter(node, argument)
                                if argument is not None else None
                            )
                            if not (
                                forward is not None and forward[1] == 0
                                and _route(self.routes, relative, forward[0].name)
                            ):
                                unreadable.append((relative, node.lineno, sink,
                                                   ast.dump(argument) if argument else ""))
                    elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                        found = self.target(relative, node.id)
                        if found is not None:
                            stack.append(found)
                    elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                        bound = self.imports[relative].get(node.value.id)
                        if (
                            bound and bound[1] is None
                            and node.attr in self.functions.get(bound[0], {})
                        ):
                            stack.append((bound[0], node.attr))
        return (
            MappingProxyType({code: frozenset(sites) for code, sites in sorted(codes.items())}),
            tuple(sorted(unreadable)),
            frozenset(seen),
        )


def raising_methods(layer="compiler"):
    """Sorted `(path, class, method)` for every method holding a `layer`-routed sink call.

    The limit `_CallIndex.raising_walk` states: a method is not followed, so a method that
    raised would hide its codes from the walk. The caller asserts this is empty.
    """
    scans, routes = table_routes()
    found = set()
    for relative, scan in scans:
        for owner in ast.walk(scan.tree):
            if not isinstance(owner, ast.ClassDef):
                continue
            for method in owner.body:
                if not isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if any(
                    isinstance(node, ast.Call)
                    and layer in _route(routes, relative, _called_name(node))
                    for node in ast.walk(method)
                ):
                    found.add((relative, owner.name, method.name))
    return tuple(sorted(found))


def _catches(handler, name):
    kinds = handler.type.elts if isinstance(handler.type, ast.Tuple) else [handler.type]
    return any(isinstance(kind, ast.Name) and kind.id == name for kind in kinds)


def _is_diagnostics_of(expression, name):
    return (
        isinstance(expression, ast.Attribute)
        and expression.attr == "diagnostics"
        and isinstance(expression.value, ast.Name)
        and expression.value.id == name
    )


def _reraised_prefixes(handler):
    """Code prefixes a handler re-raises instead of translating: the string arguments of
    `.startswith(...)` in the test of an `if` whose body is a bare `raise`."""
    prefixes = set()
    for node in ast.walk(handler):
        if not (
            isinstance(node, ast.If)
            and any(isinstance(item, ast.Raise) and item.exc is None for item in node.body)
        ):
            continue
        for call in ast.walk(node.test):
            if (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and call.func.attr == "startswith"
            ):
                prefixes.update(
                    arg.value for arg in call.args
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str)
                )
    return tuple(sorted(prefixes))


def translation_sites():
    """Every call that re-serves a CAUGHT compile refusal's own codes through a table factory.

    QA-184-s1-r17-02. `flow.collect_connector_flow_findings` runs
    `connector_resolution.validate_connector_calls` and hands each diagnostic of the
    `ProcessIRCompileError` it raises to `finding(item.code, ...)`, which selects text from
    the SEMANTIC tables. That call's code resolves from nothing, so the reader pins it, and
    its pinned reason said it "introduces no code of its own". It carries every code the
    caught refusal can, and the semantic tables registered none of them.

    The shape is closed: a `try` whose handler catches `ProcessIRCompileError` as a name and
    calls a routed sink with `<item>.code`, `<item>` iterating `<name>.diagnostics`. The
    codes are those `_CallIndex.raising_walk` finds for the compile table from every
    function the `try` body calls by name, less the prefixes the handler re-raises
    (`_reraised_prefixes`). An over-approximation: a code the walk MAY reach counts.

    Returns `{(path, lineno): mapping}` with `layer`, `sink`, `starts`, `raisable`,
    `reraised_prefixes`, `codes` and `unreadable`.
    """
    scans, routes = table_routes()
    index = _CallIndex(scans, routes)
    sites = {}
    for relative, scan in scans:
        for node in ast.walk(scan.tree):
            if not isinstance(node, ast.Try):
                continue
            for handler in node.handlers:
                if not (handler.name and handler.type is not None
                        and _catches(handler, "ProcessIRCompileError")):
                    continue
                iterated = set()
                for sub in ast.walk(handler):
                    for generator in getattr(sub, "generators", ()) or ():
                        if (_is_diagnostics_of(generator.iter, handler.name)
                                and isinstance(generator.target, ast.Name)):
                            iterated.add(generator.target.id)
                    if (isinstance(sub, ast.For) and _is_diagnostics_of(sub.iter, handler.name)
                            and isinstance(sub.target, ast.Name)):
                        iterated.add(sub.target.id)
                calls = []
                for sub in ast.walk(handler):
                    if not isinstance(sub, ast.Call):
                        continue
                    layers = _route(routes, relative, _called_name(sub))
                    argument = _argument(sub, 0, "code")
                    if (
                        layers and isinstance(argument, ast.Attribute)
                        and argument.attr == "code"
                        and isinstance(argument.value, ast.Name)
                        and argument.value.id in iterated
                    ):
                        calls.append((sub, _called_name(sub), layers))
                if not calls:
                    continue
                starts = set()
                for statement in node.body:
                    for call in ast.walk(statement):
                        if isinstance(call, ast.Call) and isinstance(call.func, ast.Name):
                            found = index.target(relative, call.func.id)
                            if found is not None:
                                starts.add(found)
                raisable, unreadable, _reached = index.raising_walk(starts, "compiler")
                prefixes = _reraised_prefixes(handler)
                for call, sink, layers in calls:
                    sites[(relative, call.lineno)] = MappingProxyType({
                        "layer": tuple(sorted(layers)),
                        "sink": sink,
                        "starts": tuple(sorted(starts)),
                        "raisable": frozenset(raisable),
                        "reraised_prefixes": prefixes,
                        "codes": frozenset(
                            code for code in raisable if not code.startswith(prefixes)),
                        "unreadable": unreadable,
                    })
    return MappingProxyType(sites)


def forward_owner_codes():
    """The codes reaching each call that forwards a NON-first parameter into a routed sink.

    `{(path, sink, dump): {"owner", "parameter", "codes"}}`. The codes are the owner's
    runtime default, read by `runtime_forward_defaults()`, plus the argument each call site
    of the owner passes, which `unresolvable_forward_arguments()` requires to be a plain
    constant: it lists any site that is not, and the caller asserts that list is empty.
    """
    scans, routes = table_routes()
    defaults, _unreadable = runtime_forward_defaults()
    found = {}
    for relative, scan in scans:
        owners = {}
        for node in ast.walk(scan.tree):
            if not isinstance(node, ast.Call) or not _route(routes, relative, _called_name(node)):
                continue
            argument = _argument(node, 0, "code")
            if argument is None or scan.resolve(argument) is not None:
                continue
            forward = scan.forwarded_parameter(node, argument)
            if forward is None:
                continue
            owner, index, param = forward
            if index == 0 and _route(routes, relative, owner.name):
                continue  # a routed wrapper's own body: its call sites are the emissions
            key = (relative, _called_name(node), ast.dump(argument))
            owners[owner.name] = (key, index, param)
            codes = set()
            default = defaults.get((relative, owner.name, param))
            if default:
                codes.add(default)
            found[key] = {"owner": owner.name, "parameter": param, "codes": codes,
                          "unreadable": []}
        for node in ast.walk(scan.tree):
            if not isinstance(node, ast.Call) or _called_name(node) not in owners:
                continue
            key, index, param = owners[_called_name(node)]
            # Reported, never dropped: an argument this reader cannot read would otherwise
            # vanish from the code set, which is the fail-open shape this module refuses.
            if any(isinstance(a, ast.Starred) for a in node.args) or any(
                k.arg is None for k in node.keywords
            ):
                found[key]["unreadable"].append((node.lineno, "unpacked arguments"))
                continue
            supplied = _argument(node, index, param)
            if supplied is None:
                continue
            resolved = scan.resolve(supplied)
            if resolved is None:
                found[key]["unreadable"].append((node.lineno, ast.dump(supplied)))
            else:
                found[key]["codes"].update(resolved)
    return MappingProxyType({
        key: MappingProxyType(dict(value, codes=frozenset(value["codes"]),
                                   unreadable=tuple(value["unreadable"])))
        for key, value in sorted(found.items())
    })


#: An UPPER_SNAKE binding name: two or more segments, with optional leading underscores.
#: One-segment names are excluded on purpose, because words such as `JSON` or `XML` are
#: ordinary prose in a remediation and would be false matches.
_INTERNAL_NAME = re.compile(r"^_*[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+$")


def _module_level_targets(statements):
    """Every name a module binds by assignment at module level, including inside a
    module-level `if`, `try`, `with` or `for` (never inside a function or class body)."""
    for statement in statements:
        if isinstance(statement, ast.Assign):
            for target in statement.targets:
                if isinstance(target, ast.Name):
                    yield target.id
        elif isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name):
            yield statement.target.id
        elif isinstance(statement, (ast.If, ast.For, ast.While, ast.With, ast.Try)):
            for field in ("body", "orelse", "finalbody"):
                yield from _module_level_targets(getattr(statement, field, None) or ())
            for handler in getattr(statement, "handlers", ()) or ():
                yield from _module_level_targets(handler.body)


def internal_constant_names():
    """`{name: frozenset(paths)}` for every module-level UPPER_SNAKE binding in the scanned
    modules, read from source.

    QA-184-s1-r17-01. The nesting remediation told its reader to reduce nesting "to at most
    PROCESS_IR_V1_MAX_CONTROL_DEPTH levels": a module constant's NAME, which no served page
    resolves, in place of its value. The case set is every such name the scanned modules
    bind, so a new constant is covered the moment it is defined.
    """
    names = {}
    for path in _iter_files():
        relative = str(path.relative_to(_ROOT))
        for name in _module_level_targets(ast.parse(path.read_text()).body):
            if _INTERNAL_NAME.match(name):
                names.setdefault(name, set()).add(relative)
    return MappingProxyType({name: frozenset(paths) for name, paths in sorted(names.items())})
