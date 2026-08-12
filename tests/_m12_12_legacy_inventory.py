"""Issue #149 (M12.12) — derivation engine for the pre-deletion legacy
reachability inventory, allowlist baseline and served-surface retraction matrix.

Not a ``test_*`` module, so pytest never collects it (this repo has no
``pytest.ini``/``pyproject.toml``/``conftest.py``, so pytest's default
``python_files = test_*.py *_test.py`` applies and a leading-underscore module is
skipped). Precedent: ``tests/_m12_11_support.py``.

The module is BOTH the library the freeze test imports and the CLI that
regenerates the committed baseline:

    PYTHONPATH=src .venv/bin/python tests/_m12_12_legacy_inventory.py --check
    PYTHONPATH=src .venv/bin/python tests/_m12_12_legacy_inventory.py \\
        --write tests/fixtures/m12_12/legacy_reachability_inventory.json
    PYTHONPATH=src .venv/bin/python tests/_m12_12_legacy_inventory.py --emit-markdown

Three properties are load-bearing and are asserted by the freeze suite rather
than merely documented here:

1. **The watched vocabulary is DERIVED from the runtime authority**, never typed
   out: builders come from ``PROCESS_FLOW_BUILDERS`` and
   ``builders.__all__``, legacy emitters from ``dir(process_emitters.legacy)``,
   legacy semantic validation from the ``legacy_bridge`` module, and the raw
   Component-XML sinks from the installed SDK's ``ComponentService``. A builder
   registered by a later endgame issue is therefore watched the day it
   registers. ``assert_vocabulary_non_vacuous()`` keeps the derivation from
   failing open.
2. **Nothing positional is frozen.** The semantic key is
   ``(census, path, symbol, form)``; line numbers ride along as
   ``evidence_line`` for humans and are excluded from equality, so an unrelated
   inserted line never breaks the gate while a genuinely new call site always
   does.
3. **Everything is read-only.** The served-artifact collectors call real
   producers, but only ones that reach no transport: the FastMCP registry, pure
   schema/catalog functions, pydantic schema generation, and error envelopes
   that provably return before touching a client.
"""

import os

# Must precede `import server` — the module reads it at import time.
# Precedent: tests/test_process_ir_authoring_contract.py:28-30.
os.environ.setdefault("BOOMI_LOCAL", "true")

import argparse  # noqa: E402
import ast  # noqa: E402
import asyncio  # noqa: E402
import hashlib  # noqa: E402
import json  # noqa: E402
import re  # noqa: E402
import sys  # noqa: E402
import textwrap  # noqa: E402
from pathlib import Path  # noqa: E402
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple  # noqa: E402

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT / "src"), str(_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Bare `boomi_mcp.` imports throughout — the `src.`-prefixed spelling used by
# tests/test_issue_135_compatibility_freeze.py creates a SECOND module object,
# and the transport-bomb test must patch the same object the collector reaches.
from boomi_mcp.models.integration_models import IntegrationComponentSpec  # noqa: E402


SCHEMA_VERSION = "1"
SCANNER_VERSION = "1"
BASELINE_SHA = "9711a9c0cb6c88dda41ada94d88694915b659f36"
BASELINE_BRANCH = "dev"
CAPTURE_DATE = "2026-08-12"

FIXTURE_RELPATH = "tests/fixtures/m12_12/legacy_reachability_inventory.json"

#: Roots the scanner walks, recorded into the baseline for the reader. The
#: mechanism that actually catches a moved package is `python_source_count` /
#: `example_document_count` in the same block — those are compared, this list is
#: documentation.
SCAN_ROOTS: Tuple[str, ...] = ("server.py", "src/boomi_mcp", "examples")

CENSUS_KINDS: Tuple[str, ...] = (
    "registry_lookup",
    "renderer_call",
    "legacy_transitive_call",
    "legacy_emitter",
    "legacy_semantic_validation",
    "component_xml_write",
    "raw_api_invoker",
    "process_kind_producer",
    "process_kind_consumer",
    "example_producer",
    "authoring_boundary",
    "unclassified_dynamic",
)

PRODUCER_SELECTORS: Tuple[str, ...] = ("process_kind", "process_type")

ROUTE_CLASSIFICATIONS: Tuple[str, ...] = (
    "raw_process_capable",
    "platform_sourced_rematerialization",
    "legacy_structured_process",
    "preserve",
    "dormant",
    "typed_non_process",
)

SURFACE_CLASSES: Tuple[str, ...] = (
    "SS-PYDANTIC",
    "SS-BUILDER-DIAGNOSTICS",
    "SS-MCP-DESCRIPTIONS",
    "SS-SAFE-EDIT",
    "SS-SCHEMA-TEMPLATES",
    "SS-RAW-API",
    "SS-CAPABILITY-CATALOG",
    "SS-ARCHETYPE-CATALOG",
)

#: Endgame issues this baseline may hand a path to. `unknown` is deliberately
#: absent — the acceptance criteria forbid it.
OWNING_ISSUES: Tuple[str, ...] = (
    "#151", "#153", "#154", "#155", "#156", "#157", "#158", "#159", "#160",
)


# ======================================================================
# Repository surface
# ======================================================================

def repo_root() -> Path:
    return _ROOT


def python_sources() -> Dict[str, str]:
    """``{repo-relative posix path: source text}`` for every scanned Python file."""
    out: Dict[str, str] = {}
    server_py = _ROOT / "server.py"
    if server_py.is_file():
        out["server.py"] = server_py.read_text(encoding="utf-8")
    for path in sorted((_ROOT / "src" / "boomi_mcp").rglob("*.py")):
        out[path.relative_to(_ROOT).as_posix()] = path.read_text(encoding="utf-8")
    return dict(sorted(out.items()))


def example_documents() -> Dict[str, Any]:
    """``{repo-relative posix path: parsed JSON}`` for every ``examples/**/*.json``.

    Globbed, never enumerated: a sixth example is a diff, not a silent pass.
    """
    out: Dict[str, Any] = {}
    examples = _ROOT / "examples"
    if not examples.is_dir():
        return out
    for path in sorted(examples.rglob("*.json")):
        out[path.relative_to(_ROOT).as_posix()] = json.loads(path.read_text(encoding="utf-8"))
    return dict(sorted(out.items()))


# ======================================================================
# Watched vocabulary — DERIVED from the runtime authority
# ======================================================================

def legacy_sink_vocabulary() -> Dict[str, Tuple[str, ...]]:
    """Reflect the live runtime for every family the scanner watches."""
    from boomi_mcp.categories.components import builders
    from boomi_mcp.categories.components.builders import process_flow_builder as pfb
    from boomi_mcp.categories.components.builders.process_emitters import legacy as legacy_mod
    from boomi_mcp.compiler.process_ir.semantic_validation import legacy_bridge

    registry_names = tuple(sorted(
        n for n in builders.__all__
        if n in {"PROCESS_FLOW_BUILDERS", "get_process_flow_builder"}
    ))

    builder_classes: Set[str] = {cls.__name__ for cls in pfb.PROCESS_FLOW_BUILDERS.values()}
    for name in builders.__all__:
        obj = getattr(builders, name, None)
        if isinstance(obj, type) and hasattr(obj, "build") \
                and getattr(obj, "__module__", "").endswith("process_flow_builder"):
            builder_classes.add(name)

    builder_methods = tuple(sorted(
        m for m in ("build", "validate_config", "lower_config")
        if any(hasattr(cls, m) for cls in pfb.PROCESS_FLOW_BUILDERS.values())
    ))

    legacy_emitters = tuple(sorted(n for n in dir(legacy_mod) if n.startswith("_emit_")))

    # Public callables of the legacy bridge PLUS the builder-side semantic
    # entry points. `_process_ir_semantic_error` (integration_builder) is named
    # by the design plan and is private, so a public-only filter over one module
    # missed it entirely — the legacy semantic route had exactly one watched
    # name instead of two.
    from boomi_mcp.categories import integration_builder

    legacy_semantic_names = {
        n for n, obj in vars(legacy_bridge).items()
        if callable(obj) and getattr(obj, "__module__", "") == legacy_bridge.__name__
        and not n.startswith("_")
    }
    legacy_semantic_names |= {
        n for n, obj in vars(integration_builder).items()
        if callable(obj)
        and getattr(obj, "__module__", "") == integration_builder.__name__
        and ("process_ir_semantic" in n or "legacy_process_config" in n)
    }
    legacy_semantic = tuple(sorted(legacy_semantic_names))

    # Raw Component-XML sinks: the repo's own shared writers plus the installed
    # SDK's mutating ComponentService verbs.
    # A MUTATING verb, derived the same way on both sides: strip the private
    # underscore, then require the leading token to be create/update. Without the
    # verb test a read helper such as `parse_component_xml` would join the write
    # census and dilute the route reconciliation.
    from boomi_mcp.categories.components import _shared
    shared_writers = {
        n for n, obj in vars(_shared).items()
        if callable(obj) and getattr(obj, "__module__", "") == _shared.__name__
        and n.lstrip("_").split("_", 1)[0] in {"create", "update"}
        and ("component_raw" in n or "component_xml" in n)
    }
    # The bulk verbs are write sinks too, and this inventory proves it in its own
    # `sdk_evidence`: `ComponentBulkRequestType` admits CREATE, UPDATE and DELETE,
    # so "the envelope parses" is not the same as "the call is a read". Excluding
    # them on a create_/update_ prefix would let a future production caller mutate
    # components through `bulk_component` without ever producing a
    # `component_xml_write` row or entering route reconciliation.
    try:
        from boomi.services.component import ComponentService
        sdk_writers = {
            m for m in dir(ComponentService)
            if not m.startswith("_")
            and (m.split("_", 1)[0] in {"create", "update"} or m.startswith("bulk_")
                 # The generic transports underneath every typed verb. Without
                 # them a hand-rolled `Serializer(...)` + `send_request()` POST
                 # to /Component is a Component-XML write the census cannot see.
                 or m.startswith("send_request") or m == "stream_request")
        }
    except Exception:  # pragma: no cover - SDK always present in this repo
        sdk_writers = set()

    return {
        "registry_names": registry_names,
        "builder_classes": tuple(sorted(builder_classes)),
        "builder_methods": builder_methods,
        "legacy_emitters": legacy_emitters,
        "legacy_semantic_validation": legacy_semantic,
        "component_xml_write_sinks": tuple(sorted(shared_writers | sdk_writers)),
        "raw_api_invokers": ("invoke_api",),
        "producer_selectors": PRODUCER_SELECTORS,
    }


def assert_vocabulary_non_vacuous(vocab: Dict[str, Tuple[str, ...]]) -> None:
    """A derivation that silently resolves to nothing is a gate that passes on
    everything. Guard the guard."""
    empty = sorted(k for k, v in vocab.items() if not v)
    if empty:
        raise AssertionError(
            "legacy sink vocabulary derived EMPTY families %s — the scanner would "
            "watch nothing. A refactor moved or renamed the runtime authority; fix "
            "the derivation, do not weaken this check." % (empty,)
        )


# ======================================================================
# AST scanner
# ======================================================================

def _row_id(prefix: str, semantic_key: Tuple[str, ...]) -> str:
    digest = hashlib.sha256("|".join(semantic_key).encode("utf-8")).hexdigest()
    return "%s-%s" % (prefix, digest[:8])


_CENSUS_PREFIX = {
    "registry_lookup": "LR",
    "renderer_call": "LR",
    "legacy_transitive_call": "LT",
    "legacy_emitter": "LE",
    "legacy_semantic_validation": "LV",
    "component_xml_write": "WR",
    "raw_api_invoker": "WR",
    "process_kind_producer": "PP",
    "process_kind_consumer": "PC",
    "example_producer": "PX",
    "authoring_boundary": "PB",
    "unclassified_dynamic": "UD",
}


class _Scanner(ast.NodeVisitor):
    """One pass per module; emits census rows keyed symbolically, not positionally."""

    def __init__(self, path: str, vocab: Dict[str, Tuple[str, ...]],
                 transitive_targets: "frozenset[Tuple[str, str]]" = frozenset(),
                 local_defs: "frozenset[str]" = frozenset(),
                 known_paths: "frozenset[str]" = frozenset()) -> None:
        self.path = path
        self.v = vocab
        self._transitive = transitive_targets
        self._imported: Set[str] = set()
        self._import_origin: Dict[str, str] = {}
        self._qualified_origin: Dict[str, Tuple[str, str]] = {}
        self._module_paths: Dict[str, str] = {}
        self._known: "frozenset[str]" = known_paths
        self._local_defs: Set[str] = set(local_defs)
        self.rows: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
        self._symbols: List[str] = []
        self._aliases: Dict[str, str] = {}     # local name -> watched base name
        self._modules: Dict[str, str] = {}     # local name -> module dotted path
        self._builder_vars: Set[str] = set()   # locals bound to a builder class
        self._skip: Set[int] = set()

        self._registry = set(vocab["registry_names"])
        self._builders = set(vocab["builder_classes"])
        self._methods = set(vocab["builder_methods"])
        self._emitters = set(vocab["legacy_emitters"])
        self._semantic = set(vocab["legacy_semantic_validation"])
        self._writes = set(vocab["component_xml_write_sinks"])
        self._invokers = set(vocab["raw_api_invokers"])
        self._selectors = set(vocab["producer_selectors"])
        self._watched = (
            self._registry | self._builders | self._emitters
            | self._semantic | self._writes | self._invokers | self._selectors
        )

    # -- bookkeeping ---------------------------------------------------
    @property
    def symbol(self) -> str:
        return ".".join(self._symbols) if self._symbols else "<module>"

    def _emit(self, census: str, form: str, line: int) -> None:
        key = (census, self.path, self.symbol, form)
        row = self.rows.get(key)
        if row is None:
            self.rows[key] = {
                "row_id": _row_id(_CENSUS_PREFIX[census], key),
                "census": census,
                "path": self.path,
                "symbol": self.symbol,
                "form": form,
                "count": 1,
                "evidence_line": line,
            }
        else:
            row["count"] += 1
            row["evidence_line"] = min(row["evidence_line"], line)

    def _resolve(self, name: str) -> str:
        return self._aliases.get(name, name)

    def _is_registry(self, node: ast.AST) -> bool:
        """True for the registry reached by ANY spelling.

        Both the bare import (`PROCESS_FLOW_BUILDERS[k]`) and the
        module-qualified form (`builders.PROCESS_FLOW_BUILDERS[k]`) must resolve.
        Accepting only `ast.Name` let the qualified spelling produce no census row
        at all — a whole legacy renderer path invisible to the gate.
        """
        if isinstance(node, ast.Name):
            return self._resolve(node.id) == "PROCESS_FLOW_BUILDERS"
        if isinstance(node, ast.Attribute):
            return node.attr == "PROCESS_FLOW_BUILDERS"
        return False

    def _qualified_callee(self, func: ast.AST) -> Optional[Tuple[str, str]]:
        """`(defining path, symbol)` for a statically resolvable call, else None.

        Three resolvable shapes, and only these:
          `f(...)`        where f is defined in this module
          `f(...)`        where f was imported by name (possibly aliased)
          `mod.f(...)`    where mod was imported as a module we can place
        Everything else — method dispatch, calls through parameters, dynamic
        attributes — is left unresolved rather than guessed at.
        """
        if isinstance(func, ast.Name):
            if func.id in self._qualified_origin:
                return self._qualified_origin[func.id]
            if func.id in self._local_defs:
                return (self.path, func.id)
            return None
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            module_path = self._module_paths.get(func.value.id)
            if module_path:
                return (module_path, func.attr)
        return None

    def _yields_builder(self, node: ast.AST) -> bool:
        """True when the expression evaluates to a process-flow builder class."""
        if isinstance(node, ast.Call):
            dotted = self._dotted(node.func) or ""
            parts = dotted.split(".")
            if parts[-1] == "get_process_flow_builder":
                return True
            if parts[-1] == "get" and len(parts) > 1 \
                    and parts[-2] == "PROCESS_FLOW_BUILDERS":
                return True
        if isinstance(node, ast.Subscript) and self._is_registry(node.value):
            return True
        return False

    def _names_a_builder(self, node: ast.AST) -> bool:
        """True when the expression names a builder class or the registry."""
        if isinstance(node, ast.Name):
            return self._resolve(node.id) in self._builders or node.id in self._builder_vars
        if isinstance(node, ast.Attribute):
            return node.attr in self._builders or node.attr == "PROCESS_FLOW_BUILDERS"
        return self._yields_builder(node)

    def _dotted(self, node: ast.AST) -> Optional[str]:
        parts: List[str] = []
        cur = node
        while isinstance(cur, ast.Attribute):
            parts.append(cur.attr)
            cur = cur.value
        if isinstance(cur, ast.Name):
            parts.append(self._resolve(cur.id))
        elif isinstance(cur, ast.Call):
            parts.append("<call>")
        else:
            return None
        return ".".join(reversed(parts))

    # -- scopes --------------------------------------------------------
    def _scoped(self, node: ast.AST, name: str) -> None:
        self._symbols.append(name)
        self.generic_visit(node)
        self._symbols.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        self._scoped(node, node.name)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        self._scoped(node, node.name)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802
        self._scoped(node, node.name)

    # -- imports -------------------------------------------------------
    def _resolve_module(self, module: Optional[str], level: int) -> Optional[str]:
        """Repo-relative path of an imported module, or None if it is not ours.

        Needed because a transitive callee's identity is `(path, symbol)`, not a
        bare name: `_build_main_process` is defined in THREE archetype modules,
        so a bare-name closure links whichever one happens to be legacy-bearing
        to callers of the other two.
        """
        if level:
            parts = self.path.split("/")[:-1]
            for _ in range(level - 1):
                parts = parts[:-1]
            target = parts + (module.split(".") if module else [])
        else:
            if not module:
                return None
            head = module.split(".")
            if head[0] != "boomi_mcp":
                return None
            target = ["src"] + head
        for candidate in ("/".join(target) + ".py", "/".join(target) + "/__init__.py"):
            if candidate in self._known:
                return candidate
        return None

    def visit_Import(self, node: ast.Import) -> None:  # noqa: N802
        for alias in node.names:
            local = alias.asname or alias.name.split(".")[0]
            self._modules[local] = alias.name
            resolved = self._resolve_module(alias.name, 0)
            if resolved:
                self._module_paths[local] = resolved
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:  # noqa: N802
        package = self._resolve_module(node.module, node.level or 0)
        for alias in node.names:
            local = alias.asname or alias.name
            self._imported.add(local)
            # The ORIGINAL name, so `import x as y` still resolves against the
            # bearing set — an aliased wrapper is exactly the escape this
            # closure exists to catch.
            self._import_origin[local] = alias.name
            # `from . import integration_builder as ib` binds a MODULE, so a
            # later `ib.build_structured_update_xml(...)` must resolve too.
            submodule = self._resolve_module(
                "%s.%s" % (node.module, alias.name) if node.module else alias.name,
                node.level or 0)
            if submodule:
                self._module_paths[local] = submodule
            elif package:
                self._qualified_origin[local] = (package, alias.name)
            if alias.name in self._watched:
                self._aliases[local] = alias.name
            if alias.name in self._builders:
                self._builder_vars.add(local)
            if alias.name in self._emitters:
                self._emit("legacy_emitter", "import %s" % alias.name, node.lineno)
        self.generic_visit(node)

    # -- assignments ---------------------------------------------------
    def visit_Assign(self, node: ast.Assign) -> None:  # noqa: N802
        value = node.value

        # `x = get_process_flow_builder(...)` / `PROCESS_FLOW_BUILDERS[k]` /
        # `PROCESS_FLOW_BUILDERS.get(k)` binds x to a builder class.
        # Every registry spelling goes through `_is_registry` / `_yields_builder`
        # so a variable bound to a module-qualified lookup is tracked exactly like
        # one bound to the bare form.
        bound = False
        if isinstance(value, ast.Call):
            dotted = self._dotted(value.func)
            if dotted and dotted.split(".")[-1] == "get_process_flow_builder":
                bound = True
            elif isinstance(value.func, ast.Attribute) and value.func.attr == "get" \
                    and self._is_registry(value.func.value):
                bound = True
        elif isinstance(value, ast.Subscript) and self._is_registry(value.value):
            bound = True
        elif isinstance(value, (ast.Name, ast.Attribute)):
            dotted = self._dotted(value)
            base = (dotted or "").split(".")[-1]
            if base in self._watched:
                for tgt in node.targets:
                    if isinstance(tgt, ast.Name):
                        self._aliases[tgt.id] = base
            if base in self._builders:
                bound = True

        if bound:
            for tgt in node.targets:
                if isinstance(tgt, ast.Name):
                    self._builder_vars.add(tgt.id)

        # `cfg["process_kind"] = ...` is a producer write.
        for tgt in node.targets:
            if isinstance(tgt, ast.Subscript):
                key = _const_str(tgt.slice)
                if key in self._selectors:
                    self._emit(
                        "process_kind_producer",
                        "subscript-assign %s=%s" % (key, _const_repr(value)),
                        node.lineno,
                    )
                    self._skip.add(id(tgt))
        self.generic_visit(node)

    # -- expressions ---------------------------------------------------
    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        dotted = self._dotted(node.func)
        base = (dotted or "").split(".")[-1]
        head = (dotted or "").split(".")[0]

        # `R[kind].build(...)` has a Subscript at the root of the attribute
        # chain, so `_dotted` cannot name it and `base` would be empty. Take the
        # method name straight off the Attribute node instead — otherwise a
        # renderer invoked directly off a subscript lookup escapes the census.
        if not base and isinstance(node.func, ast.Attribute):
            base = node.func.attr

        if dotted is not None:
            self._skip.add(id(node.func))

        if base == "get_process_flow_builder":
            self._emit("registry_lookup", "get_process_flow_builder(...)", node.lineno)
        elif base in {"get", "keys", "values", "items"} \
                and isinstance(node.func, ast.Attribute) \
                and self._is_registry(node.func.value):
            # Matches the bare AND the module-qualified spelling: keying off the
            # dotted HEAD only saw `PROCESS_FLOW_BUILDERS.get(...)` and missed
            # `builders.PROCESS_FLOW_BUILDERS.get(...)`.
            self._emit("registry_lookup", "PROCESS_FLOW_BUILDERS.%s(...)" % base, node.lineno)
        elif base in self._methods and isinstance(node.func, ast.Attribute):
            owner = node.func.value
            if isinstance(owner, ast.Name):
                oid = owner.id
                if oid in self._builder_vars:
                    label = oid if self._resolve(oid) in self._builders else "<registry-bound>"
                    self._emit("renderer_call", "%s.%s(...)" % (label, base), node.lineno)
                elif self._resolve(oid) in self._builders:
                    self._emit("renderer_call", "%s.%s(...)" % (self._resolve(oid), base),
                               node.lineno)
            elif isinstance(owner, ast.Attribute):
                od = self._dotted(owner) or ""
                if od.split(".")[-1] in self._builders:
                    self._emit("renderer_call", "%s.%s(...)" % (od.split(".")[-1], base),
                               node.lineno)
            elif self._yields_builder(owner):
                # `get_process_flow_builder(kind).build(...)` and
                # `PROCESS_FLOW_BUILDERS[kind].build(...)` — the renderer is
                # invoked straight off the lookup with no intermediate variable.
                # Missing this shape is a FAIL-OPEN, not cosmetics: the mutation
                # overlay used exactly that form and the gate stayed silent.
                self._emit("renderer_call", "<registry-bound>.%s(...)" % base, node.lineno)
        elif base in self._emitters:
            self._emit("legacy_emitter", "%s(...)" % base, node.lineno)
        elif base in self._semantic:
            self._emit("legacy_semantic_validation", "%s(...)" % base, node.lineno)
        elif base in self._writes:
            self._emit("component_xml_write", "%s(...)" % _tail(dotted, 2), node.lineno)
        elif base in self._invokers:
            self._emit("raw_api_invoker", "%s(...)" % base, node.lineno)
        elif base == "getattr":
            # Two ways a `getattr` reaches a legacy path, and BOTH must be
            # recorded or the documented fail-closed residue rule is a claim
            # rather than a property:
            #   getattr(mod, 'get_process_flow_builder')(kind)   -- watched NAME
            #   getattr(SyncPipelineBuilder, name)(config)       -- watched TARGET
            # An earlier draft handled only the first, and only against a
            # `_watched` set that excluded the builder METHODS — so
            # `getattr(SyncPipelineBuilder, 'build')(config)` invoked the legacy
            # renderer and emitted nothing at all.
            # The target branch fires ONLY when the attribute name is not a
            # constant. `getattr(builder_cls, "PRESERVATION_POLICY", None)` is
            # fully resolved at read time and is an ordinary policy read, not a
            # renderer invocation — reporting it as unresolvable residue would be
            # false, and this file has twelve such call sites.
            has_name_arg = len(node.args) > 1
            probe = _const_str(node.args[1]) if has_name_arg else None
            name_is_constant = has_name_arg and isinstance(node.args[1], ast.Constant)
            target_is_builder = bool(node.args) and self._names_a_builder(node.args[0])
            if probe and probe in self._watched:
                # A DISTINCTIVE name — `get_process_flow_builder`,
                # `_create_component_raw`, `process_kind`. The name alone is
                # evidence, whatever the target.
                self._emit("unclassified_dynamic", "getattr(..., %r)" % probe, node.lineno)
            elif probe and probe in self._methods and target_is_builder:
                # A GENERIC name — `build`, `validate_config`, `lower_config`.
                # These say nothing on their own: `getattr(plugin, "build")` in
                # unrelated code is not legacy reachability, and reporting it
                # would fail the frozen census for a harmless edit. The target
                # must be a builder.
                self._emit("unclassified_dynamic", "getattr(<builder>, %r)" % probe,
                           node.lineno)
            elif has_name_arg and not name_is_constant and target_is_builder:
                self._emit("unclassified_dynamic",
                           "getattr(<builder>, <dynamic>)", node.lineno)
        elif base == "IntegrationComponentSpec":
            for kw in node.keywords:
                if kw.arg == "type" and _const_str(kw.value) == "process":
                    self._emit("process_kind_producer",
                               "IntegrationComponentSpec(type='process')", node.lineno)

        # Transitive pass. Only a PLAIN-NAME call to a module-level function
        # that this module defines or imports counts: those are statically
        # resolvable, and they are exactly the wrapper shape the leaf census
        # misses. Method dispatch (`obj.validate_config()`) is deliberately
        # excluded — the callee cannot be resolved without type inference, and
        # matching it on the bare name made `validate_config` alone produce 48
        # rows across unrelated builder classes.
        callee = self._qualified_callee(node.func)
        if callee and callee in self._transitive \
                and (self.path, self.symbol) != callee:
            self._emit("legacy_transitive_call",
                       "%s(...) [legacy-bearing, %s]" % (callee[1], callee[0]),
                       node.lineno)

        for kw in node.keywords:
            if kw.arg in self._selectors:
                self._emit(
                    "process_kind_producer",
                    "keyword %s=%s" % (kw.arg, _const_repr(kw.value)),
                    node.lineno,
                )
            if kw.arg is None and isinstance(kw.value, ast.Dict):
                pass  # handled by visit_Dict

        # `cfg.get("process_kind")` reads; `cfg.setdefault("process_kind", ...)`
        # WRITES one — it is a producer, and treating it as a read would let a
        # new default-injecting producer land without a producer row.
        if base in {"get", "setdefault", "pop"} and node.args:
            key = _const_str(node.args[0])
            if key in self._selectors:
                if base == "setdefault":
                    # EVERY setdefault writes: the one-argument form inserts the
                    # key with `None` when absent, so it produces a process_kind
                    # just as surely as the two-argument form.
                    default = (_const_repr(node.args[1]) if len(node.args) > 1
                               else "None")
                    self._emit("process_kind_producer",
                               "setdefault %s=%s" % (key, default), node.lineno)
                else:
                    self._emit("process_kind_consumer",
                               "%s(%r)" % (_tail(dotted, 2), key), node.lineno)

        self.generic_visit(node)

    def visit_Dict(self, node: ast.Dict) -> None:  # noqa: N802
        for key, value in zip(node.keys, node.values):
            name = _const_str(key)
            if name in self._selectors:
                self._emit(
                    "process_kind_producer",
                    "dict-literal %s=%s" % (name, _const_repr(value)),
                    getattr(key, "lineno", node.lineno),
                )
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> None:  # noqa: N802
        if id(node) not in self._skip:
            if self._is_registry(node.value):
                self._emit("registry_lookup", "PROCESS_FLOW_BUILDERS[...]", node.lineno)
                self._skip.add(id(node.value))
            else:
                key = _const_str(node.slice)
                if key in self._selectors:
                    self._emit("process_kind_consumer",
                               "%s[%r]" % (_tail(self._dotted(node.value), 2) or "<expr>", key),
                               node.lineno)
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:  # noqa: N802
        if id(node) in self._skip:
            return
        if isinstance(node.ctx, ast.Load) and self._resolve(node.id) == "PROCESS_FLOW_BUILDERS":
            self._emit("registry_lookup", "PROCESS_FLOW_BUILDERS (read)", node.lineno)
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:  # noqa: N802
        if id(node) not in self._skip:
            dotted = self._dotted(node)
            if dotted and dotted.split(".")[-1] in self._emitters:
                self._emit("legacy_emitter", "%s (reference)" % dotted.split(".")[-1],
                           node.lineno)
                self._skip.add(id(node.value))
            elif node.attr == "PROCESS_FLOW_BUILDERS":
                # A qualified READ — `sorted(builders.PROCESS_FLOW_BUILDERS)`.
                # `visit_Name` catches the bare spelling, but a module-qualified
                # one is an Attribute and reached nothing, so it was
                # indistinguishable from a harmless file. Third instance of the
                # same defect class (subscript, `.get`, bare read), so recognition
                # now routes through `_is_registry` in every position rather than
                # being patched per shape.
                self._emit("registry_lookup", "PROCESS_FLOW_BUILDERS (read)",
                           node.lineno)
        self.generic_visit(node)


def _tail(dotted: Optional[str], n: int) -> str:
    if not dotted:
        return "<expr>"
    return ".".join(dotted.split(".")[-n:])


def _const_str(node: Optional[ast.AST]) -> Optional[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _const_repr(node: ast.AST) -> str:
    if isinstance(node, ast.Constant):
        return repr(node.value)
    return "<expr>"


#: Census kinds that make the enclosing function a legacy-bearing symbol, so a
#: caller of it is itself on a legacy path.
_REACHABILITY_KINDS = (
    "registry_lookup", "renderer_call", "legacy_emitter",
    "legacy_semantic_validation", "component_xml_write", "legacy_transitive_call",
)


def scan_sources(sources: Dict[str, str], vocab: Dict[str, Tuple[str, ...]]) -> List[Dict[str, Any]]:
    """Run the AST census over ``{path: source}``, then close it over callers.

    A syntax error is a hard failure, never a silent skip.

    The leaf pass alone is fail-open: a new function that merely CALLS
    `build_structured_update_xml` reaches the legacy renderer, but names no
    watched sink itself, so it produced no row and the freeze stayed green. The
    census is therefore closed to a fixed point — any function containing a
    reachability row becomes a legacy-bearing symbol, and calls to it are
    recorded as `legacy_transitive_call`, which itself makes ITS caller
    legacy-bearing. That is the invariant version of the four wrapper entry
    points an earlier draft hand-listed.
    """
    trees: Dict[str, ast.AST] = {}
    rows: List[Dict[str, Any]] = []
    known = frozenset(sources)
    for path in sorted(sources):
        try:
            trees[path] = ast.parse(sources[path], filename=path)
        except SyntaxError as exc:  # pragma: no cover - defensive
            raise AssertionError("cannot parse %s for the #149 census: %s" % (path, exc))
        scanner = _Scanner(path, vocab, local_defs=_module_level_functions(trees[path]),
                           known_paths=known)
        scanner.visit(trees[path])
        rows.extend(scanner.rows.values())

    module_level = {
        path: _module_level_functions(tree) for path, tree in trees.items()
    }
    # Only MODULE-LEVEL functions can bear transitively: a method's callers
    # cannot be resolved from a bare name.
    bearing = {
        (r["path"], r["symbol"])
        for r in rows
        if r["census"] in _REACHABILITY_KINDS
        and "." not in r["symbol"] and r["symbol"] != "<module>"
        and r["symbol"] in module_level.get(r["path"], frozenset())
    }
    seen: Set[Tuple[str, str]] = set()
    found: List[Dict[str, Any]] = []
    while bearing - seen:
        seen |= bearing
        found = []
        for path in sorted(trees):
            scanner = _Scanner(path, vocab, transitive_targets=frozenset(bearing),
                               local_defs=module_level[path], known_paths=known)
            scanner.visit(trees[path])
            found.extend(r for r in scanner.rows.values()
                         if r["census"] == "legacy_transitive_call")
        bearing = bearing | {
            (r["path"], r["symbol"]) for r in found
            if "." not in r["symbol"] and r["symbol"] != "<module>"
            and r["symbol"] in module_level.get(r["path"], frozenset())
        }
    rows = [r for r in rows if r["census"] != "legacy_transitive_call"] + found

    rows.sort(key=lambda r: (r["census"], r["path"], r["symbol"], r["form"]))
    return rows


def _module_level_functions(tree: ast.AST) -> "frozenset[str]":
    return frozenset(
        node.name for node in getattr(tree, "body", [])
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    )


# ======================================================================
# Example / boundary producer census
# ======================================================================

def _json_key_lines(text: str, selectors: Iterable[str]) -> Dict[str, List[int]]:
    """Line numbers of each selector key in a JSON document's raw text.

    The acceptance criterion asks for `file:line`. An earlier draft stamped
    `evidence_line: 0` on every example-producer row, which rendered as `-` in
    the ledger and gave #160 a file with no line to look at.
    """
    out: Dict[str, List[int]] = {}
    for lineno, line in enumerate(text.splitlines(), start=1):
        for selector in selectors:
            if '"%s"' % selector in line:
                out.setdefault(selector, []).append(lineno)
    return out


def _json_paths_with_selector(doc: Any, selectors: Iterable[str], prefix: str = "") -> List[Tuple[str, str]]:
    found: List[Tuple[str, str]] = []
    sel = set(selectors)
    if isinstance(doc, dict):
        for key, value in doc.items():
            here = "%s.%s" % (prefix, key) if prefix else str(key)
            if key in sel:
                found.append((here, value if isinstance(value, str) else "<non-string>"))
            found.extend(_json_paths_with_selector(value, sel, here))
    elif isinstance(doc, list):
        for i, value in enumerate(doc):
            found.extend(_json_paths_with_selector(value, sel, "%s[%d]" % (prefix, i)))
    return found


def scan_examples(documents: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for path in sorted(documents):
        raw = (_ROOT / path).read_text(encoding="utf-8") if (_ROOT / path).is_file() else ""
        key_lines = _json_key_lines(raw, PRODUCER_SELECTORS)
        used: Dict[str, int] = {}
        for jpath, value in _json_paths_with_selector(documents[path], PRODUCER_SELECTORS):
            selector = jpath.rsplit(".", 1)[-1].split("[", 1)[0]
            candidates = key_lines.get(selector, [])
            index = used.get(selector, 0)
            used[selector] = index + 1
            line = candidates[index] if index < len(candidates) else (
                candidates[0] if candidates else 0)
            key = ("example_producer", path, jpath, value)
            rows.append({
                "row_id": _row_id("PX", key),
                "census": "example_producer",
                "path": path,
                "symbol": jpath,
                "form": value,
                "count": 1,
                "evidence_line": line,
            })
    rows.sort(key=lambda r: (r["path"], r["symbol"], r["form"]))
    return rows


def authoring_boundaries() -> List[Dict[str, Any]]:
    """The public arbitrary-config entry points that can carry a legacy
    ``process_kind``, plus every registered archetype that emits one."""
    from boomi_mcp.authoring.contract import list_archetype_registry

    rows: List[Dict[str, Any]] = []

    def _def_line(path: str, symbol: str) -> int:
        """Line of `def <symbol>` in `path`, so every boundary row carries a real
        `file:line` rather than a placeholder zero."""
        target = _ROOT / path
        if not target.is_file():
            return 0
        try:
            tree = ast.parse(target.read_text(encoding="utf-8"), filename=path)
        except SyntaxError:  # pragma: no cover - defensive
            return 0
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                    and node.name == symbol:
                return node.lineno
        return 0

    def add(path: str, symbol: str, form: str, line: Optional[int] = None) -> None:
        key = ("authoring_boundary", path, symbol, form)
        rows.append({
            "row_id": _row_id("PB", key),
            "census": "authoring_boundary",
            "path": path,
            "symbol": symbol,
            "form": form,
            "count": 1,
            "evidence_line": _def_line(path, symbol) if line is None else line,
        })

    add("server.py", "build_integration", "hand-authored IntegrationSpecV1 config")
    add("server.py", "build_from_archetype", "archetype-expanded config")
    add("server.py", "compose_archetypes", "composed multi-archetype config")
    add("server.py", "import_integration_draft", "imported draft config")

    for entry in list_archetype_registry():
        name = entry.get("archetype") or entry.get("name") or entry.get("id")
        if not name:
            continue
        module = "src/boomi_mcp/patterns/archetypes/%s.py" % name
        if (_ROOT / module).is_file():
            add(module, str(name), "registered archetype", line=1)
    rows.sort(key=lambda r: (r["path"], r["symbol"], r["form"]))
    return rows


# ======================================================================
# Component-XML write routes — derive the sinks, classify the members,
# then prove the join is total and injective.
# ======================================================================

#: Classification of every caller-reachable Component-XML write route. The SINKS
#: are DERIVED by the AST scanner; this table only says what each one MEANS and
#: who owns it. `reconcile_routes()` proves the join is total in both directions
#: — no derived sink location is unclassified, and no route cites a location the
#: scanner cannot find — and pins the locations that legitimately host several
#: routes, so a new sharing is a diff rather than a silent reclassification.
WRITE_ROUTES: Tuple[Dict[str, Any], ...] = (
    {
        "route_id": "WRT-manage-component-dispatch",
        "locations": ("src/boomi_mcp/categories/components/manage_component.py::"
                      "manage_component_action",),
        "classification": "raw_process_capable",
        "summary": "Caller-facing dispatcher: forwards `config.xml` verbatim to the create "
                   "and update arms with no component-type restriction.",
        "owning_issue": "#160",
        "post_retraction_assertion": "the shared process-content classifier runs at the "
                                     "dispatcher, before either arm.",
    },
    {
        "route_id": "WRT-manage-component-create",
        "locations": ("src/boomi_mcp/categories/components/manage_component.py::"
                      "create_component",),
        "classification": "raw_process_capable",
        "summary": "Caller `config.xml` is posted verbatim, so "
                   "`<Component type=\"process\">` mints a process. The same function also "
                   "hosts the typed non-process builders (shared location).",
        "owning_issue": "#160",
        "post_retraction_assertion": "create with a process XML root is REJECTED by the shared "
                                     "process-content classifier, whatever the declared type.",
    },
    {
        "route_id": "WRT-manage-component-typed-create",
        "locations": ("src/boomi_mcp/categories/components/manage_component.py::"
                      "create_component",),
        "classification": "typed_non_process",
        "summary": "The typed builder arms of the same function emit connector/profile/"
                   "operation XML, never a process root.",
        "owning_issue": "#160",
        "post_retraction_assertion": "unchanged — the content guard never matches these roots.",
    },
    {
        "route_id": "WRT-manage-component-update",
        "locations": ("src/boomi_mcp/categories/components/manage_component.py::"
                      "update_component",),
        "classification": "raw_process_capable",
        "summary": "Caller `config.xml` full-replaces the named component (shared location "
                   "with the metadata smart-merge).",
        "owning_issue": "#160",
        "post_retraction_assertion": "two-sided check — payload root `process` OR live target "
                                     "type `process` refuses; lookup/parse failure fails closed.",
    },
    {
        "route_id": "WRT-manage-component-metadata",
        "locations": ("src/boomi_mcp/categories/components/manage_component.py::"
                      "update_component",),
        "classification": "preserve",
        "summary": "Metadata smart-merge: reads live XML, rewrites name/folder/description, "
                   "resubmits the full document.",
        "owning_issue": "#160",
        "post_retraction_assertion": "semantic-body identity under the single shared projection.",
    },
    {
        "route_id": "WRT-manage-component-clone",
        "locations": ("src/boomi_mcp/categories/components/manage_component.py::"
                      "clone_component",),
        "classification": "platform_sourced_rematerialization",
        "summary": "Re-posts platform-sourced XML with the identity attributes stripped, "
                   "creating a NEW component with a NEW server-assigned id — a second "
                   "unattested materialization, not metadata drift.",
        "owning_issue": "#160",
        "post_retraction_assertion": "process-typed clone REJECTED; non-process clone preserved.",
    },
    {
        "route_id": "WRT-manage-connector-create",
        "locations": ("src/boomi_mcp/categories/components/connectors.py::create_connector",),
        "classification": "raw_process_capable",
        "summary": "Raw-XML create posts caller XML with NO type check, so "
                   "`<Component type=\"process\">` mints a process (shared location with the "
                   "typed connector builders).",
        "owning_issue": "#160",
        "post_retraction_assertion": "content-based refusal on a process XML root.",
    },
    {
        "route_id": "WRT-connector-typed-build",
        "locations": ("src/boomi_mcp/categories/components/connectors.py::create_connector",),
        "classification": "typed_non_process",
        "summary": "Typed connector / connector-operation builders emit connector-family XML "
                   "only.",
        "owning_issue": "#160",
        "post_retraction_assertion": "unchanged — the content guard never matches a connector "
                                     "root.",
    },
    {
        "route_id": "WRT-manage-connector-update",
        "locations": ("src/boomi_mcp/categories/components/connectors.py::update_connector",),
        "classification": "raw_process_capable",
        "summary": "Raw-XML update full-replaces ANY `component_id`, including a process "
                   "(shared location with the metadata smart-merge).",
        "owning_issue": "#160",
        "post_retraction_assertion": "two-sided payload-OR-live-target refusal.",
    },
    {
        "route_id": "WRT-manage-connector-metadata",
        "locations": ("src/boomi_mcp/categories/components/connectors.py::update_connector",),
        "classification": "preserve",
        "summary": "Metadata smart-merge over the live connector XML.",
        "owning_issue": "#160",
        "post_retraction_assertion": "semantic-body identity under the shared projection.",
    },
    {
        "route_id": "WRT-build-integration-generic",
        "locations": ("src/boomi_mcp/categories/integration_builder.py::_execute_component",),
        "classification": "raw_process_capable",
        "summary": "Generic create/update fall-through forwards `config.xml` for ANY unhandled "
                   "declared type — `IntegrationComponentSpec.type` is unrestricted, so the "
                   "type=\"process\" plan gates (which key off `comp.type == \"process\"`) are "
                   "skipped entirely.",
        "owning_issue": "#160",
        "post_retraction_assertion": "content guard at BOTH plan and apply boundaries, placed "
                                     "BEFORE all type dispatch (the process and connector arms "
                                     "dispatch before the fall-through ever runs); "
                                     "mutation-tested with an unknown/future declared type.",
    },
    {
        "route_id": "WRT-build-integration-structured-process",
        "locations": ("src/boomi_mcp/categories/integration_builder.py::_execute_component",),
        "classification": "legacy_structured_process",
        "summary": "The structured `comp.type == \"process\"` arm resolves a legacy builder and "
                   "renders `<process>`. It does NOT forward caller raw XML — the issue text's "
                   "claim that this branch dispatches raw XML is incorrect (see the §11.2 note); "
                   "the guard still belongs before all type dispatch because the connector and "
                   "generic arms are raw-process-capable.",
        "owning_issue": "#153",
        "post_retraction_assertion": "replaced by canonical ProcessIR materialization/apply.",
    },
    {
        "route_id": "WRT-build-integration-typed-nonprocess",
        "locations": ("src/boomi_mcp/categories/integration_builder.py::_execute_component",),
        "classification": "typed_non_process",
        "summary": "Typed connector/profile/map/operation arms of the same executor.",
        "owning_issue": "#160",
        "post_retraction_assertion": "unchanged — the content guard never matches these roots.",
    },
    {
        "route_id": "WRT-build-integration-preservation-merge",
        "locations": ("src/boomi_mcp/categories/integration_builder.py::"
                      "_apply_structured_update",),
        "classification": "preserve",
        "summary": "Read-merge-write update preservation over the live document; the process "
                   "BODY it merges is produced by `build_structured_update_xml`, which rides "
                   "the legacy builder.",
        "owning_issue": "#153",
        "post_retraction_assertion": "preserved, re-homed onto the canonical apply path.",
    },
    {
        "route_id": "WRT-safe-edit-metadata",
        "locations": ("src/boomi_mcp/categories/components/safe_edit_component.py::"
                      "apply_component_edit_action",),
        "classification": "preserve",
        "summary": "The FOURTH administrative writer: metadata edits reserialize and re-submit "
                   "the FULL live process XML as a full-replace update (the #45/#50 merge). "
                   "Its process BODY edits ride the legacy builder via "
                   "`build_structured_update_xml`.",
        "owning_issue": "#160",
        "post_retraction_assertion": "route-sensitive projection — the permitted subset is "
                                     "exactly the requested name/folderId/folderName/immediate "
                                     "description; process BODY edits are REJECTED in favour of "
                                     "canonical ProcessIR apply.",
    },
    {
        "route_id": "WRT-analyze-component-merge",
        "locations": ("src/boomi_mcp/categories/components/analyze_component.py::merge_versions",),
        "classification": "platform_sourced_rematerialization",
        "summary": "Version/branch merge writes the SOURCE version's body into the target — a "
                   "semantic change to the target by design, with no caller XML.",
        "owning_issue": "#160",
        "post_retraction_assertion": "REJECT when the target is a process OR the source/merged "
                                     "root is a process — the merged root IS the source root, so "
                                     "a target-only check never sees a process-root source over "
                                     "a non-process target.",
    },
    {
        "route_id": "WRT-folders-move-component",
        "locations": ("src/boomi_mcp/categories/folders.py::_action_move_component",),
        "classification": "preserve",
        "summary": "folderId-only rewrite of the live XML, then verify.",
        "owning_issue": "#160",
        "post_retraction_assertion": "semantic-body identity under the shared projection.",
    },
    {
        "route_id": "WRT-shared-raw-create-sink",
        "locations": ("src/boomi_mcp/categories/components/_shared.py::_create_component_raw",),
        "classification": "raw_process_capable",
        "summary": "The single shared create sink every raw and typed create funnels through; "
                   "posts the XML byte-for-byte via the SDK.",
        "owning_issue": "#160",
        "post_retraction_assertion": "the shared process-content classifier is enforced at or "
                                     "before this sink for every caller.",
    },
    {
        "route_id": "WRT-shared-dormant-writer",
        "locations": ("src/boomi_mcp/categories/components/_shared.py::_update_component_xml",),
        "classification": "dormant",
        "summary": "A full-raw-XML update through the SDK with ZERO production callers at HEAD "
                   "— inventoried precisely BECAUSE it is dormant, so a future caller cannot "
                   "revive an unguarded raw write route. Its only callers are the transport "
                   "tests (tests/test_component_raw_transport.py:59,71).",
        "owning_issue": "#160",
        "post_retraction_assertion": "sits behind the two-sided guard, or is deleted.",
    },
    {
        "route_id": "WRT-shared-channel-lossless-read",
        "locations": ("src/boomi_mcp/categories/shared_resources.py::"
                      "_get_channel_raw_json",),
        "classification": "typed_non_process",
        "summary": "A deliberate SDK bypass (`Serializer` + `send_request`) that reads a "
                   "SharedCommunicationChannelComponent as raw JSON, because the typed GET "
                   "hydrates into a model whose round-trip DROPS nested protocol config. "
                   "It targets the SharedCommunicationChannelComponent endpoint, never "
                   "/Component, and it is a READ feeding the update merge.",
        "owning_issue": "#160",
        "post_retraction_assertion": "unchanged — the endpoint is not /Component and the "
                                     "call is a read, so the process-content classifier "
                                     "never applies; it is inventoried so a future edit "
                                     "cannot turn a hand-rolled transport into a write "
                                     "route unnoticed.",
    },
    {
        "route_id": "WRT-raw-api-component",
        "locations": ("server.py::invoke_boomi_api",
                      "src/boomi_mcp/categories/meta_tools.py::invoke_api"),
        "classification": "raw_process_capable",
        "summary": "The generic raw invoker reaches POST/PUT `/Component` unrestricted by type; "
                   "gated only by `confirm_write=true`. Classification splits its own copy of "
                   "the endpoint while transport interpolates the raw string.",
        "owning_issue": "#160",
        "post_retraction_assertion": "ONE canonical endpoint parser feeds classification, ID "
                                     "extraction AND transport; the reserved literal `bulk` is "
                                     "matched BEFORE the `<id>` arm and is never a componentId; "
                                     "every update-shaped call runs the two-sided process check.",
    },
)


def reconcile_routes(rows: Sequence[Dict[str, Any]]) -> Dict[str, List[str]]:
    """Join the DERIVED write-sink locations against the classification table.

    Three results, all frozen in the baseline:

    * ``unclassified`` — a derived sink location no route claims. Must be empty:
      the acceptance criteria forbid an "unknown" path.
    * ``stale_claims`` — a route claiming a location the scanner no longer finds.
      Must be empty, or the checklist #160 executes cites a call site that moved.
    * ``shared_locations`` — one function hosting several routes (e.g. the raw
      and typed create arms of ``manage_component.create_component``). Legitimate
      and common, so it is RECORDED rather than rejected — but pinned, so a NEW
      sharing is a diff instead of a silent reclassification.
    """
    located: Set[str] = {
        "%s::%s" % (r["path"], r["symbol"].split(".")[0])
        for r in rows if r["census"] in ("component_xml_write", "raw_api_invoker")
    }
    claims: Dict[str, List[str]] = {}
    for route in WRITE_ROUTES:
        for location in route["locations"]:
            claims.setdefault(location, []).append(route["route_id"])

    return {
        "unclassified": sorted(located - set(claims)),
        "stale_claims": sorted(set(claims) - located),
        "shared_locations": sorted(
            "%s -> %s" % (loc, ", ".join(sorted(ids)))
            for loc, ids in claims.items() if len(ids) > 1
        ),
    }


def dormant_writer_callers(rows: Sequence[Dict[str, Any]]) -> List[str]:
    """Production call sites of the dormant ``_update_component_xml``, if any.

    Dormancy is PRODUCTION-scoped: the two deliberate callers in
    ``tests/test_component_raw_transport.py`` are evidence the transport works,
    not violations, and the scanner never walks ``tests/`` anyway.
    """
    return sorted(
        "%s::%s" % (r["path"], r["symbol"])
        for r in rows
        if r["census"] == "component_xml_write"
        and r["form"].endswith("_update_component_xml(...)")
        and not r["path"].endswith("_shared.py")
    )


# ======================================================================
# Served-artifact collection — real producers, zero transport
# ======================================================================

#: Value shapes that are non-deterministic BY CONSTRUCTION and would make the
#: snapshot differ per process or per machine. Deliberately NOT a timestamp
#: regex: the served catalog legitimately carries static example timestamps
#: (e.g. ``2025-01-01T00:00:00Z``), which are contract text and must be frozen,
#: not rejected. A served value that embeds a LIVE clock needs no heuristic — the
#: committed baseline fails on the very next run, which is a measurement rather
#: than a guess, and `test_the_derivation_is_deterministic` pins it too.
_FORBIDDEN_VALUE_PATTERNS = (
    re.compile(r" at 0x[0-9a-fA-F]+"),
    re.compile(r"<[a-zA-Z_.]+ object at "),
)

_LEGACY_TOKENS = (
    "process_kind", "process_type", "sync_pipeline", "wrapper_subprocess",
    "database_to_api_sync", "raw process XML", "raw XML", "config.xml",
    "escape hatch", "PROCESS_FLOW_BUILDERS",
)


def _sorted_deep(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _sorted_deep(value[k]) for k in sorted(value, key=str)}
    if isinstance(value, (list, tuple)):
        return [_sorted_deep(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return repr(value)


def canonical_json(value: Any) -> str:
    return json.dumps(_sorted_deep(value), ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _assert_value_clean(artifact_id: str, canonical: str) -> None:
    root = str(_ROOT)
    if root in canonical:
        raise AssertionError(
            "served artifact %s embeds an absolute repository path — the snapshot "
            "would differ per machine" % artifact_id)
    for pattern in _FORBIDDEN_VALUE_PATTERNS:
        if pattern.search(canonical):
            raise AssertionError(
                "served artifact %s embeds a non-deterministic value matching %r"
                % (artifact_id, pattern.pattern))


#: Canonical length above which an artifact ALSO records a legacy-token excerpt
#: alongside its exact value.
#:
#: An earlier draft OMITTED the value above this threshold and kept only the hash
#: plus the excerpt. That detected drift but not what drifted: a reviewer facing
#: a changed hash on an 80 KB schema had to re-extract the value to see the
#: change — the precise drawback for which the design plan rejected hash-only
#: snapshots. The value is now always stored; the threshold only decides whether
#: a convenience excerpt of the legacy-bearing strings rides along.
_INLINE_VALUE_LIMIT = 8192


def _legacy_excerpt(value: Any, prefix: str = "") -> Dict[str, str]:
    """Every JSON path in ``value`` whose string carries a legacy token."""
    out: Dict[str, str] = {}
    if isinstance(value, dict):
        for key in sorted(value, key=str):
            out.update(_legacy_excerpt(value[key],
                                       "%s.%s" % (prefix, key) if prefix else str(key)))
    elif isinstance(value, (list, tuple)):
        for i, item in enumerate(value):
            out.update(_legacy_excerpt(item, "%s[%d]" % (prefix, i)))
    elif isinstance(value, str) and any(token in value for token in _LEGACY_TOKENS):
        out[prefix or "<root>"] = value
    return out


def _artifact(surface_class: str, producer: str, selector: str, value: Any) -> Dict[str, Any]:
    artifact_id = "%s:%s" % (surface_class, selector)
    canonical = canonical_json(value)
    _assert_value_clean(artifact_id, canonical)
    record: Dict[str, Any] = {
        "artifact_id": artifact_id,
        "surface_class": surface_class,
        "producer": producer,
        "selector": selector,
        "sha256": _sha256(canonical),
        "canonical_length": len(canonical),
    }
    record["value"] = _sorted_deep(value)
    record["value_omitted"] = False
    if len(canonical) > _INLINE_VALUE_LIMIT:
        record["legacy_excerpt"] = _legacy_excerpt(value)
    return record


def _mentions_legacy(value: Any) -> bool:
    blob = canonical_json(value)
    return any(token in blob for token in _LEGACY_TOKENS)


def _served_tools() -> Dict[str, Any]:
    import server
    loop = asyncio.new_event_loop()
    try:
        tools = loop.run_until_complete(server.mcp.list_tools())
    finally:
        loop.close()
    return {t.name: t for t in tools}


#: Tools whose served text MENTIONS a legacy token today — which is not the same
#: as steering callers at a legacy path: `index_profile_component` and
#: `infer_profile_fields` match on a NEGATED mention ("never exposing raw XML").
#: They are frozen anyway, because over-inclusion is the safe direction for a
#: retraction sweep: a false positive costs a fixture row, a false negative costs
#: #160 a missed retraction.
#:
#: This is a FLOOR, not the collection set: `collect_served_artifacts` snapshots
#: every registered tool whose description or parameter schema carries a legacy
#: token, derived from the full FastMCP registry, and only asserts that each name
#: below is among them.
#:
#: The distinction is the whole point. With a fixed list, giving an UNLISTED tool
#: a description that advertises `config.process_kind` or raw process XML would
#: change no source-file count and no frozen artifact — served-contract growth
#: invisible to the gate this issue exists to provide. Deriving makes the new
#: tool appear as a new artifact, which fails the freeze.
#: MEASURED, not hand-listed. An earlier draft asserted a floor containing
#: `build_integration`, `build_from_archetype`, `compose_archetypes`,
#: `import_integration_draft` and `apply_component_edit`; deriving showed that
#: none of them carries a legacy token in its served MCP text at all — their
#: legacy steering lives in the `IntegrationComponentSpec.config` field
#: description (SS-PYDANTIC) and in the schema templates (SS-SCHEMA-TEMPLATES),
#: both captured separately. Conversely the derivation found three surfaces the
#: hand-list never had: `index_profile_component`, `infer_profile_fields` and
#: `review_transformation`. Both directions of that correction are the argument
#: for deriving.
_LEGACY_STEERING_TOOL_FLOOR: Tuple[str, ...] = (
    "get_schema_template", "index_profile_component", "infer_profile_fields",
    "invoke_boomi_api", "manage_component", "manage_connector", "manage_process",
    "prepare_component_edit", "review_transformation",
)


def collect_served_artifacts() -> List[Dict[str, Any]]:
    """Call every real read-only producer and snapshot its exact served value."""
    from boomi_mcp.categories import meta_tools
    from boomi_mcp.categories.components import processes, safe_edit_component

    artifacts: List[Dict[str, Any]] = []

    # --- SS-PYDANTIC -------------------------------------------------
    schema = IntegrationComponentSpec.model_json_schema()
    for field, spec in sorted((schema.get("properties") or {}).items()):
        description = spec.get("description")
        if isinstance(description, str) and _mentions_legacy(description):
            artifacts.append(_artifact(
                "SS-PYDANTIC",
                "IntegrationComponentSpec.model_json_schema()",
                "properties.%s.description" % field,
                " ".join(description.split()),
            ))

    # --- SS-MCP-DESCRIPTIONS -----------------------------------------
    # DERIVED from the full registry: every registered tool whose served text
    # carries a legacy token is snapshotted, so a tool that STARTS advertising
    # one becomes a new artifact and fails the freeze.
    tools = _served_tools()
    steering = sorted(
        name for name, tool in tools.items()
        if _mentions_legacy(tool.description or "")
        or _mentions_legacy(tool.parameters or {})
    )
    missing_floor = [n for n in _LEGACY_STEERING_TOOL_FLOOR if n not in tools]
    if missing_floor:
        raise AssertionError(
            "MCP tool(s) %s are no longer registered — a served surface this "
            "inventory pins has disappeared. Update the floor in the same change "
            "that renames or removes the tool." % missing_floor)
    dropped = [n for n in _LEGACY_STEERING_TOOL_FLOOR if n not in steering]
    if dropped:
        raise AssertionError(
            "MCP tool(s) %s no longer carry any legacy token in their served "
            "text. That may be genuine progress (#160's retraction sweep), but it "
            "must be recorded deliberately: drop them from the floor in the same "
            "change." % dropped)
    for name in steering:
        tool = tools[name]
        artifacts.append(_artifact(
            "SS-MCP-DESCRIPTIONS", "server.mcp.list_tools()",
            "%s.description" % name, tool.description or ""))
        artifacts.append(_artifact(
            "SS-MCP-DESCRIPTIONS", "server.mcp.list_tools()",
            "%s.parameters" % name, tool.parameters or {}))

    # --- SS-SCHEMA-TEMPLATES -----------------------------------------
    for selector, payload in sorted(_schema_template_surfaces(meta_tools).items()):
        if _mentions_legacy(payload) or selector in _ALWAYS_FROZEN_TEMPLATES:
            artifacts.append(_artifact(
                "SS-SCHEMA-TEMPLATES", "meta_tools.get_schema_template_action(...)",
                selector, payload))
    import server as _server_mod
    artifacts.append(_artifact(
        "SS-SCHEMA-TEMPLATES", "server.get_schema_template.__doc__",
        "wrapper_docstring", _server_mod.get_schema_template.__doc__ or ""))

    # --- SS-CAPABILITY-CATALOG ---------------------------------------
    catalog = meta_tools.list_capabilities_action()
    for key in sorted(catalog):
        subtree = catalog[key]
        if _mentions_legacy(subtree):
            artifacts.append(_artifact(
                "SS-CAPABILITY-CATALOG", "meta_tools.list_capabilities_action()",
                str(key), subtree))

    # --- SS-ARCHETYPE-CATALOG ----------------------------------------
    # The SERVED descriptor, not the bare registry row: `list_archetype_registry`
    # returns only {name, version, migrated}, so filtering it for legacy tokens
    # would snapshot almost nothing. The caller-visible surface is the action's
    # description/tags/use_cases, and that is where `process_kind='sync_pipeline'`
    # is actually advertised.
    from boomi_mcp.categories.integration_authoring import (
        list_integration_archetypes_action,
    )
    served_archetypes = list_integration_archetypes_action()
    for entry in served_archetypes.get("archetypes", []):
        name = entry.get("name")
        if name and _mentions_legacy(entry):
            artifacts.append(_artifact(
                "SS-ARCHETYPE-CATALOG",
                "integration_authoring.list_integration_archetypes_action()",
                str(name), entry))

    # --- SS-BUILDER-DIAGNOSTICS --------------------------------------
    # Pure probes: each provably returns its envelope before any client access.
    artifacts.append(_artifact(
        "SS-BUILDER-DIAGNOSTICS", "processes.manage_process_action(action='create')",
        "ACTION_UNSUPPORTED",
        processes.manage_process_action(None, "p", action="create")))
    for selector, envelope in sorted(_builder_diagnostic_envelopes().items()):
        artifacts.append(_artifact(
            "SS-BUILDER-DIAGNOSTICS", "integration_builder plan preflight",
            selector, envelope))

    # --- SS-SAFE-EDIT -------------------------------------------------
    artifacts.append(_artifact(
        "SS-SAFE-EDIT", "safe_edit_component._validate_patch_shape(...)",
        "COMPONENT_EDIT_RAW_XML_UNSUPPORTED",
        safe_edit_component._validate_patch_shape({"config": {"xml": "<x/>"}})))

    # --- SS-RAW-API ---------------------------------------------------
    classification = meta_tools._classify_raw_api_request("POST", "/Component/abc")
    artifacts.append(_artifact(
        "SS-RAW-API", "meta_tools._raw_write_confirmation_guard(...)",
        "RAW_WRITE_CONFIRMATION_REQUIRED",
        meta_tools._raw_write_confirmation_guard("/Component/abc", "POST", classification)))
    artifacts.append(_artifact(
        "SS-RAW-API", "meta_tools._classify_raw_api_request('POST', '/Component/abc')",
        "component_update_classification", classification))
    artifacts.append(_artifact(
        "SS-RAW-API", "meta_tools._typed_alternatives_for_endpoint('/Component/abc')",
        "component_typed_alternatives",
        meta_tools._typed_alternatives_for_endpoint("/Component/abc")))

    artifacts.sort(key=lambda a: (a["surface_class"], a["artifact_id"]))
    return artifacts


#: Served templates frozen whatever their text says, because issue #149 names
#: them as advertised Component-XML write routes that #160 must retract.
#:
#: The token filter is the wrong instrument for these: `_COMPONENT_CLONE`'s
#: served payload is a bare `{name, folder_name, folder_id, description}`
#: skeleton with no legacy vocabulary in it at all — yet clone re-posts
#: platform-sourced XML with the identity attributes stripped (route
#: `WRT-manage-component-clone`), so its advertisement is exactly what the sweep
#: has to find. A surface can advertise a legacy route without naming one.
_ALWAYS_FROZEN_TEMPLATES: Tuple[str, ...] = (
    "resource_type=process|operation=create",
    "resource_type=component|operation=create",
    "resource_type=component|operation=clone",
)


def _schema_template_surfaces(meta_tools: Any) -> Dict[str, Any]:
    """Derive the served schema-template universe by probing the runtime, not by
    hand-listing selectors (the `_specialized_surfaces()` technique from
    tests/test_process_ir_authoring_contract.py)."""
    surfaces: Dict[str, Any] = {}

    for schema_name in meta_tools._valid_schema_names():
        surfaces["schema_name=%s" % schema_name] = \
            meta_tools.get_schema_template_action(schema_name=schema_name)

    for resource_type in meta_tools._VALID_RESOURCE_TYPES:
        overview = meta_tools.get_schema_template_action(resource_type=resource_type)
        surfaces["resource_type=%s" % resource_type] = overview

        # The OVERVIEW is the wrong place to read the operation axis from. The
        # process overview echoes `available_actions: ['list','get']` — the
        # read-only MCP actions, not the template operations, which are
        # `['create','list']` — so following it could never reach
        # `operation='create'`, the surface carrying the raw-XML escape hatch
        # this issue exists to freeze. The authoritative list is echoed on the
        # REFUSAL envelope, so probe with a deliberately invalid operation and
        # read it back; the overview keys stay as a fallback.
        refusal = meta_tools.get_schema_template_action(
            resource_type=resource_type, operation="__m12_12_invalid__")
        operations = _echoed_list(refusal, "valid_operations") or _echoed_list(
            overview, "valid_operations", "available_actions", "valid_actions",
            "operations", "actions")
        component_types = _echoed_list(overview, "valid_component_types",
                                       "component_types")

        for component_type in component_types:
            surfaces["resource_type=%s|component_type=%s" % (resource_type, component_type)] = \
                meta_tools.get_schema_template_action(
                    resource_type=resource_type, component_type=component_type)

        for operation in operations:
            payload = meta_tools.get_schema_template_action(
                resource_type=resource_type, operation=operation)
            key = "resource_type=%s|operation=%s" % (resource_type, operation)
            surfaces[key] = payload
            # `process_protocols` on the process-create payload IS the legacy
            # protocol list — `['database_to_api_sync','wrapper_subprocess',
            # 'sync_pipeline']` — and connector payloads use
            # `available_protocols`. Following only `valid_protocols` walked
            # neither, so the templates the issue calls out as "advertising
            # legacy protocols" were never frozen.
            for protocol in _protocol_axis(payload):
                surfaces["%s|protocol=%s" % (key, protocol)] = \
                    meta_tools.get_schema_template_action(
                        resource_type=resource_type, operation=operation, protocol=protocol)
            for component_type in _echoed_list(payload, "valid_component_types",
                                               "component_types") or component_types:
                ct_key = "%s|component_type=%s" % (key, component_type)
                ct_payload = meta_tools.get_schema_template_action(
                    resource_type=resource_type, operation=operation,
                    component_type=component_type)
                surfaces[ct_key] = ct_payload
                # Connector protocols hang off the component_type payload, one
                # level deeper than the operation payload.
                for protocol in _protocol_axis(ct_payload):
                    surfaces["%s|protocol=%s" % (ct_key, protocol)] = \
                        meta_tools.get_schema_template_action(
                            resource_type=resource_type, operation=operation,
                            component_type=component_type, protocol=protocol)

    assert_schema_surface_axes_non_vacuous(surfaces)
    return surfaces


def assert_schema_surface_axes_non_vacuous(surfaces: Dict[str, Any]) -> None:
    """A walk that descends no axis freezes only the overviews.

    Guard the guard: the acceptance criteria name specific served templates —
    the process-create `raw_xml_escape_hatch`, the `_COMPONENT_CREATE` skeleton
    carrying `type="process"`, and `_COMPONENT_CLONE`. If the axis walk stops at
    the overviews, none of them is pinned and the SS-SCHEMA-TEMPLATES class is
    decorative.
    """
    for axis in ("|operation=", "|component_type=", "|protocol="):
        if not any(axis in key for key in surfaces):
            raise AssertionError(
                "the schema-template walk descended no %s axis — the served "
                "templates this inventory must freeze would not be collected. "
                "An echo key was probably renamed; add the alias." % axis.strip("|="))
    # The three selectors issue #149 names by file:line. Their absence is the
    # exact vacuity that shipped once: the walk ran, produced overviews, and
    # froze none of the templates the retraction sweep has to retract.
    for required in ("resource_type=process|operation=create",
                     "resource_type=component|operation=create",
                     "resource_type=component|operation=clone"):
        if required not in surfaces:
            raise AssertionError(
                "the served template %r was not collected — issue #149 names it "
                "explicitly (raw_xml_escape_hatch / _COMPONENT_CREATE / "
                "_COMPONENT_CLONE)" % required)


#: Every key a served payload uses to echo its protocol axis. `process_protocols`
#: carries the LEGACY protocol list; `available_protocols` carries the connector
#: ones. Following only `valid_protocols` walked neither.
_PROTOCOL_ECHO_KEYS = ("valid_protocols", "process_protocols", "available_protocols",
                       "protocols", "available_standards", "valid_standards")


def _protocol_axis(payload: Any) -> List[str]:
    """Union of every protocol/standard axis the payload advertises."""
    found: List[str] = []
    for key in _PROTOCOL_ECHO_KEYS:
        for value in _echoed_list(payload, key):
            if value not in found:
                found.append(value)
    return found


def _echoed_list(payload: Any, *keys: str) -> List[str]:
    """First echoed list of strings found under any of ``keys``."""
    if not isinstance(payload, dict):
        return []
    for key in keys:
        values = payload.get(key)
        if isinstance(values, (list, tuple)) and values \
                and all(isinstance(v, str) for v in values):
            return [str(v) for v in values]
    return []


def _builder_diagnostic_envelopes() -> Dict[str, Any]:
    """Served builder diagnostics, snapshotted as CALLERS actually receive them.

    Driven through the PUBLIC plan path, not the private preflight. An earlier
    draft called `_process_component_preflight` directly and synthesized a
    `{message, error_code, field, hint}` value — but a caller receives `{error,
    error_code, field, hint}` (plus an optional `details`) nested under
    `steps[].validation_error`. Pinning the internal exception rather than the
    wrapper meant a change to the planner's serialization could alter the served
    contract without moving this artifact, which is precisely the drift this
    surface exists to catch.

    **Nothing here is caught.** An earlier draft wrapped the probe in a bare
    `except Exception` and stored `{"_probe_error": type(exc).__name__}` — which
    silently froze all three of these artifacts as `{"_probe_error":
    "TypeError"}`, so the served text they were supposed to pin was never pinned
    at all, and the transport bomb's `AssertionError` would have been swallowed
    the same way. A probe that cannot reach its envelope must fail the
    derivation, loudly, not record its own failure as the contract.
    """
    from boomi_mcp.categories import integration_builder

    probes = {
        "PROCESS_KIND_REQUIRED": {},
        "PROCESS_KIND_UNSUPPORTED": {"process_kind": "__not_a_real_kind__"},
        "PROCESS_KIND_XML_CONFLICT": {
            "process_kind": "sync_pipeline", "xml": "<Component/>"},
    }
    out: Dict[str, Any] = {}
    for selector, config in probes.items():
        plan = integration_builder._build_plan(None, {"components": [{
            "key": "p", "type": "process", "name": "P",
            # The explicit component_id is what keeps the PUBLIC plan path
            # offline — without one, `_resolve_existing_components` queries
            # component metadata and the probe cannot run without a client. The
            # action is not load-bearing (create with an id is client-free too);
            # the envelopes are identical either way.
            "action": "update", "component_id": "00000000-0000-0000-0000-000000000000",
            "config": config,
        }]})
        envelopes = [
            step["validation_error"] for step in (plan.get("steps") or [])
            if isinstance(step, dict) and step.get("validation_error")
        ]
        if len(envelopes) != 1:
            raise AssertionError(
                "the %s probe yielded %d validation envelopes through the public "
                "plan path — the served diagnostic it pins would silently stop "
                "being frozen" % (selector, len(envelopes)))
        out[selector] = envelopes[0]
        if envelopes[0].get("error_code") != selector:
            raise AssertionError(
                "the %s probe now yields %r — an earlier gate is firing first, so "
                "this artifact would pin the wrong served text"
                % (selector, envelopes[0].get("error_code")))
    return out


def _sdk_method_sources() -> Dict[str, str]:
    """Method-name -> source text, read from the SDK CLASS's own source file.

    Read from the class, not from its attributes, on purpose. The freeze suite
    arms a transport sentinel over `ComponentService.create_component` and
    friends for the whole module; resolving shapes through the live attributes
    would then read the SENTINEL's body and record `resolved: false` for every
    verb — the evidence would depend on whether a test had patched the class.
    A class's source file does not move when its attributes are reassigned.

    Reading the class source also retires the closure-walking the decorated
    `bulk_component` used to need: the file carries the real body, decorator
    and all.
    """
    import inspect as _inspect

    from boomi.services.component import ComponentService

    source = _inspect.getsource(ComponentService)
    tree = ast.parse(textwrap.dedent(source))
    out: Dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            out[node.name] = ast.get_source_segment(
                textwrap.dedent(source), node) or ""
    return out


def _sdk_call_shape(name: str, sources: Dict[str, str]) -> Dict[str, Any]:
    """The HTTP verb, URL template and Accept header a verb actually sends.

    Derived, never asserted: an earlier draft carried
    ``"update_is_post_to_component_id": True`` as a literal, which is a second
    copy of a fact the SDK already states — exactly the defect class this slice
    exists to close. The URL template is a vendor STRING, not a vendor line
    number, so it stays stable across the patch releases ``boomi>=3.0.1`` admits.
    """
    source = sources.get(name)
    if not source:
        return {"resolved": False}

    verbs = re.findall(r'\.set_method\(\s*["\']([A-Z]+)["\']', source)
    urls = re.findall(r'Environment\.DEFAULT\.url\}(/[^"\']*)', source)
    accepts = re.findall(r'\.add_header\(\s*["\']Accept["\']\s*,\s*["\']([^"\']+)["\']', source)
    if not verbs:
        # A verb whose shape cannot be read is not evidence of anything, and
        # `resolved: false` silently reads as "the SDK does not force XML".
        return {"resolved": False}
    return {
        "resolved": True,
        "http_method": verbs[0],
        "url_template": urls[0].replace("{{", "{").replace("}}", "}") if urls else None,
        "accept_header": accepts[0] if accepts else None,
    }


def sdk_evidence() -> Dict[str, Any]:
    """Semantic SDK facts only — never vendor paths or line numbers, which drift
    independently of this repository under ``boomi>=3.0.1``. An intentional SDK
    upgrade must rebaseline this block explicitly.

    The installed VERSION STRING is deliberately absent. ``server.py:206-207``
    appends a sibling ``../boomi-python/src`` checkout to ``sys.path`` when one
    exists, so ``importlib.metadata.version("boomi")`` re-walks the path and
    answers differently before and after ``import server`` on a developer
    machine that has the checkout (measured here: ``3.0.1`` then ``2.1.0``),
    while answering consistently on a machine that does not. The imported MODULE
    is unaffected — it stays the installed distribution, which is what every
    fact below is read from — but freezing the metadata string would pin a
    machine-local accident. The shape facts are the better rebaseline signal
    anyway: an SDK upgrade that matters moves a verb, an enum, or a call shape.
    """
    import inspect as _inspect

    from boomi.models import component_bulk_request as cbr_module
    from boomi.services.component import ComponentService

    method_sources = _sdk_method_sources()

    # The envelope's `type` enum: found by scanning the model's own module for a
    # member enum, so a rename inside the vendor package surfaces as a diff.
    enums: Dict[str, List[str]] = {}
    for name in dir(cbr_module):
        obj = getattr(cbr_module, name, None)
        members = getattr(obj, "__members__", None)
        if isinstance(obj, type) and members:
            enums[name] = sorted(str(m) for m in members)

    init_params = sorted(
        p for p in _inspect.signature(cbr_module.ComponentBulkRequest.__init__).parameters
        if p != "self"
    )
    optional_init_params = sorted(
        name for name, param in
        _inspect.signature(cbr_module.ComponentBulkRequest.__init__).parameters.items()
        if name != "self" and param.default is not _inspect.Parameter.empty
    )

    return {
        "component_service_write_verbs": sorted(
            m for m in dir(ComponentService)
            if not m.startswith("_") and m.split("_", 1)[0] in {"create", "update"}),
        "component_service_bulk_verbs": sorted(
            m for m in dir(ComponentService) if m.startswith("bulk_")),
        "component_bulk_request_init_params": init_params,
        "component_bulk_request_optional_init_params": optional_init_params,
        "component_bulk_request_enums": enums,
        "call_shapes": {
            name: _sdk_call_shape(name, method_sources)
            for name in ("create_component", "update_component", "bulk_component")
        },
    }


# ======================================================================
# Ledger projection, allowlist and retraction matrix
# ======================================================================

#: Which endgame issue owns each census family's rows by default. A row whose
#: path matches a more specific rule below overrides this.
_CENSUS_DEFAULT_OWNER = {
    "registry_lookup": "#160",
    "renderer_call": "#160",
    "legacy_transitive_call": "#160",
    "legacy_emitter": "#160",
    "legacy_semantic_validation": "#160",
    "component_xml_write": "#160",
    "raw_api_invoker": "#160",
    "process_kind_producer": "#159",
    "process_kind_consumer": "#160",
    "example_producer": "#159",
    "authoring_boundary": "#159",
    "unclassified_dynamic": "#160",
}

#: Path-scoped ownership overrides, most specific first. Each entry is
#: ``(path prefix, census kinds or None for all, owning issue, disposition)``.
_OWNERSHIP_RULES: Tuple[Tuple[str, Optional[Tuple[str, ...]], str, str], ...] = (
    ("src/boomi_mcp/compiler/process_ir/legacy_adapters/sync_pipeline.py", None, "#151",
     "re-home onto the neutral extraction"),
    ("src/boomi_mcp/compiler/process_ir/legacy_adapters/wrapper_subprocess.py", None, "#151",
     "re-home onto the neutral extraction"),
    ("src/boomi_mcp/compiler/process_ir/legacy_adapters/", None, "#151",
     "re-home onto the neutral extraction"),
    ("src/boomi_mcp/compiler/process_ir/semantic_validation/legacy_bridge.py", None, "#151",
     "re-home onto the neutral extraction"),
    ("src/boomi_mcp/models/_process_ir_compat.py", None, "#159",
     "migrate the compatibility codec, then delete"),
    ("src/boomi_mcp/patterns/archetypes/", None, "#159",
     "migrate the archetype to canonical ProcessIR"),
    ("examples/", None, "#159", "migrate the example to canonical ProcessIR"),
    ("src/boomi_mcp/categories/components/safe_edit_component.py", None, "#160",
     "retract the served raw-XML steer; body edits move to canonical apply"),
    ("src/boomi_mcp/categories/components/builders/process_emitters/legacy.py", None, "#160",
     "delete with the legacy renderer"),
    ("src/boomi_mcp/categories/components/builders/process_flow_builder.py", None, "#160",
     "delete the legacy semantic shell"),
    ("src/boomi_mcp/categories/meta_tools.py", None, "#160",
     "retract the served legacy guidance / guard the raw route"),
    ("src/boomi_mcp/categories/integration_builder.py", ("registry_lookup", "renderer_call"),
     "#153", "replace with canonical ProcessIR materialization/apply"),
)


def _route_disposition(path: str, symbol: str) -> Optional[Tuple[str, str]]:
    """The owning issue and disposition of the write ROUTE claiming this symbol.

    The classification table is the authority for Component-XML write sites, so
    the ledger projection must consult it instead of falling through to a census
    default. Without this, `shared_resources.py::_get_channel_raw_json` — a
    deliberate lossless GET classified `typed_non_process` / "leave unchanged" —
    was ALSO given the generic "guard behind the shared process-content
    classifier" disposition, while its caller was told to "delete or re-home".
    §11 then handed #160 three instructions for one function, one of which would
    have removed a preservation-critical read.
    """
    location = "%s::%s" % (path, symbol.split(".")[0])
    claiming = [r for r in WRITE_ROUTES if location in r["locations"]]
    if not claiming:
        return None
    issues = sorted({r["owning_issue"] for r in claiming})
    dispositions = sorted({
        "%s: %s" % (r["route_id"], r["post_retraction_assertion"]) for r in claiming
    })
    return ("/".join(issues), " · ".join(dispositions))


def _own(path: str, census: str, symbol: str = "") -> Tuple[str, str]:
    # A transitive row is an EDGE and its fate follows its callee, so it never
    # takes a path-scoped disposition of its own — a path rule would have it
    # issue an instruction ("delete the legacy semantic shell") for a site it
    # does not own, which is how §11 acquired contradictory guidance.
    if census == "legacy_transitive_call":
        return (_CENSUS_DEFAULT_OWNER[census], _DEFAULT_DISPOSITION[census])
    if census in ("component_xml_write", "raw_api_invoker"):
        routed = _route_disposition(path, symbol)
        if routed:
            return routed
    for prefix, kinds, issue, disposition in _OWNERSHIP_RULES:
        if path.startswith(prefix) and (kinds is None or census in kinds):
            return issue, disposition
    return _CENSUS_DEFAULT_OWNER[census], _DEFAULT_DISPOSITION[census]


_DEFAULT_DISPOSITION = {
    # A transitive row is an EDGE, not a site: it exists because its callee
    # bears a legacy path. Phrasing it as an instruction ("delete or re-home")
    # contradicted the callee's own row whenever that row said "leave unchanged"
    # — which is exactly the case for the lossless channel GET.
    "legacy_transitive_call": "follow the callee's row; this is an edge, not a site",
    "registry_lookup": "delete with the legacy registry",
    "renderer_call": "delete with the legacy renderer",
    "legacy_emitter": "delete with the legacy emitters",
    "legacy_semantic_validation": "delete with the legacy semantic shell",
    "component_xml_write": "guard behind the shared process-content classifier",
    "raw_api_invoker": "guard behind the canonical endpoint parser",
    "process_kind_producer": "migrate the producer to canonical ProcessIR",
    "process_kind_consumer": "delete with the legacy consumer",
    "example_producer": "migrate the example to canonical ProcessIR",
    "authoring_boundary": "migrate the boundary to canonical ProcessIR",
    "unclassified_dynamic": "resolve or guard the dynamic access",
}


def ledger_rows(census_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deduplicate the full census to ``(census, path, symbol)`` granularity —
    the grain the human ledger and #160's checklist actually work at.

    The full per-form census stays in the JSON; this projection is what §11.2–
    §11.5 tabulate and what the two-way completeness test joins on.
    """
    grouped: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for row in census_rows:
        key = (row["census"], row["path"], row["symbol"])
        entry = grouped.get(key)
        if entry is None:
            issue, disposition = _own(row["path"], row["census"], row["symbol"])
            grouped[key] = {
                "ledger_id": _row_id("LG", key),
                "census": row["census"],
                "path": row["path"],
                "symbol": row["symbol"],
                "forms": [row["form"]],
                "sites": row["count"],
                "evidence_line": row["evidence_line"],
                "owning_issue": issue,
                "disposition": disposition,
            }
        else:
            entry["forms"].append(row["form"])
            entry["sites"] += row["count"]
            entry["evidence_line"] = min(entry["evidence_line"], row["evidence_line"])
    out = []
    for entry in grouped.values():
        entry["forms"] = sorted(set(entry["forms"]))
        out.append(entry)
    out.sort(key=lambda r: (r["census"], r["path"], r["symbol"]))
    return out


#: One row per served surface CLASS — the matrix #160's retraction sweep executes.
RETRACTION_MATRIX: Tuple[Dict[str, Any], ...] = (
    {
        "surface_id": "SS-PYDANTIC",
        "surface_class": "Pydantic schema / field descriptions",
        "producer": "IntegrationComponentSpec.model_json_schema()",
        "anchors": ("src/boomi_mcp/models/integration_models.py:20-34 "
                    "(description string :22-33)",),
        "legacy_guidance": "steers callers to \"manage_component for an explicit raw process "
                           "XML escape hatch\" and advertises process_kind",
        "owning_issue": "#160",
        "post_retraction_assertion": "the served config description names no raw process XML "
                                     "escape hatch and no legacy process_kind.",
    },
    {
        "surface_id": "SS-BUILDER-DIAGNOSTICS",
        "surface_class": "Builder error texts and hints",
        "producer": "integration_builder plan preflight envelopes",
        "anchors": ("src/boomi_mcp/categories/integration_builder.py:3148",
                    "src/boomi_mcp/categories/integration_builder.py:3162",
                    "src/boomi_mcp/categories/integration_builder.py:3544",
                    "src/boomi_mcp/categories/integration_builder.py:3610",
                    "src/boomi_mcp/categories/integration_builder.py:5318",
                    "src/boomi_mcp/categories/integration_builder.py:5403-5430"),
        "legacy_guidance": "PROCESS_KIND_* hints enumerate sorted(PROCESS_FLOW_BUILDERS) and "
                           "PROCESS_KIND_XML_CONFLICT actively steers raw process XML onto "
                           "manage_component(type=\"component\", config.xml)",
        "owning_issue": "#160",
        "post_retraction_assertion": "no served builder envelope names a legacy process_kind "
                                     "value or routes raw process XML to another tool.",
    },
    {
        "surface_id": "SS-MCP-DESCRIPTIONS",
        "surface_class": "Registered MCP tool descriptions and parameter schemas",
        "producer": "server.mcp.list_tools()",
        "anchors": ("server.py:1383-1385 (manage_process)",
                    "server.py:1712 (manage_component tool)",
                    "server.py:1731-1732 (manage_component escape-hatch blessing)",
                    "server.py:2098 (manage_connector raw create)",
                    "server.py:2103 (manage_connector raw update)"),
        "legacy_guidance": "tool descriptions bless raw process XML as an escape hatch and "
                           "advertise raw-XML connector create/update",
        "owning_issue": "#160",
        "post_retraction_assertion": "no registered tool description blesses raw process XML.",
    },
    {
        "surface_id": "SS-SAFE-EDIT",
        "surface_class": "Safe-edit guidance",
        "producer": "safe_edit_component._validate_patch_shape(...)",
        "anchors": ("src/boomi_mcp/categories/components/safe_edit_component.py:176-191",),
        "legacy_guidance": "the raw-XML refusal steers callers to "
                           "manage_component(action='update') with config.xml",
        "owning_issue": "#160",
        "post_retraction_assertion": "the refusal names no full-replacement escape hatch.",
    },
    {
        "surface_id": "SS-SCHEMA-TEMPLATES",
        "surface_class": "get_schema_template templates",
        "producer": "meta_tools.get_schema_template_action(...)",
        "anchors": ("src/boomi_mcp/categories/meta_tools.py:595-598 (raw_xml_escape_hatch)",
                    "src/boomi_mcp/categories/meta_tools.py:762-785 (_COMPONENT_CREATE, "
                    "type=\"process\" at :770, workflow step 4 at :778-784)",
                    "src/boomi_mcp/categories/meta_tools.py:4989 (force-clear hint)",
                    "src/boomi_mcp/categories/meta_tools.py:5180-5190 (_COMPONENT_CLONE)",
                    "src/boomi_mcp/categories/meta_tools.py:8867 (serves _COMPONENT_CREATE)",
                    "src/boomi_mcp/categories/meta_tools.py:8880 (serves _COMPONENT_CLONE)"),
        "legacy_guidance": "served templates carry type=\"process\" raw XML, the "
                           "raw_xml_escape_hatch text, and legacy process protocols",
        "owning_issue": "#160",
        "post_retraction_assertion": "no served template emits a process-typed raw XML "
                                     "skeleton or a raw-XML escape-hatch instruction.",
    },
    {
        "surface_id": "SS-RAW-API",
        "surface_class": "Raw-API catalog and typed-alternative entries",
        "producer": "meta_tools._raw_write_confirmation_guard / "
                    "_classify_raw_api_request / _typed_alternatives_for_endpoint",
        "anchors": ("src/boomi_mcp/categories/meta_tools.py:5622 "
                    "(_classify_raw_api_request)",
                    "src/boomi_mcp/categories/meta_tools.py:5728-5760 "
                    "(_raw_write_confirmation_guard)",
                    "src/boomi_mcp/categories/meta_tools.py:5763 (invoke_api)",
                    "src/boomi_mcp/categories/meta_tools.py:5811 (transport interpolation)"),
        "legacy_guidance": "the raw invoker is type-unrestricted, so POST/PUT to /Component "
                           "can mint or replace a process; classification splits its own copy "
                           "of the endpoint while transport interpolates the raw string",
        "owning_issue": "#160",
        "post_retraction_assertion": "ONE canonical endpoint parser feeds classification, ID "
                                     "extraction AND transport; every update-shaped call runs "
                                     "the two-sided process check; `bulk` is matched before "
                                     "the <id> arm and never treated as a componentId.",
    },
    {
        "surface_id": "SS-CAPABILITY-CATALOG",
        "surface_class": "list_capabilities catalog entries, workflows and hints",
        "producer": "meta_tools.list_capabilities_action()",
        "anchors": ("src/boomi_mcp/categories/meta_tools.py:10145 "
                    "(list_capabilities_action)",),
        "legacy_guidance": "catalog entries, workflow steps and hints reference legacy "
                           "process protocols and the raw-XML route",
        "owning_issue": "#160",
        "post_retraction_assertion": "no catalog entry, workflow step or hint references a "
                                     "legacy process_kind or the raw process XML route.",
    },
    {
        "surface_id": "SS-ARCHETYPE-CATALOG",
        "surface_class": "Served archetype descriptors and parameter schemas",
        "producer": "authoring.contract.list_archetype_registry()",
        "anchors": ("src/boomi_mcp/authoring/contract.py:335 "
                    "(list_archetype_registry)",
                    "src/boomi_mcp/categories/integration_authoring.py:205 "
                    "(list_integration_archetypes_action)"),
        "legacy_guidance": "every registered archetype whose emitted spec carries a legacy "
                           "process_kind advertises it through the served descriptor",
        "owning_issue": "#159",
        "post_retraction_assertion": "no served archetype descriptor emits or documents a "
                                     "legacy process_kind.",
    },
)


# ======================================================================
# Inventory assembly
# ======================================================================

def build_inventory(sources: Optional[Dict[str, str]] = None,
                    examples: Optional[Dict[str, Any]] = None,
                    include_served: bool = True) -> Dict[str, Any]:
    """Derive the whole inventory.

    ``sources``/``examples`` are injectable so the mutation test can overlay a
    synthetic legacy caller in memory, without writing to the repository or a
    ``tmp_path``.
    """
    vocab = legacy_sink_vocabulary()
    assert_vocabulary_non_vacuous(vocab)

    src = python_sources() if sources is None else dict(sources)
    ex = example_documents() if examples is None else dict(examples)

    census = scan_sources(src, vocab)
    census.extend(scan_examples(ex))
    census.extend(authoring_boundaries())
    census.sort(key=lambda r: (r["census"], r["path"], r["symbol"], r["form"]))

    reconciliation = reconcile_routes(census)

    document: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "baseline": {
            "sha": BASELINE_SHA,
            "branch": BASELINE_BRANCH,
            "capture_date": CAPTURE_DATE,
            "scanner_version": SCANNER_VERSION,
            "issue": "#149",
        },
        "scan_contract": {
            "roots": list(SCAN_ROOTS),
            "python_source_count": len(src),
            "example_document_count": len(ex),
            "census_kinds": list(CENSUS_KINDS),
            "frozen_key": ["census", "path", "symbol", "form"],
            "excluded_from_equality": [
                "evidence_line", "column offsets", "source text", "formatting",
                "comments and docstrings", "argument values", "intra-file ordering",
                "vendor SDK paths and line numbers",
            ],
            "vocabulary": {k: list(v) for k, v in sorted(vocab.items())},
        },
        "census": census,
        "ledger_rows": ledger_rows(census),
        "component_xml_write_routes": [dict(r, locations=list(r["locations"]))
                                       for r in WRITE_ROUTES],
        "route_reconciliation": reconciliation,
        "served_surface_retraction_matrix": [dict(r, anchors=list(r["anchors"]))
                                             for r in RETRACTION_MATRIX],
        "sdk_evidence": sdk_evidence(),
    }
    if include_served:
        document["served_artifacts"] = collect_served_artifacts()
    return document


def semantic_key(row: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (row["census"], row["path"], row["symbol"], row["form"])


class Diff:
    """The comparison result. Truthy when anything drifted."""

    def __init__(self) -> None:
        self.added: List[str] = []
        self.removed: List[str] = []
        self.count_changes: List[str] = []
        self.artifact_added: List[str] = []
        self.artifact_removed: List[str] = []
        self.artifact_changed: List[str] = []
        self.scalar_changes: List[str] = []

    @property
    def added_ids(self) -> List[str]:
        return list(self.added)

    def empty(self) -> bool:
        return not any((self.added, self.removed, self.count_changes,
                        self.artifact_added, self.artifact_removed,
                        self.artifact_changed, self.scalar_changes))

    def __bool__(self) -> bool:
        return not self.empty()

    def report(self) -> str:
        lines: List[str] = []
        for label, items in (
            ("new census rows", self.added),
            ("removed census rows", self.removed),
            ("changed call counts", self.count_changes),
            ("new served artifacts", self.artifact_added),
            ("removed served artifacts", self.artifact_removed),
            ("changed served artifacts", self.artifact_changed),
            ("changed scan-contract values", self.scalar_changes),
        ):
            if items:
                lines.append("%s (%d):" % (label, len(items)))
                lines.extend("  - %s" % i for i in items)
        return "\n".join(lines) if lines else "no drift"


def compare(current: Dict[str, Any], baseline: Dict[str, Any]) -> Diff:
    diff = Diff()

    cur = {semantic_key(r): r for r in current.get("census", [])}
    base = {semantic_key(r): r for r in baseline.get("census", [])}
    for key in sorted(set(cur) - set(base)):
        diff.added.append("%s | %s | %s | %s" % key)
    for key in sorted(set(base) - set(cur)):
        diff.removed.append("%s | %s | %s | %s" % key)
    for key in sorted(set(cur) & set(base)):
        if cur[key]["count"] != base[key]["count"]:
            diff.count_changes.append(
                "%s | %s | %s | %s: %s -> %s"
                % (key + (base[key]["count"], cur[key]["count"])))

    cur_a = {a["artifact_id"]: a for a in current.get("served_artifacts", [])}
    base_a = {a["artifact_id"]: a for a in baseline.get("served_artifacts", [])}
    diff.artifact_added = sorted(set(cur_a) - set(base_a))
    diff.artifact_removed = sorted(set(base_a) - set(cur_a))
    for aid in sorted(set(cur_a) & set(base_a)):
        if cur_a[aid]["sha256"] != base_a[aid]["sha256"]:
            diff.artifact_changed.append(aid)

    for field in ("python_source_count", "example_document_count"):
        c = (current.get("scan_contract") or {}).get(field)
        b = (baseline.get("scan_contract") or {}).get(field)
        if c != b:
            diff.scalar_changes.append("scan_contract.%s: %s -> %s" % (field, b, c))
    cv = (current.get("scan_contract") or {}).get("vocabulary") or {}
    bv = (baseline.get("scan_contract") or {}).get("vocabulary") or {}
    for family in sorted(set(cv) | set(bv)):
        if cv.get(family) != bv.get(family):
            diff.scalar_changes.append(
                "scan_contract.vocabulary.%s: %s -> %s"
                % (family, bv.get(family), cv.get(family)))

    # Every remaining frozen section, compared by canonical value.
    #
    # An earlier draft stopped after the census, the artifact hashes and two
    # counts — so an allowed `boomi>=3.0.1` upgrade that moved `update_component`
    # from POST to PUT, or an edit to an ownership rule that re-assigned a path
    # from #160 to #151, left `diff.empty()` true while the ledger tests kept
    # comparing the DOCUMENT against the old fixture. A section the inventory
    # declares frozen and the comparator does not read is not frozen.
    for section in ("sdk_evidence", "ledger_rows", "component_xml_write_routes",
                    "served_surface_retraction_matrix", "route_reconciliation"):
        c = _without_evidence_lines(current.get(section))
        b = _without_evidence_lines(baseline.get(section))
        if canonical_json(c) != canonical_json(b):
            diff.scalar_changes.extend(_section_delta(section, c, b))
    return diff


def _without_evidence_lines(value: Any) -> Any:
    """Drop `evidence_line` before comparing a section.

    `ledger_rows` carries it for human navigation, and the scan contract says
    plainly that line numbers are excluded from equality. Comparing rows verbatim
    would put that brittleness straight back: inserting a blank line at the top of
    `integration_builder.py` shifts every downstream row and would fail the gate
    for an edit that changed no reachability at all.
    """
    if isinstance(value, dict):
        return {k: _without_evidence_lines(v)
                for k, v in value.items() if k != "evidence_line"}
    if isinstance(value, list):
        return [_without_evidence_lines(v) for v in value]
    return value


def _identity_of(section: str, row: Any) -> Optional[str]:
    if not isinstance(row, dict):
        return None
    for key in ("ledger_id", "route_id", "surface_id", "artifact_id"):
        if key in row:
            return str(row[key])
    return None


def _section_delta(section: str, current: Any, baseline: Any) -> List[str]:
    """Row-level detail for a changed section, so the failure names WHAT moved
    rather than just WHICH section did."""
    if isinstance(current, list) and isinstance(baseline, list) \
            and all(_identity_of(section, r) for r in current + baseline):
        cur = {_identity_of(section, r): r for r in current}
        base = {_identity_of(section, r): r for r in baseline}
        out = ["%s: added %s" % (section, rid) for rid in sorted(set(cur) - set(base))]
        out += ["%s: removed %s" % (section, rid) for rid in sorted(set(base) - set(cur))]
        out += ["%s: changed %s" % (section, rid)
                for rid in sorted(set(cur) & set(base))
                if canonical_json(cur[rid]) != canonical_json(base[rid])]
        return out
    if isinstance(current, dict) and isinstance(baseline, dict):
        return ["%s.%s: %s -> %s" % (section, key,
                                     canonical_json(baseline.get(key)),
                                     canonical_json(current.get(key)))
                for key in sorted(set(current) | set(baseline))
                if canonical_json(current.get(key)) != canonical_json(baseline.get(key))]
    return ["%s changed" % section]


def load_baseline(path: Optional[Path] = None) -> Dict[str, Any]:
    target = path or (_ROOT / FIXTURE_RELPATH)
    return json.loads(target.read_text(encoding="utf-8"))


def dumps(document: Dict[str, Any]) -> str:
    return json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


# ======================================================================
# Markdown emission — §11 tables are GENERATED, never hand-written
# ======================================================================

_MD_ESCAPE = str.maketrans({"|": "\\|", "\n": " "})


def _cell(value: Any) -> str:
    return str(value).translate(_MD_ESCAPE).strip()


def _table(headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> str:
    out = ["| " + " | ".join(headers) + " |",
           "|" + "|".join("---" for _ in headers) + "|"]
    for row in rows:
        out.append("| " + " | ".join(_cell(c) for c in row) + " |")
    return "\n".join(out)


#: How the census kinds are split across the ledger tables. This must PARTITION
#: `CENSUS_KINDS` — every kind in exactly one section — or a census family would
#: be frozen in the JSON while appearing nowhere in the document #160 reads.
#: `test_the_ledger_sections_partition_every_census_kind` enforces it.
_LEDGER_SECTIONS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("reachability", ("registry_lookup", "renderer_call", "legacy_transitive_call",
                      "legacy_emitter", "legacy_semantic_validation",
                      "unclassified_dynamic")),
    ("producers", ("process_kind_producer", "process_kind_consumer",
                   "example_producer", "authoring_boundary")),
    ("writes", ("component_xml_write", "raw_api_invoker")),
)


def ledger_section_kinds() -> Dict[str, Tuple[str, ...]]:
    return dict(_LEDGER_SECTIONS)


def emit_ledger_table(document: Dict[str, Any], section: str) -> str:
    kinds = dict(_LEDGER_SECTIONS)[section]
    rows = [r for r in document["ledger_rows"] if r["census"] in kinds]
    return _table(
        ("Ledger ID", "Census", "Path", "Symbol", "Sites", "Baseline line",
         "Owning issue", "Disposition"),
        [(r["ledger_id"], r["census"], r["path"], r["symbol"], r["sites"],
          r["evidence_line"] or "-", r["owning_issue"], r["disposition"])
         for r in rows],
    )


def emit_route_table(document: Dict[str, Any]) -> str:
    return _table(
        ("Route ID", "Derived sink location(s)", "Classification", "Summary",
         "Owning issue", "Required post-retraction assertion"),
        [(r["route_id"], "<br>".join(r["locations"]), r["classification"],
          r["summary"], r["owning_issue"], r["post_retraction_assertion"])
         for r in document["component_xml_write_routes"]],
    )


def emit_matrix_table(document: Dict[str, Any]) -> str:
    """The retraction matrix, carrying the frozen artifact IDs themselves.

    The plan's column spec says "Frozen artifact IDs" and the acceptance
    criterion says #160 executes the sweep from the matrix alone. A COUNT
    satisfies neither: it tells the reader how many strings to retract without
    telling them which. The `producer` column likewise names every producer that
    actually contributes to the row, derived from the artifacts rather than
    hand-written, so it cannot drift from what was collected.
    """
    artifacts_by_class: Dict[str, List[str]] = {}
    producers_by_class: Dict[str, Set[str]] = {}
    for artifact in document.get("served_artifacts", []):
        artifacts_by_class.setdefault(artifact["surface_class"], []).append(
            artifact["artifact_id"])
        producers_by_class.setdefault(artifact["surface_class"], set()).add(
            artifact["producer"])
    return _table(
        ("Surface ID", "Surface class", "Caller-visible producer(s)",
         "HEAD source anchor(s)", "Frozen artifact IDs", "Legacy guidance exposed",
         "Owning endgame step", "Required post-retraction assertion"),
        [(r["surface_id"], r["surface_class"],
          "<br>".join(sorted(producers_by_class.get(r["surface_id"], set()))) or "—",
          "<br>".join(r["anchors"]),
          "<br>".join(sorted(artifacts_by_class.get(r["surface_id"], []))) or "—",
          r["legacy_guidance"], r["owning_issue"], r["post_retraction_assertion"])
         for r in document["served_surface_retraction_matrix"]],
    )


def emit_allowlist_table(document: Dict[str, Any]) -> str:
    counts: Dict[Tuple[str, str], int] = {}
    for row in document["ledger_rows"]:
        counts[(row["owning_issue"], row["disposition"])] = \
            counts.get((row["owning_issue"], row["disposition"]), 0) + 1
    return _table(
        ("Owning issue", "Disposition", "Ledger rows"),
        [(issue, disposition, n) for (issue, disposition), n in sorted(counts.items())],
    )


def emit_section_11_markdown(document: Dict[str, Any]) -> Dict[str, str]:
    """The generated tables, keyed by the subsection that must contain them
    verbatim. `test_the_markdown_tables_are_regenerable_from_the_json` asserts
    each one appears in the ledger byte-for-byte."""
    return {
        "11.2": emit_ledger_table(document, "reachability"),
        "11.3": emit_ledger_table(document, "producers"),
        "11.4-routes": emit_route_table(document),
        "11.4-sites": emit_ledger_table(document, "writes"),
        "11.5": emit_allowlist_table(document),
        "11.6": emit_matrix_table(document),
    }


# ======================================================================
# Ledger parsing (the two-way completeness check)
# ======================================================================

INVENTORY_DOC = "docs/architecture/M12_COMPATIBILITY_INVENTORY.md"

_LEDGER_ID = re.compile(r"^\|\s*(LG-[0-9a-f]{8})\s*\|")
_ROUTE_ID = re.compile(r"^\|\s*(WRT-[a-z0-9-]+)\s*\|")
_SURFACE_ID = re.compile(r"^\|\s*(SS-[A-Z-]+)\s*\|")


def section_11_text() -> str:
    text = (_ROOT / INVENTORY_DOC).read_text(encoding="utf-8")
    marker = "\n# 11. Issue #149"
    idx = text.find(marker)
    if idx < 0:
        raise AssertionError(
            "section 11 not found in %s — the #149 ledger must be appended as a "
            "top-level `# 11. Issue #149 …` section." % INVENTORY_DOC)
    return text[idx:]


def parse_section_11_ids() -> Dict[str, List[str]]:
    body = section_11_text()
    ledger, routes, surfaces = [], [], []
    for line in body.splitlines():
        m = _LEDGER_ID.match(line)
        if m:
            ledger.append(m.group(1))
            continue
        m = _ROUTE_ID.match(line)
        if m:
            routes.append(m.group(1))
            continue
        m = _SURFACE_ID.match(line)
        if m:
            surfaces.append(m.group(1))
    return {"ledger_ids": ledger, "route_ids": routes, "surface_ids": surfaces}


# ======================================================================
# CLI
# ======================================================================

def _main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--write", metavar="PATH", default=None,
                        help="regenerate the committed baseline at PATH")
    parser.add_argument("--check", action="store_true",
                        help="compare the derivation to the committed baseline (default)")
    parser.add_argument("--emit-markdown", action="store_true",
                        help="print the generated section-11 tables")
    args = parser.parse_args(argv)

    document = build_inventory()

    if args.emit_markdown:
        for key, table in sorted(emit_section_11_markdown(document).items()):
            print("\n<!-- generated: %s -->" % key)
            print(table)
        return 0

    if args.write:
        target = Path(args.write)
        if not target.is_absolute():
            target = _ROOT / target
        try:
            previous = json.loads(target.read_text(encoding="utf-8"))
        except FileNotFoundError:
            previous = None
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(dumps(document), encoding="utf-8")
        if previous is None:
            print("wrote NEW baseline %s (%d census rows, %d served artifacts)"
                  % (target, len(document["census"]),
                     len(document.get("served_artifacts", []))))
        else:
            print("rebaselined %s\n%s" % (target, compare(document, previous).report()))
        return 0

    baseline = load_baseline()
    diff = compare(document, baseline)
    print(diff.report())
    return 1 if diff else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())
