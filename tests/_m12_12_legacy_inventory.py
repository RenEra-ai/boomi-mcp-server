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

**The invariant and its universe.** Every syntactic occurrence of a watched
symbol, a producer selector, or a Component-API endpoint literal is either
positively classified into a census kind or emitted as ``unclassified_reference``
residue — so a spelling nobody anticipated becomes visible rather than vanishing.
That is a property of the SOURCE TEXT, and the universe is bounded accordingly:

* Python files under the scan roots, parsed with :mod:`ast`. A wholly computed
  identifier with no literal anywhere (``"".join(["get_process_flow_", "builder"])``,
  ``"get_process_flow_%s" % "builder"``) has nothing to observe and is an inherent
  limit of a static scan, not an oversight.
* Non-Python assets are NOT read; their count is frozen by
  :func:`unscanned_assets` so their arrival is a diff.
* **Statement order is excluded at EVERY scope.** Bindings are pre-indexed
  before any body is scanned: ``visit_Module`` for the module namespace, and
  ``_scoped`` for each function's own locals. Python fixes a scope's local
  NAMES at compile time and only their values at run time, so a conservative
  reachability instrument is right to bind flow-insensitively — it
  over-approximates, which is fail-closed. Binding in traversal order was
  fail-OPEN in both halves, and not merely for a derived sub-row: an alias
  assigned after a nested ``def`` made the ENTIRE nested caller vanish, with no
  row and no residue.
* Runtime behaviour is not modelled at all — this instrument reports where the
  legacy paths ARE, and #160 owns enforcement.

Stating the boundary is part of the contract: an inventory that claims
completeness it cannot have is exactly the failure this slice exists to prevent.
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
SCAN_ROOTS: Tuple[str, ...] = ("*.py (repo root)", "src/boomi_mcp", "scripts", "examples")

#: HTTP client modules a hand-rolled request can go through, bypassing the SDK
#: entirely. `monitoring.py` already drives `httpx` against the platform with
#: real credentials, so this is an in-repo idiom rather than a hypothetical: a
#: POST to `/Component` through one of these is a Component-XML write route the
#: SDK-derived sink vocabulary cannot see.
_HTTP_CLIENT_MODULES: Tuple[str, ...] = ("httpx", "requests", "urllib", "aiohttp",
                                         "http.client", "urllib3")

#: The verbs on those clients that can carry a body to the platform.
_HTTP_WRITE_VERBS: Tuple[str, ...] = ("post", "put", "patch", "delete", "request",
                                      "send", "stream", "urlopen")

CENSUS_KINDS: Tuple[str, ...] = (
    "registry_lookup",
    "renderer_call",
    "legacy_transitive_call",
    "legacy_emitter",
    "legacy_semantic_validation",
    "component_xml_write",
    "http_client_call",
    "raw_api_invoker",
    "process_kind_producer",
    "process_kind_consumer",
    "example_producer",
    "authoring_boundary",
    "unclassified_dynamic",
    "unclassified_reference",
)

PRODUCER_SELECTORS: Tuple[str, ...] = ("process_kind", "process_type")

ROUTE_CLASSIFICATIONS: Tuple[str, ...] = (
    "raw_process_capable",
    "platform_sourced_rematerialization",
    "legacy_structured_process",
    "preserve",
    "dormant",
    "typed_non_process",
    "external_transport",
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
    """``{repo-relative posix path: source text}`` for every scanned Python file.

    EVERY repo-root module, not just `server.py`. The root holds twelve more,
    including the production entry point `server_http.py` and a set of runtime
    patch modules; scanning only `server.py` left a legacy caller in any of them
    invisible, and `python_source_count` could not move for an edit to one.
    `scripts/` is scanned for the same reason — it is caller-reachable Python
    that can construct a legacy config.
    """
    visible = _repository_files()
    out: Dict[str, str] = {}
    for path in sorted(_ROOT.glob("*.py")):
        rel = path.name
        if visible is None or rel in visible:
            out[rel] = path.read_text(encoding="utf-8")
    for root in ("src/boomi_mcp", "scripts"):
        base = _ROOT / root
        if not base.is_dir():
            continue
        for path in sorted(base.rglob("*.py")):
            rel = path.relative_to(_ROOT).as_posix()
            if visible is None or rel in visible:
                out[rel] = path.read_text(encoding="utf-8")
    return dict(sorted(out.items()))


def _repository_files() -> Optional["frozenset[str]"]:
    """Paths git considers part of the repository, or None if git is unavailable.

    The scan walked the FILESYSTEM, so it swept up gitignored files — notably
    `scripts/provision_qa_noop_fixture.py`, which contributed census rows, ledger
    rows and `python_source_count` to the committed baseline. On a clean checkout
    that file does not exist and the freeze test FAILED: the fixture pinned a
    working-copy accident. Tracked plus untracked-not-ignored is exactly "files
    that belong to the repo".

    `None` (git absent, e.g. inside a `git archive` export) falls back to the
    plain walk — correct there, because an ignored file is not in the export
    either, so both answers agree.
    """
    import subprocess

    try:
        # `-z` is load-bearing, not a style choice. Under git's default
        # `core.quotePath=true` a path containing any non-ASCII byte comes back
        # C-QUOTED — `"src/boomi_mcp/na\303\257ve.py"` — which never matches the
        # plain path, so a TRACKED source file was silently dropped from the
        # scan and the freeze stayed green over its legacy calls. That
        # re-created the machine-dependence this scoping exists to remove, keyed
        # on the developer's git config instead of their working copy. With `-z`
        # git never quotes, and embedded newlines are handled too.
        result = subprocess.run(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            cwd=str(_ROOT), capture_output=True, timeout=30, check=False)
    except (OSError, subprocess.SubprocessError):  # pragma: no cover - defensive
        return None
    if result.returncode != 0:
        return None
    return frozenset(
        entry.decode("utf-8", "surrogateescape")
        for entry in result.stdout.split(b"\0") if entry)


def unscanned_assets() -> List[str]:
    """Files under the scan roots that NO census reads.

    The scanner walks `*.py`; the producer census walks `examples/**/*.json`.
    Anything else living under those roots — a YAML manifest, a fixture, a
    template — is invisible to both and to every `scan_contract` scalar. Zero
    exist today; freezing the count makes their arrival a diff rather than a
    silent widening of the unmodelled region.
    """
    visible = _repository_files()
    out: List[str] = []
    for root, keep in (("src/boomi_mcp", {".py"}), ("scripts", {".py"}),
                       ("examples", {".json"})):
        base = _ROOT / root
        if not base.is_dir():
            continue
        for path in sorted(base.rglob("*")):
            if not path.is_file() or path.suffix in keep:
                continue
            # Same universe as the source scan. Gitignored build output
            # (`*.so`, `*.log`, `*_local_secrets.json` are all live patterns)
            # otherwise moved this count — fail-closed noise, but it made the
            # "machine-independent" claim false for a second reason.
            if visible is not None \
                    and path.relative_to(_ROOT).as_posix() not in visible:
                continue
            if "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
                continue
            # Dotfiles are machine-local droppings (`.DS_Store`), not project
            # assets. Freezing a count that includes them would pin a
            # per-machine accident — the same defect as freezing the installed
            # SDK version string.
            if any(part.startswith(".") for part in path.relative_to(_ROOT).parts):
                continue
            out.append(path.relative_to(_ROOT).as_posix())
    return out


def example_documents() -> Dict[str, Any]:
    """``{repo-relative posix path: parsed JSON}`` for every ``examples/**/*.json``.

    Globbed, never enumerated: a sixth example is a diff, not a silent pass.
    """
    out: Dict[str, Any] = {}
    examples = _ROOT / "examples"
    if not examples.is_dir():
        return out
    visible = _repository_files()
    for path in sorted(examples.rglob("*.json")):
        rel = path.relative_to(_ROOT).as_posix()
        if visible is not None and rel not in visible:
            continue
        out[rel] = json.loads(path.read_text(encoding="utf-8"))
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
    "http_client_call": "HT",
    "raw_api_invoker": "WR",
    "process_kind_producer": "PP",
    "process_kind_consumer": "PC",
    "example_producer": "PX",
    "authoring_boundary": "PB",
    "unclassified_dynamic": "UD",
    "unclassified_reference": "UR",
}


def _resolve_module_path(module: Optional[str], level: int, from_path: str,
                         known: "frozenset[str]") -> Optional[str]:
    """Repo-relative path of an imported module, or None if it is not ours.

    A transitive callee's identity is `(path, symbol)`, not a bare name:
    `_build_main_process` is defined in THREE archetype modules, so a bare-name
    closure links whichever one is legacy-bearing to callers of the other two.
    """
    if level:
        parts = from_path.split("/")[:-1]
        for _ in range(level - 1):
            parts = parts[:-1]
        target = parts + (module.split(".") if module else [])
    else:
        if not module:
            return None
        head = module.split(".")
        if head[0] == "boomi_mcp":
            target = ["src"] + head
        else:
            # The scan universe now includes repo-root modules and `scripts/`,
            # so `import server` and `import scripts.x` are OURS too. Rejecting
            # everything outside the `boomi_mcp` package left a real edge
            # invisible: `scripts/provision_qa_noop_fixture.py` imports `server`
            # and calls `server.manage_component(...)`.
            for prefix in ([], ["scripts"]):
                candidate_head = prefix + head
                for suffix in (".py", "/__init__.py"):
                    if "/".join(candidate_head) + suffix in known:
                        return "/".join(candidate_head) + suffix
            return None
    for candidate in ("/".join(target) + ".py", "/".join(target) + "/__init__.py"):
        if candidate in known:
            return candidate
    return None


def _assign_parts(statement: ast.AST) -> Tuple[List[str], Optional[ast.AST]]:
    """`(bound names, value)` for `x = …` and `x: T = …`, else `([], None)`."""
    if isinstance(statement, ast.Assign):
        return ([t.id for t in statement.targets if isinstance(t, ast.Name)],
                statement.value)
    if isinstance(statement, ast.AnnAssign) and statement.value is not None \
            and isinstance(statement.target, ast.Name):
        return ([statement.target.id], statement.value)
    return ([], None)


def _scope_body_nodes(node: ast.AST) -> List[ast.AST]:
    """Statements binding THIS scope — control-flow bodies included, nested
    ``def``/``class`` bodies excluded (those are their own scopes)."""
    out: List[ast.AST] = []

    def walk(body: Iterable[ast.AST]) -> None:
        for statement in body:
            if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef,
                                      ast.ClassDef)):
                continue
            out.append(statement)
            for attr in ("body", "orelse", "finalbody"):
                walk(getattr(statement, attr, []) or [])
            for handler in getattr(statement, "handlers", []):
                walk(handler.body)

    walk(getattr(node, "body", []) or [])
    return out


def _module_namespace_nodes(tree: ast.AST) -> List[ast.AST]:
    """Statements that bind the MODULE namespace.

    The module body plus module-level `if`/`try`/`with` bodies — conditional
    imports at module level are still re-exports — but never a function or class
    body, whose imports are local bindings.
    """
    out: List[ast.AST] = []

    def walk(body: Iterable[ast.AST]) -> None:
        for node in body:
            out.append(node)
            if isinstance(node, (ast.If, ast.Try, ast.With, ast.AsyncWith)):
                walk(getattr(node, "body", []))
                walk(getattr(node, "orelse", []))
                walk(getattr(node, "finalbody", []))
                for handler in getattr(node, "handlers", []):
                    walk(handler.body)

    walk(getattr(tree, "body", []))
    return out


def reexport_index(trees: Dict[str, ast.AST]) -> Dict[Tuple[str, str], Tuple[str, str]]:
    """`(importing module, local name) -> (source module, original name)`.

    A barrel package re-exports a symbol, so the module a caller imports it FROM
    is not the module it is DEFINED in. `get_process_flow_builder` lives in
    `process_flow_builder.py` but every caller imports it from
    `builders/__init__.py`; keying the closure on the import site therefore lost
    the edge entirely — measured as two production caller sites silently
    dropping their legacy edge. Following this index canonicalizes an import
    site to its defining site.
    """
    known = frozenset(trees)
    index: Dict[Tuple[str, str], Tuple[str, str]] = {}
    for path, tree in trees.items():
        # MODULE-NAMESPACE imports only. `ast.walk` also reaches imports nested
        # inside functions and classes, which bind a LOCAL name, not a
        # re-export; indexing one of those rewrites `(module, f)` to an
        # unrelated function and silently drops the real edge for every caller
        # of the module-level `f`.
        for node in _module_namespace_nodes(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            source = _resolve_module_path(node.module, node.level or 0, path, known)
            if not source or source == path:
                continue
            for alias in node.names:
                if alias.name == "*":
                    continue
                local = alias.asname or alias.name
                index[(path, local)] = (source, alias.name)
    return index


def canonical_definition(ref: Tuple[str, str],
                         index: Dict[Tuple[str, str], Tuple[str, str]]) -> Tuple[str, str]:
    """Follow re-export hops to the defining module (cycle-safe)."""
    seen = set()
    while ref in index and ref not in seen:
        seen.add(ref)
        ref = index[ref]
    return ref


class _Scanner(ast.NodeVisitor):
    """One pass per module; emits census rows keyed symbolically, not positionally."""

    def __init__(self, path: str, vocab: Dict[str, Tuple[str, ...]],
                 transitive_targets: "frozenset[Tuple[str, str]]" = frozenset(),
                 local_defs: "frozenset[str]" = frozenset(),
                 known_paths: "frozenset[str]" = frozenset(),
                 reexports: Optional[Dict[Tuple[str, str], Tuple[str, str]]] = None) -> None:
        self.path = path
        self.v = vocab
        self._transitive = transitive_targets
        self._imported: Set[str] = set()
        self._import_origin: Dict[str, str] = {}
        self._qualified_origin: Dict[str, Tuple[str, str]] = {}
        self._module_paths: Dict[str, str] = {}
        self._known: "frozenset[str]" = known_paths
        self._reexports = reexports or {}
        self._http_vars: Dict[str, str] = {}
        #: Pre-indexed in `visit_Module`: traversal order otherwise decided whether
        #: a composed URL folded, which made the re-pointing guard depend on
        #: statement order even though the scan contract excludes it.
        self._module_consts: Dict[str, str] = {}
        self._local_consts: List[Dict[str, str]] = []
        self._local_origins: List[Dict[str, Tuple[str, str]]] = []
        self._local_defs: Set[str] = set(local_defs)
        self.rows: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
        self._symbols: List[str] = []
        self._aliases: Dict[str, str] = {}     # local name -> watched base name
        self._modules: Dict[str, str] = {}     # local name -> module dotted path
        self._builder_vars: Set[str] = set()   # locals bound to a builder class
        self._skip: Set[int] = set()
        #: Nodes positively classified into a census kind. Everything else that
        #: MENTIONS a watched name becomes residue — see `_ResidueScanner`.
        self._consumed: Set[int] = set()

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

    def _consume(self, node: Optional[ast.AST]) -> None:
        """Mark exactly what an emitted row accounts for — never a whole subtree.

        Walking the subtree swallowed any watched mention nested inside a
        classified node, which made the total-accounting invariant FALSE and was
        exploitable on real frozen sites with zero census movement: wrapping a
        receiver in `_pick(ns, _create_component_raw)._emit_x(...)` preserved the
        row's form and count while smuggling a Component-XML write sink through
        the receiver, and `httpx.Client(base_url=".../Component").post(...)` hid
        an endpoint re-point the same way.

        Only the attribute/name SPINE of the classified expression is consumed;
        arguments, slices and nested calls stay accountable and fall to residue.
        """
        if node is None:
            return
        cur: Optional[ast.AST] = node
        while cur is not None:
            self._consumed.add(id(cur))
            if isinstance(cur, ast.Attribute):
                cur = cur.value
            elif isinstance(cur, ast.Subscript) and self._is_registry(cur.value):
                self._consumed.add(id(cur.value))
                cur = None
            else:
                cur = None

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

    def _resolved_str(self, node: Optional[ast.AST]) -> Optional[str]:
        """A string literal, a name bound to one, or a `+` composition of those."""
        direct = self._as_str(node)
        if direct is not None:
            return direct
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left = self._resolved_str(node.left)
            right = self._resolved_str(node.right)
            if left is not None and right is not None:
                return left + right
        # `"%s/Component" % BASE` — the `+` spelling was folded while four others
        # stayed `<dynamic>`, so a re-point through them moved no census row.
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):
            template = self._resolved_str(node.left)
            if template is not None:
                operands = (node.right.elts if isinstance(node.right, ast.Tuple)
                            else [node.right])
                # An unresolved field must NOT discard the resolved ones. Bailing
                # to `<dynamic>` on `"%s/Component/%s" % (BASE, component_id)`
                # meant re-pointing BASE moved nothing — the same re-point the
                # `+` and f-string branches catch. Substitute a placeholder, as
                # the f-string branch already does.
                values = [v if v is not None else "{}"
                          for v in (self._resolved_str(o) for o in operands)]
                try:
                    return template % tuple(values)
                except Exception:
                    # A statically resolvable but nonsensical format (`{0.host}`,
                    # a wrong arity) must fall back, never abort the inventory —
                    # the scanner walks unreachable and conditional bodies too.
                    return None
        # `"{}/Component".format(BASE)`
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                and node.func.attr == "format":
            template = self._resolved_str(node.func.value)
            if template is not None:
                args = [self._resolved_str(a) for a in node.args]
                kwargs = {kw.arg: self._resolved_str(kw.value)
                          for kw in node.keywords if kw.arg}
                args = [a if a is not None else "{}" for a in args]
                kwargs = {k: (v if v is not None else "{}")
                          for k, v in kwargs.items()}
                try:
                    return template.format(*args, **kwargs)
                except Exception:
                    return None
        if isinstance(node, ast.JoinedStr):
            parts = []
            for value in node.values:
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    parts.append(value.value)
                else:
                    resolved = self._resolved_str(getattr(value, "value", None))
                    parts.append(resolved if resolved is not None else "{}")
            return "".join(parts)
        return None

    def _as_str(self, node: Optional[ast.AST]) -> Optional[str]:
        """A string literal, or a plain name bound to one earlier in the module.

        `KEY = "process_kind"; cfg[KEY] = ...` is the same producer as the
        literal form; accepting only `ast.Constant` let hoisting the selector
        into a constant erase the row.
        """
        literal = _const_str(node)
        if literal is not None:
            return literal
        if isinstance(node, ast.Name):
            # Innermost binding wins, exactly as Python resolves it. One
            # scanner-wide map let a function-local `KEY = "other"` overwrite the
            # module constant and erase a producer row in an unrelated function.
            for scope in reversed(self._local_consts):
                if node.id in scope:
                    return scope[node.id]
            return self._module_consts.get(node.id)
        return None

    def _request_target(self, node: ast.Call) -> str:
        """A normalized, frozen description of what an HTTP call targets.

        Statically-known URL literals are reduced to `scheme://host/path`; an
        f-string keeps its literal segments; anything unresolvable becomes
        `<dynamic>`. The point is that re-pointing a call at `/Component` MOVES
        this string, so it cannot happen without failing the freeze.
        """
        if isinstance(node.func, ast.Attribute):
            verb = node.func.attr
        elif isinstance(node.func, ast.Name):
            verb = node.func.id
        else:  # pragma: no cover - defensive
            verb = "call"
        method = verb.upper()
        args = list(node.args)
        if verb in ("request", "stream") and args:
            literal = self._as_str(args[0])
            if literal:
                method = literal.upper()
                args = args[1:]
        url = "<dynamic>"
        for candidate in args + [kw.value for kw in node.keywords
                                 if kw.arg in ("url", "endpoint")]:
            # `_resolved_str` folds `BASE + "/Component"` through module-level
            # string constants. Reducing a composed target to `<dynamic>` meant
            # re-pointing `BASE` from an unrelated host to `api.boomi.com`
            # produced an IDENTICAL census — an `external_transport` route could
            # become a Component-API write with the freeze still green.
            literal = self._resolved_str(candidate)
            if literal:
                url = literal
                break
            if isinstance(candidate, ast.JoinedStr):
                url = "".join(
                    part.value if isinstance(part, ast.Constant)
                    and isinstance(part.value, str) else "{}"
                    for part in candidate.values)
                break
        if url != "<dynamic>":
            url = url.split("?", 1)[0].split("#", 1)[0]
        targets_component = "/component" in url.lower()
        return "%s %s%s" % (method, url, " [COMPONENT-API]" if targets_component else "")

    def _is_http_client(self, func: ast.AST) -> bool:
        """True when a call's receiver traces back to an HTTP client module.

        Walks the attribute chain to its root Name and checks what that name was
        imported as, so `httpx.post(...)`, `client.post(...)` where
        `client = httpx.Client()`, and `requests.request(...)` all match while an
        unrelated `queue.send(...)` does not.
        """
        # A DIRECTLY IMPORTED write function — `from httpx import post; post(...)`
        # or `from urllib.request import urlopen; urlopen(...)`. Rejecting every
        # bare Name let both of those bypass the census outright, even though the
        # import origin needed to identify them was already recorded.
        if isinstance(func, ast.Name):
            origin = self._modules.get(func.id, "")
            return any(origin == mod or origin.startswith(mod + ".")
                       for mod in _HTTP_CLIENT_MODULES)
        if not isinstance(func, ast.Attribute):
            return False
        root = func.value
        while isinstance(root, (ast.Attribute, ast.Call, ast.Subscript)):
            root = getattr(root, "value", None) or getattr(root, "func", None)
            if root is None:
                return False
        if not isinstance(root, ast.Name):
            return False
        origin = self._modules.get(root.id) or self._http_vars.get(root.id) or root.id
        return any(origin == mod or origin.startswith(mod + ".")
                   for mod in _HTTP_CLIENT_MODULES)

    def _binds_builder(self, value: Optional[ast.AST]) -> bool:
        """True when the value evaluates to a process-flow builder class.

        Shared by `visit_Assign` and the `visit_Module` prepass so both agree —
        the prepass recognizing fewer shapes than the visitor is what let a
        registry-bound builder declared below its caller escape.
        """
        if isinstance(value, ast.Call):
            dotted = self._dotted(value.func)
            if dotted and dotted.split(".")[-1] == "get_process_flow_builder":
                return True
            if isinstance(value.func, ast.Attribute) and value.func.attr == "get" \
                    and self._is_registry(value.func.value):
                return True
        if isinstance(value, ast.Subscript) and self._is_registry(value.value):
            return True
        if isinstance(value, (ast.Name, ast.Attribute)):
            dotted = self._dotted(value)
            if dotted and dotted.split(".")[-1] in self._builders:
                return True
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
            for scope in reversed(self._local_origins):
                if func.id in scope:
                    return scope[func.id]
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
        """Enter a scope, pre-indexing its own bindings FLOW-INSENSITIVELY.

        Python fixes a scope's local NAMES at compile time and only their values
        at run time, so for a conservative reachability instrument the right
        analysis is flow-insensitive: over-approximating is fail-closed. Binding
        in traversal order was fail-OPEN, and not merely for a derived sub-row —
        `def outer(): def inner(c,x): return al(c,x); al = legacy_callee` made
        the ENTIRE `inner` caller vanish, no row and no residue, because `al` was
        recorded only after `inner`'s body had been scanned.
        """
        self._symbols.append(name)
        consts: Dict[str, str] = {}
        origins: Dict[str, Tuple[str, str]] = {}
        self._local_consts.append(consts)
        self._local_origins.append(origins)
        for statement in _scope_body_nodes(node):
            targets, value = _assign_parts(statement)
            if value is None:
                continue
            literal = self._resolved_str(value)
            if literal is not None:
                for tgt in targets:
                    consts[tgt] = literal
            if isinstance(value, (ast.Name, ast.Attribute)):
                qualified = self._qualified_callee(value)
                if qualified is not None:
                    for tgt in targets:
                        origins[tgt] = qualified
            if self._binds_builder(value):
                self._builder_vars.update(targets)
        self.generic_visit(node)
        self._local_origins.pop()
        self._local_consts.pop()
        self._symbols.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        self._scoped(node, node.name)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        self._scoped(node, node.name)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802
        self._scoped(node, node.name)

    # -- imports -------------------------------------------------------
    def _resolve_module(self, module: Optional[str], level: int) -> Optional[str]:
        return _resolve_module_path(module, level, self.path, self._known)

    def visit_Module(self, node: ast.Module) -> None:  # noqa: N802
        """Pre-index EVERY module-namespace binding before visiting any body.

        The scan contract states that intra-file ordering is excluded from
        equality — and for bindings resolved out of traversal state it was
        false. A module-level import or alias declared BELOW the function that
        uses it simply did not exist yet when the call was visited, so swapping
        two adjacent statements silently removed a real `legacy_transitive_call`
        row: no census row, no residue, no scalar. Python binds the whole module
        namespace before any of it runs, so the scanner must too.

        Third instance of this class (constants were pre-indexed one round ago,
        local defs the round before), so it is fixed for ALL binding kinds here
        rather than one more per round.
        """
        namespace = _module_namespace_nodes(node)
        for statement in namespace:
            if isinstance(statement, ast.Import):
                self._bind_import(statement)
            elif isinstance(statement, ast.ImportFrom):
                self._bind_import_from(statement)
        for statement in namespace:
            targets, value = _assign_parts(statement)
            if value is None:
                continue
            # LAST binding wins, as Python does — `setdefault` gave the first.
            literal = self._resolved_str(value)
            if literal is not None:
                for tgt in targets:
                    self._module_consts[tgt] = literal
        for statement in namespace:
            targets, value = _assign_parts(statement)
            if value is None:
                continue
            if isinstance(value, (ast.Name, ast.Attribute)):
                qualified = self._qualified_callee(value)
                if qualified is not None:
                    for tgt in targets:
                        self._qualified_origin[tgt] = qualified
            # A registry-bound builder declared BELOW its caller entered
            # `_builder_vars` only after that caller had been visited, so the
            # renderer call produced neither a row nor residue.
            if self._binds_builder(value):
                self._builder_vars.update(targets)
        self.generic_visit(node)

    def _bind_import(self, node: ast.Import) -> None:
        for alias in node.names:
            local = alias.asname or alias.name.split(".")[0]
            self._modules[local] = alias.name
            resolved = self._resolve_module(alias.name, 0)
            if resolved:
                self._module_paths[local] = resolved

    def _bind_import_from(self, node: ast.ImportFrom) -> None:
        package = self._resolve_module(node.module, node.level or 0)
        for alias in node.names:
            local = alias.asname or alias.name
            self._imported.add(local)
            self._import_origin[local] = alias.name
            submodule = self._resolve_module(
                "%s.%s" % (node.module, alias.name) if node.module else alias.name,
                node.level or 0)
            if submodule:
                self._module_paths[local] = submodule
            elif package:
                self._qualified_origin[local] = (package, alias.name)
            if node.module and not node.level:
                self._modules.setdefault(local, "%s.%s" % (node.module, alias.name))
            if alias.name in self._watched:
                self._aliases[local] = alias.name
            if alias.name in self._builders:
                self._builder_vars.add(local)

    def visit_Import(self, node: ast.Import) -> None:  # noqa: N802
        self._bind_import(node)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:  # noqa: N802
        self._bind_import_from(node)
        for alias in node.names:
            if alias.name in self._emitters:
                self._emit("legacy_emitter", "import %s" % alias.name, node.lineno)
        self._consume(node)
        self.generic_visit(node)

    # -- assignments ---------------------------------------------------
    def visit_Assign(self, node: ast.Assign) -> None:  # noqa: N802
        value = node.value

        # `x = get_process_flow_builder(...)` / `PROCESS_FLOW_BUILDERS[k]` /
        # `PROCESS_FLOW_BUILDERS.get(k)` binds x to a builder class.
        # Every registry spelling goes through `_is_registry` / `_yields_builder`
        # so a variable bound to a module-qualified lookup is tracked exactly like
        # one bound to the bare form.
        bound = self._binds_builder(value)
        if isinstance(value, (ast.Name, ast.Attribute)):
            dotted = self._dotted(value)
            base = (dotted or "").split(".")[-1]
            if base in self._watched:
                for tgt in node.targets:
                    if isinstance(tgt, ast.Name):
                        self._aliases[tgt.id] = base
            # `alias = build_structured_update_xml` rebinds a LEGACY-BEARING
            # function. Only `_watched` names were aliased, and a bearing
            # function is not a watched sink name, so `alias(...)` resolved to
            # nothing and added a real legacy caller with zero diff. The plan
            # names simple assignment aliases as a resolution form; carry the
            # qualified identity across the rebind.
            qualified = self._qualified_callee(value)
            if qualified is not None:
                # Scope-local, exactly like the constant map. A scanner-wide
                # binding let a helper's `alias = safe` overwrite a module-level
                # `alias = legacy_sink`, so a LATER function's `alias(...)`
                # resolved to the safe callee and the real edge vanished.
                # Module scope is likewise owned by the prepass.
                if self._local_origins:
                    for tgt in node.targets:
                        if isinstance(tgt, ast.Name):
                            self._local_origins[-1][tgt.id] = qualified
            if base in self._builders:
                bound = True

        if bound:
            for tgt in node.targets:
                if isinstance(tgt, ast.Name):
                    self._builder_vars.add(tgt.id)

        # `client = httpx.Client()` binds an HTTP client to a local name.
        if isinstance(value, ast.Call):
            root = value.func
            while isinstance(root, ast.Attribute):
                root = root.value
            if isinstance(root, ast.Name):
                origin = self._modules.get(root.id, root.id)
                if any(origin == mod or origin.startswith(mod + ".")
                       for mod in _HTTP_CLIENT_MODULES):
                    for tgt in node.targets:
                        if isinstance(tgt, ast.Name):
                            self._http_vars[tgt.id] = origin

        # `KEY = "process_kind"` then `cfg[KEY] = ...`. Constant propagation for
        # plain string names only — without it, hoisting the selector into a
        # constant hid the producer from the census entirely.
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            # MODULE scope is owned by the `visit_Module` prepass, which records
            # the LAST binding as Python does. Letting traversal write here
            # re-introduced the ordering bug the prepass exists to remove: it
            # overwrote the final value with the first one on the way past.
            if self._local_consts:
                for tgt in node.targets:
                    if isinstance(tgt, ast.Name):
                        self._local_consts[-1][tgt.id] = value.value

        # `cfg["process_kind"] = ...` is a producer write.
        for tgt in node.targets:
            if isinstance(tgt, ast.Subscript):
                key = self._as_str(tgt.slice)
                if key in self._selectors:
                    self._emit(
                        "process_kind_producer",
                        "subscript-assign %s=%s" % (key, _const_repr(value)),
                        node.lineno,
                    )
                    self._skip.add(id(tgt))
                    self._consume(tgt.slice)
        self.generic_visit(node)

    # -- expressions ---------------------------------------------------
    def _bind_http_context(self, node: Any) -> None:
        """`with httpx.Client(...) as client:` binds a client without an Assign.

        The context-manager form is how this repo actually opens HTTP clients
        (`schema_discovery.py:458`), so handling only `client = httpx.Client()`
        saw none of them.
        """
        for item in getattr(node, "items", []):
            target = item.optional_vars
            value = item.context_expr
            if not isinstance(target, ast.Name) or not isinstance(value, ast.Call):
                continue
            root = value.func
            while isinstance(root, ast.Attribute):
                root = root.value
            if isinstance(root, ast.Name):
                origin = self._modules.get(root.id) or self._http_vars.get(root.id) \
                    or root.id
                if any(origin == mod or origin.startswith(mod + ".")
                       for mod in _HTTP_CLIENT_MODULES):
                    self._http_vars[target.id] = origin

    def visit_With(self, node: ast.With) -> None:  # noqa: N802
        self._bind_http_context(node)
        self.generic_visit(node)

    def visit_AsyncWith(self, node: ast.AsyncWith) -> None:  # noqa: N802
        self._bind_http_context(node)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        _before = len(self.rows) + sum(r["count"] for r in self.rows.values())
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
        elif base in _HTTP_WRITE_VERBS and self._is_http_client(node.func):
            # The TARGET is part of the frozen identity. Keying on the verb
            # alone let an existing `external_transport` route be re-pointed at
            # `/Component` with no census row, no count change and no
            # reconciliation change — the stale route claim stayed green while
            # the call became a Component-XML write.
            self._emit("http_client_call",
                       "%s -> %s (hand-rolled HTTP)"
                       % (_tail(dotted, 2) if dotted else base,
                          self._request_target(node)),
                       node.lineno)
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
        elif base == "setattr" and len(node.args) > 1:
            attr = self._as_str(node.args[1])
            if attr in self._selectors:
                self._emit("process_kind_producer",
                           "setattr %s=%s" % (attr, _const_repr(node.args[2])
                                              if len(node.args) > 2 else "<expr>"),
                           node.lineno)
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
        if callee:
            callee = canonical_definition(callee, self._reexports)
        if callee and callee in self._transitive \
                and (self.path, self.symbol) != callee:
            self._emit("legacy_transitive_call",
                       "%s(...) [legacy-bearing, %s]" % (callee[1], callee[0]),
                       node.lineno)

        if (len(self.rows) + sum(r["count"] for r in self.rows.values())) != _before:
            self._consume(node.func)

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
            key = self._as_str(node.args[0])
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
            name = self._as_str(key)
            if name in self._selectors:
                self._emit(
                    "process_kind_producer",
                    "dict-literal %s=%s" % (name, _const_repr(value)),
                    getattr(key, "lineno", node.lineno),
                )
                self._consume(key)
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> None:  # noqa: N802
        if id(node) not in self._skip:
            if self._is_registry(node.value):
                self._emit("registry_lookup", "PROCESS_FLOW_BUILDERS[...]", node.lineno)
                self._skip.add(id(node.value))
                self._consume(node.value)
            else:
                key = self._as_str(node.slice)
                if key in self._selectors:
                    self._consume(node.slice)
                    self._emit("process_kind_consumer",
                               "%s[%r]" % (_tail(self._dotted(node.value), 2) or "<expr>", key),
                               node.lineno)
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:  # noqa: N802
        if id(node) in self._skip:
            return
        if isinstance(node.ctx, ast.Load) and self._resolve(node.id) == "PROCESS_FLOW_BUILDERS":
            self._emit("registry_lookup", "PROCESS_FLOW_BUILDERS (read)", node.lineno)
            self._consume(node)
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:  # noqa: N802
        if id(node) not in self._skip:
            dotted = self._dotted(node)
            if dotted and dotted.split(".")[-1] in self._emitters:
                self._emit("legacy_emitter", "%s (reference)" % dotted.split(".")[-1],
                           node.lineno)
                self._skip.add(id(node.value))
                self._consume(node)
            elif node.attr == "PROCESS_FLOW_BUILDERS":
                self._consume(node)
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


#: Literal markers for the RESOURCE this inventory protects. Keying residue on
#: the Component API path — not on a list of HTTP client libraries or verbs —
#: is what makes an unknown transport mechanism visible: `client.post(".../Component")`
#: mentions no watched SYMBOL, but it names the endpoint, and the endpoint is the
#: thing #160 must guard.
#: `/component` as a real path SEGMENT. A bare substring test matched prose and
#: unrelated paths — `/intent/integration_spec/components/`, `name/component_name`,
#: `references/components/map_component.md` — and the `</Component>` closing tag in
#: served XML examples: 7 of 9 endpoint rows were false positives, so an unrelated
#: prose edit tripped the freeze. The segment must not be followed by another
#: identifier character, and must not be a closing tag.
#: A trailing SPACE means prose ("… ProcessIR/topology/component is validated …"),
#: not a path. A real endpoint literal continues with `/`, `?`, `#`, a quote, or
#: ends. `Component/bulk` is matched in its own right.
#: `^Component/` also matches: with a base-URL-configured client,
#: `client.post("Component/{id}", …)` is a valid relative call to the Component
#: API. Anchored at string start so the bare word in prose does not match.
_COMPONENT_ENDPOINT_RE = re.compile(
    r"(?<!<)/component(?![a-z0-9_ ])|component/bulk|^component/", re.IGNORECASE)

#: The BARE collection path. `client.post("Component", data=xml)` against a
#: base-URL client IS the create route, and relative sub-resources were covered
#: while the collection itself was not. Case-SENSITIVE and exact: measured over
#: 59,763 string/bytes literals in the scan universe it matches ZERO of them, so
#: the noise cost is nil, while a case-insensitive match would sweep up ordinary
#: prose.
#: `?`/`#` end the path just as end-of-string does — `Component?foo=bar` is the
#: same collection URL, and anchoring on `$` alone let it through.
_COMPONENT_COLLECTION_RE = re.compile(r"^Component(?=$|[?#])")

#: Interpolation placeholders, stripped BEFORE the anchors are applied.
#:
#: The residue pass enumerates literals totally, but the predicate it applied to
#: each literal was still an anchor enumeration — a hand-listed answer to "what
#: may sit next to the marker". Three consecutive rounds each patched one
#: adjacency (`/Component`, `Component/`, `Component?`), and a placeholder glued
#: directly in front defeated all of them: `"%sComponent/%s" % (base, cid)` is
#: the UPDATE route for a specific component and was emitted as neither a
#: classified row nor residue, which made the module's own stated invariant
#: false. A placeholder means "some unknown prefix" — exactly what string-start
#: and `/` already mean — so normalise it away and let the existing anchors
#: decide. Measured over 59,763 literals in the scan universe: zero marginal
#: false positives.
_PLACEHOLDER_RE = re.compile(
    r"%\([^)]*\)[-#0-9.*+ ]*[sdrifgeExXoc]"   # %(name)s, %(name)1s, %(n)-10.3f
    r"|%[-#0-9.*+ ]*[sdrifgeExXoc]"   # %s, %-10.3f
    r"|\{[^{}]*\}"                    # {}, {base}, {0!r:>10}
    r"|\$\{[^}]*\}"                   # ${base}
)


def _mentions_component_endpoint(value: str) -> bool:
    """True when the literal names the Component API, placeholders or not."""
    for candidate in (value, _PLACEHOLDER_RE.sub("", value)):
        if _COMPONENT_ENDPOINT_RE.search(candidate) \
                or _COMPONENT_COLLECTION_RE.match(candidate):
            return True
    return False


def _folded_str(node: ast.AST) -> Optional[str]:
    """A string literal, including a simple `"a" + "b"` concatenation."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _folded_str(node.left), _folded_str(node.right)
        if left is not None and right is not None:
            return left + right
    return None


class _ResidueScanner(ast.NodeVisitor):
    """Total accounting: every mention of a watched name that was NOT classified.

    This is the structural replacement for enumerating recognized shapes. Five
    consecutive review rounds each found a NEW spelling that the scanner did not
    recognize and therefore emitted nothing for — a dispatch dict, an attribute
    assignment, a client on `self`, an aliased import, a cross-module constant.
    Each was fixed individually and the class recurred, which the repo's
    structural-fix rule says to stop doing.

    The invariant here is derived from the SOURCE, not from a list of shapes:
    *every syntactic occurrence of a watched name or selector is either
    positively classified into a census kind or recorded as residue.* A new
    spelling can no longer vanish — at worst it lands in `unclassified_reference`
    and, because the residue set is frozen, still fails the gate. It converts an
    unbounded "shapes we forgot" problem into a bounded, visible one.
    """

    def __init__(self, path: str, vocab: Dict[str, Tuple[str, ...]],
                 consumed: Set[int],
                 aliases: Optional[Dict[str, str]] = None) -> None:
        self.path = path
        # An ALIASED watched import (`… import get_process_flow_builder as g`)
        # is spelled `g` at every use site, so matching the raw `node.id` saw
        # nothing and the whole path — dispatch table included — produced neither
        # a census row nor residue. The residue pass gets the scanner's alias
        # table for exactly this reason.
        self._aliases = dict(aliases or {})
        self.rows: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
        self._consumed = consumed
        self._symbols: List[str] = []
        self._docstrings: Set[int] = set()
        #: Node ids this pass accounted for, so conservation can be checked
        #: per OCCURRENCE rather than per module.
        self._reported: Set[int] = set()
        self._watched = (
            set(vocab["registry_names"]) | set(vocab["builder_classes"])
            | set(vocab["legacy_emitters"]) | set(vocab["legacy_semantic_validation"])
            | set(vocab["component_xml_write_sinks"]) | set(vocab["raw_api_invokers"])
        )
        self._selectors = set(vocab["producer_selectors"])

    @property
    def symbol(self) -> str:
        return ".".join(self._symbols) if self._symbols else "<module>"

    def _note_docstring(self, node: ast.AST) -> None:
        body = getattr(node, "body", None)
        if body and isinstance(body[0], ast.Expr) \
                and isinstance(body[0].value, ast.Constant) \
                and isinstance(body[0].value.value, str):
            self._docstrings.add(id(body[0].value))

    def _emit(self, form: str, line: int) -> None:
        key = ("unclassified_reference", self.path, self.symbol, form)
        row = self.rows.get(key)
        if row is None:
            self.rows[key] = {
                "row_id": _row_id("UR", key),
                "census": "unclassified_reference",
                "path": self.path,
                "symbol": self.symbol,
                "form": form,
                "count": 1,
                "evidence_line": line,
            }
        else:
            row["count"] += 1
            row["evidence_line"] = min(row["evidence_line"], line)

    def visit_Module(self, node: ast.Module) -> None:  # noqa: N802
        self._note_docstring(node)
        self.generic_visit(node)

    def _scoped(self, node: ast.AST, name: str) -> None:
        self._note_docstring(node)
        self._symbols.append(name)
        self.generic_visit(node)
        self._symbols.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        self._scoped(node, node.name)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        self._scoped(node, node.name)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802
        self._scoped(node, node.name)

    def visit_Name(self, node: ast.Name) -> None:  # noqa: N802
        resolved = self._aliases.get(node.id, node.id)
        if id(node) not in self._consumed:
            if resolved in self._watched:
                label = (resolved if resolved == node.id
                         else "%s (as %s)" % (resolved, node.id))
                self._reported.add(id(node))
                self._emit("%s (unclassified reference)" % label, node.lineno)
            elif resolved in self._selectors:
                # `process_kind: str = "sync_pipeline"` — an annotated field on a
                # typed spec, which is the spelling M12's own direction produces.
                # `visit_Attribute` and `_check_string` both tested selectors;
                # this branch tested only `_watched`, so 40 live occurrences were
                # accounted for by nothing.
                self._reported.add(id(node))
                self._emit("%s (unclassified selector name)" % resolved, node.lineno)
        self.generic_visit(node)

    def visit_arg(self, node: ast.arg) -> None:  # noqa: N802
        if node.arg in self._selectors and id(node) not in self._consumed:
            self._reported.add(id(node))
            self._emit("%s (unclassified selector parameter)" % node.arg, node.lineno)
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:  # noqa: N802
        if id(node) not in self._consumed:
            if node.attr in self._watched:
                self._reported.add(id(node))
                self._emit("%s (unclassified attribute)" % node.attr, node.lineno)
            elif node.attr in self._selectors:
                self._reported.add(id(node))
                # `spec.process_kind = "sync_pipeline"` — a typed producer, which
                # the subscript-shaped producer branch never saw.
                self._emit("%s (unclassified selector attribute)" % node.attr,
                           node.lineno)
        self.generic_visit(node)

    def visit_BinOp(self, node: ast.BinOp) -> None:  # noqa: N802
        folded = _folded_str(node)
        if folded is not None and id(node) not in self._consumed:
            self._check_string(folded, node.lineno, concatenated=True)
        # ALWAYS descend: returning early when the fold did not match hid a
        # matching child inside a non-matching concatenation.
        self.generic_visit(node)

    def _check_string(self, value: str, line: int, concatenated: bool = False) -> None:
        note = " (concatenated)" if concatenated else ""
        if value in self._selectors:
            self._emit("%r (unclassified selector literal%s)" % (value, note), line)
        elif value in self._watched:
            # A watched SYMBOL reached as a string — `globals()["..."]`,
            # `getattr(m, "...")`, a dispatch table key.
            self._emit("%r (unclassified symbolic literal%s)" % (value, note), line)
        elif _mentions_component_endpoint(value):
            self._emit("%r (unclassified Component-endpoint literal%s)" % (value, note),
                       line)

    def visit_Constant(self, node: ast.Constant) -> None:  # noqa: N802
        if id(node) in self._consumed or id(node) in self._docstrings:
            return
        if isinstance(node.value, str):
            self._reported.add(id(node))
            self._check_string(node.value, node.lineno)
        elif isinstance(node.value, (bytes, bytearray)):
            self._reported.add(id(node))
            # `b"/Component"` / `b"process_kind"` are the same evidence.
            try:
                self._check_string(bytes(node.value).decode("utf-8"), node.lineno)
            except UnicodeDecodeError:  # pragma: no cover - defensive
                pass


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
    "legacy_semantic_validation", "component_xml_write", "http_client_call",
    "legacy_transitive_call",
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
    reexports: Dict[Tuple[str, str], Tuple[str, str]] = {}
    for path in sorted(sources):
        try:
            trees[path] = ast.parse(sources[path], filename=path)
        except SyntaxError as exc:  # pragma: no cover - defensive
            raise AssertionError("cannot parse %s for the #149 census: %s" % (path, exc))
        scanner = _Scanner(path, vocab, local_defs=_module_level_functions(trees[path]),
                           known_paths=known)
        scanner.visit(trees[path])
        rows.extend(scanner.rows.values())
        residue = _ResidueScanner(path, vocab, scanner._consumed,
                                  aliases=scanner._aliases)
        residue.visit(trees[path])
        rows.extend(residue.rows.values())

    reexports = reexport_index(trees)

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
                               local_defs=module_level[path], known_paths=known,
                               reexports=reexports)
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
    """Functions bound in the MODULE namespace.

    Reading only `tree.body` missed every def inside a module-level `if`/`try` —
    which is how `server.py` registers most of its MCP tools (`if invoke_api:`).
    Those functions were therefore never legacy-BEARING, so a script importing
    `server` and calling one produced no edge even once the import resolved.
    """
    return frozenset(
        node.name for node in _module_namespace_nodes(tree)
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
        "route_id": "WRT-external-transport-oauth-callback",
        "locations": ("server.py::web_callback",),
        "classification": "external_transport",
        "summary": "OAuth callback posts to the identity provider's token endpoint via a "
                   "hand-rolled HTTP client. Never targets the Boomi Component API.",
        "owning_issue": "#160",
        "post_retraction_assertion": "unchanged — assert the target host/path is the "
                                     "configured token endpoint, never /Component.",
    },
    {
        "route_id": "WRT-external-transport-listener-probe",
        "locations": ("src/boomi_mcp/categories/deployment/orchestration.py::"
                      "_listener_probe",),
        "classification": "external_transport",
        "summary": "Probes a deployed listener's own URL with `urllib.request.urlopen` to "
                   "confirm it is serving. Not a platform API call.",
        "owning_issue": "#160",
        "post_retraction_assertion": "unchanged — the URL comes from the listener's "
                                     "endpoint, never from a Component route.",
    },
    {
        "route_id": "WRT-external-transport-marketplace",
        "locations": ("src/boomi_mcp/categories/marketplace.py::"
                      "search_marketplace_recipes_action",),
        "classification": "external_transport",
        "summary": "Public, unauthenticated Marketplace GraphQL query over httpx. Carries no "
                   "platform credentials and has no install/write path.",
        "owning_issue": "#160",
        "post_retraction_assertion": "unchanged — assert the endpoint is the Marketplace "
                                     "GraphQL host and no component XML is submitted.",
    },
    {
        "route_id": "WRT-external-transport-schema-discovery",
        "locations": ("src/boomi_mcp/categories/schema_discovery.py::_fetch",),
        "classification": "external_transport",
        "summary": "Fetches a caller-supplied OpenAPI/WSDL/OData document over httpx under "
                   "the SSRF/redirect guards. Reads a third-party URL, never the platform.",
        "owning_issue": "#160",
        "post_retraction_assertion": "unchanged — the SSRF guard already forbids platform "
                                     "hosts; assert no /Component target is reachable.",
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
        for r in rows
        if r["census"] in ("component_xml_write", "http_client_call", "raw_api_invoker")
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


def _mcp_tool_surface(tool: Any) -> Any:
    """Everything FastMCP actually serves for a tool, not just two fields.

    `to_mcp_tool()` emits `outputSchema`, `title`, `annotations`, `meta` and more
    alongside `description`/`inputSchema` — this repo already has a non-null
    output schema on `plan_integration_design` and annotations on many tools.
    Digesting only description+parameters left those machine-served fields free
    to change while the digest claimed to be exhaustive.
    """
    to_mcp = getattr(tool, "to_mcp_tool", None)
    if callable(to_mcp):
        try:
            served = to_mcp()
        except Exception:  # pragma: no cover - defensive
            served = None
        if served is not None:
            dump = getattr(served, "model_dump", None)
            if callable(dump):
                return dump(exclude_none=True, mode="json")
    return {"description": tool.description or "", "parameters": tool.parameters or {}}


def _mcp_wire_model(item: Any) -> Any:
    """The item's served wire model, whatever `to_mcp_*()` it exposes."""
    for accessor in ("to_mcp_prompt", "to_mcp_resource", "to_mcp_template",
                     "to_mcp_resource_template"):
        method = getattr(item, accessor, None)
        if callable(method):
            try:
                served = method()
            except Exception:  # pragma: no cover - defensive
                continue
            dump = getattr(served, "model_dump", None)
            if callable(dump):
                return dump(exclude_none=True, mode="json")
    dump = getattr(item, "model_dump", None)
    if callable(dump):
        try:
            return dump(exclude_none=True, mode="json")
        except Exception:  # pragma: no cover - defensive
            pass
    return {"repr": type(item).__name__}


def _non_tool_key(item: Any, index: int) -> str:
    for attr in ("name", "uri_template", "uriTemplate", "uri"):
        value = getattr(item, attr, None)
        if value:
            return str(value)
    return str(index)


def _non_tool_mcp_surface() -> Dict[str, Any]:
    """Digest of every non-tool MCP surface the server registers."""
    import server

    loop = asyncio.new_event_loop()
    out: Dict[str, Any] = {}
    try:
        for label, accessor in (("resources", "list_resources"),
                                ("prompts", "list_prompts"),
                                ("resource_templates", "list_resource_templates")):
            method = getattr(server.mcp, accessor, None)
            if method is None:
                out[label] = "<accessor absent>"
                continue
            try:
                items = loop.run_until_complete(method())
            except Exception as exc:  # pragma: no cover - defensive
                out[label] = "<error: %s>" % type(exc).__name__
                continue
            # The FULL wire model, as for tools: prompt arguments/title/meta,
            # resource MIME type/annotations/icons and a template's
            # `uriTemplate` are all machine-served, and templates expose no
            # `.uri` at all — hashing description+uri would have frozen almost
            # nothing once a surface was actually registered.
            out[label] = {
                _non_tool_key(item, index): _sha256(canonical_json(
                    _mcp_wire_model(item)))
                for index, item in enumerate(items)
            }
    finally:
        loop.close()
    return out


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

    # EXHAUSTIVE identity over the ENTIRE registered surface, token filter or
    # not. The filter decides which tools get a full frozen value; this decides
    # nothing — every registered tool's description and parameter schema is
    # digested, so a tool that starts steering callers at the legacy route in
    # words the token list does not contain still fails the freeze.
    # Resources, prompts and resource templates are served MCP surfaces that
    # `list_tools()` does not cover. All three are empty today and nothing
    # asserted they stay empty, so registering a legacy-steering resource was
    # served-surface growth with no freeze movement.
    artifacts.append(_artifact(
        "SS-MCP-DESCRIPTIONS", "server.mcp.list_{resources,prompts,resource_templates}()",
        "non_tool_surface_digest", _non_tool_mcp_surface()))

    artifacts.append(_artifact(
        "SS-MCP-DESCRIPTIONS", "server.mcp.list_tools() [all tools]",
        "registered_surface_digest",
        {tool_name: _sha256(canonical_json(_mcp_tool_surface(tool)))
         for tool_name, tool in sorted(tools.items())}))

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

    # EXHAUSTIVE identity over the whole walked template surface, so detection
    # does not depend on the token filter. A template can acquire legacy
    # guidance in words `_LEGACY_TOKENS` does not list — measured: adding
    # "pass the complete component XML document in config['xml']" to a served
    # description produced zero artifact churn. Full values stay reserved for
    # the in-scope surfaces; every OTHER surface is pinned by digest, so any
    # change to any of them fails the freeze and gets read by a human.
    artifacts.append(_artifact(
        "SS-SCHEMA-TEMPLATES", "meta_tools.get_schema_template_action(...) [all axes]",
        "walked_surface_digest",
        {selector: _sha256(canonical_json(payload))
         for selector, payload in sorted(_schema_template_surfaces(meta_tools).items())}))

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
        # Some axes are advertised ONLY by the overview. `_TP_OVERVIEW` lists the
        # trading-partner `standards` while its `operation=create` payload just
        # defaults to x12 and lists none — so seeding the standard axis from the
        # operation payload alone left edifact, hl7 and the rest outside the
        # digest entirely.
        overview_standards = _standard_axis(overview)
        # Protocols can be advertised by the OVERVIEW alone — the
        # trading-partner overview lists seven `communication_protocols` its
        # `operation=create` payload does not repeat, so none of those templates
        # entered the digest.
        overview_protocols = _protocol_axis(overview)

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
            # UNION, not `or`: a payload advertising its own protocols used to
            # suppress the overview's entirely. Non-lossy today, latent otherwise.
            for protocol in _merged_axis(_protocol_axis(payload), overview_protocols):
                surfaces["%s|protocol=%s" % (key, protocol)] = \
                    meta_tools.get_schema_template_action(
                        resource_type=resource_type, operation=operation, protocol=protocol)
            for standard in _merged_axis(_standard_axis(payload), overview_standards):
                surfaces["%s|standard=%s" % (key, standard)] = \
                    meta_tools.get_schema_template_action(
                        resource_type=resource_type, operation=operation, standard=standard)
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
                for standard in _standard_axis(ct_payload):
                    surfaces["%s|standard=%s" % (ct_key, standard)] = \
                        meta_tools.get_schema_template_action(
                            resource_type=resource_type, operation=operation,
                            component_type=component_type, standard=standard)

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
    for axis in ("|operation=", "|component_type=", "|protocol=", "|standard="):
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
                       "communication_protocols", "protocols")

#: `standard` is a SEPARATE selector from `protocol`. Folding these keys into the
#: protocol list would have passed a standard as `protocol=`, which the action
#: does not accept — a dormant miswalk rather than coverage.
_STANDARD_ECHO_KEYS = ("valid_standards", "available_standards", "standards")


def _merged_axis(primary: List[str], fallback: List[str]) -> List[str]:
    """Order-preserving union of two axis listings."""
    out = list(primary)
    for value in fallback:
        if value not in out:
            out.append(value)
    return out


def _protocol_axis(payload: Any) -> List[str]:
    """Union of every protocol axis the payload advertises."""
    found: List[str] = []
    for key in _PROTOCOL_ECHO_KEYS:
        for value in _echoed_list(payload, key):
            if value not in found:
                found.append(value)
    return found


def _standard_axis(payload: Any) -> List[str]:
    found: List[str] = []
    for key in _STANDARD_ECHO_KEYS:
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
    "http_client_call": "#160",
    "raw_api_invoker": "#160",
    "process_kind_producer": "#159",
    "process_kind_consumer": "#160",
    "example_producer": "#159",
    "authoring_boundary": "#159",
    "unclassified_dynamic": "#160",
    "unclassified_reference": "#160",
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
    if census in ("component_xml_write", "http_client_call", "raw_api_invoker"):
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
    "http_client_call": "hand-rolled HTTP: prove it never reaches /Component, or guard it",
    "raw_api_invoker": "guard behind the canonical endpoint parser",
    "process_kind_producer": "migrate the producer to canonical ProcessIR",
    "process_kind_consumer": "delete with the legacy consumer",
    "example_producer": "migrate the example to canonical ProcessIR",
    "authoring_boundary": "migrate the boundary to canonical ProcessIR",
    "unclassified_dynamic": "resolve or guard the dynamic access",
    "unclassified_reference": "residue: a watched name mentioned in a shape the census does not classify",
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
        "source_modules": ("src/boomi_mcp/models/integration_models.py",),
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
        "source_modules": ("src/boomi_mcp/categories/integration_builder.py",
                            "src/boomi_mcp/categories/components/processes.py"),
        "surface_class": "Builder error texts and hints",
        "producer": "integration_builder plan preflight envelopes",
        "anchors": ("src/boomi_mcp/categories/components/processes.py:224-241 "
                    "(ACTION_UNSUPPORTED)",
                    "src/boomi_mcp/categories/integration_builder.py:3148",
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
        "source_modules": ("server.py",),
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
        "source_modules": ("src/boomi_mcp/categories/components/safe_edit_component.py",),
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
        "source_modules": ("src/boomi_mcp/categories/meta_tools.py", "server.py"),
        "surface_class": "get_schema_template templates",
        "producer": "meta_tools.get_schema_template_action(...)",
        "anchors": ("src/boomi_mcp/categories/meta_tools.py:595-598 (raw_xml_escape_hatch)",
                    "src/boomi_mcp/categories/meta_tools.py:762-785 (_COMPONENT_CREATE, "
                    "type=\"process\" at :770, workflow step 4 at :778-784)",
                    "src/boomi_mcp/categories/meta_tools.py:4989 (force-clear hint)",
                    "src/boomi_mcp/categories/meta_tools.py:5180-5190 (_COMPONENT_CLONE)",
                    "src/boomi_mcp/categories/meta_tools.py:8867 (serves _COMPONENT_CREATE)",
                    "src/boomi_mcp/categories/meta_tools.py:8880 (serves _COMPONENT_CLONE)",
                    "server.py:3178 (get_schema_template wrapper docstring)"),
        "legacy_guidance": "served templates carry type=\"process\" raw XML, the "
                           "raw_xml_escape_hatch text, and legacy process protocols",
        "owning_issue": "#160",
        "post_retraction_assertion": "no served template emits a process-typed raw XML "
                                     "skeleton or a raw-XML escape-hatch instruction, AND no "
                                     "template advertises a legacy process protocol "
                                     "(database_to_api_sync / wrapper_subprocess / "
                                     "sync_pipeline).",
    },
    {
        "surface_id": "SS-RAW-API",
        "source_modules": ("src/boomi_mcp/categories/meta_tools.py",),
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
        "source_modules": ("src/boomi_mcp/categories/meta_tools.py",),
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
        "source_modules": ("src/boomi_mcp/authoring/contract.py",
                            "src/boomi_mcp/categories/integration_authoring.py"),
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
            # Non-Python assets under the scan roots are read by NOTHING here.
            # Zero exist today, so this is latent — but #152/#157/#159 land
            # manifests and fixtures, and a count nobody freezes is growth the
            # gate cannot see.
            "unscanned_asset_count": len(unscanned_assets()),
            "census_kinds": list(CENSUS_KINDS),
            "frozen_key": ["census", "path", "symbol", "form"],
            "excluded_from_equality": [
                "evidence_line", "column offsets", "source text", "formatting",
                "comments and docstrings", "argument values",
                # True at EVERY scope: module bindings are pre-indexed by
                # `visit_Module`, function-local ones by `_scoped`, both before
                # any body is scanned. Ordering was fail-OPEN in both halves —
                # an alias assigned after a nested `def` erased the whole nested
                # caller — so this claim is now earned rather than asserted.
                "intra-file ordering (bindings are pre-indexed at every scope)",
                "vendor SDK paths and line numbers",
            ],
            "vocabulary": {k: list(v) for k, v in sorted(vocab.items())},
        },
        "census": census,
        "ledger_rows": ledger_rows(census),
        "component_xml_write_routes": [dict(r, locations=list(r["locations"]))
                                       for r in WRITE_ROUTES],
        "route_reconciliation": reconciliation,
        "served_surface_retraction_matrix": [
            dict(r, anchors=list(r["anchors"]),
                 source_modules=list(r["source_modules"]))
            for r in RETRACTION_MATRIX],
        "sdk_evidence": sdk_evidence(),
    }
    if include_served:
        document["served_artifacts"] = collect_served_artifacts()
        document["served_surface_retraction_matrix"] = _resolve_matrix_producers(
            document["served_surface_retraction_matrix"], document["served_artifacts"])
    return document


def _resolve_matrix_producers(matrix: List[Dict[str, Any]],
                              artifacts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Fill each matrix row's producers and artifact IDs from what was COLLECTED.

    The `producer` cell used to be hand-written, and it went stale: the
    SS-BUILDER-DIAGNOSTICS row named only the integration_builder preflight while
    its artifacts also come from `processes.manage_process_action`. Worse, the
    Markdown emitter derived the column at render time, so the rendered table
    looked right while the authoritative JSON stayed wrong — #160 reading the
    fixture got a different answer from #160 reading §11.6.

    Deriving both here means the record cannot disagree with itself, and the
    hand-maintained `anchors` are checked against the derived producers by
    `test_the_retraction_matrix_anchors_cover_every_producer_module`.
    """
    by_class: Dict[str, List[Dict[str, Any]]] = {}
    for artifact in artifacts:
        by_class.setdefault(artifact["surface_class"], []).append(artifact)
    resolved = []
    for row in matrix:
        owned = by_class.get(row["surface_id"], [])
        # `producer` was the hand-written, stale key this function exists to
        # replace — carrying it alongside the derived `producers` left two
        # disagreeing answers in one record for any consumer still reading it.
        resolved.append(dict(
            {k: v for k, v in row.items() if k != "producer"},
            producers=sorted({a["producer"] for a in owned}),
            artifact_ids=sorted(a["artifact_id"] for a in owned),
        ))
    return resolved


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

    for field in ("python_source_count", "example_document_count",
                  "unscanned_asset_count"):
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
                      "unclassified_dynamic", "unclassified_reference")),
    ("producers", ("process_kind_producer", "process_kind_consumer",
                   "example_producer", "authoring_boundary")),
    ("writes", ("component_xml_write", "http_client_call", "raw_api_invoker")),
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
    # Renders the FROZEN fields. Re-deriving them here once masked a stale
    # authoritative record: the rendered table was right while the JSON #160
    # would quote was wrong.
    return _table(
        ("Surface ID", "Surface class", "Caller-visible producer(s)",
         "HEAD source anchor(s)", "Frozen artifact IDs", "Legacy guidance exposed",
         "Owning endgame step", "Required post-retraction assertion"),
        [(r["surface_id"], r["surface_class"],
          "<br>".join(r.get("producers") or []) or "—",
          "<br>".join(r["anchors"]),
          "<br>".join(r.get("artifact_ids") or []) or "—",
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
