"""#184 correction batch 21b (CDX-184-r20-02) — the behaviour corpus's guard, harvest and producer.

Not a ``test_*`` module, so pytest never collects it. It is a pytest PLUGIN with two modes,
and the CLI that produces the packaged corpus.

**Guard.** ``tests/conftest.py`` declares ``pytest_plugins``, so the guard is on in every
run under ``tests/`` — a single module, the full suite, the wave gate; ``-p
_revision_corpus`` turns it on from anywhere else. While a COVERED module is IMPORTED and
while each of its tests runs (:func:`covered_targets`), every call it makes to a compiler
entry point (:data:`boomi_mcp.authoring.revision_corpus.ENTRY_POINTS`) is recorded exactly
and looked up in the packaged corpus. The test FAILS, naming the producer command, when an
input is absent, when an input cannot be recorded exactly, or when it reaches one of the
four internal funnels in :data:`_PROBES` with no entry point on the stack — and what the
module did while it was IMPORTED is carried to every test in it (a covered module that
collects no test fails its own collection instead, and a refusal no test report delivered
— under ``-k``, a marker skip, ``--collect-only`` — fails the session from
``pytest_sessionfinish``). One-directional — harvest ⊆ corpus — and checked on the very run
that exercises the inputs, so no test runs twice.

**The window, stated as a rule rather than a count.** What the guard sees is what runs in
THIS interpreter between a covered module's collect report and its last teardown. Measured
instances outside it, none reachable today and none silently inside the corpus: a call from
``tests/conftest.py`` itself (imported at preparse, before any wrapper exists — which is why
that file carries nothing but its declaration, and the guard module pins every conftest on
the collection path); a call in a SUBPROCESS (the child interpreter has no guard); and a
call after the session ends (an ``atexit`` handler, a ``__del__`` at shutdown), which
``pytest_sessionfinish`` has already run past (CDX-184-r7-G3-ATEXIT-01).

**Harvest.** The producer's child run: the same recording, written out instead of checked,
with every recorded call replayed in place and compared with what the test observed::

    PYTHONPATH=src .venv/bin/python tests/_revision_corpus.py --write

What is recorded: the OUTERMOST entry-point call only (a resolver that validates internally
is one input; its nested calls are functions of it); never a call made while
``_compiler_revision_payload`` computes or the corpus replays, which are revision material
already; and every module copy of an entry point, because the suite imports the package as
both ``boomi_mcp.*`` and ``src.boomi_mcp.*``.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from functools import wraps
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

CORPUS_PATH = SRC / "boomi_mcp" / "authoring" / "revision_corpus_v1.json.gz"
GUARD_MODULE = "tests/test_issue_184_revision_corpus.py"
PLUGIN_DECLARATION = "tests/conftest.py"


def producer_command():
    """The one command that rewrites the corpus, read from the module that reads the corpus
    (``boomi_mcp.authoring.revision_corpus.PRODUCER_COMMAND``) rather than spelled again
    here. Imported lazily: this plugin is registered in every run, and importing the package
    at registration time would change what a test sees as the first import."""
    from boomi_mcp.authoring.revision_corpus import PRODUCER_COMMAND

    return PRODUCER_COMMAND


#: The producer's child writes its harvest here and fails no test.
HARVEST_OUT_ENV = "REVISION_CORPUS_HARVEST_OUT"
#: Further modules treated as covered — for the guard's own non-vacuity witness. Additive only.
EXTRA_COVERED_ENV = "REVISION_CORPUS_EXTRA_COVERED"

#: Set for the HARVEST pass only. A revision-invariant test asserts that its own source
#: mutants move the served revision, which cannot hold while the corpus is the stale one
#: being regenerated — and this producer refuses to write until the covered tests are clean.
#: The two deadlock on a merged tree; with this set, that one assertion branch stands down so
#: the harvest completes in a single pass. It is NOT set for `--check` or for the suite, so
#: the invariant is asserted everywhere the corpus is real (CDX-184-r8, batch 21a merge).
#: NOTHING IN THIS TREE READS IT: the reader is 21a's own test, which is why a grep here finds
#: only this producer and its witness (CDX-184-r9-R5-BOOTSTRAP-NOCONSUMER-05).
BOOTSTRAP_ENV = "REVISION_CORPUS_BOOTSTRAP"

#: The four funnels every recorded entry kind passes through, watched so a covered test
#: cannot reach the behaviour BEHIND an entry point without the corpus seeing the input.
#: Deliberately these four and not every compiler internal: a covered test that calls
#: `emitter_registry.emit_process`, `lowering.lower_process_ir_to_cfg` or
#: `prepare_validation_context` directly reaches real behaviour with no entry point on the
#: stack, and widening the probes would mean making each of those a recorded ENTRY KIND —
#: its own encoding, its own verdict projection, its own corpus rows — rather than a
#: refusal. The served bound says "through an entry point's verdict" for exactly that
#: reason (CDX-184-r5-CORPUS-04); what this set buys is that the entry points cannot be
#: bypassed from inside, which is how the recorded inputs stop being representative.
_PROBES = (
    ("compiler.process_ir.semantic_validation.lineage", "_walk_lineage"),
    ("compiler.process_ir.semantic_validation.pipeline", "_validate_prepared"),
    ("compiler.process_ir.pipeline", "_compile_parsed_process_ir_v1"),
    ("authoring.process_ir_effects", "_entry_contract_bindings"),
)
_EXCLUDED = (
    ("authoring.contract", "_compiler_revision_payload"),
    ("authoring.revision_corpus", "replay"),
)
_SPELLINGS = ("boomi_mcp.", "src.boomi_mcp.")


def covered_targets():
    """The covered test modules: every #184 module, and the lineage and effect-declaration
    suites. Never this corpus's own guard module, whose witnesses perturb the compiler."""
    modules = sorted(path.relative_to(ROOT).as_posix()
                     for path in (ROOT / "tests").glob("test_issue_184_*.py"))
    modules += ["tests/test_process_ir_semantic_lineage.py", "tests/test_process_ir_effect_declarations.py"]
    return [module for module in modules if module != GUARD_MODULE]


def covered_paths():
    paths = {str((ROOT / module).resolve()) for module in covered_targets()}
    for extra in filter(None, os.environ.get(EXTRA_COVERED_ENV, "").split(os.pathsep)):
        paths.add(str(Path(extra).resolve()))
    return frozenset(paths)


_COVERED = None


def covered():
    """:func:`covered_paths`, computed once: it is asked for every collector in the run."""
    global _COVERED
    if _COVERED is None:
        _COVERED = covered_paths()
    return _COVERED


_RESOLVED = {}


def _resolve(path):
    if path not in _RESOLVED:
        _RESOLVED[path] = str(Path(path).resolve())
    return _RESOLVED[path]


def _item_path(item):
    return _resolve(str(item.fspath))


class _Session:
    """One run's instrumentation: installed once, active only while a covered test runs."""

    def __init__(self, harvest_out, covered):
        from boomi_mcp.authoring import revision_corpus as corpus

        self.corpus = corpus
        self.covered = covered
        self.handle = open(harvest_out, "a", encoding="utf-8") if harvest_out else None
        self.packaged, self.unreadable = None, None
        self.active = False
        self.depth = 0
        self.excluded = 0
        self.node = None
        self.problems = []
        #: The import hook, removed at session end.
        self.finder = None
        #: ``{covered module path: problems recorded while it was imported}``.
        self.import_problems = {}
        #: Covered module paths whose import-time refusal reached a test report.
        self.delivered = set()
        self.seen = set()
        #: ids of the wrappers this session made, and the objects they replaced — held, so no
        #: id can be reused while `install` is deciding what is already wrapped.
        self.wrappers = set()
        self.installed = []
        self.stats = {"calls": 0, "recorded": 0, "unrecordable": 0, "fidelity_mismatch": 0,
                      "probe_hits": 0, "absent": 0}

    def digests(self):
        """The packaged inputs, read on the first recorded call. Read LATE: the session is
        created before a covered module is imported, and a run whose covered tests call no
        entry point must not pay for parsing the corpus."""
        if self.packaged is None:
            self.packaged = frozenset()
            try:
                self.packaged = frozenset(
                    self.corpus.digest(record) for record in self.corpus.load_corpus())
            except Exception as exc:  # noqa: BLE001 - every covered test reports it instead
                self.unreadable = type(exc).__name__
        return self.packaged

    # --- installation ---------------------------------------------------------------
    def _targets(self):
        for kind, (module_name, name) in self.corpus.ENTRY_POINTS.items():
            yield module_name, name, lambda real, kind=kind: self._entry_wrapper(kind, real)
        for module_name, name in _PROBES:
            yield module_name, name, lambda real, name=name: self._probe_wrapper(name, real)
        for module_name, name in _EXCLUDED:
            yield module_name, name, self._excluded_wrapper

    def install(self):
        """Wrap every loaded copy of every target, then repoint every module-level reference.

        Idempotent by WRAPPER IDENTITY, never by module name. Keying completed work as
        ``(name, attribute)`` modelled "this module object is wrapped" with a fact about a
        name, and a re-import or a ``reload`` replaces the object behind that name: the round-7
        import hook then fired for precisely the import it could not act on, leaving every
        entry point of the fresh copy unwrapped and its calls outside the guard
        (CDX-184-r8-HOOK4-REIMPORT-01, measured on eviction, reload, threads and atexit).
        The question asked here is the one that matters — is what this name holds one of MY
        wrappers — and the wrappers are held, so no id can be reused while it is answered.
        """
        replacements = {}
        for spelling in _SPELLINGS:
            for module_name, name, make in self._targets():
                module = sys.modules.get(spelling + module_name)
                if module is None:
                    continue
                # A module can be in sys.modules and still be running its own body — the
                # import hook installs as each target finishes, and a cycle means an earlier
                # one finishes first. Wrapping waits for the name to exist rather than
                # raising out of the importing module.
                real = getattr(module, name, None)
                if real is None or id(real) in self.wrappers:
                    continue
                wrapper = make(real)
                self.wrappers.add(id(wrapper))
                self.installed.append((module, name, real, wrapper))
                replacements[id(real)] = (real, wrapper)
                setattr(module, name, wrapper)
        if not replacements:
            return
        for module in list(sys.modules.values()):
            namespace = getattr(module, "__dict__", None)
            if not isinstance(namespace, dict):
                continue
            for attribute, value in list(namespace.items()):
                replacement = replacements.get(id(value))
                if replacement is not None and replacement[0] is value:
                    namespace[attribute] = replacement[1]

    def refresh(self):
        """Install on any copy imported since the last pass. Cheap when there is none: the
        scan below only runs when something was actually wrapped."""
        self.install()

    def uninstall(self):
        """Put every wrapped binding back. Called at session end so a second session in the
        same interpreter starts from the real functions rather than from this one's wrappers
        (CDX-184-r8-HOOK4-SESSIONEND-03)."""
        originals = {id(wrapper): real for _module, _name, real, wrapper in self.installed}
        for module in list(sys.modules.values()):
            namespace = getattr(module, "__dict__", None)
            if not isinstance(namespace, dict):
                continue
            for attribute, value in list(namespace.items()):
                real = originals.get(id(value))
                if real is not None:
                    namespace[attribute] = real
        self.installed, self.wrappers = [], set()

    # --- wrappers -------------------------------------------------------------------
    def _excluded_wrapper(self, real):
        @wraps(real)
        def excluded(*args, **kwargs):
            self.excluded += 1
            try:
                return real(*args, **kwargs)
            finally:
                self.excluded -= 1
        return excluded

    def _probe_wrapper(self, name, real):
        @wraps(real)
        def probe(*args, **kwargs):
            if self.active and self.depth == 0 and not self.excluded:
                caller = sys._getframe(1).f_code
                self.stats["probe_hits"] += 1
                self._problem("reached {0} from {1}:{2} with no entry point on the stack, so the "
                              "corpus cannot see what it exercised".format(
                                  name, Path(caller.co_filename).name, caller.co_name))
            return real(*args, **kwargs)
        return probe

    def _entry_wrapper(self, kind, real):
        @wraps(real)
        def entry(*args, **kwargs):
            if not self.active or self.depth or self.excluded:
                return real(*args, **kwargs)
            return self._record(kind, real, args, kwargs)
        return entry

    def _problem(self, message):
        self.problems.append(message)
        if self.handle is not None:
            self.write({"node": self.node, "problem": message})

    def write(self, row):
        self.handle.write(json.dumps(row) + "\n")

    def _record(self, kind, real, args, kwargs):
        corpus = self.corpus
        self.stats["calls"] += 1
        answers, raised = [], []
        try:
            record = corpus.encode_call(kind, real, args, kwargs)
        except corpus.Unrecordable as exc:
            record = exc
        except Exception as exc:  # noqa: BLE001 - e.g. a call its own signature cannot bind
            record = corpus.Unrecordable("encode:" + type(exc).__name__)
        answering = not isinstance(record, corpus.Unrecordable) and kind == "resolve" and record["symbols_for"]
        if answering:
            bound = corpus.bound_arguments(real, args, kwargs)
            inner = bound["symbols_for"]

            def recording(key, root):
                try:
                    table = inner(key, root)
                except BaseException:
                    raised.append(key)
                    raise
                answers.append((key, table))
                return table

            bound["symbols_for"] = recording
            args, kwargs = (), bound
        self.depth += 1
        result, error = None, None
        try:
            result = real(*args, **kwargs)
        except BaseException as exc:  # noqa: BLE001 - re-raised below, untouched
            error = exc
        finally:
            self.depth -= 1
        if answering:
            try:
                if raised:
                    # An injected fault is not an input: there is no answer to replay.
                    raise corpus.Unrecordable("symbols-for-raised")
                record["symbols_for"] = corpus.encode_symbols_for(answers)
            except corpus.Unrecordable as exc:
                record = exc
        if isinstance(record, corpus.Unrecordable):
            self.stats["unrecordable"] += 1
            self._problem("a {0} input the corpus cannot record exactly ({1})".format(kind, record.reason))
        elif self.handle is not None:
            self._harvest(kind, record, result, error)
        else:
            self.stats["recorded"] += 1
            key = corpus.digest(record)
            packaged = self.digests()
            if self.unreadable:
                self._problem("the packaged corpus could not be read ({0})".format(self.unreadable))
            elif key not in packaged:
                self.stats["absent"] += 1
                self._problem("a {0} input absent from the packaged corpus ({1})".format(kind, key[:16]))
        if error is not None:
            raise error
        return result

    def _harvest(self, kind, record, result, error):
        corpus = self.corpus
        key = corpus.digest(record)
        self.stats["recorded"] += 1
        row = {"node": self.node, "entry": kind, "digest": key}
        if key not in self.seen:
            self.seen.add(key)
            row["record"] = record
        if error is None or isinstance(error, Exception):
            # Replayed HERE, under whatever the test has patched, so a difference can only be
            # the record failing to rebuild what the test handed over.
            observed = corpus.project_outcome(kind, result, error)
            self.depth += 1
            try:
                replayed = corpus.replay(record)
            finally:
                self.depth -= 1
            if observed != replayed:
                self.stats["fidelity_mismatch"] += 1
                row["observed"], row["replayed"] = observed, replayed
        self.write(row)


class _InstallingLoader:
    """The real loader, plus one call: wrap the module the instant its body has run."""

    def __init__(self, loader, session):
        self._loader, self._session = loader, session

    def create_module(self, spec):
        return self._loader.create_module(spec)

    def exec_module(self, module):
        self._loader.exec_module(module)
        # Hand the module back its real loader: the wrapper exists for the instant after the
        # body runs, and leaving it on `__spec__`/`__loader__` would be a permanent, uninvited
        # change to another module's import metadata (CDX-184-r8-HOOK4-SPECMUT-02).
        spec = getattr(module, "__spec__", None)
        if spec is not None:
            spec.loader = self._loader
        module.__loader__ = self._loader
        self._session.install()

    def __getattr__(self, name):  # get_data, get_resource_reader, is_package, …
        return getattr(self._loader, name)


class _InstallOnImport:
    """Wrap a target module's entry points the moment that module is imported.

    `install()` can only wrap what is already in `sys.modules` and `refresh()` runs at
    collect/test boundaries, so a copy first imported INSIDE a covered test stayed unwrapped
    for that whole test. Measured on the `src.boomi_mcp.` spelling: the same covered module
    was silently green alone and refused after another module had warmed that copy — the
    guard's answer depended on import order (CDX-184-r7-G3-GUARD-SRC-01). Both spellings are
    covered here because `_SPELLINGS` is the authority, not a list of names written twice.
    """

    def __init__(self, session):
        self.session = session
        self.targets = {spelling + module_name
                        for spelling in _SPELLINGS
                        for module_name, _name, _make in session._targets()}
        self.resolving = set()

    def find_spec(self, name, path=None, target=None):
        """A FRESH spec wrapping the real loader, never the live one.

        `importlib.util.find_spec` returns a module's own `__spec__` when the name is already
        imported — which is exactly the `reload` path — so assigning to `spec.loader` mutated
        the loaded module's metadata and nested one wrapper per reload
        (CDX-184-r8-HOOK4-SPECMUT-02). The remaining finders are asked directly, with the
        `path` Python passed, and what they answer is copied rather than edited.
        """
        if name not in self.targets or name in self.resolving:
            return None
        import importlib.machinery

        self.resolving.add(name)
        try:
            for finder in list(sys.meta_path):
                if finder is self:
                    continue
                find = getattr(finder, "find_spec", None)
                if find is None:
                    continue
                try:
                    spec = find(name, path, target)
                except Exception:  # noqa: BLE001 - another finder's failure is not ours to raise
                    continue
                if spec is None or spec.loader is None or isinstance(spec.loader, _InstallingLoader):
                    continue
                wrapped = importlib.machinery.ModuleSpec(
                    spec.name, _InstallingLoader(spec.loader, self.session), origin=spec.origin,
                    is_package=spec.submodule_search_locations is not None)
                wrapped.submodule_search_locations = spec.submodule_search_locations
                wrapped.has_location = spec.has_location
                wrapped.cached = spec.cached
                return wrapped
        finally:
            self.resolving.discard(name)
        return None


_SESSION = None


def _session():
    """The instrumentation, created the first time a covered module is reached.

    Created lazily, so a run that collects no covered test neither imports the package nor
    reads the corpus — and created BEFORE the covered module is imported, with every entry
    point's own module loaded and wrapped first: a call a covered module makes in its BODY
    is an input like any other, and installing after collection made those calls invisible
    to both the harvest and the guard (CDX-184-r5-CORPUS-02).
    """
    global _SESSION
    if _SESSION is None:
        import importlib

        from boomi_mcp.authoring import revision_corpus as corpus

        # Every module a wrapper is installed on, EXCLUSIONS INCLUDED. Import order decided
        # the guard's answer otherwise: when a covered test was the first code to import
        # `boomi_mcp.authoring.contract`, `_compiler_revision_payload` stayed unwrapped for
        # that whole test and the entry-point calls the served revision makes inside it were
        # recorded as the test's own inputs — two covered modules FAILED when run on their
        # own, and no regeneration could clear it (CDX-184-r6-GUARD-R2-01).
        for module_name, _name in list(corpus.ENTRY_POINTS.values()) + list(_PROBES) + list(_EXCLUDED):
            importlib.import_module("boomi_mcp." + module_name)
        _SESSION = _Session(os.environ.get(HARVEST_OUT_ENV), covered())
        _SESSION.install()
        # …and every copy imported from here on, under either spelling, is wrapped as it
        # loads rather than at the next collect/test boundary. Removed again at session end.
        _SESSION.finder = _InstallOnImport(_SESSION)
        sys.meta_path.insert(0, _SESSION.finder)
    return _SESSION


def _collector_path(collector):
    path = getattr(collector, "path", None) or getattr(collector, "fspath", None)
    return None if path is None else _resolve(str(path))


@pytest.hookimpl(hookwrapper=True)
def pytest_make_collect_report(collector):
    """The recording window around a covered module's own IMPORT.

    ``collector.collect()`` is what imports a test module, so this wrapper brackets exactly
    the import — and every later collection step of the same file. A module-body entry-point
    call is then recorded like a call from a test body.

    A refusal is carried to that module's OWN TESTS rather than failing the collection: a
    failed collect report interrupts the whole session, so one stale import-time input would
    hide every other result in the suite. A covered module that collects no test has nowhere
    to carry it, and only then is the report itself failed.
    """
    path = _collector_path(collector)
    session = _session() if path is not None and path in covered() else None
    if session is not None:
        session.refresh()
        session.node = getattr(collector, "nodeid", None) or path
        session.problems = []
        session.active = True
    outcome = yield
    if session is None:
        return
    session.active = False
    problems, session.problems = list(dict.fromkeys(session.problems)), []
    if session.handle is not None or not problems:
        return
    report = outcome.get_result()
    if getattr(report, "result", None):
        session.import_problems.setdefault(path, []).extend(problems)
        return
    report.outcome = "failed"
    message = _refusal(problems)
    report.longrepr = message if not report.longrepr else "{0}\n\n{1}".format(report.longrepr, message)


def pytest_runtest_setup(item):
    path = _item_path(item)
    session = _session() if path in covered() else _SESSION
    if session is not None:
        session.refresh()
        session.node = item.nodeid
        # What the module did while it was IMPORTED belongs to every test in it: the module
        # body ran once, for all of them, and there is no earlier report to carry it.
        session.problems = list(session.import_problems.get(path, ()))
        session.delivered.add(path)
        session.active = path in session.covered


def _refusal(problems):
    return "\n".join(
        ["#184 behaviour corpus: this covered test hands the compiler input the served "
         "compiler revision does not replay:"]
        + ["  - " + problem for problem in problems]
        + ["Regenerate the packaged corpus: " + producer_command()]
    )


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    session = _SESSION
    if session is None or not session.active:
        return
    if call.when == "teardown":
        session.active = False
    if session.handle is not None or not session.problems or call.when == "setup":
        return
    report = outcome.get_result()
    message = _refusal(dict.fromkeys(session.problems))
    session.problems = []
    if hasattr(report, "wasxfail"):
        del report.wasxfail
    report.outcome = "failed"
    report.longrepr = message if not report.longrepr else "{0}\n\n{1}".format(report.longrepr, message)


def pytest_sessionfinish(session, exitstatus):  # noqa: D401 - `session` is pytest's
    """An import-time refusal must not depend on a test running.

    It is carried to the module's own tests, and `-k`, a marker skip, `--collect-only` or a
    deselection can mean none of them reports: the refusal was then swallowed and the run
    exited 0 (CDX-184-r6-GUARD-R2-03). Anything undelivered is printed here and fails the
    session, whatever the selection was.
    """
    global _SESSION, _COVERED

    pytest_session = session
    # The covered-path set is cached for every collector, so a run that built no session still
    # populated it — and it survived into the next in-process session
    # (CDX-184-r9-R5-SESSION-COVERED-LEAK-04). Dropped before anything else can return.
    _COVERED = None
    if _SESSION is None:
        return
    session, _SESSION = _SESSION, None
    if session.finder is not None and session.finder in sys.meta_path:
        sys.meta_path.remove(session.finder)
    session.uninstall()
    if session.handle is not None:
        session.write({"stats": session.stats, "exitstatus": int(exitstatus)})
        session.handle.close()
        return
    undelivered = {path: problems for path, problems in session.import_problems.items()
                   if path not in session.delivered}
    if not undelivered:
        return
    reporter = pytest_session.config.pluginmanager.get_plugin("terminalreporter")
    for path, problems in sorted(undelivered.items()):
        message = "{0}\n{1}".format(path, _refusal(dict.fromkeys(problems)))
        if reporter is not None:
            reporter.write_line(message, red=True)
        else:  # pragma: no cover - a run with no terminal reporter still fails below
            print(message)
    # RAISED, never lowered: a run interrupted by an unrelated collection error exits 2, and
    # overwriting that with a plain "tests failed" would tell a reader the wrong thing about
    # a run that already carries a refusal (CDX-184-r7-G3-EXITSTATUS-01).
    pytest_session.exitstatus = max(int(exitstatus or 0), 1)


# ---------------------------------------------------------------------------
# the producer
# ---------------------------------------------------------------------------


def child_env(env=None):
    """The documented suite environment (``PYTHONPATH=src``, no plugin autoload, local and
    offline), plus this directory so ``-p _revision_corpus`` resolves to the same module a
    ``pytest_plugins`` declaration registers. No harvest or coverage setting is inherited."""
    child = dict(os.environ)
    child.pop(HARVEST_OUT_ENV, None)
    child.pop(EXTRA_COVERED_ENV, None)
    child.pop(BOOTSTRAP_ENV, None)
    child.pop("PYTHONHASHSEED", None)
    child.update({
        "PYTHONPATH": os.pathsep.join([str(SRC), str(ROOT / "tests")]),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "BOOMI_LOCAL": "true",
        "BOOMI_DOCS_ENABLED": "false",
        "BOOMI_GOTCHAS_ENABLED": "false",
    })
    child.update(env or {})
    return child


def run_pytest(targets, *, python=None, env=None):
    """pytest over ``targets`` in a fresh interpreter, with this plugin loaded by name."""
    argv = [python or sys.executable, "-m", "pytest", "-p", "_revision_corpus", "-p", "no:cacheprovider",
            "-q", "--no-header", "-rfE", *targets]
    return subprocess.run(argv, cwd=str(ROOT), env=child_env(env), capture_output=True, text=True)


def read_harvest(out_path):
    harvest = {"records": {}, "rows": [], "problems": [], "stats": None}
    with open(out_path, encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if "stats" in row:
                harvest["stats"] = row
            elif "problem" in row:
                harvest["problems"].append(row)
            else:
                if "record" in row:
                    harvest["records"][row["digest"]] = row["record"]
                harvest["rows"].append(row)
    return harvest




def harvest(targets=None, python=None, env=None):
    """``(process, harvest)`` for the covered tests (or ``targets``) in harvest mode."""
    with tempfile.TemporaryDirectory() as scratch:
        out = os.path.join(scratch, "harvest.jsonl")
        Path(out).write_text("", encoding="utf-8")
        child = dict(env or {})
        child[HARVEST_OUT_ENV] = out
        child.setdefault(BOOTSTRAP_ENV, "1")
        process = run_pytest(covered_targets() if targets is None else targets, python=python,
                             env=child)
        return process, read_harvest(out)


def write(corpus_path=CORPUS_PATH, python=None):
    """Harvest the covered tests and write the corpus — only from a clean, faithful harvest."""
    from boomi_mcp.authoring import revision_corpus as corpus

    process, harvested = harvest(python=python)
    problems = ["{0}: {1}".format(row["node"], row["problem"]) for row in harvested["problems"]]
    problems += ["{0}: a {1} input replays to a verdict the test did not observe".format(row["node"], row["entry"])
                 for row in harvested["rows"] if "observed" in row]
    stats = harvested["stats"]
    if stats is None or process.returncode != 0 or stats["exitstatus"] != 0:
        tail = "\n".join((process.stdout + process.stderr).splitlines()[-30:])
        problems.insert(0, "the covered tests did not run clean (exit {0}), so the harvest is "
                           "incomplete:\n{1}".format(process.returncode, tail))
    if problems:
        return False, "\n".join(problems)
    blob = corpus.serialize_corpus(harvested["records"].values())
    Path(corpus_path).write_bytes(blob)
    return True, "wrote {0} inputs from {1} calls ({2} bytes) to {3}".format(
        len(harvested["records"]), stats["stats"]["calls"], len(blob), corpus_path)


def _main(argv):
    parser = argparse.ArgumentParser(description="Produce or check the compiler-revision behaviour corpus.")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--write", action="store_true", help="harvest the covered tests and write the corpus")
    action.add_argument("--check", action="store_true",
                        help="run the covered tests with the guard on, as the suite does")
    parser.add_argument("--corpus", default=str(CORPUS_PATH), help="where --write writes")
    options = parser.parse_args(argv)
    if options.write:
        ok, report = write(corpus_path=options.corpus)
        print(report)
        return 0 if ok else 1
    process = run_pytest(covered_targets())
    sys.stdout.write(process.stdout)
    sys.stderr.write(process.stderr)
    return process.returncode


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
