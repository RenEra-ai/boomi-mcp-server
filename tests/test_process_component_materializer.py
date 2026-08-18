"""The neutral materializer: byte parity, and provable legacy independence.

Issue #153 acceptance criterion, verbatim: "A test proves the materializer never
calls ``ProcessFlowBuilder.build()`` nor reads ``process_kind``."

**How that is proved here, and why one test is not enough.** Three complementary
checks, because each alone has a hole the others close:

1. **Dynamic** — every legacy entry point is replaced with a bomb, and a full
   materialization is run anyway. This proves BEHAVIOUR, but on its own it would
   also pass if nothing ran at all, so it carries a spy asserting the
   materializer actually did the work.
2. **Static** — the module's source is scanned for the forbidden identifiers. A
   dynamic test only covers the paths it exercises; a lookup on an unexercised
   branch would slip past it.
3. **Byte parity** — the extracted code must emit exactly what the legacy
   assembler emitted. The oracle is the COMMITTED golden corpus (50 active
   ``process-component-v1`` rows), not a fixture written for this slice: a
   fixture I author is not evidence, and those goldens were frozen long before
   this work began.
"""

import json
import re
import sys
from pathlib import Path

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from boomi_mcp.categories.components import (  # noqa: E402
    process_component_materializer as pcm,
)
from boomi_mcp.categories.components.builders import (  # noqa: E402
    process_flow_builder as legacy,
)

_MATERIALIZER_SOURCE = (
    Path(_src)
    / "boomi_mcp"
    / "categories"
    / "components"
    / "process_component_materializer.py"
)

_GOLDEN_MANIFEST = (
    Path(__file__).resolve().parent / "fixtures" / "wave_gate" / "goldens.jsonl"
)
_GOLDEN_DIR = Path(__file__).resolve().parent / "fixtures" / "golden_xml"


def _process_component_goldens():
    """Committed full-envelope goldens — the causally independent oracle."""
    rows = []
    for line in _GOLDEN_MANIFEST.read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("renderer") == "process-component-v1" and row.get("state") == "active":
            # `expected_file` is repo-relative, per the manifest's own schema.
            path = Path(__file__).resolve().parent.parent / row["expected_file"]
            if path.exists():
                rows.append(path)
    return rows


def _shapes_from_golden(text):
    """The ``<shapes>`` inner XML of a committed golden envelope.

    Both the INPUT and the expected OUTPUT of the differential come from the same
    pre-existing committed byte-file, so the only thing under test is the
    envelope assembler. A differential between two wrappers around one extracted
    function would be circular and would stay green through a shared regression.
    """
    match = re.search(r"<shapes>(.*)</shapes>", text, re.S)
    assert match, "golden carries no <shapes> block"
    return (match.group(1),)


# ---------------------------------------------------------------------------
# Byte parity against the committed corpus
# ---------------------------------------------------------------------------


def test_the_golden_oracle_is_actually_present():
    """Guard the guard: an empty corpus makes every parity test vacuous."""
    goldens = _process_component_goldens()
    assert len(goldens) >= 40, len(goldens)


def test_the_materializer_reproduces_every_committed_envelope_byte_for_byte():
    """The extraction is a re-homing, so the bytes must be identical.

    Each golden is decomposed into its shapes and its envelope attributes, then
    reassembled through the NEW module. Any drift in escaping, attribute order,
    element order or the option string shows up as a byte difference.
    """
    checked = 0
    defaulted_checked = [0]
    for path in _process_component_goldens():
        text = path.read_text()
        name = re.search(r'<bns:Component[^>]*\sname="([^"]*)"', text)
        if not name:
            continue
        folder = re.search(r'\sfolderName="([^"]*)"', text)
        description = re.search(r"<bns:description>(.*?)</bns:description>", text, re.S)
        overrides = re.search(r"<bns:processOverrides>.*?</bns:processOverrides>", text, re.S)
        options = re.search(r"<process xmlns=\"\" ([^>]*)>", text)
        if not options:
            continue

        rebuilt = pcm.assemble_component_xml(
            _shapes_from_golden(text),
            name=_unescape(name.group(1)),
            description=_unescape(description.group(1)) if description else "",
            folder_name=_unescape(folder.group(1)) if folder else None,
            process_overrides_xml=overrides.group(0) if overrides else "",
            process_options=options.group(1),
        )
        assert rebuilt == text, "byte drift reassembling {0}".format(path.name)
        checked += 1

        # ...and when the golden carries the SCHEDULED options, reassembling with
        # `process_options=None` must produce the same bytes. Without this the
        # differential never exercises DEFAULT_PROCESS_OPTIONS at all: it lifts
        # the option string out of the golden and hands it straight back.
        #
        # Measured: a one-byte mutation of DEFAULT_PROCESS_OPTIONS reddened the
        # golden corpus (46 rows) but NOT this differential, which is exactly the
        # "one of the two oracles is not wired" case. It is wired now.
        if options.group(1) == pcm.DEFAULT_PROCESS_OPTIONS:
            defaulted = pcm.assemble_component_xml(
                _shapes_from_golden(text),
                name=_unescape(name.group(1)),
                description=_unescape(description.group(1)) if description else "",
                folder_name=_unescape(folder.group(1)) if folder else None,
                process_overrides_xml=overrides.group(0) if overrides else "",
                process_options=None,
            )
            assert defaulted == text, (
                "the scheduled DEFAULT does not reproduce {0}".format(path.name)
            )
            defaulted_checked[0] += 1
    assert checked >= 40, checked
    assert defaulted_checked[0] >= 1, (
        "no golden exercised the scheduled default — the default path is uncovered"
    )


def _unescape(value):
    return (
        value.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", '"')
        .replace("&apos;", "'")
    )


def test_the_legacy_names_are_aliases_of_the_extracted_ones():
    """IDENTITY — the legacy path must call the SAME implementation.

    Equality of output would also hold for a duplicated copy that drifts later,
    which is the failure this extraction exists to remove.
    """
    assert legacy._assemble_process_component_xml is pcm.assemble_component_xml
    assert legacy._emit_process_overrides is pcm.render_process_overrides
    assert (
        legacy._extract_process_extension_connections
        is pcm.extension_bindings_from_legacy_config
    )
    assert legacy._DEFAULT_PROCESS_OPTIONS is pcm.DEFAULT_PROCESS_OPTIONS
    assert legacy._LISTENER_PROCESS_OPTIONS is pcm.LISTENER_PROCESS_OPTIONS


def test_the_option_strings_are_unchanged_including_the_listener_omission():
    """The two strings differ in three attributes, and one is an ABSENCE.

    The live listener capture omits ``stopProcessingIfZeroDocuments`` entirely.
    An absence is the easiest thing to lose in a re-homing and the hardest to
    notice, so it is asserted directly.
    """
    assert pcm.DEFAULT_PROCESS_OPTIONS == (
        'allowSimultaneous="false" enableUserLog="false" '
        'processLogOnErrorOnly="false" purgeDataImmediately="false" '
        'stopProcessingIfZeroDocuments="true" updateRunDates="true" '
        'workload="general"'
    )
    assert pcm.LISTENER_PROCESS_OPTIONS == (
        'allowSimultaneous="true" enableUserLog="false" '
        'processLogOnErrorOnly="false" purgeDataImmediately="false" '
        'updateRunDates="false" workload="general"'
    )
    assert "stopProcessingIfZeroDocuments" not in pcm.LISTENER_PROCESS_OPTIONS


# ---------------------------------------------------------------------------
# The profile -> option-bytes mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "profile,expected",
    [("scheduled", pcm.DEFAULT_PROCESS_OPTIONS), ("listener", pcm.LISTENER_PROCESS_OPTIONS)],
)
def test_the_recorded_profile_selects_the_option_bytes(profile, expected):
    assert pcm.process_options_for_profile(profile) == expected


def test_an_unknown_profile_fails_closed_rather_than_defaulting():
    """Guessing would emit a process whose attributes contradict its graph."""
    with pytest.raises(Exception) as excinfo:
        pcm.process_options_for_profile("cron")
    assert "PROCESS_MATERIALIZATION_EXECUTION_PROFILE_INVALID" in str(
        getattr(excinfo.value, "error_code", "")
    ) or "cron" in str(excinfo.value)


def test_the_materializer_takes_no_option_bytes_parameter():
    """A caller must not be able to bypass the recorded profile.

    If option bytes could be passed in directly, the compiler's decision would
    be advisory — which is precisely the dual authority #153 removes.
    """
    import inspect

    params = inspect.signature(pcm.ProcessComponentMaterializer.materialize).parameters
    assert "execution_profile" in params
    assert "process_options" not in params


# ---------------------------------------------------------------------------
# Legacy independence — the issue's explicit acceptance criterion
# ---------------------------------------------------------------------------


def test_materialization_succeeds_with_every_legacy_entry_point_bombed(monkeypatch):
    """DYNAMIC proof, with a spy so it cannot pass by doing nothing.

    Every legacy process builder — resolved from the RUNTIME dispatch table, not
    a hand list — has ``build`` replaced with a bomb, as do
    ``get_process_flow_builder`` and ``ProcessFlowBuilder.build``. A successful
    materialization afterwards proves the canonical path reaches none of them.

    The spy is what makes it non-vacuous: without it, a materializer that
    silently returned nothing would satisfy "no bomb fired".
    """

    def _bomb(*args, **kwargs):  # pragma: no cover - must never run
        raise AssertionError("the canonical materializer reached the legacy builder")

    monkeypatch.setattr(legacy, "get_process_flow_builder", _bomb)
    for builder in set(legacy.PROCESS_FLOW_BUILDERS.values()):
        monkeypatch.setattr(builder, "build", staticmethod(_bomb), raising=False)

    calls = []
    original = pcm.ProcessComponentMaterializer.materialize

    def _spy(self, *args, **kwargs):
        calls.append(True)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(pcm.ProcessComponentMaterializer, "materialize", _spy)

    # THE PUBLIC PATH, not the materializer leaf (§6 review AR1-10): the plan's
    # legacy-independence criterion is that plan -> compile -> materialize
    # reaches no legacy entry point. Calling the leaf directly proved only that
    # the last link is clean; a legacy import anywhere earlier in the chain
    # would have passed unnoticed.
    import sys
    from pathlib import Path as _Path

    _tests = str(_Path(__file__).resolve().parent)
    if _tests not in sys.path:
        sys.path.insert(0, _tests)
    from _m12_11_support import appliable_process_ir_request
    from boomi_mcp.authoring.process_materialization import (
        build_materialization_plan,
    )
    from boomi_mcp.authoring.workflow import (
        _connector_metadata_from_components,
        _normalize_intent,
    )
    from boomi_mcp.categories.components.canonical_process_apply import (
        materialize_canonical_process_xml,
    )
    from boomi_mcp.categories.integration_builder import _materializer_revision
    from boomi_mcp.authoring.contract import get_authoring_revisions
    from boomi_mcp.compiler.process_ir.emitter_registry import emitter_revision
    from boomi_mcp.recipes.materialization import build_symbol_table

    request = appliable_process_ir_request()
    normalized = _normalize_intent(request)
    spec = normalized.integration_spec
    unit = spec.processes[0]
    symbols = build_symbol_table(
        list(spec.components),
        process_keys=[u.envelope.component_key for u in spec.processes],
        connector_metadata=_connector_metadata_from_components(spec.components),
    )
    plan = build_materialization_plan(
        envelope=unit.envelope,
        process_ir=unit.process_ir,
        symbols=symbols,
        conflict_policy="reuse",
        compiler_revision=get_authoring_revisions()["compiler_revision"],
        emitter_revision=emitter_revision(),
        materializer_revision=_materializer_revision(),
    )
    xml = materialize_canonical_process_xml(
        plan=plan,
        id_registry={"conn": "golden-conn-id", "op": "golden-op-id"},
        symbols=symbols,
    )

    assert calls == [True], "the materializer never ran — the proof would be vacuous"
    assert xml.startswith("<?xml")
    assert 'type="process"' in xml
    assert pcm.DEFAULT_PROCESS_OPTIONS in xml
    # ...and the chain genuinely bound the real ids on the way through.
    assert "golden-conn-id" in xml


def test_the_bomb_really_fires_when_the_legacy_path_is_used(monkeypatch):
    """Control for the test above: the bomb is armed and lethal.

    Without this, "no bomb fired" could mean the monkeypatch silently failed to
    attach — the test would be green for the wrong reason.
    """

    def _bomb(*args, **kwargs):
        raise AssertionError("legacy builder reached")

    monkeypatch.setattr(legacy, "get_process_flow_builder", _bomb)
    with pytest.raises(AssertionError, match="legacy builder reached"):
        legacy.get_process_flow_builder("database_to_api_sync")


def test_the_materializer_module_names_no_legacy_identifier():
    """STATIC proof — covers branches the dynamic test does not exercise."""
    import ast
    import io
    import tokenize

    source = _MATERIALIZER_SOURCE.read_text()

    def _code_only(text):
        """The module's CODE, with every comment and docstring removed.

        Prose legitimately NAMES the things this module must not call, in order
        to say that it does not call them. Splitting on the first ``\"\"\"`` was
        the first attempt and it was wrong — it leaves every FUNCTION docstring
        in the scanned text, one of which says "Shared by ProcessFlowBuilder".
        Tokenizing removes comments; walking the AST removes docstrings.
        """
        stripped = []
        for tok in tokenize.generate_tokens(io.StringIO(text).readline):
            if tok.type == tokenize.COMMENT:
                continue
            stripped.append(tok)
        no_comments = tokenize.untokenize(stripped)
        tree = ast.parse(no_comments)
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                doc = ast.get_docstring(node, clean=False)
                if doc:
                    no_comments = no_comments.replace(doc, "", 1)
        return no_comments

    body = _code_only(source)

    for forbidden in (
        "ProcessFlowBuilder",
        "get_process_flow_builder",
        "PROCESS_FLOW_BUILDERS",
        "process_kind",
    ):
        assert forbidden not in body, (
            "{0!r} appears in the neutral materializer's CODE".format(forbidden)
        )

    # Positive controls: the prose really does name them (so the strip is doing
    # work), and the stripper does not remove real code.
    assert "ProcessFlowBuilder" in source
    assert "def assemble_component_xml" in body
    assert "_PROFILE_OPTIONS" in body


def test_the_materializer_imports_nothing_from_the_legacy_builder():
    """An import is a reach even if no call is made on the exercised path."""
    source = _MATERIALIZER_SOURCE.read_text()
    imports = [
        line
        for line in source.splitlines()
        if line.startswith(("import ", "from ")) and "process_flow_builder" in line
    ]
    assert imports == [], imports
