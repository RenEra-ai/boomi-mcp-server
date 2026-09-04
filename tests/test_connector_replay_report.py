"""The derived report: tracked, in sync with the registry, and deterministic."""

from __future__ import annotations

import subprocess
from pathlib import Path

from boomi_mcp.connector_replay.registry import ReplayRegistry
from boomi_mcp.connector_replay.report import (
    REPORT_RELATIVE_PATH,
    render,
)

_REPO = Path(__file__).resolve().parents[1]
_REPORT = _REPO / REPORT_RELATIVE_PATH


def test_the_report_is_tracked_not_merely_present():
    """A working-tree-only report lets the registry and its summary drift unseen.

    `docs/*` is ignored wholesale in this repository, so this file exists only
    because of an explicit carve-out. Checking the file is present would pass even
    if the carve-out were removed; checking git tracks it is the real question.
    """
    assert _REPORT.is_file(), "the report has not been generated"
    out = subprocess.run(
        ["git", "check-ignore", str(_REPORT.relative_to(_REPO))],
        cwd=_REPO, capture_output=True, text=True,
    )
    assert out.returncode != 0, (
        "the report is git-ignored ({0}); the carve-out in .gitignore is missing, so "
        "the registry's published summary would never be reviewable".format(out.stdout.strip())
    )
    listed = subprocess.run(
        ["git", "ls-files", "--error-unmatch", str(_REPORT.relative_to(_REPO))],
        cwd=_REPO, capture_output=True, text=True,
    )
    assert listed.returncode == 0, "the report is not tracked by git"


def test_the_tracked_report_matches_what_the_registry_renders():
    """Drift between the registry and its published summary is the failure here."""
    assert _REPORT.read_text() == render(), (
        "the tracked report is stale. Regenerate it — the report is generated, and a "
        "hand-edit to it is a claim the registry does not make."
    )


def test_rendering_is_deterministic():
    assert render() == render()


def test_an_empty_registry_says_so_in_words():
    """An empty table with no explanation reads like a rendering bug."""
    text = render(ReplayRegistry((), ()))
    assert "No actions have been verified" in text
    assert "refuses a retry" in text
    assert "No connector types are mapped" in text


def test_the_report_states_the_fail_closed_consequence():
    text = render()
    assert "unverified" in text and "refuses a retry" in text


def test_no_credential_material_in_the_report():
    text = _REPORT.read_text().lower()
    for needle in ("password", "secret", "token", "credential://", "api_key", "@"):
        assert needle not in text, "the published report contains {0!r}".format(needle)


def test_the_served_report_states_what_the_registry_holds():
    """Parsed back and compared to the REGISTRY, not to the renderer.

    The existing checks assert the tracked file equals `render()`, which is true
    whatever `render()` says — a renderer that omitted a whole section would keep
    them green, and one did: the report called itself complete while never
    publishing the packaged operation records, the rows that actually authorise a
    retry. Parsing the served text gives something to compare against the facts.
    """
    from boomi_mcp.connector_replay.registry import load_registry
    from boomi_mcp.connector_replay.report import parse, render

    registry = load_registry()
    served = parse(render(registry))

    # WHOLE ROWS, SORTED — not a set of one field. A single-field set comparison
    # passes with the family, action, semantics or revision corrupted, and it
    # COLLAPSES multiplicity: one abstract contract may now be recorded for
    # several accounts, so a set keyed on the reference hides a row the renderer
    # dropped. Sorted lists compare the values and the count together.
    assert sorted(
        (r["contract_ref"], r["family"], r["action"], r["semantics_id"],
         r["semantics_revision"]) for r in served["operation_records"]
    ) == sorted(
        (r.contract_ref, r.family, r.action, r.semantics_id, r.semantics_revision)
        for r in registry.operation_records
    ), "the report and the registry disagree about the packaged contract records"

    assert sorted(
        (v["platform_connector_type"], v["family"], v["action_source"])
        for v in served["vocabulary"]
    ) == sorted(
        (v.platform_connector_type, v.family, v.action_source.value)
        for v in registry.vocabulary
    ), "the report and the registry disagree about the connector vocabulary"

    # The evidence section, ALL SIX fields. Comparing family and action alone let
    # the side effect, the retry safety, the execution count and the capture be
    # corrupted or truncated while this stayed green — and those four are what
    # the section exists to state.
    assert sorted(
        (r["family"], r["action"], r["side_effect"], r["retry_safety"],
         r["executions"], r["capture"])
        for r in served["observed_actions"]
    ) == sorted(
        (r.family, r.action, r.side_effect.value, r.retry_safety.value,
         len(r.execution_ids), r.capture_digest[:12])
        for r in registry.evidence_records
    ), "the report and the registry disagree about what was observed"

    # NON-VACUITY: every compared collection must be non-empty, or the equalities
    # above are pairs of empty lists agreeing with each other.
    assert registry.operation_records and registry.vocabulary and registry.evidence_records


def test_the_report_parser_refuses_a_row_it_cannot_read():
    """A partial view of a report is how a missing section becomes an empty one."""
    import pytest

    from boomi_mcp.connector_replay.report import parse

    with pytest.raises(ValueError, match="five columns"):
        parse("## Operation contract records\n\n| a | b |\n")
    with pytest.raises(ValueError, match="not an integer"):
        parse("## Operation contract records\n\n| `r` | rest | GET | sem | many |\n")


def test_the_report_publishes_the_terms_its_citations_name():
    """Every collection the registry holds is stated, not just the ones cited.

    `ARCH-155-r8-08` recorded that the tracked report rendered neither the key
    semantics definitions nor the projection allowlists. Both are the TERMS of
    claims the report already made: an operation record cites a semantics id and
    revision, and a configuration digest decides "unchanged" over a closed
    projection. Publishing the citation while withholding the definition asked a
    reader to trust a contract whose text was not served.

    Derived from the registry rather than pinned to strings, so a collection that
    grows a member is a report that must state it.
    """
    from boomi_mcp.connector_replay.registry import load_registry
    from boomi_mcp.connector_replay.report import parse, render

    reg = load_registry()
    read = parse(render(reg))

    assert len(read["semantics_definitions"]) == len(reg.semantics_definitions)
    for spec in reg.semantics_definitions:
        served = [r for r in read["semantics_definitions"]
                  if r["semantics_id"] == spec.semantics_id
                  and r["revision"] == spec.revision]
        assert len(served) == 1, f"{spec.semantics_id} is not served exactly once"
        # THE TERMS, each one. A row naming the definition and omitting what it
        # means would satisfy a count and publish nothing a reader can act on.
        assert served[0]["mechanism"] == spec.mechanism.value
        assert served[0]["key_scope"] == spec.key_scope.value
        assert served[0]["duplicate_guarantee"] == spec.duplicate_guarantee.value

    assert len(read["projection_allowlists"]) == len(reg.projection_allowlists)
    for spec in reg.projection_allowlists:
        served = [r for r in read["projection_allowlists"]
                  if r["family"] == spec.family
                  and r["component_kind"] == spec.component_kind
                  and r["projection_version"] == spec.projection_version]
        assert len(served) == 1, f"{spec.component_kind} is not served exactly once"
        # The WIDTH is derived from the projection's own members, so a projection
        # that gains or loses a field changes the served text. A hand-written
        # number here would state a width the projection had at some past moment.
        assert served[0]["included"] == (
            len(spec.included_attributes) + len(spec.included_value_fields)
            + len(spec.included_property_fields) + len(spec.included_elements)
            + len(spec.included_scope_attributes))
        assert served[0]["excluded"] == (
            len(spec.excluded_fields) + len(spec.excluded_scope_attributes))


def test_a_report_missing_the_new_sections_is_refused():
    """THE NON-VACUITY WITNESS: the parser must refuse what it used to accept.

    Before this change a report with no semantics and no projection section was a
    valid report — which is exactly how the omission survived. Each heading is
    removed in turn from a rendered report and the parse must fail.
    """
    import pytest

    from boomi_mcp.connector_replay.report import parse, render

    text = render()
    for heading in ("Contract key semantics", "Component projection allowlists"):
        stripped, dropping = [], False
        for line in text.splitlines():
            if line.startswith("## "):
                dropping = line == f"## {heading}"
            if not dropping:
                stripped.append(line)
        with pytest.raises(ValueError):
            parse("\n".join(stripped))
