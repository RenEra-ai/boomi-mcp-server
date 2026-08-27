"""Regression pins for the Stage-2 review's findings.

Every test here is the constructed case that DEMONSTRATED the defect before it was
fixed — each one failed against the reviewed tree. They are kept together because
they share a theme: each is a way the evidence pipeline could have granted replay
safety it had not observed, which is the one direction this system must not fail in.
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest

from boomi_mcp.connector_replay.capture import CaptureRefused, summarize
from boomi_mcp.connector_replay.digests import (
    ConfigDigestRefused,
    RouteDigestRefused,
    component_config_digest_v1,
    route_digest_v1,
)
from boomi_mcp.connector_replay.ids import is_execution_id
from boomi_mcp.connector_replay.ingest import IngestRefused, classify, verify_archive
from boomi_mcp.connector_replay.models import RetrySafetyV1, SideEffectV1

_REPO = Path(__file__).resolve().parents[1]
_ROOT = _REPO / "docs" / "architecture" / "evidence" / "issue-155"
_C = _ROOT / "captures"
_CONN = (_C / "cap155-e1-conn-readback" / "rest-conn-c4281346.xml").read_text()
_OP = '<Operation><field id="path" value="/z"/></Operation>'
_REAL = "execution-1957bb8f-9a89-4254-b169-9ddbf41fddf8-2026.08.26"


def test_a_declared_method_absent_from_the_log_is_refused():
    """The worst case in the round: a read's status minting a delete's row.

    A log holding a single UNRELATED request used to have its status returned and
    then paired with the DECLARED action, so a successful HEAD could produce an
    idempotent DELETE row.
    """
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp) / "cap"
        shutil.copytree(_C / "cap155-e4-head-status", d)
        log = d / "mock_access_log.txt"
        log.write_text(next(l for l in log.read_text().splitlines() if '"HEAD ' in l) + "\n")
        with pytest.raises(CaptureRefused) as err:
            summarize(d, "DELETE")
        assert "no DELETE request" in str(err.value)


def test_a_deleted_manifest_entry_is_detected():
    """Walking only what exists cannot see what is gone.

    A capture that lost its counterparty log used to verify clean and then be
    classified on whatever evidence remained.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "a"
        shutil.copytree(_ROOT, root)
        (root / "captures" / "cap155-e4-head-status" / "mock_access_log.txt").unlink()
        with pytest.raises(IngestRefused) as err:
            verify_archive(root, root / "captures" / "cap155-e4-head-status")
        assert "MISSING" in str(err.value)


def test_a_replay_verdict_requires_two_executions():
    """Staged readbacks alone are not a double execution."""
    summary = summarize(_C / "cap155-e3b-patch-canonical")
    assert len(summary.execution_ids) == 2

    one_run = summary.model_copy(update={"runs": summary.runs[:1]})
    assert len(one_run.execution_ids) == 1
    side_effect, retry = classify(
        one_run.model_copy(update={
            "runs": tuple(r.model_copy(update={"counterparty_status": 200}) for r in one_run.runs)
        })
    )
    assert retry is not RetrySafetyV1.CONDITIONALLY_IDEMPOTENT


def test_the_replay_is_evaluated_across_every_subject():
    """Negative controls must count against the verdict, not be filtered out.

    Only subjects the first call moved used to be evaluated, so a replay that
    unexpectedly changed a previously untouched resource was invisible — and that
    is the strongest possible evidence AGAINST replay safety.
    """
    summary = summarize(_C / "cap155-e3b-patch-canonical")
    subjects = {c.subject for c in summary.convergence}
    assert subjects == {"target", "template"}

    # Make the untouched control move on the replay, with a non-volatile field.
    poisoned = tuple(
        c.model_copy(update={"replay_changed_state": True,
                             "fields_differing_on_replay": ("status",)})
        if c.subject == "template" else c
        for c in summary.convergence
    )
    attested = summary.model_copy(update={
        "convergence": poisoned,
        "runs": tuple(r.model_copy(update={"counterparty_status": 200}) for r in summary.runs),
    })
    side_effect, retry = classify(attested)
    assert retry is RetrySafetyV1.NON_IDEMPOTENT, (
        "a side effect on a subject the calls never targeted must defeat the "
        "conditionally-idempotent verdict"
    )


def test_a_dynamically_bound_path_has_no_static_route_digest():
    """Every blank-path operation would otherwise share one digest."""
    with pytest.raises(RouteDigestRefused) as err:
        route_digest_v1(_CONN, '<Operation><field id="path" value=""/></Operation>')
    assert "dynamically bound" in str(err.value)


def test_request_altering_operation_fields_reach_the_config_digest():
    """These are `<field id=...>` entries, not element tags — measured.

    The earlier code scanned for element tags with those names, which no component
    carries, so the branch was dead and two operations differing in redirect
    behaviour digested identically.
    """
    a = '<Operation><field id="path" value="/x"/><field id="followRedirects" value="true"/></Operation>'
    b = '<Operation><field id="path" value="/x"/><field id="followRedirects" value="false"/></Operation>'
    assert component_config_digest_v1(a, "operation") != component_config_digest_v1(b, "operation")


def test_header_and_parameter_keys_reach_the_config_digest_but_values_do_not():
    """The fixture is GENERATED BY THE EMITTER, never hand-written.

    Two previous versions of this digest read a shape no component carries — first
    an element tag, then a `name` attribute — and each time the test agreed with the
    code because the test invented the same shape the code expected. Every archived
    capture has an EMPTY customProperties block, so the archive could not settle it
    either; the authority is the builder that emits these components, whose own
    docstring records the live components its shape was verified against.

    Generating the fixture from that emitter means the test cannot construct a
    shape the system never produces.
    """
    from boomi_mcp.categories.components.builders.connector_builder import (
        RestClientOperationBuilder,
    )

    emit = RestClientOperationBuilder._build_customproperties_field
    wrap = lambda body: '<Operation><field id="path" value="/x"/>' + body + "</Operation>"
    digest = lambda body: component_config_digest_v1(wrap(body), "operation")

    empty = emit("requestHeaders", {})
    one_key = emit("requestHeaders", {"X-Api-Key": "s3cret"})
    same_key_other_value = emit("requestHeaders", {"X-Api-Key": "DIFFERENT"})
    extra_key = emit("requestHeaders", {"X-Api-Key": "s3cret", "X-Trace": "1"})

    # Non-vacuity: the generated fixture must actually be populated, or every
    # assertion below compares two empty blocks and passes for the wrong reason.
    assert "properties" in one_key and "X-Api-Key" in one_key

    assert digest(empty) != digest(one_key), "a header key must reach the digest"
    assert digest(one_key) != digest(extra_key), "an added key must reach the digest"
    assert digest(one_key) == digest(same_key_other_value), (
        "the VALUE must NOT reach the digest — a static header value is where an "
        "API key gets parked, and this digest is published"
    )


def test_internal_empty_path_segments_are_route_significant():
    c1 = _CONN.replace('value="http://host.docker.internal:8081"', 'value="http://h/a//b"')
    c2 = _CONN.replace('value="http://host.docker.internal:8081"', 'value="http://h/a/b"')
    assert route_digest_v1(c1, _OP) != route_digest_v1(c2, _OP)


def test_a_trailing_separator_is_still_insignificant():
    """The control for the test above: not every separator difference is a route."""
    c1 = _CONN.replace('value="http://host.docker.internal:8081"', 'value="http://h/a/b"')
    c2 = _CONN.replace('value="http://host.docker.internal:8081"', 'value="http://h/a/b/"')
    assert route_digest_v1(c1, _OP) == route_digest_v1(c2, _OP)


def test_an_execution_id_with_a_trailing_newline_is_rejected():
    """`$` is not a whole-string anchor in Python; it matches before a final newline."""
    assert is_execution_id(_REAL)
    assert not is_execution_id(_REAL + "\n")


def test_a_route_duplicate_field_reports_the_route_code():
    dup = '<C><field id="url" value="http://a/x"/><field id="url" value="http://b/y"/></C>'
    with pytest.raises(RouteDigestRefused) as err:
        route_digest_v1(dup, _OP)
    assert err.value.code == "CONNECTOR_REPLAY_ROUTE_DIGEST_REFUSED"
    # ...and the config digest still reports its own.
    with pytest.raises(ConfigDigestRefused) as cfg:
        component_config_digest_v1(dup, "connection")
    assert cfg.value.code == "CONNECTOR_REPLAY_CONFIGURATION_DIGEST_REFUSED"


def test_the_real_archive_still_verifies_and_classifies():
    """The control for the whole batch: none of it broke the real evidence."""
    for name, action in [("cap155-e4-head-status", "HEAD"),
                         ("cap155-e4-options-status", "OPTIONS"),
                         ("cap155-e4-trace-status", "TRACE")]:
        verify_archive(_ROOT, _C / name)
        summary = summarize(_C / name, action)
        assert classify(summary) == (SideEffectV1.READ, RetrySafetyV1.IDEMPOTENT)
    verify_archive(_ROOT, _C / "cap155-e4-negative-control")
    control = summarize(_C / "cap155-e4-negative-control", "DELETE")
    assert classify(control) == (SideEffectV1.UNKNOWN, RetrySafetyV1.UNVERIFIED)


def test_property_key_serialization_is_injective():
    """Separator-joined values collide, and the builder accepts the colliding inputs.

    `','.join(keys)` made `["a,b"]` and `["a", "b"]` identical, and an empty key
    indistinguishable from no keys. Both are reachable: the builder accepts
    arbitrary non-secret strings as keys, verified by emitting them here rather
    than assuming.
    """
    from boomi_mcp.categories.components.builders.connector_builder import (
        RestClientOperationBuilder,
    )

    emit = RestClientOperationBuilder._build_customproperties_field
    wrap = lambda body: '<Operation><field id="path" value="/x"/>' + body + "</Operation>"
    digest = lambda body: component_config_digest_v1(wrap(body), "operation")

    comma_in_key = emit("requestHeaders", {"a,b": "1"})
    two_keys = emit("requestHeaders", {"a": "1", "b": "2"})
    assert "a,b" in comma_in_key, "the builder refused a comma key; this test is vacuous"
    assert digest(comma_in_key) != digest(two_keys)

    assert digest(emit("requestHeaders", {})) != digest(emit("requestHeaders", {"": "1"}))

    # The controls: the fix must not have flattened the distinctions that matter.
    assert digest(emit("requestHeaders", {"X": "1"})) != digest(emit("requestHeaders", {"Y": "1"}))
    assert digest(emit("requestHeaders", {"X": "1"})) == digest(emit("requestHeaders", {"X": "2"}))


def test_a_field_value_cannot_impersonate_an_adjacent_field():
    """The same injectivity hazard one level up, which the report did not name.

    The payload used to be newline-joined `key=value` lines, so a value containing
    a newline could have been read as a separate field.
    """
    honest = '<Operation><field id="path" value="/x"/></Operation>'
    smuggled = ('<Operation><field id="path" value="/x&#10;field:followRedirects=true"/>'
                '</Operation>')
    also = ('<Operation><field id="path" value="/x"/>'
            '<field id="followRedirects" value="true"/></Operation>')
    assert component_config_digest_v1(smuggled, "operation") != component_config_digest_v1(also, "operation")
    assert component_config_digest_v1(smuggled, "operation") != component_config_digest_v1(honest, "operation")
