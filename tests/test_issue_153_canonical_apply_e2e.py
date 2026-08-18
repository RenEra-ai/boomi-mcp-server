"""#153 (M12.15): the canonical apply, driven END TO END through the dispatcher.

**Why this file exists.** Round-2 live QA found five sequential Critical defects
between ``build_integration(action="apply")`` and a materialized process
component — a missing import, a binder that demanded an applied id for the root
being created, a silently dropped root, a second unguarded component lookup, and
both attestations discarded on success. Every one of them was a 100%-reproducible
FIRST-CALL failure, and all 71 unit tests over the canonical modules were green,
because ``grep -rn "_execute_canonical_process" tests/`` returned nothing: the
tests called the pieces directly with hand-built plans, and nothing drove the
seam that assembles them.

So the discipline here is deliberately different from a unit test. Every test
below enters through ``build_integration_action`` — the same function the MCP
tool layer calls — with only the network boundary faked. A defect anywhere in the
wiring surfaces as a failure here, which is precisely what the unit tests could
not do.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _m12_11_support import (  # noqa: E402
    APPLIABLE_CONN,
    APPLIABLE_OP,
    appliable_process_ir_request as process_ir_request,
    appliable_process_unit as process_unit,
)
from boomi_mcp.authoring.workflow import (  # noqa: E402
    CompiledBundle,
    compile_authoring_request_v1,
)
from boomi_mcp.categories import integration_builder  # noqa: E402
from boomi_mcp.categories.integration_builder import (  # noqa: E402
    _BUILD_REGISTRY,
    build_integration_action,
)
from boomi_mcp.errors import (  # noqa: E402
    AUTHORING_COMPILE_BLOCKED,
    PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE,
)
from boomi_mcp.models.process_component import (  # noqa: E402
    ProcessConnectionOverrideV1,
    ProcessExtensionBindingsV1,
    ProcessOverrideFieldV1,
)

_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"
_EXECUTE = "boomi_mcp.categories.integration_builder._execute_component"
_CREATE = "boomi_mcp.categories.integration_builder.create_component"
_GET_XML = "boomi_mcp.categories.integration_builder.component_get_xml"

_PROFILE = "qa_profile"
_PROCESS_ID = "process-cid-1"

#: Fallback readback XML for the non-process components, which this test does
#: not materialize and whose live shape it therefore does not model.
_LIVE_COMPONENT_XML = (
    '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
    'type="connector-settings"><bns:object><connection/></bns:object>'
    "</bns:Component>"
)


@pytest.fixture(autouse=True)
def _clean_registry():
    _BUILD_REGISTRY.clear()
    yield
    _BUILD_REGISTRY.clear()


@pytest.fixture(autouse=True)
def _no_live_metadata_queries(monkeypatch):
    monkeypatch.setattr(integration_builder, "paginate_metadata", lambda *a, **k: [])


def _bound_payload(request=None):
    """A compiled, binding-carrying apply payload for a direct ProcessIR intent."""
    request = request if request is not None else process_ir_request()
    result, _internals = compile_authoring_request_v1(
        request, boomi_client=MagicMock(), profile=_PROFILE
    )
    payload = request.model_dump(mode="json")
    payload["expected_capability_revision"] = result.revision_binding.capability_revision
    payload["expected_compile_hash"] = result.revision_binding.compile_hash
    return payload


#: The XML the materializer submitted for the process root, captured by the last
#: `_apply`. Used as the LIVE readback so verify round-trips the real emitted
#: artifact instead of a hand-written stub whose graph would have to be modelled
#: here — a stub is a second hand-model of the emitter's output, and it would
#: silently decide what verify sees.
_SUBMITTED: dict = {}


def _apply(payload, *, create_result=None):
    """Drive a real apply with only the network boundary faked."""
    created = {"n": 0}
    _SUBMITTED.clear()

    def _component(*_args, **_kwargs):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml:
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = create_result or _create
        get_xml.side_effect = _live_xml
        return build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={"authoring_request": payload, "dry_run": False},
        )


def _live_xml(_client, component_id, *_a, **_k):
    """The live component read-back, faked at the network boundary only."""
    if component_id == _PROCESS_ID and "xml" in _SUBMITTED:
        return {"type": "process", "xml": _SUBMITTED["xml"]}
    return {"type": "connector-settings", "xml": _LIVE_COMPONENT_XML}


def _spec_with_canonical_root():
    """The legacy ``integration_spec`` config root, carrying a canonical root."""
    return {
        "name": "M12.15 Integration",
        "components": [APPLIABLE_CONN, APPLIABLE_OP],
        "processes": [process_unit().model_dump(mode="json")],
    }


def _plan_or_compile(action, request):
    with patch(_PAGINATE) as paginate:
        paginate.return_value = []
        return build_integration_action(
            MagicMock(),
            _PROFILE,
            action,
            config={"authoring_request": request.model_dump(mode="json")},
        )


# ---------------------------------------------------------------------------
# the capability itself (QA-153-r2-01/02/03/04)
# ---------------------------------------------------------------------------


def test_a_direct_process_ir_apply_creates_the_process_component():
    """The headline capability, through the public dispatcher.

    This single assertion is what the whole r2 finding cluster reduces to: a
    caller who compiles a ProcessIR root and applies the binding gets a process
    component. It fails on the pre-fix tree for four independent reasons, which
    is exactly why it is one test and not four.
    """
    result = _apply(_bound_payload())

    assert result["_success"] is True, result.get("error")
    assert "proc" in result["execution_order"]
    assert result["results"]["proc"]["component_id"] == _PROCESS_ID
    assert result["results"]["proc"]["status"] == "created"
    assert result["results"]["proc"]["type"] == "process"


def test_the_applied_spec_still_carries_the_roots_the_compile_described():
    """QA-153-r2-01: the bundle must not be the SERVED projection.

    The served preview withholds the authored roots (they are caller content),
    and the bundle's spec is also the apply INPUT — so building one from the
    preview deleted every process root on its way to the builder while the
    envelope still reported a successful mutation.
    """
    result = _apply(_bound_payload())

    assert "proc" in result["results"]
    # The dropped-root shape reported success with the root simply absent, so
    # asserting success alone would have passed on the defect.
    assert [k for k in result["results"] if k == "proc"] == ["proc"]


def test_a_bundle_built_from_the_served_projection_is_refused():
    """The non-vacuity witness for the invariant that enforces the above.

    Constructs exactly the object the defect constructed. The guard is a
    ``__post_init__`` on ``CompiledBundle``, so this is the concrete case it
    excludes — and if the invariant were removed, this test is what notices.
    """
    request = process_ir_request()
    compile_result, _internals = compile_authoring_request_v1(
        request, boomi_client=MagicMock(), profile=_PROFILE
    )
    # The compile DOES describe the root...
    assert [s.component_key for s in compile_result.process_cfg] == ["proc"]
    # ...and the served preview does NOT carry it.
    assert list(compile_result.integration_spec_preview.processes) == []

    with pytest.raises(Exception) as excinfo:
        CompiledBundle(
            integration_spec=compile_result.integration_spec_preview,
            compile_result=compile_result,
            request=request,
        )
    assert getattr(excinfo.value, "code", None) == AUTHORING_COMPILE_BLOCKED


def test_the_design_doctrine_advisory_survives_a_canonical_root():
    """QA-153-r2-02: the second unguarded ``components_by_key`` read.

    Driven through the LEGACY ``integration_spec`` root, which is where the
    KeyError actually surfaces — and where QA measured it. Routing this through
    the typed root instead makes the test pass on the broken tree for the wrong
    reason: `_legacy_plan_echo` wraps `_build_plan` in a bare
    ``except Exception``, so the crash is swallowed and the typed plan reports
    success with its component-plan lint silently disabled.
    """
    spec = _spec_with_canonical_root()
    result = build_integration_action(
        MagicMock(), _PROFILE, "plan", config={"integration_spec": spec}
    )

    assert result.get("exception_type") != "KeyError", result.get("error")
    assert result["_success"] is True, result.get("error")
    step = [s for s in result["steps"] if s["key"] == "proc"][0]
    assert step["type"] == "process"
    assert step["materialization"] == "process_ir_v1"
    assert "proc" in result["execution_order"]


def test_the_component_plan_lint_still_runs_for_a_canonical_root():
    """The other half of QA-153-r2-02, and the reason it hid for a whole round.

    ``_legacy_plan_echo`` swallows any exception from ``_build_plan`` and the
    typed plan then reports the lint as *unavailable* rather than as failed. So
    the KeyError did not merely crash the legacy route — it silently switched
    OFF the duplicate-connection, base-URL, folder and name lints on the typed
    route, and reported success while doing it.
    """
    result = _plan_or_compile("plan", process_ir_request())

    assert result["_success"] is True, result.get("error")
    messages = [
        diagnostic["message"]
        for diagnostic in result["authoring_result"]["warnings"]
    ]
    assert not [m for m in messages if "lint did not run" in m], messages


def test_a_plan_step_naming_no_component_is_a_named_refusal():
    """The other half of the accessor's contract: fail CLOSED, but by name.

    A genuinely missing component must still be an error — the fix must not turn
    the crash into a silent skip. It becomes an ``IntegrationDependencyError``
    carrying a code a caller can branch on, rather than a bare ``KeyError`` the
    outermost handler serves as ``"Integration builder failed: 'proc'"``.
    """
    from boomi_mcp.categories.integration_builder import (
        IntegrationDependencyError,
        _step_component,
    )

    with pytest.raises(IntegrationDependencyError) as excinfo:
        _step_component({"key": "absent"}, {})
    assert excinfo.value.error_code

    # ...and the canonical step is skipped rather than looked up at all.
    assert (
        _step_component(
            {"key": "proc", "materialization": "process_ir_v1"}, {}
        )
        is None
    )


def test_the_emitted_process_carries_real_ids_and_no_placeholders():
    """QA-153-r2-04: late binding, observed on the bytes that were submitted.

    The binder resolves only what the root DECLARES and leaves other symbols on
    their placeholder, so the proof that nothing leaked has to be taken from the
    artifact rather than from the binder's own bookkeeping.
    """
    result = _apply(_bound_payload())

    assert result["_success"] is True, result.get("error")
    xml = _SUBMITTED["xml"]
    assert "id-db_conn" not in xml
    assert "id-api_conn" not in xml
    assert "$ref:" not in xml
    # Non-vacuity: the real applied ids ARE embedded, so the absence above is a
    # real binding rather than an artifact that references nothing at all.
    assert "cid-" in xml


# ---------------------------------------------------------------------------
# mutation accounting (QA-153-r2-05)
# ---------------------------------------------------------------------------


def test_both_attestations_survive_a_SUCCESSFUL_apply():
    """QA-153-r2-05: they were computed, then dropped on the success path.

    Served on the envelope AND recorded on the build — the record is what a
    later verify can reach, the envelope is what the caller can.
    """
    result = _apply(_bound_payload())
    assert result["_success"] is True, result.get("error")

    mutations = result["process_mutations"]
    readbacks = result["process_readbacks"]
    assert [m["component_key"] for m in mutations] == ["proc"]
    assert [r["component_key"] for r in readbacks] == ["proc"]

    mutation = mutations[0]
    assert mutation["action"] == "create"
    assert mutation["result_component_id"] == _PROCESS_ID
    assert mutation["target_component_id"] is None
    assert mutation["plan_fingerprint"].startswith("sha256:")
    assert mutation["account_scope_hash"].startswith("sha256:")
    assert mutation["submitted_xml_digest"].startswith("sha256:")
    assert readbacks[0]["component_id"] == _PROCESS_ID

    record = _BUILD_REGISTRY[result["build_id"]]
    assert record["process_mutations"] == mutations
    assert record["process_readbacks"] == readbacks


def test_a_build_with_no_canonical_root_keeps_its_original_record_shape():
    """The attestation keys are ADDITIVE — absent, not null, when unused."""
    from _m12_11_support import appliable_request

    result = _apply(_bound_payload(appliable_request()))
    assert result["_success"] is True, result.get("error")

    record = _BUILD_REGISTRY[result["build_id"]]
    assert "process_mutations" not in record
    assert "process_readbacks" not in record
    assert "process_mutations" not in result


# ---------------------------------------------------------------------------
# verify (QA-153-r2-06)
# ---------------------------------------------------------------------------


def test_verify_covers_the_process_root_and_reports_no_false_drift():
    """QA-153-r2-06: verify walked ``spec.components``, provenance walked results.

    Two hand-modelled universes: the typed provenance recorded an apply-time
    fingerprint for the root, verify never observed it, and the comparison
    called a healthy build drifted.
    """
    applied = _apply(_bound_payload())
    assert applied["_success"] is True, applied.get("error")

    with patch(_GET_XML) as get_xml, patch(_PAGINATE) as paginate:
        paginate.return_value = []
        get_xml.side_effect = _live_xml
        verified = build_integration_action(
            MagicMock(), _PROFILE, "verify", config={"build_id": applied["build_id"]}
        )

    assert "proc" in verified["verification"]
    assert verified["verification"]["proc"]["component_id"] == _PROCESS_ID
    # The root is graph-verified like any other process, not merely existence-checked.
    graph = verified["verification"]["proc"]["process_graph"]
    assert graph["errors"] == [], graph
    assert verified["verification"]["proc"]["verified"] is True

    comparison = verified.get("live_comparison") or {}
    assert "proc" not in (comparison.get("missing_components") or [])
    # The whole point of r2-06: a healthy build must verify CLEAN.
    assert verified["_success"] is True, verified.get("error")


# ---------------------------------------------------------------------------
# relocatability, decided before any write (QA-153-r2-07)
# ---------------------------------------------------------------------------


def _literal_id_request():
    """A root whose extension binding names a LITERAL component id."""
    envelope_extra = {
        "process_extensions": ProcessExtensionBindingsV1(
            connections=(
                ProcessConnectionOverrideV1(
                    connection_id="35813b90-1f42-4dcb-98f5-82d8f96be61d",
                    connector_type="rest",
                    fields=(ProcessOverrideFieldV1(id="url", label="x"),),
                ),
            )
        )
    }
    return process_ir_request(units=(process_unit(**envelope_extra),))


def test_a_literal_component_id_is_REPORTED_by_plan():
    """QA-153-r2-07(a): plan is a report, so it reports rather than refuses.

    Deliberately asserted in plan's own vocabulary — ``_success`` stays true and
    the finding lands in ``errors`` with ``is_valid: false`` — because demanding
    a refusal here would be asserting a contract this surface does not have.
    """
    result = _plan_or_compile("plan", _literal_id_request())

    payload = result["authoring_result"]
    assert payload["validation_report"]["is_valid"] is False
    assert PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE in _all_cause_codes(result)
    assert [
        d["path"]
        for d in payload["errors"]
        if d["code"] == PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE
    ] == ["/process_extensions/connections/0/connection_id"]


def test_a_literal_component_id_is_REFUSED_by_compile_before_any_write():
    """QA-153-r2-07(a)/(b): the refusal existed, but only inside the apply loop.

    A caller learned their binding was unusable only AFTER the connector
    components had been created — a partial write charged for an answer that is
    fully decidable from the request alone.
    """
    result = _plan_or_compile("compile", _literal_id_request())

    assert result["_success"] is False
    assert PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE in _all_cause_codes(result)


@pytest.mark.parametrize("action", ["plan", "compile"])
def test_the_relocatability_probe_is_not_simply_refusing_everything(action):
    """The control for the test above: the same shape with a ``$ref`` passes."""
    envelope_extra = {
        "process_extensions": ProcessExtensionBindingsV1(
            connections=(
                ProcessConnectionOverrideV1(
                    connection_id="$ref:conn",
                    connector_type="rest",
                    fields=(ProcessOverrideFieldV1(id="url", label="x"),),
                ),
            )
        )
    }
    request = process_ir_request(units=(process_unit(**envelope_extra),))
    result = _plan_or_compile(action, request)
    assert result["_success"] is True, result.get("error")


def _all_cause_codes(result):
    """Every code a refusal envelope carries, wherever the surface puts it."""
    codes = {result.get("error_code")}
    payload = result.get("authoring_result") or {}
    diagnostics = list(result.get("authoring_diagnostics") or ())
    for bucket in ("errors", "warnings", "diagnostics"):
        diagnostics.extend(payload.get(bucket) or ())
    for diagnostic in diagnostics:
        codes.add(diagnostic.get("code"))
        codes.update(diagnostic.get("cause_codes") or ())
    return {code for code in codes if code}


# ---------------------------------------------------------------------------
# refusal envelopes (QA-153-r2-08)
# ---------------------------------------------------------------------------


def _duplicate_key_payload():
    """A request whose units collide on ``component_key``, as a RAW payload.

    Built as a dict on purpose: the typed model refuses the duplicate at
    construction, so a model-built request can never reach the dispatcher arm
    these two tests are about.
    """
    payload = process_ir_request().model_dump(mode="json")
    # The collision is COMPONENT key vs PROCESS key, not unit vs unit: the unit
    # list has its own validator that refuses a duplicate at parse time, and that
    # route is already value-free. The shared key-namespace rule fires inside
    # `IntegrationSpecV1` construction instead, which is the arm QA measured the
    # echo on.
    collide = dict(payload["intent"]["components"][0])
    collide["key"] = payload["intent"]["units"][0]["envelope"]["component_key"]
    payload["intent"]["components"] = payload["intent"]["components"] + [collide]
    return payload


def test_a_validation_refusal_does_not_echo_the_authored_value():
    """QA-153-r2-08(a): pydantic's ``input_value=`` put caller content on the wire.

    The canary goes in ``integration_name`` because pydantic renders the FIRST
    field in full and elides deeper ones — measured by QA as a 16-character
    prefix reaching the served error.
    """
    canary = "hunter2-Sup3rSecret-9f1c"
    payload = _duplicate_key_payload()
    payload["intent"]["integration_name"] = canary

    result = build_integration_action(
        MagicMock(), _PROFILE, "compile", config={"authoring_request": payload}
    )

    assert result["_success"] is False
    assert canary not in str(result)
    # 12 characters, not 16: pydantic clipped the measured leak at 15
    # (`hunter2-Sup3rSe`), so a 16-character probe sits just PAST the window and
    # passes on the broken tree — the exact "guard that never exercised the
    # breaking path" shape this slice has hit twice.
    assert canary[:12] not in str(result)
    # Non-vacuity: the refusal still says WHAT went wrong and WHERE, so this is
    # a value-free diagnostic rather than an emptied one.
    assert "integration_component_key_duplicate" in str(result)


def test_an_apply_route_refusal_carries_the_typed_apply_fields():
    """QA-153-r2-08(c): the blanket handlers omitted the fields the contract names.

    This surface's own guidance is "branch on ``mutation_status``, not on one
    code" — a refusal that omits it sends the caller back to guessing.
    """
    # The LEGACY root, because that is the only apply input that reaches the
    # dispatcher's blanket handlers: a typed request is caught earlier by
    # `_reject_malformed_authoring_request`, which already decorates. Asserting
    # this through the typed root passes on the broken tree for the wrong reason.
    spec = _spec_with_canonical_root()
    spec["processes"][0]["envelope"]["component_key"] = "conn"

    result = build_integration_action(
        MagicMock(), _PROFILE, "apply", config={"integration_spec": spec, "dry_run": False}
    )

    assert result["_success"] is False
    assert result["action"] == "apply"
    assert result["mutation_performed"] is False
    assert result["mutation_status"] == "none"
    # ...and it is value-free on that arm too.
    assert "input_value=" not in result["error"]


# ---------------------------------------------------------------------------
# the structural invariant itself
# ---------------------------------------------------------------------------


def test_no_unguarded_component_lookup_survives_in_the_apply_pipeline():
    """The executable half of the structural fix — a prose rule would not hold.

    The defect class is a per-participant read that models the universe as
    ``spec.components`` while the runtime authority now also carries
    ``spec.processes``. It appeared three times in this slice, and the first two
    were fixed by guarding the individual call site, which is exactly what let
    the third ship. So the invariant is asserted on the SOURCE: every read of a
    ``*components_by_key`` mapping goes through :func:`_step_component` or
    :func:`_component_for_key`, which raise a NAMED refusal instead of a bare
    ``KeyError``. A fourth instance cannot be written without failing here.

    This guard earned its place immediately: it caught the ``_verify_build``
    lookup added in the very same batch that introduced it.

    Assignments are deliberately unaffected — the construction site
    ``components_by_key[key] = wrapper`` is how the mapping is built.
    """
    import re

    source = (
        Path(__file__).resolve().parent.parent
        / "src/boomi_mcp/categories/integration_builder.py"
    ).read_text()

    #: The one sanctioned read: the accessor's own lookup.
    sanctioned = "return components_by_key[key]"
    read = re.compile(r"\w*components_by_key\[[^\]]+\]")
    #: `components_by_key[k] = v` BUILDS the mapping; only reads are in scope.
    assignment = re.compile(r"^\w*components_by_key\[[^\]]+\]\s*=[^=]")

    offenders = [
        (number, stripped)
        for number, line in enumerate(source.splitlines(), start=1)
        for stripped in (line.strip(),)
        if read.search(stripped)
        and not assignment.match(stripped)
        and sanctioned not in stripped
        and not stripped.startswith("#")
        and not stripped.startswith("``")
    ]
    assert offenders == [], (
        "unguarded components_by_key read(s) — route them through "
        "_step_component()/_component_for_key(): %r" % (offenders,)
    )


def test_an_apply_that_throws_mid_flight_does_not_claim_it_was_retry_safe():
    """Mutation accounting for the one case the server cannot know.

    `_apply_plan` returns its own partial-results envelope for every failure it
    anticipates, so `_mutation_status` is exact there. An exception that ESCAPES
    the loop carries no `results` at all — and a status computed from an empty
    envelope reads `none`, which tells the caller the failure is retry-safe. It
    may not be: components may already exist, and a retry under
    `conflict_policy="clone"` would duplicate them.

    Written as a witness because the honest answer here is invisible to every
    other test: the field is present either way, and only its VALUE is wrong.
    """
    payload = _bound_payload()

    def _boom(*_args, **_kwargs):
        raise RuntimeError("connection reset mid-apply")

    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(_GET_XML) as get_xml:
        paginate.return_value = []
        get_xml.side_effect = _live_xml
        execute.side_effect = _boom
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={"authoring_request": payload, "dry_run": False},
        )

    assert result["_success"] is False
    assert result["action"] == "apply"
    assert result["mutation_status"] == "possible"
    assert result["mutation_performed"] is True
    assert "cannot confirm" in result["hint"]
