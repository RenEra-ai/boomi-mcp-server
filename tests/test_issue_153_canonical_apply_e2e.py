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
from types import SimpleNamespace
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
    # The PREFIX assertion is kept as evidence, and it is non-vacuous: QA
    # measured pydantic rendering a head-position value's first 15 characters,
    # so 12 is inside the window and 16 would have sat past it.
    assert canary[:12] not in str(result)
    # ...but the prefix is not the invariant. Pydantic elides the MIDDLE and
    # keeps BOTH ends, so a tail-position value leaks its last ~24 characters
    # with no prefix at all — and a prefix sweep scored 0 on the QA-153-r3-01
    # envelope, which does render `input_value=`. The rendering marker is
    # position-independent, so that is what the rule is asserted on.
    assert "input_value=" not in str(result)
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


def test_the_raw_route_refuses_a_literal_reference_before_writing_anything():
    """§6 AR2-02: the raw `integration_spec` route had no compile step.

    So the relocatability answer — fully decidable from the request — was
    reached only inside the process root's execution turn, which topological
    order runs AFTER its dependencies have been created. A measured probe
    recorded two supporting-component writes before the refusal, and a literal
    reference inside the IR did not even serve the named code: it died inside
    compilation and surfaced as an internal error.

    The predicate is the same `envelope_relocatability_offenders` the typed
    route and the plan model already use — consulted at a third site, never
    restated.
    """
    literal = "35813b90-1f42-4dcb-98f5-82d8f96be61d"
    unit = process_unit(process_extensions=ProcessExtensionBindingsV1(
        connections=(ProcessConnectionOverrideV1(
            connection_id=literal, connector_type="rest",
            fields=(ProcessOverrideFieldV1(id="url", label="x"),),
        ),)
    ))
    spec = {
        "name": "M12.15 Integration",
        "components": [APPLIABLE_CONN, APPLIABLE_OP],
        "processes": [unit.model_dump(mode="json")],
    }

    writes = {"n": 0}

    def _component(*_a, **_k):
        writes["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % writes["n"]}

    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml:
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = AssertionError("no process may be created")
        get_xml.side_effect = _live_xml
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"integration_spec": spec, "dry_run": False},
        )

    assert result["_success"] is False
    assert result["error_code"] == PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE
    # The whole point: NOTHING was written for an answer we always had.
    assert writes["n"] == 0, "dependencies were created before the refusal"
    assert result["partial_results"] == {}
    from boomi_mcp.categories.integration_builder import _mutation_status
    assert _mutation_status(result) == "none"


def test_a_literal_reference_inside_the_ir_serves_the_named_code():
    """§6 AR2-02, the second half: an IR literal died inside compilation.

    The model validator that raises the named code runs only once the plan is
    CONSTRUCTED — after `compile_process_ir_v1`, which fails first on a literal
    reference and was served as an internal error. Deciding relocatability
    before compilation gives both reference locations the one documented code.
    """
    from boomi_mcp.authoring.process_materialization import (
        build_materialization_plan,
    )
    from boomi_mcp.categories.integration_builder import (
        _named_error_code_from_validation,
    )

    literal_unit = process_unit()
    ir = literal_unit.process_ir.model_dump(mode="json")
    # A literal Boomi id where a `$ref:KEY` belongs, inside the IR itself.
    steps = ir["body"]["steps"]
    bearing = [st for st in steps if "connection_ref" in st]
    assert bearing, steps
    bearing[0]["connection_ref"] = "35813b90-1f42-4dcb-98f5-82d8f96be61d"
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    with pytest.raises(Exception) as caught:
        build_materialization_plan(
            envelope=literal_unit.envelope,
            process_ir=parse_process_ir_v1(ir),
            symbols={},
            conflict_policy="reuse",
            compiler_revision="r", emitter_revision="r",
            materializer_revision="r",
        )
    assert _named_error_code_from_validation(caught.value) == (
        PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE
    ), caught.value


def test_an_update_without_a_target_is_already_decided_before_the_loop():
    """§6 AR2-02 SIBLING SWEEP, witnessed rather than asserted.

    The sweep asks: which other canonical refusals are fully request-decidable
    yet decided inside the mutation loop? Only relocatability was. The other
    candidate — `action="update"` with no resolvable target — is already
    refused before anything runs: the raw route's plan resolution reports
    `error_missing_target` and executes nothing. The in-loop arm that also
    refuses it is defensive depth, not the deciding site, so a third copy in
    the preflight would be an unreachable guard. This pins that measurement, so
    the sweep's claim is checkable rather than a note.
    """
    unit = process_unit(action="update")
    spec = {
        "name": "M12.15 Integration",
        "components": [APPLIABLE_CONN, APPLIABLE_OP],
        "processes": [unit.model_dump(mode="json")],
    }
    writes = {"n": 0}

    def _component(*_a, **_k):
        writes["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % writes["n"]}

    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml:
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = AssertionError("no process may be created")
        get_xml.side_effect = _live_xml
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"integration_spec": spec, "dry_run": False},
        )

    assert result["_success"] is False
    assert writes["n"] == 0, "dependencies were created before the refusal"
    assert [s["planned_action"] for s in result["unresolvable_steps"]] == [
        "error_missing_target"
    ]
    from boomi_mcp.categories.integration_builder import _mutation_status
    assert _mutation_status(result) == "none"


def test_a_thrown_apply_serves_the_record_of_what_it_did_write():
    """§6 AR2-03: an exception exit must carry the evidence, not just the doubt.

    `_partial_failure` gives a RETURNED failure its build id, its partial
    results and its attestations. An exception that escaped the loop got none of
    them — so the one exit where a caller most needs the durable record served
    the least, and the durable row stayed `in_progress` for good. The status
    stays `possible` (that IS the honest reading); what changes is that the
    caller can now name the record it must reconcile.
    """
    from boomi_mcp.categories import integration_builder as ib

    unit = process_unit()
    second = process_unit(key="proc_two", name="M12.15 Second")
    ib._BUILD_REGISTRY.clear()

    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    calls = {"n": 0}
    real = ib._execute_canonical_process

    def _canonical(**kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("connection reset between roots")
        return real(**kwargs)

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml, patch.object(
        ib, "_execute_canonical_process", side_effect=_canonical
    ):
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = _create
        get_xml.side_effect = _live_xml
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": _bound_payload(
                process_ir_request(units=(unit, second))
            ), "dry_run": False},
        )

    assert result["_success"] is False
    # The honest status is unchanged — this is not a downgrade of the doubt.
    assert result["mutation_status"] == "possible"
    # ...but the evidence is now served: the id, the partials, the attestation
    # of the root that DID land.
    build_id = result.get("build_id")
    assert build_id, result
    assert result.get("partial_results"), result
    assert result.get("process_mutations"), result
    # ...and the durable row is TERMINAL, not stuck mid-flight forever.
    row = ib._BUILD_REGISTRY[build_id]
    assert row["status"] == "failed_partial", row["status"]
    assert row.get("process_mutations")


def test_a_throw_after_every_write_never_reports_no_mutation():
    """§6 AR2-03, the worse half: the post-loop region was outside the guard.

    Every statement after the loop runs once all writes have LANDED. A raw
    escape there reached the blanket handler, which serves
    `mutation_status="none"` — mutations performed, caller told none, and a
    retry under `conflict_policy="clone"` duplicates every one of them. The
    pre-loop preflight deliberately stays outside the guard: it decides before
    anything can be written and must keep saying `none`.
    """
    from boomi_mcp.categories import integration_builder as ib

    ib._BUILD_REGISTRY.clear()
    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    def _boom(*_a, **_k):
        raise RuntimeError("registry write failed after every mutation landed")

    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml, patch.object(
        ib, "_authoring_build_provenance", side_effect=_boom
    ):
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = _create
        get_xml.side_effect = _live_xml
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": _bound_payload(), "dry_run": False},
        )

    assert result["_success"] is False
    # The whole point: writes happened, so "none" is a lie the caller acts on.
    assert result["mutation_status"] == "possible", result
    assert result.get("build_id"), result
    assert ib._BUILD_REGISTRY[result["build_id"]]["status"] == "failed_partial"


# ---------------------------------------------------------------------------
# value-free served errors (QA-153-r3-01, QA-153-r4-01)
# ---------------------------------------------------------------------------


def test_no_served_envelope_renders_pydantic_input_values():
    """The structural invariant for QA-153-r3-01 — asserted on BEHAVIOUR.

    The defect class is "a framework's default error rendering serving caller
    content". It was first fixed at two call sites, and the third — written in
    that same batch — kept `str(exc)` and shipped. Patching the third site would
    make three patches and no invariant, so the rule is stated once, over every
    refusal shape this surface can produce.

    `input_value=` is the marker pydantic itself emits, and it is
    position-independent: a prefix sweep only catches a value pydantic rendered
    from the head, and pydantic keeps BOTH ends while eliding the middle.
    """
    canary = "ZQXJ-Sup3rSecret-tail-marker-0123456789"
    shapes = []

    head = _duplicate_key_payload()
    head["intent"]["integration_name"] = canary
    shapes.append(("compile", {"authoring_request": head}))

    deep = _duplicate_key_payload()
    deep["intent"]["units"][0]["envelope"]["description"] = canary
    shapes.append(("compile", {"authoring_request": deep}))

    # The LEGACY root on the apply route — the arm that reaches the blanket
    # handlers, which is where the missed third rendering lived.
    spec = _spec_with_canonical_root()
    spec["processes"][0]["envelope"]["component_key"] = "conn"
    spec["processes"][0]["envelope"]["description"] = canary
    shapes.append(("apply", {"integration_spec": spec, "dry_run": False}))

    malformed = process_ir_request().model_dump(mode="json")
    malformed["intent"]["units"][0]["envelope"].pop("name")
    malformed["intent"]["integration_name"] = canary
    shapes.append(("plan", {"authoring_request": malformed}))

    offenders = []
    for action, config in shapes:
        with patch(_PAGINATE) as paginate:
            paginate.return_value = []
            served = str(build_integration_action(MagicMock(), _PROFILE, action, config=config))
        if "input_value=" in served or canary in served:
            offenders.append(action)

    assert offenders == [], (
        "served refusal(s) render caller content: %r — route the message through "
        "_validation_error_message()" % (offenders,)
    )


def test_every_served_exception_goes_through_the_value_free_renderer():
    """The source half of the same invariant, asserted on the AST.

    The behavioural sweep above can only cover refusal shapes a test can reach,
    and QA measured that the arm this rule was written for was NOT reachable
    with caller-authored content in 31 envelopes — a latent path with proven
    serving behaviour. So the rule is also asserted where it cannot depend on
    reachability.

    Two earlier versions of this guard were too narrow, both caught by QA. The
    first matched only ``{exc}`` f-string interpolation, while ``str(exc)`` is
    the spelling actually used at ~20 sites in the same file; a line-based
    widening then could not tell a real site from a docstring that merely
    mentions one. So the question is asked of the SYNTAX TREE.

    Scope is derived, not enumerated: a handler is OPEN when it can receive an
    exception this module did not raise — bare, or catching ``Exception`` /
    ``BaseException`` / ``ValueError`` (pydantic's ``ValidationError`` subclasses
    ``ValueError``, confirmed at runtime). A handler catching a specific type
    this repo defines renders a message the repo itself wrote, and is out of
    scope by construction rather than by allowlist.
    """
    import ast

    path = (
        Path(__file__).resolve().parent.parent
        / "src/boomi_mcp/categories/integration_builder.py"
    )
    tree = ast.parse(path.read_text())

    #: Catching any of these can deliver a framework-constructed exception.
    OPEN = {"Exception", "BaseException", "ValueError"}
    #: The renderer itself bottoms out in `str(exc)`; that IS the sanctioned path.
    RENDERER = "_validation_error_message"

    def caught_names(handler):
        node = handler.type
        if node is None:
            return {"BaseException"}
        parts = node.elts if isinstance(node, ast.Tuple) else [node]
        return {p.id for p in parts if isinstance(p, ast.Name)}

    def sanctioned_names(handler):
        """Every `exc` Name that is already an argument of the renderer.

        A sanctioned site can still LOOK like a rendering: a `%`-formatted
        message whose substituted value is `_validation_error_message(exc)`
        contains an `exc` Name under a `%` BinOp. Node IDENTITY is used, not the
        name, because two occurrences are distinct nodes.
        """
        safe = set()
        for node in ast.walk(ast.Module(body=handler.body, type_ignores=[])):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in (RENDERER, "type")
            ):
                for inner in ast.walk(node):
                    if isinstance(inner, ast.Name) and inner.id == handler.name:
                        safe.add(id(inner))
        return safe

    def renders(node, name, safe):
        """Does this node turn `name` into text, outside the renderer?"""

        def raw(candidate):
            return (
                isinstance(candidate, ast.Name)
                and candidate.id == name
                and id(candidate) not in safe
            )

        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id == "str":
                return any(raw(arg) for arg in node.args)
            if isinstance(func, ast.Attribute) and func.attr == "format":
                return any(raw(arg) for arg in node.args)
        if isinstance(node, ast.FormattedValue):
            return raw(node.value)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):
            return any(raw(inner) for inner in ast.walk(node.right))
        return False

    offenders = set()
    for func in ast.walk(tree):
        if not isinstance(func, ast.FunctionDef) or func.name == RENDERER:
            continue
        for handler in ast.walk(func):
            if not isinstance(handler, ast.ExceptHandler) or not handler.name:
                continue
            if not (caught_names(handler) & OPEN):
                continue
            safe = sanctioned_names(handler)
            for node in ast.walk(ast.Module(body=handler.body, type_ignores=[])):
                if renders(node, handler.name, safe):
                    offenders.add((getattr(node, "lineno", 0), func.name))

    assert offenders == set(), (
        "raw exception rendering(s) in an OPEN except arm — use %s(exc): %r"
        % (RENDERER, sorted(offenders))
    )


# ---------------------------------------------------------------------------
# Codex Stage-2 review, round 1
# ---------------------------------------------------------------------------


def test_authored_text_that_looks_like_a_reference_is_not_one():
    """Codex F6/F10: a text scan stood in for a schema question.

    `$ref:` in a `message` body is caller PROSE. It was walked out of the IR's
    JSON dump and registered as a component dependency, so authoring a message
    that mentions a ref token earned `INTEGRATION_DEPENDENCY_REQUIRED` for a
    dependency the document does not have.
    """
    from boomi_mcp.models.process_ir import ProcessIRV1, iter_component_refs

    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {"kind": "source", "connection_ref": "$ref:conn", "operation_ref": "$ref:op"},
                {"kind": "message", "text": "sends $ref:ghost then reads id-conn"},
                {"kind": "return_documents"},
            ],
        },
    }
    refs = [ref for _path, ref in iter_component_refs(ProcessIRV1(**doc))]
    assert sorted(refs) == ["$ref:conn", "$ref:op"]

    payload = process_ir_request().model_dump(mode="json")
    payload["intent"]["units"][0]["process_ir"] = doc
    with patch(_PAGINATE) as paginate:
        paginate.return_value = []
        result = build_integration_action(
            MagicMock(), _PROFILE, "compile", config={"authoring_request": payload}
        )
    assert result["_success"] is True, result.get("error")


def test_a_root_that_references_itself_is_refused_before_any_write():
    """Codex F6: an unbindable self-reference used to be excused at planning.

    A root's own id does not exist until it is created, so the reference can
    never bind — and discovering that at materialization means discovering it
    after the dependencies have been written.

    Authored through the envelope's extension binding, because `process_call` is
    not an admissible step in this body shape; the reference still lands in a
    `ComponentRefV1` field, which is what the rule is about.
    """
    from boomi_mcp.categories.integration_builder import (
        _check_process_root_dependencies,
    )
    from boomi_mcp.models.integration_models import IntegrationSpecV1

    def _unit_binding(connection_id):
        return process_unit(
            process_extensions=ProcessExtensionBindingsV1(
                connections=(
                    ProcessConnectionOverrideV1(
                        connection_id=connection_id,
                        connector_type="rest",
                        fields=(ProcessOverrideFieldV1(id="url", label="x"),),
                    ),
                )
            )
        )

    self_ref = _unit_binding("$ref:proc")
    payload = process_ir_request(units=(self_ref,)).model_dump(mode="json")
    with patch(_PAGINATE) as paginate:
        paginate.return_value = []
        result = build_integration_action(
            MagicMock(), _PROFILE, "compile", config={"authoring_request": payload}
        )
    assert result["_success"] is False
    # Refused at COMPILE, so nothing is written either way. The specific guard is
    # asserted directly below, because the legacy component-plan lint also
    # refuses this shape and would otherwise mask it.
    assert "INTEGRATION_DEPENDENCY_REQUIRED" in str(result)

    spec = IntegrationSpecV1(
        name="self-ref", components=[APPLIABLE_CONN, APPLIABLE_OP], processes=[self_ref]
    )
    error = _check_process_root_dependencies(spec)
    assert error is not None
    assert "references itself" in str(error)

    # Control: the same envelope naming a REAL dependency is accepted, so the
    # guard is not simply rejecting every extension binding.
    assert _check_process_root_dependencies(
        IntegrationSpecV1(
            name="ok",
            components=[APPLIABLE_CONN, APPLIABLE_OP],
            processes=[_unit_binding("$ref:conn")],
        )
    ) is None


def test_a_pre_write_refusal_does_not_report_a_possible_mutation():
    """Codex F4: `failed` counted as a writing attempt, so a refusal read as one.

    A canonical root rejected while compiling or materializing has issued no
    create and no update — but `_mutation_status` treated any non-`reused` step
    as writing, so the caller was told to reconcile an account nothing touched.
    """
    from boomi_mcp.categories.integration_builder import _mutation_status

    refused = {"results": {"proc": {"status": "refused", "component_id": None}}}
    assert _mutation_status(refused) == "none"

    # The control: a step that DID attempt a write still reads as uncertain.
    attempted = {"results": {"proc": {"status": "failed", "component_id": None}}}
    assert _mutation_status(attempted) == "possible"


def test_the_compile_binding_covers_the_materialization_plan():
    """Codex F3: a binding that cannot notice the artifact changing.

    The materializer revision, emitter revision and preservation policy all feed
    the materialization plan. None reached the compile hash, so changing any of
    them left every previously-issued binding still verifying.
    """
    from boomi_mcp.authoring.workflow import compile_authoring_request_v1

    request = process_ir_request()
    with patch(_PAGINATE) as paginate:
        paginate.return_value = []
        before, _ = compile_authoring_request_v1(
            request, boomi_client=MagicMock(), profile=_PROFILE
        )
        kinds = {a.artifact_kind for a in before.artifact_fingerprints}
        assert "process_component_materialization_plan" in kinds

        # Move the MATERIALIZER revision only. Nothing about the IR or the
        # emission plan changes; the binding must still move.
        with patch(
            "boomi_mcp.categories.integration_builder._materializer_revision",
            # Digest-shaped, because the plan model types every revision as a
            # digest (§6 AR3-09) — the point is only that it MOVED.
            return_value="sha256:" + "b" * 64,
        ):
            after, _ = compile_authoring_request_v1(
                request, boomi_client=MagicMock(), profile=_PROFILE
            )

    assert (
        before.revision_binding.compile_hash != after.revision_binding.compile_hash
    ), "a materializer change left the compile binding unchanged"


def test_a_malformed_process_extension_block_is_refused_not_emptied():
    """Codex F9: malformed recipe bindings were silently converted to empty.

    The strict typed models never saw the bad data, so apply emitted a process
    WITHOUT the environment overrides the caller asked for — a successful-looking
    build of the wrong component.
    """
    from boomi_mcp.authoring.workflow import (
        AuthoringWorkflowError,
        _extension_bindings_from_config,
    )

    assert _extension_bindings_from_config(None).connections == ()

    for bad in ("not-an-object", 42, ["connections"]):
        with pytest.raises(AuthoringWorkflowError):
            _extension_bindings_from_config(bad)

    with pytest.raises(AuthoringWorkflowError):
        _extension_bindings_from_config({"connections": ["not-an-object"]})

    with pytest.raises(AuthoringWorkflowError):
        _extension_bindings_from_config(
            {"connections": [{"connection_id": "$ref:conn", "fields": ["bad"]}]}
        )


def test_the_dependency_summary_describes_both_participant_kinds():
    """Codex F11: process edges were absent from the served summary.

    The unified execution graph enforces a root's declared edges; the summary
    described none of them, because it walked `spec.components` only and was
    then fed a projection with the roots withheld.
    """
    result = _plan_or_compile("plan", process_ir_request())
    edges = result["authoring_result"]["component_dependencies"]
    process_edges = sorted(
        e["depends_on"] for e in edges if e["component_key"] == "proc"
    )
    assert process_edges == ["conn", "op"], edges


# ---------------------------------------------------------------------------
# Codex Stage-2 review, round 2
# ---------------------------------------------------------------------------


def test_the_compiled_plan_fingerprint_uses_the_REQUESTED_conflict_policy():
    """Codex round 2 F1: compile fingerprinted a plan apply never executed.

    `conflict_policy` is covered by the plan material, and compile hard-coded
    `"reuse"` — so for a `clone` or `fail` apply the served
    `artifact_fingerprints[].digest` could never equal the
    `process_mutations[].plan_fingerprint` the mutation recorded.
    """
    from boomi_mcp.authoring.workflow import compile_authoring_request_v1

    def _digest(policy):
        request = process_ir_request().model_copy(deep=True)
        intent = request.intent.model_copy(update={"conflict_policy": policy})
        request = request.model_copy(update={"intent": intent})
        with patch(_PAGINATE) as paginate:
            paginate.return_value = []
            result, _ = compile_authoring_request_v1(
                request, boomi_client=MagicMock(), profile=_PROFILE
            )
        return next(
            a.digest
            for a in result.artifact_fingerprints
            if a.artifact_kind == "process_component_materialization_plan"
        )

    # The policy is covered, so the digest must MOVE with it — which is exactly
    # why hard-coding one value made compile describe the wrong plan.
    assert _digest("reuse") != _digest("clone")
    assert _digest("reuse") != _digest("fail")
    # ...and it is stable for a fixed policy, so the difference above is the
    # policy and not nondeterminism.
    assert _digest("clone") == _digest("clone")


def test_an_ambiguous_create_does_not_silently_duplicate():
    """Codex round 2 F2: ambiguity was handled for updates only.

    A create-by-name matching two live processes left the step as a plain
    create with no existing id, so `reuse`, `fail` and `clone` all performed an
    ordinary unsuffixed create — every collision policy violated at once.
    """
    from boomi_mcp.categories import integration_builder as ib

    two = [{"component_id": "cid-a"}, {"component_id": "cid-b"}]
    spec = _spec_with_canonical_root()

    def _plan(policy):
        with patch.object(ib, "_resolve_existing_components", return_value=two), \
             patch(_PAGINATE) as paginate:
            paginate.return_value = []
            result = build_integration_action(
                MagicMock(), _PROFILE, "plan",
                config={"integration_spec": spec, "conflict_policy": policy},
            )
        return next(s for s in result["steps"] if s["key"] == "proc")

    # `clone` can proceed — it creates something new either way.
    assert _plan("clone")["planned_action"] == "create_clone"
    # The others cannot pick a target and must refuse rather than duplicate.
    for policy in ("reuse", "fail"):
        assert _plan(policy)["planned_action"] == "error_ambiguous_match", policy


def test_recipe_extension_normalization_matches_the_legacy_authority():
    """Codex round 2 F3/F4: a hand-copy of an authority is wrong BOTH ways.

    The previous version rejected `[]`, which the legacy reader accepts as an
    explicit no-op, and accepted a misspelled `connections` key, which the legacy
    reader refuses — so a caller's environment overrides were silently dropped
    and the process deployed without them. Both are gone because the question is
    now asked of `extension_bindings_from_legacy_config` itself.
    """
    from boomi_mcp.authoring.workflow import (
        AuthoringWorkflowError,
        _extension_bindings_from_config,
    )

    # Legacy-accepted no-ops stay no-ops.
    for benign in (None, {}, [], {"connections": []}):
        assert _extension_bindings_from_config(benign).connections == ()

    # Legacy-refused shapes are refused — including the three F3 named, which
    # the hand-written version coalesced to empty bindings.
    for bad in (
        "not-an-object",
        42,
        {"connection": [{"connection_id": "$ref:conn"}]},
        {"connections": None},
        {"connections": {}},
        {"connections": ["not-an-object"]},
        {"connections": [{"connection_id": "$ref:conn", "fields": ["bad"]}]},
        {"connections": [{"connection_id": "  ", "fields": [{"id": "u", "label": "l"}]}]},
    ):
        with pytest.raises(AuthoringWorkflowError):
            _extension_bindings_from_config(bad)

    # ...and a well-formed block still normalizes.
    good = _extension_bindings_from_config(
        {"connections": [{"connection_id": " $ref:conn ", "fields": [{"id": " u ", "label": "L"}]}]}
    )
    assert good.connections[0].connection_id == "$ref:conn"
    assert good.connections[0].fields[0].id == "u"
    # `label` is NOT stripped — the renderer emits its exact bytes.
    assert good.connections[0].fields[0].label == "L"


def test_an_update_attests_the_folder_id_the_platform_received():
    """§6 AR2-04: the update attested a folder by NAME with a null id.

    Preservation echoes the live root's attributes into the merged bytes, so
    those bytes carry `folderId` alongside `folderName` — the id was in hand and
    discarded, and the attestation recorded less than what the platform
    received. Both facts now come from ONE read of the merged bytes, so they
    cannot describe different reads; the create branch is untouched (its
    authority is the readback, not the request).
    """
    import boomi_mcp.authoring.process_materialization as pm
    from boomi_mcp.categories.components.canonical_process_apply import (
        applied_placement, build_mutation_attestation,
    )

    # A REAL plan, captured from the public chain rather than hand-built, so
    # the attestation under test is the one production constructs.
    captured = {}
    real = pm.build_materialization_plan

    def _capture(*args, **kwargs):
        plan_obj = real(*args, **kwargs)
        captured.setdefault("plan", plan_obj)
        return plan_obj

    def _component(*_a, **_k):
        return {"_success": True, "component_id": "cid-x"}

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml, patch.object(
        pm, "build_materialization_plan", _capture
    ):
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = _create
        get_xml.side_effect = _live_xml
        build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": _bound_payload(), "dry_run": False},
        )
    plan = captured["plan"]
    merged = '<bns:Component xmlns:bns="x" folderName="F" folderId="fid"/>'
    place = applied_placement(merged)
    attestation = build_mutation_attestation(
        plan=plan, action="update", target_component_id="tid",
        result_component_id="tid", submitted_xml=merged,
        account_scope_hash="sha256:" + "b" * 64,
        applied_folder_name=place["folder_name"],
        applied_folder_id=place["folder_id"],
    )
    resolved = attestation.resolved_placement
    assert resolved.folder_name == "F"
    assert resolved.folder_id == "fid"

    # The control: merged bytes with no folder attributes attest neither —
    # account-root placement stays absent rather than guessed.
    bare = '<bns:Component xmlns:bns="x"/>'
    place = applied_placement(bare)
    attestation = build_mutation_attestation(
        plan=plan, action="update", target_component_id="tid",
        result_component_id="tid", submitted_xml=bare,
        account_scope_hash="sha256:" + "b" * 64,
        applied_folder_name=place["folder_name"],
        applied_folder_id=place["folder_id"],
    )
    assert attestation.resolved_placement.folder_name is None
    assert attestation.resolved_placement.folder_id is None


def test_the_update_preservation_codes_are_in_the_shared_taxonomy():
    """Codex round 2 F5: a served stable code consumers could not classify.

    Registered under **#45**, which introduced the family — not under #153,
    which would have put an #153-owned code outside its three declared prefixes
    and broken the one-introducer-per-family biconditional. Registering the whole
    family rather than only the new member is what actually closes the gap.
    """
    from boomi_mcp.errors import ERROR_TAXONOMY

    family = {
        code: spec for code, spec in ERROR_TAXONOMY.items()
        if code.startswith("UPDATE_PRESERVATION_")
    }
    # DERIVED from what the two emitting modules actually serve, not hand-listed
    # — the hand-listed version pinned an incomplete catalog as complete, which
    # is the same defect the registration was meant to fix (Codex round 3).
    import re

    emitted = set()
    for module in (
        "src/boomi_mcp/categories/integration_builder.py",
        "src/boomi_mcp/categories/components/component_update_preservation.py",
    ):
        text = (Path(__file__).resolve().parent.parent / module).read_text()
        emitted.update(re.findall(r"UPDATE_PRESERVATION_[A-Z_]+", text))
    assert len(emitted) >= 7, sorted(emitted)
    assert set(family) == emitted, sorted(set(family) ^ emitted)
    assert {spec.owner for spec in family.values()} == {"#45"}
    # The stakes asymmetry is declared, not incidental: a fetch failure wrote
    # nothing and is retry-safe; a push failure may already have landed.
    assert family["UPDATE_PRESERVATION_FETCH_FAILED"].retryable is True
    assert family["UPDATE_PRESERVATION_PUSH_FAILED"].retryable is False


# ---------------------------------------------------------------------------
# Codex Stage-2 review, round 3
# ---------------------------------------------------------------------------


def test_an_ambiguous_UPDATE_never_selects_an_arbitrary_target():
    """Codex round 3 F1: `clone` is a CREATE-only escape.

    The round-2 mirror applied it to both actions, so an `action="update"` with
    two same-name matches took `candidates[0]` as its target — and
    `_execute_canonical_process` reads the ACTION, not `planned_action`, so it
    would have updated an arbitrary one of them. Same data-loss shape as the
    clone-overwrite defect, one round later.
    """
    from boomi_mcp.categories import integration_builder as ib

    two = [
        {"component_id": "cid-a", "name": "M12.15 Process", "folder_name": "f"},
        {"component_id": "cid-b", "name": "M12.15 Process", "folder_name": "f"},
    ]

    def _plan(action, policy):
        spec = _spec_with_canonical_root()
        spec["processes"][0]["envelope"]["action"] = action
        with patch.object(ib, "_resolve_existing_components", return_value=two), \
             patch(_PAGINATE) as paginate:
            paginate.return_value = []
            result = build_integration_action(
                MagicMock(), _PROFILE, "plan",
                config={"integration_spec": spec, "conflict_policy": policy},
            )
        return next(s for s in result["steps"] if s["key"] == "proc")

    # An ambiguous UPDATE has no safe target under ANY policy.
    for policy in ("reuse", "fail", "clone"):
        step = _plan("update", policy)
        assert step["planned_action"] == "error_ambiguous_match", policy
        assert step["existing_component_id"] is None, policy

    # A CREATE can still sidestep ambiguity under clone — it writes something new.
    assert _plan("create", "clone")["planned_action"] == "create_clone"


def test_an_ambiguous_canonical_step_carries_its_candidates():
    """Codex round 3 F2: the plan omitted the ids needed to disambiguate.

    Apply's fail-fast message defaults the missing list to `[]` and then reports
    that ZERO components matched — on the one step whose problem is that several
    did.
    """
    from boomi_mcp.categories import integration_builder as ib

    two = [
        {"component_id": "cid-a", "name": "M12.15 Process", "folder_name": "Home"},
        {"component_id": "cid-b", "name": "M12.15 Process", "folder_name": "Other"},
    ]
    spec = _spec_with_canonical_root()
    with patch.object(ib, "_resolve_existing_components", return_value=two), \
         patch(_PAGINATE) as paginate:
        paginate.return_value = []
        result = build_integration_action(
            MagicMock(), _PROFILE, "plan",
            config={"integration_spec": spec, "conflict_policy": "fail"},
        )

    step = next(s for s in result["steps"] if s["key"] == "proc")
    assert [c["component_id"] for c in step["candidates"]] == ["cid-a", "cid-b"]
    # The same sanitized shape component steps carry — no extra live metadata.
    assert set(step["candidates"][0]) == {"component_id", "name", "folder_name"}


def test_a_malformed_extension_path_is_a_json_pointer():
    """Codex round 3 F3: bracket notation survived into a served `path`.

    The builder reports `connections[0].fields[0]`; replacing only dots emitted
    `/process_extensions/connections[0]/fields[0]`, so a consumer following the
    pointer looked for a key literally named `connections[0]`.
    """
    from boomi_mcp.authoring.workflow import (
        AuthoringWorkflowError,
        _extension_bindings_from_config,
        _json_pointer,
    )

    assert _json_pointer("process_extensions.connections[0].fields[0]") == (
        "/process_extensions/connections/0/fields/0"
    )

    with pytest.raises(AuthoringWorkflowError) as excinfo:
        _extension_bindings_from_config(
            {"connections": [{"connection_id": "$ref:conn", "fields": [{"id": ""}]}]}
        )
    paths = [d.path for d in excinfo.value.diagnostics]
    assert paths and all("[" not in p and "]" not in p for p in paths), paths


def test_a_platform_renamed_component_is_reported_as_renamed():
    """QA-153-r7-01, which supersedes r6-01 by disproving its premise.

    Boomi treats a soft-deleted predecessor's name as taken and appends a
    counter, so authoring `X` against a deleted `X` creates `"X 2"`. The metadata
    query and its filter are BOTH correct — nothing named `X` exists. What was
    wrong was the apply: it recorded the REQUESTED name as though it were the
    actual one, and compared nothing. Nothing is then ever named `X`, so a later
    `conflict_policy` lookup finds nothing and re-applies duplicate without end
    (QA measured `x 3`, `x 4`, `x 5`).

    The readback is already fetched, so the comparison costs nothing — and it is
    the only place the truth exists, because the create response does not carry
    the assigned name.
    """
    renamed = _LIVE_COMPONENT_XML  # placeholder, replaced below

    def _live(_client, component_id, *_a, **_k):
        if component_id == _PROCESS_ID and "xml" in _SUBMITTED:
            # The platform hands back a DIFFERENT name than was authored.
            return {
                "type": "process",
                "xml": _SUBMITTED["xml"].replace(
                    'name="M12.15 Process"', 'name="M12.15 Process 2"', 1
                ),
            }
        return {"type": "connector-settings", "xml": _LIVE_COMPONENT_XML}

    created = {"n": 0}

    def _component(*_args, **_kwargs):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    _SUBMITTED.clear()
    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml:
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = _create
        get_xml.side_effect = _live
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": _bound_payload(), "dry_run": False},
        )

    assert result["_success"] is True, result.get("error")
    step = result["results"]["proc"]
    # The ACTUAL name is recorded, not the requested one.
    assert step["name"] == "M12.15 Process 2"
    assert step["requested_name"] == "M12.15 Process"
    assert step["name_reassigned_by_platform"] is True
    # ...and the caller is told, in terms they can act on.
    warning = [w for w in (result.get("warnings") or []) if "not the authored" in w]
    assert warning, result.get("warnings")
    assert "M12.15 Process 2" in warning[0]
    # It states the FACT and offers the likely cause; it must not ASSERT one.
    # QA-153-r8 Q2: exact inequality cannot miss a rename, but a platform that
    # normalizes an accepted name would fire this on a component nobody renamed
    # — and telling that caller to delete a stale component invents one.
    assert "usual cause" in warning[0]
    assert "delete the stale component" not in warning[0]


def test_a_normally_named_component_reports_no_reassignment():
    """The control. The r6 version of this guard fired on EVERY first-time
    create, which QA measured and correctly called noise — a warning nobody can
    act on trains people to ignore warnings. This one must stay silent on a
    healthy create."""
    result = _apply(_bound_payload())

    assert result["_success"] is True, result.get("error")
    step = result["results"]["proc"]
    # `requested_name` and `applied_name_verified` are present on EVERY canonical
    # result by construction (Codex round 6) — the healthy case is that the two
    # names agree and nothing is flagged, not that the fields are absent.
    assert step["applied_name_verified"] is True
    assert step["name"] == step["requested_name"] == "M12.15 Process"
    assert "name_reassigned_by_platform" not in step
    assert not [w for w in (result.get("warnings") or []) if "not the authored" in w]


# ---------------------------------------------------------------------------
# Codex Stage-2 review, round 5
# ---------------------------------------------------------------------------


def test_an_unverifiable_name_is_marked_unverified_not_asserted():
    """Codex round 5 F1: the round-4 fix covered the happy path only.

    When the post-write readback times out or returns unparseable XML,
    `applied_name` is None — and the result went on presenting `envelope.name`
    as though it were fact, on exactly the path where the platform may have
    assigned `"X 2"`. That is QA-153-r7-01 reproduced on its failure path.
    """
    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    def _readback_fails(_client, component_id, *_a, **_k):
        if component_id == _PROCESS_ID:
            raise RuntimeError("read-back timed out")
        return {"type": "connector-settings", "xml": _LIVE_COMPONENT_XML}

    _SUBMITTED.clear()
    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml:
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = _create
        get_xml.side_effect = _readback_fails
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": _bound_payload(), "dry_run": False},
        )

    assert result["_success"] is True, result.get("error")
    step = result["results"]["proc"]
    assert step["applied_name_verified"] is False
    assert step["requested_name"] == "M12.15 Process"
    assert [w for w in (result.get("warnings") or []) if "did not confirm" in w]

    # The control: a healthy readback marks the name VERIFIED, so the flag is
    # not simply always-false.
    healthy = _apply(_bound_payload())
    assert healthy["results"]["proc"]["applied_name_verified"] is True


def test_execution_warnings_survive_a_partial_failure():
    """Codex round 5 F4: the partial-failure return bypassed the warning merge.

    An execution warning describes a mutation that ALREADY happened. A later
    step failing must not swallow the remediation for an earlier one.
    """
    from boomi_mcp.categories import integration_builder as ib

    src = (
        Path(__file__).resolve().parent.parent
        / "src/boomi_mcp/categories/integration_builder.py"
    ).read_text()
    # The partial-failure envelope and the success envelope must BOTH carry the
    # accumulated execution warnings.
    partial = src.index('"partial_results": results,')
    window = src[partial:partial + 1400]
    assert "apply_warnings" in window, (
        "the canonical partial-failure envelope drops execution warnings"
    )
    assert ib is not None


# ---------------------------------------------------------------------------
# Codex Stage-2 review, round 6
# ---------------------------------------------------------------------------


def test_every_canonical_result_carries_the_name_discriminator():
    """Codex round 6 F1: the field must exist on EVERY exit, including early ones.

    A create that reports success without a `component_id` returns before the
    readback — a documented path that may already have created the component —
    and used to expose the requested name with nothing marking it unverified.
    Initializing at construction makes that unrepresentable rather than
    remembered.
    """
    def _no_id(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True}          # success, no component_id

    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    _SUBMITTED.clear()
    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml:
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = _no_id
        get_xml.side_effect = _live_xml
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": _bound_payload(), "dry_run": False},
        )

    assert result["_success"] is False
    step = result["partial_results"]["proc"]
    assert step["applied_name_verified"] is False
    assert step["requested_name"] == "M12.15 Process"


def test_one_constructor_builds_every_partial_failure_envelope():
    """Codex round 6 F2, asserted structurally.

    Three rounds running, a field was present on one failing exit and missing
    from the others. The invariant is that `_apply_plan` has exactly ONE place
    where a partial-failure envelope is built, so a new exit cannot omit a field
    and a new field is added once.
    """
    import ast

    source = (
        Path(__file__).resolve().parent.parent
        / "src/boomi_mcp/categories/integration_builder.py"
    ).read_text()
    tree = ast.parse(source)
    apply_plan = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "_apply_plan"
    )

    # EVERY return inside the mutation loop must call the constructor.
    #
    # The first version only rejected dict returns that ALREADY carried
    # `partial_results` — so a new failure return omitting it, warnings and the
    # attestations (the exact regression this guard exists for) had no such key
    # and passed (Codex round 7). The question is asked the other way round now:
    # what does this return DO, not what does it happen to contain.
    loop = next(
        node for node in ast.walk(apply_plan)
        if isinstance(node, ast.For)
        and isinstance(node.target, ast.Name)
        and node.target.id == "key"
    )

    def _is_constructor_call(node):
        return (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "_partial_failure"
        )

    offenders = [
        node.lineno
        for node in ast.walk(loop)
        if isinstance(node, ast.Return)
        and node.value is not None
        and not _is_constructor_call(node.value)
    ]
    assert offenders == [], (
        "return(s) in the mutation loop bypass _partial_failure() at line(s) %r — "
        "every failing exit must build the envelope through the one constructor"
        % offenders
    )

    calls = [
        node.lineno
        for node in ast.walk(loop)
        if _is_constructor_call(node)
    ]
    assert len(calls) >= 3, calls


def test_a_reuse_does_not_claim_its_name_went_unconfirmed():
    """Codex round 8: a `reuse` succeeds WITHOUT writing.

    `applied_name_verified` is False by construction on every result, so the
    warning condition has been narrowed twice — first off refusals and rejected
    writes, then off reuse, which performs no write and whose name was already
    observed by the exact-name lookup that found it. The split is derived from
    `_NON_WRITING_STEP_STATUSES`, not from a fresh list of write statuses.
    """
    from boomi_mcp.categories import integration_builder as ib

    existing = [{"component_id": "cid-existing", "name": "M12.15 Process",
                 "folder_name": "Home"}]
    spec = _spec_with_canonical_root()
    with patch.object(ib, "_resolve_existing_components", return_value=existing), \
         patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _GET_XML
    ) as get_xml:
        paginate.return_value = []
        execute.side_effect = lambda *a, **k: {"_success": True, "component_id": "cid-1"}
        get_xml.side_effect = _live_xml
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"integration_spec": spec, "conflict_policy": "reuse",
                    "dry_run": False},
        )

    step = (result.get("results") or result.get("partial_results") or {}).get("proc")
    assert step is not None, result
    assert step["status"] == "reused"
    assert not [w for w in (result.get("warnings") or []) if "did not confirm" in w]

    # The split is DERIVED, so `reused` really is in the non-writing set.
    assert "reused" in ib._NON_WRITING_STEP_STATUSES


def test_a_write_that_committed_then_failed_still_warns_about_its_name():
    """QA-153-r10-01 — the answer to my own adversarial question.

    Having narrowed this condition twice off false positives, I asked whether it
    had become too narrow. It had, in exactly one direction: `_success is True`
    excluded commit-then-fail — a create whose component provably exists, or an
    update whose push landed — which is precisely where an unconfirmed name
    matters most.

    Driven through `build_integration_action` (Codex round 10). The first
    version of this test reimplemented the predicate locally, so restoring the
    `_success` clause — or deleting the production warning outright — would have
    left it green. It protected nothing. Fifth time in this slice a guard I wrote
    tested a copy of the rule instead of the rule.
    """
    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _commit_then_fail(_client, _profile, payload_in):
        # The write LANDED — the platform has the component — and the response
        # did not come back. This is the case that must warn.
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": False, "error": "connection reset after commit"}

    _SUBMITTED.clear()
    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml:
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = _commit_then_fail
        get_xml.side_effect = _live_xml
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": _bound_payload(), "dry_run": False},
        )

    assert result["_success"] is False
    step = result["partial_results"]["proc"]
    assert step["status"] == "failed"
    assert step["applied_name_verified"] is False
    assert [w for w in (result.get("warnings") or []) if "did not confirm" in w], result


def test_a_failure_that_provably_wrote_nothing_does_not_warn():
    """Codex rounds 10 and 11: `failed` is not uniformly post-write.

    A preservation FETCH failure returns before `update_component_raw`, so
    nothing was written — and `_apply_plan` would otherwise say a write may have
    landed.

    The discriminator is NOT the taxonomy's `retryable` flag, which was the
    previous attempt and the wrong authority: retryability describes whether a
    REQUEST can be retried, and four merge failures that provably write nothing
    are correctly `retryable=False`. It is `write_attempted`, reported by
    `_apply_structured_update` itself, stamped by position relative to its own
    push.

    Driven through a real canonical UPDATE with an existing component id
    (Codex round 11). The first version mocked `create_component` returning an
    update-preservation code that a production create can never emit, so it
    never touched `_apply_structured_update` at all.
    """
    from _m12_11_support import appliable_process_unit

    unit = appliable_process_unit(component_id="cid-existing")
    unit = unit.model_copy(
        update={"envelope": unit.envelope.model_copy(update={"action": "update"})}
    )
    payload = _bound_payload(process_ir_request(units=(unit,)))

    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _get_xml(_client, component_id, *_a, **_k):
        if component_id == "cid-existing":
            # The live read fails, so the merge never starts and the push is
            # never issued — the exact pre-write shape.
            raise RuntimeError("could not read the live component")
        return {"type": "connector-settings", "xml": _LIVE_COMPONENT_XML}

    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _GET_XML
    ) as get_xml:
        paginate.return_value = []
        execute.side_effect = _component
        get_xml.side_effect = _get_xml
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": payload, "dry_run": False},
        )

    assert result["_success"] is False
    step = result["partial_results"]["proc"]
    # Provably wrote nothing -> `refused`, a NON-writing status, so no warning
    # tells the caller to reconcile an update that never left the process.
    assert step["status"] == "refused", step
    assert not [w for w in (result.get("warnings") or []) if "did not confirm" in w]

    # THE DISCRIMINATING CASE. A fetch failure is retryable=True, so the fetch
    # shape alone cannot tell the two candidate authorities apart — both call it
    # pre-write. A MERGE failure is where they disagree: nothing was written, and
    # the code is correctly retryable=False. If this still reported `failed`, the
    # discriminator would be reading retryability rather than write evidence.
    from boomi_mcp.errors import ERROR_TAXONOMY

    for code in ("UPDATE_PRESERVATION_XML_PARSE_FAILED",
                 "UPDATE_PRESERVATION_OBJECT_MISSING",
                 "UPDATE_PRESERVATION_MERGE_FAILED",
                 "UPDATE_PRESERVATION_TYPE_MISMATCH"):
        assert ERROR_TAXONOMY[code].retryable is False, code

    def _live_ok(_client, component_id, *_a, **_k):
        if component_id == "cid-existing":
            return {"type": "process", "xml": _LIVE_COMPONENT_XML}
        return {"type": "connector-settings", "xml": _LIVE_COMPONENT_XML}

    def _merge_fails(*_a, **_k):
        from boomi_mcp.categories.components.builders.connector_builder import (
            BuilderValidationError,
        )
        raise BuilderValidationError(
            "type/subType do not align",
            error_code="UPDATE_PRESERVATION_TYPE_MISMATCH",
            field="type",
        )

    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _GET_XML
    ) as get_xml, patch(
        "boomi_mcp.categories.integration_builder.merge_for_update", _merge_fails
    ):
        paginate.return_value = []
        execute.side_effect = _component
        get_xml.side_effect = _live_ok
        merged = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": _bound_payload(process_ir_request(units=(unit,))),
                    "dry_run": False},
        )

    merged_step = merged["partial_results"]["proc"]
    assert merged_step["status"] == "refused", merged_step
    assert not [w for w in (merged.get("warnings") or []) if "did not confirm" in w]


def test_every_structured_update_exit_reports_whether_it_wrote():
    """Codex round 11's invariant, asserted over the exits themselves.

    `write_attempted` is the discriminator the canonical arm reads, so an exit
    that omits it silently falls back to "a write may have landed". Driven
    through `_apply_structured_update` in each failure mode rather than asserted
    on the source, because the value has to be RIGHT, not merely present.
    """
    from boomi_mcp.categories import integration_builder as ib
    from boomi_mcp.categories.components.builders._process_preservation import (
        PROCESS_PRESERVATION_POLICY,
    )
    from boomi_mcp.categories.components.builders.connector_builder import (
        BuilderValidationError,
    )

    shim = ib._process_update_shim(process_unit().envelope)
    xml = '<bns:Component xmlns:bns="http://api.platform.boomi.com/" type="process"/>'

    def _call(**patches):
        with patch.object(ib, "component_get_xml", patches["get_xml"]), \
             patch.object(ib, "merge_for_update", patches["merge"]):
            client = MagicMock()
            client.component.update_component_raw.side_effect = patches["push"]
            return ib._apply_structured_update(
                client, _PROFILE, "cid-existing", shim, xml,
                PROCESS_PRESERVATION_POLICY,
            )

    ok_get = lambda *a, **k: {"type": "process", "xml": xml}
    ok_merge = lambda *a, **k: xml

    def _boom(*_a, **_k):
        raise RuntimeError("boom")

    def _merge_boom(*_a, **_k):
        raise BuilderValidationError(
            "mismatch", error_code="UPDATE_PRESERVATION_TYPE_MISMATCH", field="type"
        )

    # PRE-write exits: the push was never issued.
    fetch = _call(get_xml=_boom, merge=ok_merge, push=None)
    assert fetch["write_attempted"] is False, fetch

    merge = _call(get_xml=ok_get, merge=_merge_boom, push=None)
    assert merge["write_attempted"] is False, merge

    # POST-write exits: the request left this process.
    push = _call(get_xml=ok_get, merge=ok_merge, push=_boom)
    assert push["write_attempted"] is True, push

    success = _call(get_xml=ok_get, merge=ok_merge, push=None)
    assert success["write_attempted"] is True, success

    # Non-vacuity: the flag really does vary — a check that only ever saw one
    # value would pass against a constant.
    assert {fetch["write_attempted"], push["write_attempted"]} == {False, True}


def test_nothing_before_the_push_can_write():
    """Pins the assumption `write_attempted`'s positional stamp rests on.

    `_apply_structured_update` stamps its exits by position relative to
    `update_component_raw`: returns before it say no write was attempted. That is
    only sound while nothing reachable before the push can write, which QA-153
    round 11 verified structurally and then flagged — if a preservation helper
    ever gains a network call, the stamp goes silently wrong and a lost write
    gets reported as "nothing happened".

    Asserted on the call graph rather than by eye: `merge_for_update` and its
    helpers may not touch a Boomi client. Cheap, and it fails at the moment the
    assumption stops being true instead of at the incident.
    """
    import ast

    module = (
        Path(__file__).resolve().parent.parent
        / "src/boomi_mcp/categories/components/component_update_preservation.py"
    )
    tree = ast.parse(module.read_text())

    #: Names that would mean this module reached the network.
    WRITERS = (
        "boomi_client", "update_component_raw", "create_component_raw",
        "component_get_xml", "requests", "urlopen", "httpx", "session",
    )
    offenders = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id in WRITERS:
            offenders.append((node.lineno, node.id))
        if isinstance(node, ast.Attribute) and node.attr in WRITERS:
            offenders.append((node.lineno, node.attr))
    assert offenders == [], (
        "the pre-push merge path reached a client — `write_attempted`'s "
        "positional stamp is no longer sound: %r" % offenders
    )

    # Non-vacuity: the probe CAN see these names when they are present.
    planted = ast.parse("def f(boomi_client):\n    boomi_client.component.update_component_raw(1, 2)\n")
    found = [
        n.attr for n in ast.walk(planted)
        if isinstance(n, ast.Attribute) and n.attr in WRITERS
    ]
    assert "update_component_raw" in found


# ---------------------------------------------------------------------------
# §6 architect review, round 1 (AR1-01 / AR1-02 / AR1-03 / AR1-07)
# ---------------------------------------------------------------------------


def test_a_literal_component_id_in_the_IR_is_not_relocatable():
    """AR1-02: relocatability covers the WHOLE plan, not just the envelope.

    The reviewer constructed a validated plan whose canonical bytes carried a
    literal account id through a `ComponentRefV1` field of the IR — the exact
    violation the fingerprint exists to make unrepresentable. The rule now walks
    the IR through the same schema-derived enumeration the slot inventory uses.
    """
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {
                    "kind": "source",
                    # A literal Boomi id — valid generic ProcessIR, but not
                    # materializable (plan §3: reject at the model).
                    "connection_ref": "35813b90-1f42-4dcb-98f5-82d8f96be61d",
                    "operation_ref": "$ref:op",
                },
                {"kind": "message", "text": "hello"},
                {"kind": "return_documents"},
            ],
        },
    }
    payload = process_ir_request().model_dump(mode="json")
    payload["intent"]["units"][0]["process_ir"] = doc
    with patch(_PAGINATE) as paginate:
        paginate.return_value = []
        result = build_integration_action(
            MagicMock(), _PROFILE, "compile", config={"authoring_request": payload}
        )
    assert result["_success"] is False
    assert "PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE" in str(result)

    # Control: the $ref form of the same document compiles.
    ok = process_ir_request().model_dump(mode="json")
    with patch(_PAGINATE) as paginate:
        paginate.return_value = []
        control = build_integration_action(
            MagicMock(), _PROFILE, "compile", config={"authoring_request": ok}
        )
    assert control["_success"] is True, control.get("error")


def test_a_typed_apply_executes_the_stored_compiled_plan():
    """AR1-01: apply consumes the plan compile certified — never a rebuild.

    Compile fingerprinted the plan and threw the object away, so apply rebuilt
    one whose self-check proved only that it matched itself. The bundle now
    retains the keyed plans, `__post_init__` refuses a bundle missing one, and
    the mutation attestation's `plan_fingerprint` must equal the served compile
    artifact digest.

    Discriminator: a SPY on the plan builder. A typed apply invokes it exactly
    once — inside the preflight recompile — and never again in the apply arm.
    The pre-fix tree calls it twice (preflight + the arm's rebuild).
    """
    import boomi_mcp.authoring.process_materialization as pm

    calls = {"n": 0}
    real = pm.build_materialization_plan

    def _spy(*args, **kwargs):
        calls["n"] += 1
        return real(*args, **kwargs)

    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    payload = _bound_payload()
    _SUBMITTED.clear()
    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml, patch.object(
        pm, "build_materialization_plan", _spy
    ):
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = _create
        get_xml.side_effect = _live_xml
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": payload, "dry_run": False},
        )

    assert result["_success"] is True, result.get("error")
    assert calls["n"] == 1, (
        "expected exactly the preflight compile's plan build; %d calls means "
        "the apply arm rebuilt" % calls["n"]
    )

    # ...and the executed plan IS the compiled artifact, by digest.
    mutation = result["process_mutations"][0]
    with patch(_PAGINATE) as paginate:
        paginate.return_value = []
        from boomi_mcp.authoring.workflow import compile_authoring_request_v1

        compiled, _ = compile_authoring_request_v1(
            process_ir_request(), boomi_client=MagicMock(), profile=_PROFILE
        )
    compiled_digest = next(
        a.digest for a in compiled.artifact_fingerprints
        if a.artifact_kind == "process_component_materialization_plan"
    )
    assert mutation["plan_fingerprint"] == compiled_digest


def test_a_clone_attests_the_COMPILED_plan_and_emits_the_suffixed_name():
    """AR1-01 clone half: the clone name is an execution overlay.

    Renaming the envelope before plan construction put a clone-generated name
    into covered fingerprint material — an explicit plan exclusion — and made
    `process_mutations[].plan_fingerprint` permanently unequal to the compiled
    artifact digest. Now the plan and its fingerprint stay what compile
    certified; only the emitted XML and the attestation carry the suffix.

    The existing-component resolver is patched for the WHOLE test — binding
    compile and apply alike — because the legacy lint's evidence feeds the plan
    hash, and a resolver that answers differently between the two phases makes
    the binding legitimately stale (measured while writing this test).
    """
    from boomi_mcp.categories import integration_builder as ib
    from boomi_mcp.authoring.workflow import compile_authoring_request_v1
    from boomi_mcp.models.authoring_workflow import parse_authoring_request_v1

    def _resolve(_client, comp):
        # Only the PROCESS name collides; components resolve to nothing.
        if getattr(comp, "type", None) == "process":
            return [{"component_id": "cid-live", "name": "M12.15 Process",
                     "folder_name": "Home"}]
        return []

    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    base = process_ir_request().model_dump(mode="json")
    base["intent"]["conflict_policy"] = "clone"

    _SUBMITTED.clear()
    with patch.object(ib, "_resolve_existing_components", _resolve),          patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml:
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = _create
        get_xml.side_effect = _live_xml

        request = parse_authoring_request_v1(dict(base))
        compiled, _ = compile_authoring_request_v1(
            request, boomi_client=MagicMock(), profile=_PROFILE
        )
        payload = dict(base)
        payload["expected_capability_revision"] = (
            compiled.revision_binding.capability_revision
        )
        payload["expected_compile_hash"] = compiled.revision_binding.compile_hash
        compiled_digest = next(
            a.digest for a in compiled.artifact_fingerprints
            if a.artifact_kind == "process_component_materialization_plan"
        )

        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": payload, "dry_run": False},
        )

    assert result["_success"] is True, result.get("error")
    # The EMITTED artifact carries the suffixed name...
    assert 'name="M12.15 Process-clone"' in _SUBMITTED["xml"]
    # ...the attestation is action=create and certifies the COMPILED plan...
    mutation = result["process_mutations"][0]
    assert mutation["action"] == "create"
    assert mutation["plan_fingerprint"] == compiled_digest
    # ...and a deliberate clone is NOT reported as a platform rename.
    step = result["results"]["proc"]
    assert step["requested_name"] == "M12.15 Process-clone"
    assert "name_reassigned_by_platform" not in step


def test_folder_placement_resolves_to_exactly_one_live_folder():
    """AR1-06: the placement plan item, previously unimplemented.

    `PROCESS_MATERIALIZATION_PLACEMENT_NOT_FOUND` and `_AMBIGUOUS` were
    registered with no reachable producer — the same published-but-unreachable
    condition this slice's own r1-03 row treats as a defect. Resolution is
    exact-name, non-deleted, UNIQUE, decided before any write, and the create
    attestation records the resolved id.
    """
    from boomi_mcp.categories import integration_builder as ib

    def _payload_with_folder():
        unit = process_unit(folder_name="Target Folder")
        return _bound_payload(process_ir_request(units=(unit,)))

    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    def _apply_with_folders(folders):
        _SUBMITTED.clear()
        created["n"] = 0
        with patch("boomi_mcp.categories.folders._query_all_folders",
                   return_value=folders), \
             patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
            _CREATE
        ) as create, patch(_GET_XML) as get_xml:
            paginate.return_value = []
            execute.side_effect = _component
            create.side_effect = _create
            get_xml.side_effect = _live_xml
            return build_integration_action(
                MagicMock(), _PROFILE, "apply",
                config={"authoring_request": _payload_with_folder(), "dry_run": False},
            )

    one = [{"id": "folder-1", "name": "Target Folder", "deleted": False}]
    two = one + [{"id": "folder-2", "name": "Target Folder", "deleted": False}]

    # UNIQUE match: apply proceeds and the create attestation records the id.
    ok = _apply_with_folders(one)
    assert ok["_success"] is True, ok.get("error")
    placement = ok["process_mutations"][0]["resolved_placement"]
    assert placement["folder_name"] == "Target Folder"
    assert placement["folder_id"] == "folder-1"

    # ZERO matches: refused BEFORE the mutation loop even starts (Codex round
    # 13) — placement is fully pre-decidable, so not even the root's
    # DEPENDENCIES are written. Empty partial_results is the proof.
    none = _apply_with_folders([])
    assert none["_success"] is False
    assert none["error_code"] == "PROCESS_MATERIALIZATION_PLACEMENT_NOT_FOUND"
    assert none["failed_step"] == "proc"
    assert none["partial_results"] == {}

    # MULTIPLE matches: refused, never first-match guessed — same pre-loop stop.
    many = _apply_with_folders(two)
    assert many["_success"] is False
    assert many["error_code"] == "PROCESS_MATERIALIZATION_PLACEMENT_AMBIGUOUS"
    assert many["partial_results"] == {}

    # Account-root placement (no folder named) stays legitimate.
    root = _apply(_bound_payload())
    assert root["_success"] is True, root.get("error")


def test_partial_mutation_evidence_survives_the_lost_response():
    """AR1-04: the durable `in_progress` -> `failed_partial` record.

    Attestations lived only in the response envelope, so a caller that lost it
    had no record of a write that happened. A typed canonical apply now
    allocates its build record BEFORE the first mutation, appends each root's
    attestations durably as they land, and a later failure transitions the
    record to `failed_partial` and serves its `build_id`.
    """
    from boomi_mcp.categories.integration_builder import _BUILD_REGISTRY
    from _m12_11_support import appliable_process_unit

    # Two roots; the second one's create is rejected AFTER the first landed.
    first = appliable_process_unit(key="proc_ok", name="M12.15 OK")
    second = appliable_process_unit(
        key="proc_bad", name="M12.15 Bad", depends_on=("conn", "op", "proc_ok")
    )
    payload = _bound_payload(process_ir_request(units=(first, second)))

    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    calls = {"n": 0}

    def _create(_client, _profile, payload_in):
        calls["n"] += 1
        _SUBMITTED["xml"] = payload_in["xml"]
        if calls["n"] == 1:
            return {"_success": True, "component_id": "proc-ok-id"}
        return {"_success": False, "error": "platform 400 on the second root"}

    _SUBMITTED.clear()
    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml:
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = _create
        get_xml.side_effect = _live_xml
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": payload, "dry_run": False},
        )

    assert result["_success"] is False
    build_id = result.get("build_id")
    assert build_id, "the failure envelope must carry the durable build_id"

    # THE POINT: throw the response away — the registry still has the record.
    record = _BUILD_REGISTRY[build_id]
    assert record["status"] == "failed_partial"
    assert [m["component_key"] for m in record["process_mutations"]] == ["proc_ok"]
    assert record["process_mutations"][0]["result_component_id"] == "proc-ok-id"
    # The failed root and unapplied roots have NO successful attestation.
    assert all(m["component_key"] != "proc_bad" for m in record["process_mutations"])

    # Control: a fully successful apply completes the SAME id it allocated.
    _SUBMITTED.clear()
    ok = _apply(_bound_payload())
    assert ok["_success"] is True, ok.get("error")
    assert _BUILD_REGISTRY[ok["build_id"]]["status"] == "complete"


def test_the_attested_scope_hash_cannot_be_chosen_by_the_caller():
    """AR1-05(a): the scope hash binds to the CONNECTED account.

    Execution passed `config.get("account_id")` into the attestation while the
    typed preflight had already derived the real account — so a caller could
    pick the attested scope. The apply arm now derives it from the client,
    exactly as preflight and verify do.
    """
    def _one_apply(extra_config):
        created = {"n": 0}

        def _component(*_a, **_k):
            created["n"] += 1
            return {"_success": True, "component_id": "cid-%d" % created["n"]}

        def _create(_client, _profile, payload_in):
            _SUBMITTED["xml"] = payload_in["xml"]
            return {"_success": True, "component_id": _PROCESS_ID}

        _SUBMITTED.clear()
        with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
            _CREATE
        ) as create, patch(_GET_XML) as get_xml:
            paginate.return_value = []
            execute.side_effect = _component
            create.side_effect = _create
            get_xml.side_effect = _live_xml
            return build_integration_action(
                MagicMock(), _PROFILE, "apply",
                config={"authoring_request": _bound_payload(), "dry_run": False,
                        **extra_config},
            )

    honest = _one_apply({})
    influenced = _one_apply({"account_id": "attacker-chosen-scope"})
    assert honest["_success"] and influenced["_success"]
    assert (
        honest["process_mutations"][0]["account_scope_hash"]
        == influenced["process_mutations"][0]["account_scope_hash"]
    ), "a caller-supplied account_id changed the attested scope hash"


def test_the_submitted_digest_is_raw_sha256_of_the_submitted_bytes():
    """AR1-05(b)/(c): the exact UTF-8 bytes, hashed as themselves.

    The digest wrapped the XML in a canonical-JSON object before hashing, so it
    was a hash of DIFFERENT material than the bytes the platform received — the
    reviewer confirmed the two digests differ. It is now raw SHA-256 over the
    submitted bytes, computed immediately before the wire call on each path.
    """
    import hashlib as _hashlib

    result = _apply(_bound_payload())
    assert result["_success"] is True, result.get("error")
    mutation = result["process_mutations"][0]
    expected = "sha256:" + _hashlib.sha256(
        _SUBMITTED["xml"].encode("utf-8")
    ).hexdigest()
    assert mutation["submitted_xml_digest"] == expected

    # Non-vacuity: the OLD convention over the same bytes is a different value.
    from boomi_mcp.authoring.revisions import sha256_fingerprint

    assert mutation["submitted_xml_digest"] != sha256_fingerprint(
        {"component_xml": _SUBMITTED["xml"]}
    )


def test_an_unknown_envelope_field_serves_its_registered_code():
    """AR1-08 sub-claim 4: `PROCESS_COMPONENT_SCHEMA_UNKNOWN_FIELD` is reachable.

    The plan registers the code for exactly this refusal; the implementation
    served every unknown field as generic `INVALID_INPUT`. Scoped by location —
    unknown fields OUTSIDE the process unit keep the generic code, so this
    widens what is named, never what is refused.
    """
    payload = process_ir_request().model_dump(mode="json")
    payload["intent"]["units"][0]["envelope"]["not_a_field"] = "x"
    result = build_integration_action(
        MagicMock(), _PROFILE, "compile", config={"authoring_request": payload}
    )
    assert result["_success"] is False
    assert result["error_code"] == "PROCESS_COMPONENT_SCHEMA_UNKNOWN_FIELD"
    assert any(
        e["type"] == "extra_forbidden" for e in result["validation_errors"]
    )

    # Control: an unknown field at the REQUEST level stays generic.
    other = process_ir_request().model_dump(mode="json")
    other["not_a_field"] = "x"
    generic = build_integration_action(
        MagicMock(), _PROFILE, "compile", config={"authoring_request": other}
    )
    assert generic["_success"] is False
    assert generic["error_code"] == "INVALID_INPUT"


def test_a_healthy_clone_does_not_warn_about_a_platform_rename():
    """Codex round 13: the outer `requested_name` must be overlay-aware too.

    The step-level fields read through `_requested_name()`, but the arm's outer
    return still reported `envelope.name` — and the loop's rename warning
    compares THAT against the live name, so every healthy clone warned that the
    platform had renamed it.
    """
    from boomi_mcp.categories import integration_builder as ib

    def _resolve(_client, comp):
        if getattr(comp, "type", None) == "process":
            return [{"component_id": "cid-live", "name": "M12.15 Process",
                     "folder_name": "Home"}]
        return []

    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    base = process_ir_request().model_dump(mode="json")
    base["intent"]["conflict_policy"] = "clone"

    from boomi_mcp.authoring.workflow import compile_authoring_request_v1
    from boomi_mcp.models.authoring_workflow import parse_authoring_request_v1

    _SUBMITTED.clear()
    with patch.object(ib, "_resolve_existing_components", _resolve), \
         patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml:
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = _create
        get_xml.side_effect = _live_xml
        request = parse_authoring_request_v1(dict(base))
        compiled, _ = compile_authoring_request_v1(
            request, boomi_client=MagicMock(), profile=_PROFILE
        )
        payload = dict(base)
        payload["expected_capability_revision"] = (
            compiled.revision_binding.capability_revision
        )
        payload["expected_compile_hash"] = compiled.revision_binding.compile_hash
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": payload, "dry_run": False},
        )

    assert result["_success"] is True, result.get("error")
    # A DELIBERATE clone is not a platform rename and not an unconfirmed name.
    assert not [
        w for w in (result.get("warnings") or [])
        if "not the authored" in w or "did not confirm" in w
    ], result.get("warnings")


def test_an_unknown_field_on_a_spec_nested_unit_serves_the_registered_code():
    """Codex round 13: the location predicate covers `.processes.` paths too."""
    spec_payload = {
        "contract_version": "2",
        "intent": {
            "intent_kind": "integration_spec",
            "integration_spec": {
                "name": "X",
                "components": [APPLIABLE_CONN, APPLIABLE_OP],
                "processes": [
                    {**process_unit().model_dump(mode="json"), "not_a_field": "x"}
                ],
            },
        },
    }
    result = build_integration_action(
        MagicMock(), _PROFILE, "compile", config={"authoring_request": spec_payload}
    )
    assert result["_success"] is False
    assert result["error_code"] == "PROCESS_COMPONENT_SCHEMA_UNKNOWN_FIELD"


def test_the_typed_arm_never_renders_a_mutated_units_values():
    """§6 AR2-01: the composer arm was fixed; the DIRECT arm was not.

    `ProcessAuthoringUnitV1` is frozen, but the `ProcessIRV1` it holds is not,
    so an in-process caller can hand over a parsed unit and mutate its body
    afterwards. Normalization put that same object into the spec, and the
    semantic validator's snapshot then dumped it with warnings enabled —
    rendering the caller's authored content, secret included, into a pydantic
    warning, followed by a raw ValidationError carrying it again. Same class as
    the composer defect, on the arm the composer fix did not cover.
    """
    import warnings as _warnings

    from boomi_mcp.authoring.workflow import plan_authoring_request_v1

    request = process_ir_request(units=(process_unit(),))
    # Mutate the model the CALLER still holds, after the request is built.
    object.__setattr__(request.intent.units[0].process_ir, "body",
                       {"password": "hunter2-S3cret-AR2"})

    with _warnings.catch_warnings(record=True) as caught:
        _warnings.simplefilter("always")
        try:
            result = plan_authoring_request_v1(
                request, boomi_client=MagicMock(), profile=_PROFILE
            )
            served = repr(result)
            raised = None
        except Exception as exc:  # the refusal channel is what matters here
            served = repr(exc)
            raised = exc

    leaked = [w for w in caught if "hunter2" in str(w.message)]
    assert leaked == [], "the serializer warning rendered the mutated value"
    assert "hunter2" not in served, "the served refusal rendered the mutated value"
    # ...and the refusal is the TYPED channel, not a raw pydantic escape.
    if raised is not None:
        from boomi_mcp.authoring.workflow import AuthoringWorkflowError
        assert isinstance(raised, AuthoringWorkflowError), type(raised).__name__


def test_reparsing_a_mutated_root_does_not_render_its_values():
    """Codex round 13: the reparse dump must not WARN the secret onto stderr.

    Dumping a mutated model makes pydantic emit a serializer warning that
    renders `input_value` — so the exact case the reparse exists for wrote the
    caller's authored content to stderr before the value-free parser ran.
    """
    import warnings as _warnings

    from boomi_mcp.models.process_ir import parse_process_ir_v1
    from boomi_mcp.recipes.composer import _validated_direct_roots
    from boomi_mcp.recipes.errors import RecipeError

    root = parse_process_ir_v1({
        "version": "1",
        "body": {"kind": "sequence", "steps": [
            {"kind": "source", "connection_ref": "$ref:conn", "operation_ref": "$ref:op"},
            {"kind": "message", "text": "hello"},
            {"kind": "return_documents"},
        ]},
    })
    object.__setattr__(root, "body", {"password": "hunter2-S3cret"})

    with _warnings.catch_warnings(record=True) as caught:
        _warnings.simplefilter("always")
        try:
            _validated_direct_roots({"proc": root})
            raised = False
        except RecipeError:
            raised = True

    assert raised, "a mutated root must be refused, not composed"
    leaked = [w for w in caught if "hunter2" in str(w.message)]
    assert leaked == [], "the serializer warning rendered the mutated value"


def test_a_reused_root_is_not_blocked_by_a_moved_folder():
    """Codex round 14: a reuse writes nothing and uses no folder id.

    The pre-loop placement pass validated every canonical root's folder, so a
    moved or deleted folder rejected an otherwise valid idempotent re-apply
    whose plan was `reuse` — a step that never materializes.
    """
    from boomi_mcp.categories import integration_builder as ib

    def _resolve(_client, comp):
        if getattr(comp, "type", None) == "process":
            return [{"component_id": "cid-live", "name": "M12.15 Process",
                     "folder_name": "Old"}]
        return []

    unit = process_unit(folder_name="Folder That Moved")
    payload = None
    with patch.object(ib, "_resolve_existing_components", _resolve), \
         patch("boomi_mcp.categories.folders._query_all_folders", return_value=[]), \
         patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _GET_XML
    ) as get_xml:
        paginate.return_value = []
        execute.side_effect = lambda *a, **k: {"_success": True, "component_id": "cid-x"}
        get_xml.side_effect = _live_xml
        payload = _bound_payload(process_ir_request(units=(unit,)))
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": payload, "dry_run": False},
        )

    assert result["_success"] is True, result.get("error")
    assert result["results"]["proc"]["status"] == "reused"

    # Control: the same vanished folder still refuses a root that would CREATE.
    with patch("boomi_mcp.categories.folders._query_all_folders", return_value=[]), \
         patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _GET_XML
    ) as get_xml:
        paginate.return_value = []
        execute.side_effect = lambda *a, **k: {"_success": True, "component_id": "cid-x"}
        get_xml.side_effect = _live_xml
        refused = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": _bound_payload(process_ir_request(units=(unit,))),
                    "dry_run": False},
        )
    assert refused["_success"] is False
    assert refused["error_code"] == "PROCESS_MATERIALIZATION_PLACEMENT_NOT_FOUND"


def test_a_malformed_nested_processir_keeps_the_generic_code():
    """Codex round 14: ProcessIR schema failures are not envelope failures.

    An extra field INSIDE the nested ProcessIR sits under `.units.`/`.processes.`
    too, so the location predicate wrongly served the component-schema code for
    an IR document defect. `.process_ir.` paths are excluded on both intent
    shapes.
    """
    # The INTEGRATION_SPEC intent shape, which is where the defect lives: the
    # typed process_ir intent parses its IR through the canonical parser and
    # never reaches this predicate with an IR path (measured — the pre-fix
    # control only reddens on this shape).
    unit_doc = process_unit().model_dump(mode="json")
    unit_doc["process_ir"]["body"]["not_a_field"] = "x"
    payload = {
        "contract_version": "2",
        "intent": {
            "intent_kind": "integration_spec",
            "integration_spec": {
                "name": "X",
                "components": [APPLIABLE_CONN, APPLIABLE_OP],
                "processes": [unit_doc],
            },
        },
    }
    result = build_integration_action(
        MagicMock(), _PROFILE, "compile", config={"authoring_request": payload}
    )
    assert result["_success"] is False
    assert result["error_code"] != "PROCESS_COMPONENT_SCHEMA_UNKNOWN_FIELD"

    # ...while a genuine envelope unknown-field keeps the registered code.
    envelope_bad = process_ir_request().model_dump(mode="json")
    envelope_bad["intent"]["units"][0]["envelope"]["not_a_field"] = "x"
    named = build_integration_action(
        MagicMock(), _PROFILE, "compile", config={"authoring_request": envelope_bad}
    )
    assert named["error_code"] == "PROCESS_COMPONENT_SCHEMA_UNKNOWN_FIELD"


def test_an_ignored_placement_is_never_attested_as_applied():
    """QA-153-r12-01: the platform ignores `folderName` on create.

    Measured live for every builder and every spelling — so a create can
    validate the folder, resolve it, attest `resolved_placement.folder_id`, and
    still land at the account root. An attestation of a placement that never
    happened is exactly the mutation-accounting defect this slice exists to
    forbid. The fix is the r10 pattern: placement is verified from the READBACK,
    the resolved id is attested only when the platform honoured the request, and
    an ignored placement gets an actionable warning naming where the component
    actually is.
    """
    unit = process_unit(folder_name="Target Folder")
    folders = [{"id": "folder-1", "name": "Target Folder", "deleted": False}]

    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    def _run(readback_for_process):
        _SUBMITTED.clear()
        created["n"] = 0

        def _live(_client, component_id, *_a, **_k):
            if component_id == _PROCESS_ID:
                return {"type": "process", "xml": readback_for_process()}
            return {"type": "connector-settings", "xml": _LIVE_COMPONENT_XML}

        with patch("boomi_mcp.categories.folders._query_all_folders",
                   return_value=folders), \
             patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
            _CREATE
        ) as create, patch(_GET_XML) as get_xml:
            paginate.return_value = []
            execute.side_effect = _component
            create.side_effect = _create
            get_xml.side_effect = _live
            return build_integration_action(
                MagicMock(), _PROFILE, "apply",
                config={"authoring_request": _bound_payload(
                    process_ir_request(units=(unit,))
                ), "dry_run": False},
            )

    # THE HONOURING PLATFORM: readback carries the folder -> id is attested.
    honoured = _run(lambda: _SUBMITTED["xml"].replace(
        'name="M12.15 Process"',
        'name="M12.15 Process" folderFullPath="Acct/Target Folder"', 1,
    ))
    assert honoured["_success"] is True, honoured.get("error")
    placement = honoured["process_mutations"][0]["resolved_placement"]
    assert placement["folder_name"] == "Target Folder"
    assert placement["folder_id"] == "folder-1"
    assert honoured["results"]["proc"]["placement_verified"] is True
    assert not [w for w in (honoured.get("warnings") or []) if "NOT placed" in w]

    # THE IGNORING PLATFORM (what QA measured): the component sits at root.
    # The root readback's folderFullPath is a SINGLE segment — the account
    # name, not a folder — so the attestation must carry NO folder_name at all
    # (Codex round 16 F1: the leaf reduction attested the account name as a
    # placement that never happened).
    ignored = _run(lambda: _SUBMITTED["xml"].replace(
        'name="M12.15 Process"',
        'name="M12.15 Process" folderFullPath="Acct"', 1,
    ).replace(' folderName="Target Folder"', "", 1))
    assert ignored["_success"] is True, ignored.get("error")
    placement = ignored["process_mutations"][0]["resolved_placement"]
    # The observed placement is attested; the resolved id is NOT.
    assert placement["folder_id"] is None
    assert placement["folder_name"] is None
    step = ignored["results"]["proc"]
    assert step["placement_verified"] is False
    assert step["requested_folder_name"] == "Target Folder"
    warning = [w for w in (ignored.get("warnings") or []) if "NOT placed" in w]
    assert warning, ignored.get("warnings")
    assert "Target Folder" in warning[0]
    assert "the account root" in warning[0]


def test_a_folder_named_like_the_account_cannot_fake_a_placement():
    """Codex round 16 F1, the trap case: request a folder whose NAME equals the
    account name. The root readback's single-segment folderFullPath IS the
    account name, so a leaf-name comparison confirms a placement the platform
    never performed — folder IDENTITY (root-vs-folder, and the readback's own
    folderId when present) is what may confirm, never leaf-name equality.
    """
    unit = process_unit(folder_name="Acct")
    folders = [{"id": "folder-acct", "name": "Acct", "deleted": False}]

    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    def _run(readback_for_process):
        _SUBMITTED.clear()
        created["n"] = 0

        def _live(_client, component_id, *_a, **_k):
            if component_id == _PROCESS_ID:
                return {"type": "process", "xml": readback_for_process()}
            return {"type": "connector-settings", "xml": _LIVE_COMPONENT_XML}

        with patch("boomi_mcp.categories.folders._query_all_folders",
                   return_value=folders),              patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
            _CREATE
        ) as create, patch(_GET_XML) as get_xml:
            paginate.return_value = []
            execute.side_effect = _component
            create.side_effect = _create
            get_xml.side_effect = _live
            return build_integration_action(
                MagicMock(), _PROFILE, "apply",
                config={"authoring_request": _bound_payload(
                    process_ir_request(units=(unit,))
                ), "dry_run": False},
            )

    # The trap: root readback, full path == the requested folder's name.
    trapped = _run(lambda: _SUBMITTED["xml"].replace(
        'name="M12.15 Process"',
        'name="M12.15 Process" folderFullPath="Acct"', 1,
    ).replace(' folderName="Acct"', "", 1))
    assert trapped["_success"] is True, trapped.get("error")
    placement = trapped["process_mutations"][0]["resolved_placement"]
    assert placement["folder_id"] is None
    assert placement["folder_name"] is None
    assert trapped["results"]["proc"]["placement_verified"] is False
    assert [w for w in (trapped.get("warnings") or []) if "NOT placed" in w]

    # The control: a genuine two-segment placement in that folder confirms.
    genuine = _run(lambda: _SUBMITTED["xml"].replace(
        'name="M12.15 Process"',
        'name="M12.15 Process" folderFullPath="Root/Acct"', 1,
    ))
    assert genuine["_success"] is True, genuine.get("error")
    placement = genuine["process_mutations"][0]["resolved_placement"]
    assert placement["folder_name"] == "Acct"
    assert placement["folder_id"] == "folder-acct"
    assert genuine["results"]["proc"]["placement_verified"] is True

    # The identity check outranks the leaf: a readback whose own folderId names
    # a DIFFERENT folder does not confirm, even with the leaf name matching.
    imposter = _run(lambda: _SUBMITTED["xml"].replace(
        'name="M12.15 Process"',
        'name="M12.15 Process" folderFullPath="Root/Acct"'
        ' folderId="folder-OTHER"', 1,
    ))
    assert imposter["_success"] is True, imposter.get("error")
    assert imposter["results"]["proc"]["placement_verified"] is False
    # Codex round 17 F1: the KNOWN identity is carried, not discarded — the
    # attestation records the readback's own folderId (never the requested
    # resolution), and the step + warning name the full observed path, so the
    # actual placement is distinguishable from the requested same-named folder.
    placement = imposter["process_mutations"][0]["resolved_placement"]
    assert placement["folder_id"] == "folder-OTHER"
    assert placement["folder_name"] == "Acct"
    step = imposter["results"]["proc"]
    assert step["observed_folder"] == "Root/Acct"
    assert step["observed_folder_id"] == "folder-OTHER"
    warning = [w for w in (imposter.get("warnings") or []) if "NOT placed" in w]
    assert warning, imposter.get("warnings")
    assert "Root/Acct" in warning[0]
    # ...and the SAME folderId confirms through the identity branch.
    identified = _run(lambda: _SUBMITTED["xml"].replace(
        'name="M12.15 Process"',
        'name="M12.15 Process" folderFullPath="Root/Acct"'
        ' folderId="folder-acct"', 1,
    ))
    assert identified["_success"] is True, identified.get("error")
    assert identified["results"]["proc"]["placement_verified"] is True


def test_an_id_only_readback_is_a_folder_not_the_root():
    """Codex round 18: folderFullPath/folderName are OPTIONAL response
    metadata, so a readback can carry only folderId. That id is folder
    evidence — classifying the location as root while forwarding the id
    attested one create as simultaneously "in folder <id>" and "at the account
    root". An id-bearing readback is a folder with an unknown name: compared by
    id, named by id in the warning, and never called the root.
    """
    unit = process_unit(folder_name="Target Folder")
    folders = [{"id": "folder-1", "name": "Target Folder", "deleted": False}]

    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    def _run(readback_for_process):
        _SUBMITTED.clear()
        created["n"] = 0

        def _live(_client, component_id, *_a, **_k):
            if component_id == _PROCESS_ID:
                return {"type": "process", "xml": readback_for_process()}
            return {"type": "connector-settings", "xml": _LIVE_COMPONENT_XML}

        with patch("boomi_mcp.categories.folders._query_all_folders",
                   return_value=folders), \
             patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
            _CREATE
        ) as create, patch(_GET_XML) as get_xml:
            paginate.return_value = []
            execute.side_effect = _component
            create.side_effect = _create
            get_xml.side_effect = _live
            return build_integration_action(
                MagicMock(), _PROFILE, "apply",
                config={"authoring_request": _bound_payload(
                    process_ir_request(units=(unit,))
                ), "dry_run": False},
            )

    # An id-only readback whose id matches the resolution CONFIRMS by identity.
    matching = _run(lambda: _SUBMITTED["xml"].replace(
        'name="M12.15 Process"',
        'name="M12.15 Process" folderId="folder-1"', 1,
    ).replace(' folderName="Target Folder"', "", 1))
    assert matching["_success"] is True, matching.get("error")
    placement = matching["process_mutations"][0]["resolved_placement"]
    assert placement["folder_id"] == "folder-1"
    assert matching["results"]["proc"]["placement_verified"] is True
    assert not [w for w in (matching.get("warnings") or [])
                if "NOT placed" in w]

    # A DIFFERENT id refuses — and the warning names the id, never the root.
    different = _run(lambda: _SUBMITTED["xml"].replace(
        'name="M12.15 Process"',
        'name="M12.15 Process" folderId="folder-OTHER"', 1,
    ).replace(' folderName="Target Folder"', "", 1))
    assert different["_success"] is True, different.get("error")
    placement = different["process_mutations"][0]["resolved_placement"]
    assert placement["folder_id"] == "folder-OTHER"
    assert placement["folder_name"] is None
    step = different["results"]["proc"]
    assert step["placement_verified"] is False
    assert step["observed_folder_id"] == "folder-OTHER"
    warning = [w for w in (different.get("warnings") or []) if "NOT placed" in w]
    assert warning, different.get("warnings")
    assert "folder id 'folder-OTHER'" in warning[0]
    assert "the account root" not in warning[0]


def test_the_roots_own_folder_id_does_not_make_it_a_folder():
    """Codex round 19, against live capture: Boomi's account root IS a folder —
    `tests/fixtures/live_xml/m11/processproperty_minimal.xml` shows a rooted
    component's readback carrying folderFullPath="Renera" (single segment),
    folderName="Renera" AND folderId. The round-18 tie-break let that id flip
    the classification to non-root, so a rooted create would be attested with
    the root's folderId and warned about as an unknown folder id. The
    single-segment path stays authoritative: root, id never propagated as a
    placement.
    """
    unit = process_unit(folder_name="Target Folder")
    folders = [{"id": "folder-1", "name": "Target Folder", "deleted": False}]

    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    # The LIVE root shape, all three attributes exactly as the platform serves
    # them for a rooted component.
    def _live(_client, component_id, *_a, **_k):
        if component_id == _PROCESS_ID:
            return {"type": "process", "xml": _SUBMITTED["xml"].replace(
                'name="M12.15 Process"',
                'name="M12.15 Process" folderFullPath="Acct"'
                ' folderId="Rjo4NjMyNjEx"', 1,
            ).replace(' folderName="Target Folder"', ' folderName="Acct"', 1)}
        return {"type": "connector-settings", "xml": _LIVE_COMPONENT_XML}

    with patch("boomi_mcp.categories.folders._query_all_folders",
               return_value=folders), \
         patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml:
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = _create
        get_xml.side_effect = _live
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": _bound_payload(
                process_ir_request(units=(unit,))
            ), "dry_run": False},
        )

    assert result["_success"] is True, result.get("error")
    step = result["results"]["proc"]
    assert step["placement_verified"] is False
    # The root's own id is NOT a placement: never on the step...
    assert "observed_folder_id" not in step
    # ...never in the attestation...
    placement = result["process_mutations"][0]["resolved_placement"]
    assert placement["folder_id"] is None
    assert placement["folder_name"] is None
    # ...and the warning calls it the root, not an unknown folder id.
    warning = [w for w in (result.get("warnings") or []) if "NOT placed" in w]
    assert warning, result.get("warnings")
    assert "the account root" in warning[0]
    assert "folder id" not in warning[0]


def test_the_placement_classifier_agrees_with_every_live_capture():
    """The identity classifier, graded against the WHOLE live-capture corpus.

    Codex rounds 16-19 each found one more readback shape the classifier
    mis-read, and round 19's was refutable from a fixture already in the repo.
    This witness closes that loop structurally: every live capture under
    tests/fixtures/live_xml/ that carries folder attributes must classify to
    exactly what the platform said — a single-segment full path is the account
    root (id retained, never a placement), a multi-segment path is that folder
    with the capture's own folderId. A future shape the platform serves lands
    here as a fixture, not as a review round.
    """
    import pathlib
    import xml.etree.ElementTree as ET
    from boomi_mcp.categories.components.canonical_process_apply import (
        observed_folder_identity,
    )

    corpus = sorted(pathlib.Path("tests/fixtures/live_xml").rglob("*.xml"))
    graded = 0
    for fixture in corpus:
        xml_text = fixture.read_text()
        try:
            attrs = ET.fromstring(xml_text).attrib
        except ET.ParseError:
            continue
        full_path = attrs.get("folderFullPath")
        if not full_path:
            continue
        graded += 1
        identity = observed_folder_identity(xml_text)
        assert identity is not None, fixture
        segments = [part for part in full_path.rstrip("/").split("/") if part]
        if len(segments) <= 1:
            assert identity["is_root"] is True, (fixture, identity)
            assert identity["folder_id"] == (attrs.get("folderId") or None), (
                fixture, identity)
        else:
            assert identity["is_root"] is False, (fixture, identity)
            assert identity["leaf"] == segments[-1], (fixture, identity)
            assert identity["full_path"] == full_path, (fixture, identity)
            assert identity["folder_id"] == (attrs.get("folderId") or None), (
                fixture, identity)
    # Non-vacuity: the corpus must actually exercise BOTH classifications.
    assert graded >= 10, graded
    roots = [f for f in corpus
             if 'folderFullPath="Renera"' in f.read_text()]
    assert roots, "corpus lost its live root capture"


def test_an_explicitly_requested_root_folder_confirms_by_its_own_id():
    """Codex round 20: the account root is itself a folder row, so a caller may
    name it in `folder_name` and resolution accepts its id. The platform then
    honours the request — the readback is the root WITH that same id — and
    refusing to verify it (the round-19 suppression applied unconditionally)
    served a false refusal and a false warning for an honoured placement. The
    retained root id is compared BEFORE suppression: a match confirms and
    attests name+id like any folder; suppression applies only to a root the
    caller did not ask for.
    """
    unit = process_unit(folder_name="Acct")
    folders = [{"id": "folder-root", "name": "Acct", "deleted": False,
                "full_path": "Acct", "parent_id": ""}]

    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    # The live root shape, with the ROOT's own id echoing the resolution.
    def _live(_client, component_id, *_a, **_k):
        if component_id == _PROCESS_ID:
            return {"type": "process", "xml": _SUBMITTED["xml"].replace(
                'name="M12.15 Process"',
                'name="M12.15 Process" folderFullPath="Acct"'
                ' folderId="folder-root"', 1,
            ).replace(' folderName="Acct"', "", 1)}
        return {"type": "connector-settings", "xml": _LIVE_COMPONENT_XML}

    with patch("boomi_mcp.categories.folders._query_all_folders",
               return_value=folders), \
         patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml:
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = _create
        get_xml.side_effect = _live
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": _bound_payload(
                process_ir_request(units=(unit,))
            ), "dry_run": False},
        )

    assert result["_success"] is True, result.get("error")
    assert result["results"]["proc"]["placement_verified"] is True
    placement = result["process_mutations"][0]["resolved_placement"]
    assert placement["folder_id"] == "folder-root"
    assert placement["folder_name"] == "Acct"
    assert not [w for w in (result.get("warnings") or []) if "NOT placed" in w]


def test_a_failed_readback_never_claims_the_component_is_at_root():
    """Codex round 16 F2: when the post-create readback cannot be fetched or
    parsed, the component's location is UNKNOWN — the warning must say the
    placement is unverified, never assert the component "shows in the account
    root". The parsed-mismatch wording is reserved for a readback that was
    actually read.
    """
    unit = process_unit(folder_name="Target Folder")
    folders = [{"id": "folder-1", "name": "Target Folder", "deleted": False}]

    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    def _live(_client, component_id, *_a, **_k):
        if component_id == _PROCESS_ID:
            raise RuntimeError("readback unavailable")
        return {"type": "connector-settings", "xml": _LIVE_COMPONENT_XML}

    with patch("boomi_mcp.categories.folders._query_all_folders",
               return_value=folders),          patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _CREATE
    ) as create, patch(_GET_XML) as get_xml:
        paginate.return_value = []
        execute.side_effect = _component
        create.side_effect = _create
        get_xml.side_effect = _live
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": _bound_payload(
                process_ir_request(units=(unit,))
            ), "dry_run": False},
        )

    assert result["_success"] is True, result.get("error")
    step = result["results"]["proc"]
    assert step["placement_verified"] is False
    # An unread location is UNKNOWN: no observed_folder claim on the step...
    assert "observed_folder" not in step
    # ...no folder_name in the attestation...
    placement = result["process_mutations"][0]["resolved_placement"]
    assert placement["folder_id"] is None
    assert placement["folder_name"] is None
    # ...and the warning says UNVERIFIED instead of asserting a location.
    warnings = result.get("warnings") or []
    unverified = [w for w in warnings if "placement is UNVERIFIED" in w]
    assert unverified, warnings
    assert "Target Folder" in unverified[0]
    # Codex round 17 F2: this path never parsed anything — the fetch itself
    # failed — so the warning must not diagnose a parse failure.
    assert "could not be parsed" not in unverified[0]
    assert not [w for w in warnings if "NOT placed" in w]
    assert not [w for w in warnings if "read-back shows it in" in w]


def test_every_registered_process_component_code_has_a_reachable_producer():
    """§6 AR2-07, the structural half: registered ≠ reachable.

    This slice registered five `PROCESS_COMPONENT_SCHEMA_*` /
    `PROCESS_COMPONENT_REFERENCE_*` contracts and served exactly one of them:
    the wrapper special-cased unknown fields and collapsed everything else to
    the generic input code, so four documented codes could never be observed by
    the callers they document. That is the same pair as the placement codes
    registered with no producer (QA-153-r1-03/AR1-06), which is why the answer
    here is an invariant rather than another individual mapping.

    The code set is DERIVED from the taxonomy, not hand-listed, so registering
    a sixth code without a producer fails this test rather than shipping.
    """
    from boomi_mcp.categories.integration_builder import _NAMED_VALIDATION_CODES
    from boomi_mcp import errors as _errors

    registered = {
        name: getattr(_errors, name)
        for name in dir(_errors)
        if name.startswith("PROCESS_COMPONENT_")
        and isinstance(getattr(_errors, name), str)
    }
    assert registered, "no PROCESS_COMPONENT_* codes found — guard is vacuous"

    # A code is REACHABLE when the served wrapper can emit it: either the
    # shared pydantic map produces it, or the wrapper names it directly.
    served = set(_NAMED_VALIDATION_CODES.values())
    import inspect
    from boomi_mcp.categories import integration_builder as _ib

    wrapper_source = inspect.getsource(_ib._reject_invalid_typed_request)

    unreachable = sorted(
        code for name, code in registered.items()
        if code not in served and code not in wrapper_source
    )
    assert unreachable == [], (
        "registered with no reachable producer at the served boundary: "
        f"{unreachable}"
    )


def test_a_direct_root_refusal_keeps_every_cause_and_its_path():
    """§6 AR2-08: the preflight carried cause codes and dropped every path.

    One aggregate diagnostic told a caller WHAT was wrong and never WHERE — and
    a multi-fault root collapsed to a single entry. The plan requires both the
    ProcessIR cause codes and their paths to survive the translation; they are
    the authority's own, so nothing is re-derived here.
    """
    from boomi_mcp.models.process_ir import parse_process_ir_v1
    from boomi_mcp.recipes.composer import _validated_direct_roots
    from boomi_mcp.recipes.errors import RecipeError

    root = parse_process_ir_v1({
        "version": "1",
        "body": {"kind": "sequence", "steps": [
            {"kind": "source", "connection_ref": "$ref:conn",
             "operation_ref": "$ref:op"},
            {"kind": "message", "text": "hello"},
            {"kind": "return_documents"},
        ]},
    })
    # A shape the parser refuses, reached through the same mutation route the
    # reparse exists for.
    object.__setattr__(root, "body", {"kind": "sequence", "steps": []})

    with pytest.raises(RecipeError) as caught:
        _validated_direct_roots({"proc": root})

    diagnostics = caught.value.diagnostics
    assert diagnostics, caught.value
    # Every itemized cause keeps its own path AND its own code — no aggregate.
    assert all(d.target == "direct_process:proc" for d in diagnostics)
    assert any(d.path for d in diagnostics), [
        (d.code, d.path, d.cause_codes) for d in diagnostics
    ]
    assert all(len(d.cause_codes) <= 1 for d in diagnostics), [
        d.cause_codes for d in diagnostics
    ]


def test_an_all_reuse_apply_that_fails_late_never_says_reconcile():
    """Codex round 22: deriving "did we write" from an assumption, again.

    An apply whose every root is a `reuse` writes nothing. When the post-loop
    finalization raised, the new guard wrapped it and reported
    `mutation_status="possible"` with a `failed_partial` durable row — telling
    the caller to reconcile an account this run never touched. The discriminator
    is the one `_mutation_status` already reads, and it is consulted only where
    the record can answer: after the loop, where every step has recorded its own
    status.
    """
    from boomi_mcp.categories import integration_builder as ib

    ib._BUILD_REGISTRY.clear()
    unit = process_unit()
    # EVERY root must match an existing component, or the supporting
    # components still create and the apply is not all-reuse at all — which is
    # what the first draft of this witness measured, and why it reported
    # `possible` correctly for the wrong reason.
    existing = [
        {"component_id": "existing-proc", "name": unit.envelope.name,
         "type": "process"},
        {"component_id": "existing-conn", "name": APPLIABLE_CONN["name"],
         "type": "connector-settings"},
        {"component_id": "existing-op", "name": APPLIABLE_OP["name"],
         "type": "connector-action"},
    ]

    def _boom(*_a, **_k):
        raise RuntimeError("registry write failed after a no-write apply")

    def _component(*_a, **_k):
        return {"_success": True, "component_id": "cid-x"}

    def _boom2(*_a, **_k):
        raise RuntimeError("registry write failed after a no-write apply")

    with patch(_PAGINATE, return_value=existing), patch(
        _EXECUTE, side_effect=_component
    ), patch(_CREATE) as create, patch(_GET_XML) as get_xml, patch.object(
        ib, "_authoring_build_provenance", side_effect=_boom2
    ):
        create.side_effect = AssertionError("a reuse must not create")
        get_xml.side_effect = _live_xml
        # The policy travels INSIDE the typed request — passing it as a legacy
        # config root alongside `authoring_request` is refused up front, which
        # is how the first draft of this witness passed without ever reaching
        # the apply.
        request = process_ir_request(units=(unit,))
        request = request.model_copy(update={
            "intent": request.intent.model_copy(
                update={"conflict_policy": "reuse"}
            )
        })
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": _bound_payload(request),
                    "dry_run": False},
        )

    assert result["_success"] is False, result
    # Nothing was written, so nothing is uncertain.
    assert result["mutation_status"] == "none", result
    assert "reconcile" not in result.get("hint", "").lower(), result.get("hint")
    build_id = result.get("build_id")
    if build_id:
        assert ib._BUILD_REGISTRY[build_id]["status"] != "failed_partial"


def test_a_reused_root_is_not_refused_for_a_body_it_never_compiles():
    """Codex round 22: the pre-decidable pass validated an unused input.

    A `reuse` returns before the authored body is compiled or materialized, so
    a literal reference inside that body decides nothing — refusing on it
    rejected a valid idempotent no-write re-apply. Round 14 learned exactly this
    for the placement check and fixed it there alone; the skip is now made once
    for the whole pass, which is what stops the next check repeating it.
    """
    literal = "35813b90-1f42-4dcb-98f5-82d8f96be61d"
    unit = process_unit(process_extensions=ProcessExtensionBindingsV1(
        connections=(ProcessConnectionOverrideV1(
            connection_id=literal, connector_type="rest",
            fields=(ProcessOverrideFieldV1(id="url", label="x"),),
        ),)
    ))
    spec = {
        "name": "M12.15 Integration",
        "components": [APPLIABLE_CONN, APPLIABLE_OP],
        "processes": [unit.model_dump(mode="json")],
    }
    existing = [{"component_id": "existing-proc", "name": unit.envelope.name,
                 "type": "process"}]

    def _component(*_a, **_k):
        return {"_success": True, "component_id": "cid-x"}

    with patch(_PAGINATE, return_value=existing), patch(
        _EXECUTE, side_effect=_component
    ), patch(_CREATE) as create, patch(_GET_XML) as get_xml:
        create.side_effect = AssertionError("a reuse must not create")
        get_xml.side_effect = _live_xml
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"integration_spec": spec, "conflict_policy": "reuse",
                    "dry_run": False},
        )

    assert result["_success"] is True, result.get("error")
    assert result["results"]["proc"]["status"] == "reused", result["results"]

    # THE CONTROL: the same literal reference on a root that would CREATE is
    # still refused before anything is written.
    with patch(_PAGINATE, return_value=[]), patch(
        _EXECUTE, side_effect=_component
    ), patch(_CREATE) as create, patch(_GET_XML) as get_xml:
        create.side_effect = AssertionError("a refused root must not create")
        get_xml.side_effect = _live_xml
        refused = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"integration_spec": spec, "dry_run": False},
        )
    assert refused["_success"] is False
    assert refused["error_code"] == PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE


def test_a_reference_inside_the_ir_keeps_the_ir_taxonomy():
    """Codex round 22: one error type, two schemas, one wrong answer.

    `process_ir_reference_invalid_format` is the ProcessIR authority's own type
    with its own registered code. Mapping it globally to the process-COMPONENT
    code served the wrong taxonomy for a reference inside a unit's IR. Location
    decides, using the same classifier the unknown-field rule consults — a
    second copy of that path test is precisely how two overlapping schemas get
    judged two ways.
    """
    from boomi_mcp.categories.integration_builder import (
        _is_process_component_path, _named_code_from_locations,
    )
    from boomi_mcp.errors import (
        PROCESS_COMPONENT_REFERENCE_INVALID_FORMAT,
        PROCESS_IR_REFERENCE_INVALID_FORMAT,
    )

    ir_path = {
        "path": "intent.units.0.process_ir.body.steps.0.connection_ref",
        "type": "process_ir_reference_invalid_format",
    }
    envelope_path = {
        "path": "intent.units.0.envelope.process_extensions.connections.0."
                "connection_id",
        "type": "process_ir_reference_invalid_format",
    }
    assert _is_process_component_path(ir_path["path"]) is False
    assert _is_process_component_path(envelope_path["path"]) is True
    assert _named_code_from_locations([ir_path]) == (
        PROCESS_IR_REFERENCE_INVALID_FORMAT
    )
    assert _named_code_from_locations([envelope_path]) == (
        PROCESS_COMPONENT_REFERENCE_INVALID_FORMAT
    )


def test_plan_does_not_warn_about_a_refusal_that_will_not_happen():
    """Codex round 23: plan and apply must agree about which roots the rule hits.

    The round-22 fix made apply SKIP relocatability for a root it will reuse —
    a reuse never compiles the authored body — but plan kept warning that
    "apply refuses this before writing anything". A preview that predicts a
    refusal the apply will not perform is worse than silence. Both are now
    generated from the same planned-action decision.
    """
    literal = "35813b90-1f42-4dcb-98f5-82d8f96be61d"
    unit = process_unit(process_extensions=ProcessExtensionBindingsV1(
        connections=(ProcessConnectionOverrideV1(
            connection_id=literal, connector_type="rest",
            fields=(ProcessOverrideFieldV1(id="url", label="x"),),
        ),)
    ))
    spec = {
        "name": "M12.15 Integration",
        "components": [APPLIABLE_CONN, APPLIABLE_OP],
        "processes": [unit.model_dump(mode="json")],
    }
    existing = [{"component_id": "existing-proc", "name": unit.envelope.name,
                 "type": "process"}]

    def _plan(paginate_rows, **config_extra):
        with patch(_PAGINATE, return_value=paginate_rows), patch(
            _EXECUTE
        ) as execute, patch(_GET_XML) as get_xml:
            execute.side_effect = AssertionError("plan writes nothing")
            get_xml.side_effect = _live_xml
            config = {"integration_spec": spec}
            config.update(config_extra)
            return build_integration_action(
                MagicMock(), _PROFILE, "plan", config=config
            )

    # A root that will be REUSED: no refusal is coming, so no warning.
    reused = _plan(existing, conflict_policy="reuse")
    assert reused["_success"] is True, reused.get("error")
    assert not [w for w in (reused.get("warnings") or [])
                if "literal component id" in w], reused.get("warnings")

    # THE CONTROL: the same root with nothing to reuse WILL be refused, and
    # plan says so.
    creating = _plan([])
    assert creating["_success"] is True, creating.get("error")
    assert [w for w in (creating.get("warnings") or [])
            if "literal component id" in w], creating.get("warnings")


def test_a_retry_safe_typed_failure_still_carries_a_machine_code():
    """Codex round 23: the no-write escape served no `error_code` at all.

    Every other no-write typed failure carries the validation-required code —
    `_decorate_typed_apply` applies that rule on the returned paths. The escape
    reaches `_decorate_refusal_route` instead, so the one failure a caller can
    safely retry arrived unclassifiable.
    """
    from boomi_mcp.categories import integration_builder as ib
    from boomi_mcp.errors import (
        ERROR_TAXONOMY, PROCESS_MATERIALIZATION_FINALIZATION_FAILED,
    )

    ib._BUILD_REGISTRY.clear()
    unit = process_unit()
    existing = [
        {"component_id": "existing-proc", "name": unit.envelope.name,
         "type": "process"},
        {"component_id": "existing-conn", "name": APPLIABLE_CONN["name"],
         "type": "connector-settings"},
        {"component_id": "existing-op", "name": APPLIABLE_OP["name"],
         "type": "connector-action"},
    ]

    def _component(*_a, **_k):
        return {"_success": True, "component_id": "cid-x"}

    def _boom(*_a, **_k):
        raise RuntimeError("finalizer failed after a no-write apply")

    with patch(_PAGINATE, return_value=existing), patch(
        _EXECUTE, side_effect=_component
    ), patch(_CREATE) as create, patch(_GET_XML) as get_xml, patch.object(
        ib, "_authoring_build_provenance", side_effect=_boom
    ):
        create.side_effect = AssertionError("a reuse must not create")
        get_xml.side_effect = _live_xml
        request = process_ir_request(units=(unit,))
        request = request.model_copy(update={
            "intent": request.intent.model_copy(
                update={"conflict_policy": "reuse"}
            )
        })
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": _bound_payload(request),
                    "dry_run": False},
        )

    assert result["_success"] is False
    assert result["mutation_status"] == "none"
    assert result["error_code"] == PROCESS_MATERIALIZATION_FINALIZATION_FAILED, result
    # ...and the code's REGISTERED semantics must agree with the envelope's
    # prose (Codex round 24): the first draft served a code registered as
    # non-retryable beside a hint saying a retry is safe, so a code-based
    # client and a human reading the same response drew opposite conclusions.
    assert ERROR_TAXONOMY[result["error_code"]].retryable is True
    assert "retry is safe" in result["hint"]


def test_both_readers_of_the_named_code_map_apply_the_location_rule():
    """QA-153-r14-01: one map, two readers, the rule added to one of them.

    Codex round 22 reserved the process-COMPONENT reference code for the
    component's own paths, using a location classifier — and that rule went
    into the typed-request reader only. The other reader of the same map serves
    the raw `integration_spec` route and the apply-escape handler, so those two
    kept serving the component code for a reference inside a unit's IR while
    the prose beside it named the ProcessIR type. Measured live by QA against a
    baseline sandbox: both locations collapsed to the component code.

    Asserted on BOTH readers with the same inputs, which is the property that
    was missing — not on a third copy of the classifier.
    """
    from pydantic_core import PydanticCustomError, ValidationError as _VE

    from boomi_mcp.categories.integration_builder import (
        _named_code_from_locations, _named_error_code_from_validation,
    )
    from boomi_mcp.errors import (
        PROCESS_COMPONENT_REFERENCE_INVALID_FORMAT,
        PROCESS_IR_REFERENCE_INVALID_FORMAT,
    )

    def _error_at(loc):
        return _VE.from_exception_data("Req", [{
            "type": PydanticCustomError(
                "process_ir_reference_invalid_format", "bad ref"
            ),
            "loc": loc,
            "input": None,
        }])

    ir_loc = ("intent", "units", 0, "process_ir", "body", "steps", 0,
              "connection_ref")
    env_loc = ("intent", "units", 0, "envelope", "process_extensions",
               "connections", 0, "connection_id")

    for loc, expected in (
        (ir_loc, PROCESS_IR_REFERENCE_INVALID_FORMAT),
        (env_loc, PROCESS_COMPONENT_REFERENCE_INVALID_FORMAT),
    ):
        rows = [{"path": ".".join(str(part) for part in loc),
                 "type": "process_ir_reference_invalid_format"}]
        # The typed-request reader...
        assert _named_code_from_locations(rows) == expected, loc
        # ...and the exception reader, which is what regressed.
        assert _named_error_code_from_validation(_error_at(loc)) == expected, loc


def test_an_update_says_so_when_its_requested_folder_is_not_applied():
    """QA-153-r14-02: validated, then silently discarded — on the update path.

    An update's `folder_name` is fully validated (an unresolvable one refuses
    the update outright), and then update preservation keeps the component
    where it lives. The envelope said nothing at all: no requested folder, no
    verification flag, no warning — the exact shape QA-153-r12-01 found on the
    create path. The attestation was already honest after AR2-04; this is the
    envelope catching up, and the warning names the UPDATE mechanism, because
    "this platform ignores folderName on create" is false for an update (a raw
    component update does honour a folder id on this account).
    """
    unit = process_unit(action="update", component_id="existing-proc",
                        folder_name="Target Folder")
    folders = [{"id": "folder-1", "name": "Target Folder", "deleted": False}]
    # The live component sits SOMEWHERE ELSE, and update preservation keeps it
    # there: the readback after the update still reports the live folder.
    elsewhere = ('<bns:Component xmlns:bns="x" type="process" '
                 'folderFullPath="Root/Elsewhere" folderName="Elsewhere" '
                 'folderId="folder-elsewhere"/>')

    def _component(*_a, **_k):
        return {"_success": True, "component_id": "cid-x"}

    def _live_xml_for(_client, component_id, *_a, **_k):
        if component_id == "existing-proc":
            return {"type": "process", "xml": elsewhere}
        return {"type": "connector-settings", "xml": _LIVE_COMPONENT_XML}

    def _update(_client, _profile, _target, _comp, xml, _policy):
        return {"_success": True, "component_id": "existing-proc",
                "submitted_xml": xml}

    with patch("boomi_mcp.categories.folders._query_all_folders",
               return_value=folders), \
         patch(_PAGINATE, return_value=[]), patch(
        _EXECUTE, side_effect=_component
    ), patch(_GET_XML, side_effect=_live_xml_for), patch(
        "boomi_mcp.categories.integration_builder._apply_structured_update",
        side_effect=_update,
    ):
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": _bound_payload(
                process_ir_request(units=(unit,))
            ), "dry_run": False},
        )

    assert result["_success"] is True, result.get("error")
    step = result["results"]["proc"]
    # The envelope now SAYS what happened to the request.
    assert step["requested_folder_name"] == "Target Folder"
    assert step["placement_verified"] is False
    assert step["observed_folder"] == "Root/Elsewhere"
    warning = [w for w in (result.get("warnings") or [])
               if "requested folder" in w]
    assert warning, result.get("warnings")
    # ...and it names the UPDATE mechanism, not the create one.
    assert "update preservation" in warning[0], warning[0]
    assert "ignores folderName on create" not in warning[0], warning[0]


def test_a_confirmed_write_is_durable_before_it_is_attested():
    """§6 AR3-03: the durable record lost a component that provably exists.

    The mutation attestation is appended by the apply loop AFTER the step
    function returns. Everything between the platform's confirmation and that
    return — the readback, the placement identity, the attestation construction
    itself — happens with the component already created, so an exception in any
    of it left the durable row `failed_partial` with an EMPTY attestation list
    and no trace of the write. The note now lands the instant the platform
    confirms, carrying what is known then: the key, the platform's own id, and
    the digest of the bytes that were sent. It is deliberately not an
    attestation — recording less, earlier, is the point.
    """
    from boomi_mcp.categories import integration_builder as ib
    from boomi_mcp.categories.components import canonical_process_apply as cpa

    ib._BUILD_REGISTRY.clear()

    def _component(*_a, **_k):
        return {"_success": True, "component_id": "dep-1"}

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    def _boom(*_a, **_k):
        raise RuntimeError("attestation construction failed after the write")

    _SUBMITTED.clear()
    with patch(_PAGINATE, return_value=[]), patch(
        _EXECUTE, side_effect=_component
    ), patch(_CREATE, side_effect=_create), patch(
        _GET_XML, side_effect=_live_xml
    ), patch.object(cpa, "build_mutation_attestation", side_effect=_boom):
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": _bound_payload(), "dry_run": False},
        )

    assert result["_success"] is False
    assert result["mutation_status"] == "possible"
    build_id = result.get("build_id")
    assert build_id, result
    # The attestation genuinely never got built...
    assert not result.get("process_mutations")
    # ...and the write is recorded anyway, durably and on the envelope.
    writes = ib._BUILD_REGISTRY[build_id].get("process_writes")
    assert writes, ib._BUILD_REGISTRY[build_id]
    assert writes[0]["result_component_id"] == _PROCESS_ID
    assert writes[0]["component_key"] == "proc"
    assert writes[0]["submitted_xml_digest"].startswith("sha256:")
    assert result.get("process_writes") == writes


def test_the_integration_spec_arm_also_reparses_its_units():
    """§6 AR3-01: the AR2-01 sweep covered one mechanism, not one question.

    It asked "which dumps render caller values?" and fixed those. It never
    asked "which arms hand the compiler a caller-owned nested IR?" — and this
    one did: the spec was returned verbatim, so a caller mutating a unit's IR
    after building the request got a raw pydantic ValidationError out of the
    semantic validator's snapshot, carrying the mutated value.
    """
    import warnings as _warnings

    from boomi_mcp.authoring.workflow import (
        AuthoringWorkflowError, plan_authoring_request_v1,
    )
    from boomi_mcp.models.authoring_workflow import (
        AuthoringRequestV1, IntegrationSpecAuthoringIntentV1,
    )
    from boomi_mcp.models.integration_models import IntegrationSpecV1

    unit = process_unit()
    spec = IntegrationSpecV1(
        name="M12.15 Integration",
        components=[APPLIABLE_CONN, APPLIABLE_OP],
        processes=(unit,),
    )
    request = AuthoringRequestV1(
        intent=IntegrationSpecAuthoringIntentV1(integration_spec=spec)
    )
    # Mutate the model the CALLER still holds, after the request is built.
    object.__setattr__(request.intent.integration_spec.processes[0].process_ir,
                       "body", {"password": "hunter2-S3cret-AR3SPEC"})

    with _warnings.catch_warnings(record=True) as caught:
        _warnings.simplefilter("always")
        with patch(_PAGINATE, return_value=[]):
            try:
                result = plan_authoring_request_v1(
                    request, boomi_client=MagicMock(), profile=_PROFILE
                )
                served, raised = repr(result), None
            except Exception as exc:
                served, raised = repr(exc), exc

    assert [w for w in caught if "hunter2" in str(w.message)] == []
    assert "hunter2" not in served, "the served refusal rendered the mutated value"
    if raised is not None:
        assert isinstance(raised, AuthoringWorkflowError), type(raised).__name__


def test_a_bad_reference_type_refuses_before_any_dependency_is_written():
    """§6 AR3-02: relocatability was pre-decided; the rest of the plan was not.

    The AR2-02 pass closed ONE request-decidable question. The compiler decides
    many more — here a step referencing a component of the wrong type — and on
    the raw route none of them was asked until the root's execution turn, which
    topological order runs after its dependencies exist. A probe measured both
    supporting components written, then the process failing with the generic
    internal-error code. The plan is now built in the pre-write pass and CACHED,
    so the refusal comes first and the compiler's own code and path travel.
    """
    unit = process_unit()
    ir = unit.process_ir.model_dump(mode="json")
    bearing = [st for st in ir["body"]["steps"] if "operation_ref" in st]
    assert bearing, ir["body"]["steps"]
    # Point the OPERATION reference at the connection component: a real key, the
    # wrong type — decidable from the request, nothing to do with the platform.
    bearing[0]["operation_ref"] = "$ref:conn"
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    swapped = unit.model_copy(update={"process_ir": parse_process_ir_v1(ir)})
    spec = {
        "name": "M12.15 Integration",
        "components": [APPLIABLE_CONN, APPLIABLE_OP],
        "processes": [swapped.model_dump(mode="json")],
    }

    writes = {"n": 0}

    def _component(*_a, **_k):
        writes["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % writes["n"]}

    with patch(_PAGINATE, return_value=[]), patch(
        _EXECUTE, side_effect=_component
    ), patch(_CREATE) as create, patch(_GET_XML, side_effect=_live_xml):
        create.side_effect = AssertionError("no process may be created")
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"integration_spec": spec, "dry_run": False},
        )

    assert result["_success"] is False, result
    # The whole point: nothing was written for an answer the request settles.
    assert writes["n"] == 0, "dependencies were created before the refusal"
    assert result["partial_results"] == {}
    from boomi_mcp.categories.integration_builder import _mutation_status
    assert _mutation_status(result) == "none"
    # ...and the COMPILER's own code travels, not the generic server-fault one.
    assert result["error_code"] != "PROCESS_MATERIALIZATION_INTERNAL_ERROR", result
    assert result["error_code"].startswith("PROCESS_IR_"), result["error_code"]


def test_the_pre_write_plan_is_consumed_not_rebuilt():
    """§6 AR3-02, the cost half: moving the compile must not duplicate it.

    The pre-write pass builds the raw route's plan so a request-decidable
    failure refuses before any write. If the execution turn then rebuilt it,
    every apply would pay twice. The built plan is cached and consumed.
    """
    import boomi_mcp.authoring.process_materialization as pm

    calls = {"n": 0}
    real = pm.build_materialization_plan

    def _spy(*args, **kwargs):
        calls["n"] += 1
        return real(*args, **kwargs)

    spec = {
        "name": "M12.15 Integration",
        "components": [APPLIABLE_CONN, APPLIABLE_OP],
        "processes": [process_unit().model_dump(mode="json")],
    }

    def _component(*_a, **_k):
        return {"_success": True, "component_id": "dep-1"}

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    _SUBMITTED.clear()
    with patch(_PAGINATE, return_value=[]), patch(
        _EXECUTE, side_effect=_component
    ), patch(_CREATE, side_effect=_create), patch(
        _GET_XML, side_effect=_live_xml
    ), patch.object(pm, "build_materialization_plan", _spy):
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"integration_spec": spec, "dry_run": False},
        )

    assert result["_success"] is True, result.get("error")
    assert calls["n"] == 1, (
        "the raw route built its plan %d times; the pre-write build must be "
        "consumed by the execution turn, not repeated" % calls["n"]
    )


def test_a_process_only_plan_is_not_called_empty():
    """§6 AR3-06: a canonical root IS an executable step.

    The emptiness warning read `spec.components` alone — the whole participant
    universe before #153 added `processes` — so a spec authoring a process and
    no supporting components was told its plan had zero executable steps while
    the planner was busy building a step for it. Same participant-universe
    question DC-2 covers, on the warning surface.

    Reaching it takes care: a ProcessIR sequence must start with a connector
    source, so a component-free root is only representable when its references
    are LITERAL ids — which the dependency check does not refuse (it checks
    `$ref:` declarations) and `plan` reports rather than refuses. The first
    draft of this witness used `$ref` and was refused earlier, making it
    vacuous.
    """
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    unit = process_unit(depends_on=())
    ir = unit.process_ir.model_dump(mode="json")
    for step in ir["body"]["steps"]:
        if "connection_ref" in step:
            step["connection_ref"] = "35813b90-1f42-4dcb-98f5-82d8f96be61d"
            step["operation_ref"] = "45813b90-1f42-4dcb-98f5-82d8f96be61e"
    literal = unit.model_copy(update={"process_ir": parse_process_ir_v1(ir)})
    spec = {
        "name": "M12.15 Integration",
        "components": [],
        "processes": [literal.model_dump(mode="json")],
    }
    with patch(_PAGINATE, return_value=[]), patch(_GET_XML, side_effect=_live_xml):
        result = build_integration_action(
            MagicMock(), _PROFILE, "plan", config={"integration_spec": spec}
        )

    assert result["_success"] is True, result.get("error")
    assert [w for w in (result.get("warnings") or [])
            if "literal component id" in w], result.get("warnings")
    # The step exists, so the plan is NOT empty.
    assert [w for w in (result.get("warnings") or [])
            if "zero executable steps" in w] == [], result.get("warnings")

    # THE CONTROL: a spec with neither components nor processes IS empty, and
    # still says so.
    with patch(_PAGINATE, return_value=[]), patch(_GET_XML, side_effect=_live_xml):
        bare = build_integration_action(
            MagicMock(), _PROFILE, "plan",
            config={"integration_spec": {"name": "Empty", "components": []}},
        )
    assert [w for w in (bare.get("warnings") or [])
            if "zero executable steps" in w], bare.get("warnings")


def test_the_compiler_revision_covers_the_execution_profile_derivation():
    """§6 AR3-07, and then AR4-01 when the first fix proved insufficient.

    `compiler_revision` is what a caller binds to, and it is deliberately a
    manifest of published contracts rather than a source hash. The
    execution-profile derivation — which decides whether a process is scheduled
    or listener — was absent from it, so a stale binding kept validating across a
    change to that rule.

    The first fix projected the derivation's VOCABULARY: the two profile labels
    and the listener family set. This test's first version widened the family set
    and asserted the revision moved, which it did — and proved nothing about the
    rule, because the family table is exactly what a vocabulary projection reads.
    The §6 gate then measured the case that mattered: replacing
    `derive_process_execution_profile` with an always-scheduled implementation
    left the served revision byte-identical.

    So the projection now CALLS the rule over a derived case set, and this test
    asserts the property the earlier one only appeared to: a change in
    CLASSIFICATION moves the revision, and a change that classifies identically
    does not.
    """
    import boomi_mcp.compiler.process_ir.execution_profile as ep
    from boomi_mcp.authoring.contract import (
        _compiler_revision,
        _execution_profile_behaviour_oracle,
    )

    baseline = _compiler_revision()
    oracle = _execution_profile_behaviour_oracle()

    # NON-DEGENERACY, before either mutation. A case set that classified
    # everything the same way, or that lost its per-family rows, would move under
    # the mutant below for the wrong reason.
    assert oracle != "unavailable" and oracle["cases"], oracle
    assert set(oracle["cases"].values()) == {"scheduled", "listener"}, oracle
    from boomi_mcp.authoring.contract import _connector_member, _literal_options

    connector_roles = _literal_options(_connector_member(), "role")
    assert len(connector_roles) >= 2, connector_roles
    for role in connector_roles:
        rows = {k for k in oracle["cases"]
                if k.startswith("connector-%s-" % role)
                and k[len("connector-%s-" % role):] in ep.LISTENER_CONNECTOR_TYPES}
        assert len(rows) == len(ep.LISTENER_CONNECTOR_TYPES), (role, sorted(rows))
    # ...and the two roles disagree, or the role test is not covered at all.
    listener_role, other_role = connector_roles[0], connector_roles[1]
    family = sorted(ep.LISTENER_CONNECTOR_TYPES)[0]
    assert oracle["cases"]["connector-%s-%s" % (listener_role, family)] == "listener"
    assert oracle["cases"]["connector-%s-%s" % (other_role, family)] == "scheduled"

    # THE THREE DISCRIMINANTS MUST BE SCHEDULED FOR THEIR OWN REASON. They all
    # report "scheduled", and so does a connector row in the non-listener role —
    # so a discriminant carrying the wrong role is scheduled for the WRONG reason
    # and probes nothing, while looking identical in the output. That is not a
    # hypothetical: pointing the discriminants at the other role left every
    # assertion above green when this check was absent.
    #
    # The role is now ASKED OF THE RULE rather than read off declaration order
    # (L2 round 45), so the check is that the rule's answer really is the
    # listener-yielding role — with the classifier passed in the same shape the
    # oracle passes it, so this cannot pass against a different question.
    from boomi_mcp.authoring.contract import _listener_role

    def _classify_role(role):
        return oracle["cases"].get("connector-%s-%s" % (role, family))

    chosen = _listener_role(_classify_role, sorted(ep.LISTENER_CONNECTOR_TYPES))
    assert oracle["cases"]["connector-%s-%s" % (chosen, family)] == "listener", chosen

    # ...and it must still be the right role when the schema's declaration order
    # is REVERSED, which is the only condition under which asking the rule and
    # reading the first option differ. Without this the fix is untested: `source`
    # happens to be declared first, so a version that reads declaration order
    # gives the same answer on today's schema.
    reversed_first = tuple(reversed(connector_roles))[0]
    assert reversed_first != chosen, connector_roles

    def _classify_reversed(role):
        return oracle["cases"].get("connector-%s-%s" % (role, family))

    from unittest.mock import patch as _patch

    with _patch(
        "boomi_mcp.authoring.contract._literal_options",
        side_effect=lambda model, field: tuple(reversed(connector_roles))
        if field == "role"
        else _literal_options(model, field),
    ):
        assert _listener_role(
            _classify_reversed, sorted(ep.LISTENER_CONNECTOR_TYPES)
        ) == chosen, "the listener role is being read off declaration order"

    # ...and EVERY entry kind the schema admits is exercised, derived from the
    # schema on both sides so the check cannot pass by agreeing with itself
    # (L2 round 43: the case set stood one `message` node in for "any
    # non-connector entry", which made the whole connector_call family
    # invisible). `connector` is the one kind absent from these rows — it is
    # covered per family, per role, above.
    from boomi_mcp.authoring.contract import _cfg_semantic_members

    schema_kinds = {
        member.model_fields["semantic_kind"].default
        for member in _cfg_semantic_members()
    }
    assert len(schema_kinds) > 10, sorted(schema_kinds)
    exercised = {k[len("entry-kind-"):].split("-role-")[0].removesuffix(
        "-listener-shaped") for k in oracle["cases"] if k.startswith("entry-kind-")}
    assert exercised == schema_kinds - {"connector"}, sorted(
        (schema_kinds - {"connector"}) ^ exercised
    )

    # ...and every kind that declares a ROLE is probed with each role its OWN
    # schema admits (L2 round 44). The previous version stamped `role="source"`
    # on all of them, which for `connector_call` — whose schema admits only
    # `entry|downstream` — meant the kind most likely to become listener-eligible
    # was probed with a role it can never carry. Both sides derive from the
    # schema, so the check cannot pass by agreeing with the oracle's own copy.
    # ...and probed with the roles that are legal AT THE ENTRY POSITION, which is
    # narrower than what the field admits (L2 round 45): `_classify` installs the
    # probed node as the entry, and `check_cfg_invariants` rejects a
    # `connector_call` entry in any role but `entry`. Probing the illegal one
    # made the served revision rotate for a shape production cannot produce.
    # Asserted in BOTH directions — every legal role present, every illegal one
    # absent — because "all declared roles present" was the previous version of
    # this assertion and it is exactly what round 45 found wrong.
    from boomi_mcp.authoring.contract import _literal_options
    from boomi_mcp.compiler.process_ir.invariants import ENTRY_ROLE_RESTRICTIONS

    def _connector_call_member():
        return next(m for m in _cfg_semantic_members()
                    if m.model_fields["semantic_kind"].default == "connector_call")

    for member in _cfg_semantic_members():
        kind = member.model_fields["semantic_kind"].default
        if kind == "connector":
            continue
        # The expectation comes from `ENTRY_ROLE_RESTRICTIONS` — the AUTHORITY —
        # not from `_entry_roles`, the consumer being checked. Deriving it from
        # the consumer made this assertion agree with itself: a mutant that
        # ignored the restriction entirely widened both sides and passed.
        declared = _literal_options(member, "role")
        allowed = ENTRY_ROLE_RESTRICTIONS.get(kind)
        legal = set(declared if allowed is None else allowed)
        for role in declared:
            key = "entry-kind-%s-role-%s" % (kind, role)
            assert (key in oracle["cases"]) == (role in legal), (kind, role, legal)

    # 1. A BEHAVIOUR mutant — the reviewer's own: a derivation that classifies
    #    every graph as scheduled. This is the case the vocabulary projection
    #    could not see.
    original_derive = ep.derive_process_execution_profile
    try:
        ep.derive_process_execution_profile = lambda cfg, symbols: ep.SCHEDULED
        always_scheduled = _compiler_revision()
    finally:
        ep.derive_process_execution_profile = original_derive
    assert always_scheduled != baseline, (
        "replacing the derivation with an always-scheduled rule left the served "
        "compiler revision unchanged — the revision covers the vocabulary, not "
        "the behaviour"
    )
    assert _compiler_revision() == baseline

    # 2. An EQUIVALENCE wrapper — same classifications, different code. The
    #    revision must NOT move, or it is hashing identity rather than behaviour
    #    and every unrelated refactor would break a caller's binding.
    try:
        ep.derive_process_execution_profile = (
            lambda cfg, symbols: original_derive(cfg, symbols)
        )
        wrapped = _compiler_revision()
    finally:
        ep.derive_process_execution_profile = original_derive
    assert wrapped == baseline, (
        "a behaviour-preserving wrapper moved the served revision"
    )

    # 3. THE REGRESSION THE SCHEMA CAN ACTUALLY PRODUCE: a derivation that
    #    starts classifying listener-family `connector_call` entries.
    #
    #    L2 round 43 raised this and its mutant keyed on `role == "source"`,
    #    which round 44 then showed `ConnectorCallSemanticV1` does not admit —
    #    its roles are `entry|downstream`. That mutant is WITHDRAWN rather than
    #    kept beside this one: a control asserting the revision moves for a node
    #    the schema cannot construct would be claiming coverage of an impossible
    #    case, which is worse than no control. Both admitted roles are asserted —
    #    `entry` is the one a listener would plausibly become, and `downstream`
    #    proves the coverage is not one lucky value.
    def _valid_role_listener(valid_role):
        def _rule(cfg, symbols):
            entry = ep._entry_node(cfg)
            if entry is None:
                return ep.SCHEDULED
            semantic = entry.semantic
            if (
                getattr(semantic, "semantic_kind", None) == "connector_call"
                and getattr(semantic, "role", None) == valid_role
            ):
                family = ep._operation_connector_family(
                    symbols, getattr(semantic, "operation_ref", "")
                )
                if family and family in ep.LISTENER_CONNECTOR_TYPES:
                    return ep.LISTENER
                return ep.SCHEDULED
            return original_derive(cfg, symbols)

        return _rule

    # BOTH DIRECTIONS, and they differ — which is the whole content of round 45.
    # A regression on the role that CAN be the entry must move the revision; one
    # confined to the role that cannot must NOT, because rotating a caller's
    # binding for a graph the compiler rejects is a cost with no behaviour behind
    # it. Round 44's version of this control asserted `downstream` moved it too,
    # which is what round 45 found wrong.
    entry_legal = set(ENTRY_ROLE_RESTRICTIONS["connector_call"])
    for role in _literal_options(_connector_call_member(), "role"):
        try:
            ep.derive_process_execution_profile = _valid_role_listener(role)
            probed = _compiler_revision()
        finally:
            ep.derive_process_execution_profile = original_derive
        assert (probed != baseline) == (role in entry_legal), (role, entry_legal)
        assert _compiler_revision() == baseline

    # 4. The family table is still covered — the property the first fix had.
    original_families = ep.LISTENER_CONNECTOR_TYPES
    try:
        ep.LISTENER_CONNECTOR_TYPES = frozenset(set(original_families) | {"zzz-probe"})
        widened = _compiler_revision()
    finally:
        ep.LISTENER_CONNECTOR_TYPES = original_families
    assert widened != baseline
    assert _compiler_revision() == baseline


def test_the_materializer_revision_covers_the_preservation_policy():
    """§6 AR3-10: the materializer revision omitted the preservation authority.

    It hashes the option sets and the XML layouts this materializer emits, and
    the plan mandates the preservation policy alongside them — the policy
    decides what an update keeps, which is materialization behaviour by any
    reading. Taken as the projection's own canonical text so it cannot become
    another hand-model of the runtime policy.
    """
    import dataclasses

    import boomi_mcp.categories.components.builders._process_preservation as pres
    from boomi_mcp.categories.integration_builder import _materializer_revision

    baseline = _materializer_revision()
    original = pres.PROCESS_PRESERVATION_POLICY
    try:
        # A MATERIALLY different policy — not `mode="replace"`, which is the
        # default and would be an inert mutant.
        pres.PROCESS_PRESERVATION_POLICY = dataclasses.replace(
            original, owned_root_attrs=tuple(original.owned_root_attrs) + ("zzz",)
        )
        moved = _materializer_revision()
    finally:
        pres.PROCESS_PRESERVATION_POLICY = original

    assert moved != baseline, (
        "changing the preservation authority left the materializer revision "
        "unchanged — the policy is not covered"
    )
    assert _materializer_revision() == baseline


def test_the_served_workflow_contract_names_what_this_slice_added():
    """§6 AR3-05: the served contract described a pipeline the server outgrew.

    `authoring_workflow_contract()` is machine-facing — it is what an agent
    reads to learn the phases and the vocabulary — and it carried no mention of
    canonical process units, the relocatable plan they compile to, the late
    binding that turns its placeholders into real ids, or the two separate
    attestations an apply returns. Every one of those is a promise this slice
    made and serves.

    The expected terms are derived from the served MODELS, not hand-typed, so
    renaming an attestation model fails this rather than leaving stale prose.
    """
    from boomi_mcp.authoring.contract import authoring_workflow_contract
    from boomi_mcp.models.authoring_workflow import (
        ProcessLiveReadbackAttestationV1, ProcessMutationAttestationV1,
    )

    contract = authoring_workflow_contract()
    terminology = contract["terminology"]

    for key in ("process_units", "materialization_plan", "late_binding",
                "mutation_attestation", "readback_attestation"):
        assert key in terminology, sorted(terminology)
        assert terminology[key].strip(), key

    # The attestation entries name their own models, so a rename cannot leave
    # the served vocabulary describing something that no longer exists.
    assert ProcessMutationAttestationV1.__name__ in terminology[
        "mutation_attestation"
    ]
    assert ProcessLiveReadbackAttestationV1.__name__ in terminology[
        "readback_attestation"
    ]

    # ...and the phases themselves say what compile and apply now do.
    phases = {p["step"]: p["purpose"] for p in contract["phases"]}
    assert "relocatable" in phases[6]
    assert "placeholder" in phases[6]
    assert "bound" in phases[7].lower() and "attestation" in phases[7]


def test_the_raw_route_also_records_a_confirmed_write():
    """QA-153-r15-01: the write note was gated on the durable build record.

    That record is minted for TYPED builds only — deliberately, so a legacy
    build keeps its original five-key shape — and gating the note on it meant
    the raw `integration_spec` route, which creates process components just as
    well, left an escaped component in no served field and no record at all.
    QA measured two such components live: real, readback-verified, and named
    nowhere in the envelope the caller gets back.
    """
    from boomi_mcp.categories.components import canonical_process_apply as cpa

    spec = {
        "name": "M12.15 Integration",
        "components": [APPLIABLE_CONN, APPLIABLE_OP],
        "processes": [process_unit().model_dump(mode="json")],
    }

    def _component(*_a, **_k):
        return {"_success": True, "component_id": "dep-1"}

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    def _boom(*_a, **_k):
        raise RuntimeError("attestation construction failed after the write")

    _SUBMITTED.clear()
    with patch(_PAGINATE, return_value=[]), patch(
        _EXECUTE, side_effect=_component
    ), patch(_CREATE, side_effect=_create), patch(
        _GET_XML, side_effect=_live_xml
    ), patch.object(cpa, "build_mutation_attestation", side_effect=_boom):
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"integration_spec": spec, "dry_run": False},
        )

    assert result["_success"] is False
    # No durable record on this route by design...
    assert not result.get("build_id")
    # ...and the write is reported anyway, which is the whole point: this is
    # the only place the caller learns the component exists.
    writes = result.get("process_writes")
    assert writes, result
    assert writes[0]["result_component_id"] == _PROCESS_ID
    assert writes[0]["component_key"] == "proc"


def test_a_reference_of_the_wrong_kind_is_refused_before_any_write():
    """QA-153-r15-02: the slot's expected types were derived circularly.

    `expected_component_types` recorded whatever the key resolved to, so it
    could never disagree with anything — and the MIRROR swap (a
    `connection_ref` naming the operation) passed the pre-write pass, wrote
    both dependencies, and failed at bind time under the generic
    materialization internal error, which AR3-02 had set out to retire. The
    types now come from the reference's ROLE, using the compiler module that
    already owns them, and the check fires inside the plan build — which is
    inside the pre-write pass.
    """
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    def _swapped(field, value):
        unit = process_unit()
        ir = unit.process_ir.model_dump(mode="json")
        for step in ir["body"]["steps"]:
            if field in step:
                step[field] = value
        return unit.model_copy(update={"process_ir": parse_process_ir_v1(ir)})

    for field, value in (("connection_ref", "$ref:op"),
                         ("operation_ref", "$ref:conn")):
        spec = {
            "name": "M12.15 Integration",
            "components": [APPLIABLE_CONN, APPLIABLE_OP],
            "processes": [_swapped(field, value).model_dump(mode="json")],
        }
        writes = {"n": 0}

        def _component(*_a, **_k):
            writes["n"] += 1
            return {"_success": True, "component_id": "cid-%d" % writes["n"]}

        with patch(_PAGINATE, return_value=[]), patch(
            _EXECUTE, side_effect=_component
        ), patch(_CREATE) as create, patch(_GET_XML, side_effect=_live_xml):
            create.side_effect = AssertionError("no process may be created")
            result = build_integration_action(
                MagicMock(), _PROFILE, "apply",
                config={"integration_spec": spec, "dry_run": False},
            )

        assert result["_success"] is False, (field, result)
        assert writes["n"] == 0, (
            "%s: dependencies were created before the refusal" % field
        )
        assert result["partial_results"] == {}
        assert result["error_code"] != "PROCESS_MATERIALIZATION_INTERNAL_ERROR", (
            field, result["error_code"],
        )

    # THE CONTROL: the correctly-typed pair still applies.
    good = {
        "name": "M12.15 Integration",
        "components": [APPLIABLE_CONN, APPLIABLE_OP],
        "processes": [process_unit().model_dump(mode="json")],
    }

    def _ok(*_a, **_k):
        return {"_success": True, "component_id": "dep-1"}

    def _create_ok(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": _PROCESS_ID}

    _SUBMITTED.clear()
    with patch(_PAGINATE, return_value=[]), patch(
        _EXECUTE, side_effect=_ok
    ), patch(_CREATE, side_effect=_create_ok), patch(
        _GET_XML, side_effect=_live_xml
    ):
        applied = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"integration_spec": good, "dry_run": False},
        )
    assert applied["_success"] is True, applied.get("error")


def test_the_dry_emit_binds_symbols_the_ir_never_names():
    """Codex round 29: the dry registry was built from the recorded slots.

    A first-class `connector_call` records only its `operation_ref` in the IR —
    its connection is DERIVED from the operation symbol and therefore never
    appears in `unresolved_symbol_slots`. A stand-in registry built from the
    slots alone left that connection on its placeholder, so the pre-write dry
    emit refused a request the real apply handles correctly: every valid
    connector_call process would have become unappliable. Every symbol in the
    table gets a stand-in instead.

    The e2e fixtures use `source`/`send` steps, which name both refs — which is
    exactly why the first version of the dry emit passed its own tests and
    would have broken this shape.
    """
    from boomi_mcp.authoring.process_materialization import (
        build_materialization_plan,
    )
    from boomi_mcp.categories.integration_builder import _dry_emit_canonical_plan
    from boomi_mcp.compiler.process_ir.contracts import (
        ComponentSymbolV1, SymbolTableV1,
    )
    from boomi_mcp.models.process_component import ProcessComponentEnvelopeV1
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    symbols = SymbolTableV1(symbols=(
        ComponentSymbolV1(
            ref="$ref:conn", component_id="id-conn",
            component_type="connector-settings", connector_type="rest",
        ),
        ComponentSymbolV1(
            ref="$ref:op", component_id="id-op",
            component_type="connector-action", connector_type="rest",
            action_type="GET", connection_ref="$ref:conn",
        ),
    ))
    plan = build_materialization_plan(
        envelope=ProcessComponentEnvelopeV1(
            component_key="proc", name="P", action="create",
            # DECLARED, as a valid request must: ordered apply creates these
            # first, which is what makes binding them in the dry run faithful
            # rather than optimistic (Codex round 30). The first version of
            # this witness declared nothing, so it asserted that the dry emit
            # binds symbols apply would NOT have — a premise that hid the
            # ordering defect round 30 found.
            depends_on=("conn", "op"),
        ),
        process_ir=parse_process_ir_v1({
            "version": "1",
            "body": {"kind": "sequence", "steps": [
                {"kind": "connector_call", "operation_ref": "$ref:op"},
                {"kind": "return_documents"},
            ]},
        }),
        symbols=symbols, conflict_policy="reuse",
        compiler_revision="sha256:" + "a" * 64,
        emitter_revision="sha256:" + "b" * 64,
        materializer_revision="sha256:" + "c" * 64,
    )

    # The premise: the connection is NOT a recorded slot — the IR never names
    # it, so a registry built from the slots alone would leave it unbound.
    slot_refs = {slot.ref for slot in plan.unresolved_symbol_slots}
    assert slot_refs == {"$ref:op"}, slot_refs

    # ...and the dry emit binds it from the DECLARED dependencies, so the
    # request stays appliable.
    _dry_emit_canonical_plan(plan, symbols, {"proc": ("conn", "op")})

    # ...INCLUDING when the root declares only the operation and the OPERATION
    # declares the connection (Codex round 31): ordered apply walks that edge
    # too, so the closure must be transitive or a valid request is refused.
    _dry_emit_canonical_plan(
        plan, symbols, {"proc": ("op",), "op": ("conn",)}
    )


def test_an_undeclared_derived_dependency_refuses_before_the_first_write():
    """Codex round 30: the all-symbol dry registry MASKED an ordering defect.

    A connector action's connection is derived from the operation symbol, so a
    root can reference the operation while declaring neither — and topological
    order may then place the process before the connection. Binding every
    symbol in the dry run made that request preview cleanly and fail during the
    real materialization, after earlier independent creates had already landed.
    Binding only the DECLARED closure makes the preview faithful in both
    directions: it accepts what apply accepts, and it refuses this before the
    first write.
    """
    from boomi_mcp.authoring.process_materialization import (
        build_materialization_plan,
    )
    from boomi_mcp.categories.components.canonical_process_apply import (
        CanonicalProcessApplyError,
    )
    from boomi_mcp.categories.integration_builder import _dry_emit_canonical_plan
    from boomi_mcp.compiler.process_ir.contracts import (
        ComponentSymbolV1, SymbolTableV1,
    )
    from boomi_mcp.models.process_component import ProcessComponentEnvelopeV1
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    symbols = SymbolTableV1(symbols=(
        ComponentSymbolV1(
            ref="$ref:conn", component_id="id-conn",
            component_type="connector-settings", connector_type="rest",
        ),
        ComponentSymbolV1(
            ref="$ref:op", component_id="id-op",
            component_type="connector-action", connector_type="rest",
            action_type="GET", connection_ref="$ref:conn",
        ),
    ))
    ir = parse_process_ir_v1({
        "version": "1",
        "body": {"kind": "sequence", "steps": [
            {"kind": "connector_call", "operation_ref": "$ref:op"},
            {"kind": "return_documents"},
        ]},
    })

    def _plan(depends_on):
        return build_materialization_plan(
            envelope=ProcessComponentEnvelopeV1(
                component_key="proc", name="P", action="create",
                depends_on=depends_on,
            ),
            process_ir=ir, symbols=symbols, conflict_policy="reuse",
            compiler_revision="sha256:" + "a" * 64,
            emitter_revision="sha256:" + "b" * 64,
            materializer_revision="sha256:" + "c" * 64,
        )

    # The connection is derived and UNDECLARED: nothing guarantees it precedes
    # this root, so the preview must refuse rather than assume.
    # Nothing declares the connection ANYWHERE in the graph, so nothing
    # guarantees it precedes this root.
    with pytest.raises(CanonicalProcessApplyError):
        _dry_emit_canonical_plan(_plan(("op",)), symbols, {"proc": ("op",)})

    # THE CONTROLS: declared directly, and declared transitively by the
    # operation — ordered apply guarantees it either way.
    _dry_emit_canonical_plan(
        _plan(("conn", "op")), symbols, {"proc": ("conn", "op")}
    )
    _dry_emit_canonical_plan(
        _plan(("op",)), symbols, {"proc": ("op",), "op": ("conn",)}
    )


def test_a_confirmed_write_with_no_id_is_recorded_as_unidentified():
    """§6 evaluation 4: the note's condition was hand-enumerated.

    It additionally required a component id, so a create the platform CONFIRMED
    without returning one — the single case where a reconciler most needs a
    pointer — was the only case that produced no note at all. Second instance of
    the QA-153-r15-01 pair, so the fix is at the condition: the note fires on
    what the platform confirmed, read once, and an unidentified write is
    recorded AS unidentified.
    """
    from boomi_mcp.categories import integration_builder as ib

    ib._BUILD_REGISTRY.clear()

    def _component(*_a, **_k):
        return {"_success": True, "component_id": "dep-1"}

    def _create(_client, _profile, payload_in):
        _SUBMITTED["xml"] = payload_in["xml"]
        # Exactly what a componentId-less 200 yields.
        return {"_success": True, "component_id": ""}

    _SUBMITTED.clear()
    with patch(_PAGINATE, return_value=[]), patch(
        _EXECUTE, side_effect=_component
    ), patch(_CREATE, side_effect=_create), patch(_GET_XML, side_effect=_live_xml):
        result = build_integration_action(
            MagicMock(), _PROFILE, "apply",
            config={"authoring_request": _bound_payload(), "dry_run": False},
        )

    assert result["_success"] is False, result
    writes = result.get("process_writes")
    assert writes, result
    note = writes[0]
    assert note["component_key"] == "proc"
    assert note["result_component_id"] is None
    assert note["result_component_id_missing"] is True
    assert note["submitted_xml_digest"].startswith("sha256:")
    # ...and it is durable, not only served.
    build_id = result.get("build_id")
    assert build_id and ib._BUILD_REGISTRY[build_id].get("process_writes") == writes


def test_no_write_note_is_recorded_for_a_step_that_never_wrote():
    """The OTHER direction, and it has to reach the write site to mean anything.

    Firing on "the platform confirmed" is only correct if a step that REACHED
    the platform and was refused produces nothing. The first attempt at this
    control used a folder refusal, which never enters the write site at all —
    so it passed against a mutant that fired the note unconditionally, and was
    worth exactly nothing. This one drives the create to a refusal
    (`write_attempted: False`, the discriminator the write issuer stamps) and
    asserts silence there.
    """
    from boomi_mcp.categories import integration_builder as ib

    ib._BUILD_REGISTRY.clear()

    def _refused(_client, _profile, _payload):
        return {"_success": False, "write_attempted": False,
                "error": "refused before the push", "error_code": "REFUSED"}

    result = _apply(_bound_payload(), create_result=_refused)

    assert result["_success"] is False
    assert not result.get("process_writes"), result.get("process_writes")
    # ...and the step really did reach the write site and get refused there.
    proc = (result.get("partial_results") or {}).get("proc") or {}
    assert proc.get("status") == "refused", proc


def test_a_successful_apply_serves_no_write_notes_and_would_serve_an_uncovered_one():
    """The third exit, both directions.

    A successful apply attests every write, so the derived subset is empty and
    the envelope does not grow a field — that is the drift control. The other
    direction is what makes the derivation worth having: hand the finalizer a
    note no attestation covers and it is served, so the "reaching here means
    attested" claim can never quietly stop being true.
    """
    from boomi_mcp.categories import integration_builder as ib

    ib._BUILD_REGISTRY.clear()
    applied = _apply(_bound_payload())
    assert applied["_success"] is True, applied
    assert applied.get("process_mutations"), applied
    # Covered: nothing extra is served.
    assert "process_writes" not in applied

    attested_key = applied["process_mutations"][0]["component_key"]
    covered = {"component_key": attested_key, "action": "create",
               "result_component_id": "x", "attestation_pending": True}
    uncovered = {"component_key": "a-key-no-attestation-covers",
                 "action": "create", "result_component_id": None,
                 "result_component_id_missing": True,
                 "attestation_pending": True}

    def _finalize(writes):
        return ib._finalize_apply_success(
            spec=SimpleNamespace(name="s", model_dump=lambda: {}),
            profile=_PROFILE, boomi_client=MagicMock(), durable_build_id=None,
            authoring_bundle=None, results={}, execution_order=[],
            process_mutations=[
                SimpleNamespace(model_dump=lambda **_k: {"component_key": attested_key})
            ],
            process_readbacks=[], process_writes=writes,
            apply_warnings=[], planned={},
        )

    assert "process_writes" not in _finalize([covered])
    served = _finalize([covered, uncovered])["process_writes"]
    assert served == [uncovered]


def _plan_for_named_code_probe():
    """A real materialization plan, taken from a real compile.

    Built through `compile_authoring_request_v1` rather than assembled here: a
    plan I hand-build could disagree with what production produces, and the
    point of this probe is what a CALLER's refusal looks like.
    """
    with patch(_PAGINATE) as paginate:
        paginate.return_value = []
        compiled = compile_authoring_request_v1(
            process_ir_request(), boomi_client=MagicMock(), profile=_PROFILE
        )
    from boomi_mcp.authoring.process_materialization import (
        ProcessComponentMaterializationPlanV1 as _Plan,
    )

    internals = compiled[1]
    plans = [
        value
        for value in (internals.materialization_plans or {}).values()
        if isinstance(value, _Plan)
    ]
    assert plans, "the compile produced no materialization plan to probe"
    return plans[0]


def test_a_plan_model_refusal_reaches_the_caller_with_its_own_code():
    """QA round 16 (QA-153-r16-01): the named code never left the building.

    Both plan-model refusals — the pre-existing fingerprint mismatch and the
    slot-inventory disagreement added this round — are registered codes, and the
    compile wrapper reported `cause_codes: ["ValidationError"]` for both: the
    Python class name, not the code. Relocatability escaped only because it has
    a dedicated pre-check ahead of the model, which is why the gap survived a
    surface that looked well covered.

    Asserted for BOTH validators, because one passing would prove only that the
    resolver runs, not that it reads the validation's own registered types.
    """
    from boomi_mcp.authoring.workflow import _cause_codes_for
    from boomi_mcp.errors import (
        PROCESS_MATERIALIZATION_FINGERPRINT_MISMATCH,
        PROCESS_MATERIALIZATION_PLAN_INVALID,
    )
    from pydantic import ValidationError as _VE

    def _refusal(build):
        with pytest.raises(_VE) as excinfo:
            build()
        return _cause_codes_for(excinfo.value)

    from boomi_mcp.authoring.process_materialization import (
        ProcessComponentMaterializationPlanV1 as _Plan,
    )

    real = _plan_for_named_code_probe()
    material = real.model_dump(mode="python")

    # 1. the slot-inventory refusal
    slots_dropped = dict(material, unresolved_symbol_slots=())
    assert _refusal(lambda: _Plan(**slots_dropped)) == (
        PROCESS_MATERIALIZATION_PLAN_INVALID,
    )
    # 2. the fingerprint refusal
    bad_fp = dict(material, plan_fingerprint="sha256:" + "0" * 64)
    assert _refusal(lambda: _Plan(**bad_fp)) == (
        PROCESS_MATERIALIZATION_FINGERPRINT_MISMATCH,
    )
    # 3. ...and an unrecognised failure still reports the type name, so this
    #    widened what is NAMED without changing any pre-existing envelope.
    assert _cause_codes_for(RuntimeError("nothing registered")) == ("RuntimeError",)


def test_every_reader_of_the_named_code_map_resolves_a_bare_custom_error():
    """QA-153-r17-02: the map had two readers and they disagreed again.

    A plan-model rule raising `PydanticCustomError` OUTSIDE a validation context
    carries no `.errors()`, so the map's normal walk finds nothing. The apply
    route grew a private lookup for that form (QA-153-r15-02); the compile
    wrapper added this round did not — which is the same divergence QA-153-r14-01
    found when the location rule was added to one reader and not the other.

    The rule now lives in the shared resolver, so this asserts every reader
    agrees rather than asserting the resolver works: a fix that added a third
    private copy would pass a resolver-only test and fail this one.
    """
    from pydantic_core import PydanticCustomError

    from boomi_mcp.authoring.workflow import _cause_codes_for
    from boomi_mcp.categories.integration_builder import (
        _named_error_code_from_validation,
        _canonical_plan_failure,
    )
    from boomi_mcp.errors import PROCESS_MATERIALIZATION_PLAN_INVALID

    bare = PydanticCustomError(
        "process_materialization_plan_invalid", "raised outside a validation"
    )

    # reader 1 — the shared resolver itself
    assert _named_error_code_from_validation(bare) == PROCESS_MATERIALIZATION_PLAN_INVALID
    # reader 2 — the apply route
    assert _canonical_plan_failure(bare)[0] == PROCESS_MATERIALIZATION_PLAN_INVALID
    # reader 3 — the compile wrapper
    assert _cause_codes_for(bare) == (PROCESS_MATERIALIZATION_PLAN_INVALID,)

    # ...and an UNREGISTERED bare error is still not named by any of them, so
    # this widened what is resolved and not what is claimed.
    stranger = PydanticCustomError("no_such_registered_type", "unknown")
    assert _named_error_code_from_validation(stranger) is None
    assert _cause_codes_for(stranger) == ("PydanticCustomError",)


def test_the_entry_role_restriction_is_pinned_to_the_invariant_that_enforces_it():
    """L2 round 45: the probe built a `connector_call` entry the compiler rejects.

    `ENTRY_ROLE_RESTRICTIONS` now says which roles may sit at the entry, and the
    oracle reads it — so it is a second statement of a rule `check_cfg_invariants`
    already enforces, and the two must be pinned to each other or they will
    drift, which is the class this slice exists to close.

    Pinned BOTH ways: the restricted role is one the invariant accepts, and every
    OTHER declared role is one it rejects. A one-directional check would pass for
    a restriction that simply listed every role, so the final assertion refuses
    that too.

    The graph is a REAL compiled CFG with its entry node's role swapped, not a
    hand-assembled one. Hand-building a valid `SemanticCfgV1` means restating the
    node and edge models' required fields — a hand-model larger than the one this
    test is pinning, and the first attempt at it silently swallowed its own
    construction errors and reported the legal role as rejected.
    """
    from boomi_mcp.compiler.process_ir import compile_process_ir_v1
    from boomi_mcp.compiler.process_ir.contracts import ConnectorCallSemanticV1
    from boomi_mcp.compiler.process_ir.invariants import (
        ENTRY_CALL_ROLE,
        ENTRY_ROLE_RESTRICTIONS,
        check_cfg_invariants,
    )
    from boomi_mcp.authoring.contract import _literal_options
    unit = process_unit()
    request = process_ir_request(units=(unit,))
    with patch(_PAGINATE) as paginate:
        paginate.return_value = []
        compiled = compile_authoring_request_v1(
            request, boomi_client=MagicMock(), profile=_PROFILE
        )
    # The compile's OWN symbol table, not one rebuilt here: a second table could
    # resolve an operation differently and the CFG would then describe a compile
    # nobody ran.
    cfg, _plan = compile_process_ir_v1(unit.process_ir, compiled[1].symbols)

    entry = next(n for n in cfg.nodes if n.node_id == cfg.entry_node_id)
    # This fixture's entry lowers to a `connector`, so the entry SEMANTIC — and
    # only it — is replaced with a connector call. Everything else about the
    # graph stays exactly as the compiler produced it, which is what keeps this
    # from becoming the hand-built CFG the docstring rejects.
    assert entry.semantic.semantic_kind in ("connector", "connector_call"), entry.semantic

    def _accepted(role):
        swapped = entry.model_copy(
            update={"semantic": ConnectorCallSemanticV1(
                role=role, operation_ref=entry.semantic.operation_ref
            )}
        )
        candidate = cfg.model_copy(
            update={"nodes": tuple(
                swapped if n.node_id == entry.node_id else n for n in cfg.nodes
            )}
        )
        try:
            check_cfg_invariants(candidate)
            return True
        except Exception:
            return False

    declared = _literal_options(ConnectorCallSemanticV1, "role")
    allowed = ENTRY_ROLE_RESTRICTIONS["connector_call"]
    assert set(allowed) <= set(declared), (allowed, declared)
    assert ENTRY_CALL_ROLE in allowed
    for role in declared:
        assert _accepted(role) == (role in allowed), (
            "the entry-role restriction and the invariant disagree about %r" % role
        )
    # ...and the restriction is not vacuously everything.
    assert set(allowed) != set(declared), (allowed, declared)


# --------------------------------------------------------------------------
# §6 AR5-01 — the served revision must bind MULTI-SYMBOL lookup behaviour
# --------------------------------------------------------------------------

def _family_lookup_mutants(ep):
    """Variants of `_operation_connector_family` that are byte-identical to the
    real one on every 0- or 1-symbol table.

    That equivalence is the whole finding: the oracle passed at most one symbol
    per case, so each of these left the served `compiler_revision` unchanged
    while changing what a real request materializes. They are defined here in
    one place because both the kill test and the "the oracle could not see them"
    non-vacuity control need exactly the same set.
    """
    listener = ep.LISTENER_CONNECTOR_TYPES

    def _fold(value):
        return value.strip().lower() if isinstance(value, str) else None

    def first0_refchecked(symbols, ref):
        rows = tuple(symbols.symbols or ())
        if rows and rows[0].ref == ref:
            return _fold(rows[0].connector_type)
        return None

    def first0_refblind(symbols, ref):
        rows = tuple(symbols.symbols or ())
        return _fold(rows[0].connector_type) if rows else None

    def single_symbol_only(symbols, ref):
        rows = tuple(symbols.symbols or ())
        if len(rows) > 1:
            return None
        return next((_fold(r.connector_type) for r in rows if r.ref == ref), None)

    def wrong_loop_var(symbols, ref):
        rows = tuple(symbols.symbols or ())
        for row in rows:
            if row.ref == ref:
                return _fold(rows[0].connector_type)
        return None

    def listener_anywhere(symbols, ref):
        rows = tuple(symbols.symbols or ())
        if not any(r.ref == ref for r in rows):
            return None
        for row in rows:
            folded = _fold(row.connector_type)
            if folded in listener:
                return folded
        return next((_fold(r.connector_type) for r in rows if r.ref == ref), None)

    return {
        "first0-refchecked": first0_refchecked,
        "first0-refblind": first0_refblind,
        "single-symbol-only": single_symbol_only,
        "wrong-loop-var": wrong_loop_var,
        "listener-anywhere": listener_anywhere,
    }


def test_the_served_revision_binds_multi_symbol_family_lookup():
    """§6 AR5-01: every probe passed at most ONE symbol; the rule searches a table.

    The reviewer found the blind spot and then read it in the harmless direction
    — real=listener flipping to scheduled, which needs a graph lowering refuses.
    Three independent verifications took the other direction and all three built
    a request that COMPILES today and is materialized WRONG: a correctly-
    scheduled process stamped with listener `<process>` bytes. It needs no
    listener entry at all, only a listener-family symbol somewhere in the table
    — and `build_symbol_table` puts every component of the spec in one table —
    plus a lookup that answers from the wrong row. The apply-time re-derive
    cannot catch it, because it calls the same function.

    Asserted on `_compiler_revision()`, the SERVED value, not on the oracle
    helper: what a caller binds to is the revision.
    """
    import boomi_mcp.compiler.process_ir.execution_profile as ep
    from boomi_mcp.authoring.contract import _compiler_revision

    baseline = _compiler_revision()
    original = ep._operation_connector_family
    mutants = _family_lookup_mutants(ep)

    survived = []
    try:
        for name, mutant in mutants.items():
            ep._operation_connector_family = mutant
            if _compiler_revision() == baseline:
                survived.append(name)
    finally:
        ep._operation_connector_family = original
    assert survived == [], (
        "the served revision is blind to these lookup regressions: %s" % survived
    )
    assert _compiler_revision() == baseline

    # CONTROL 1 — EQUIVALENCE. A semantically identical, order-independent
    # reimplementation must leave the revision byte-identical, or the new rows
    # are pinning an implementation shape rather than behaviour and every
    # unrelated refactor would break a caller's binding.
    def _by_index(symbols, ref):
        index = {
            row.ref: row.connector_type for row in (symbols.symbols or ())
        }
        value = index.get(ref)
        return value.strip().lower() if isinstance(value, str) else None

    try:
        ep._operation_connector_family = _by_index
        assert _compiler_revision() == baseline, (
            "an equivalent lookup moved the served revision"
        )
    finally:
        ep._operation_connector_family = original

    # CONTROL 2 — the mutants really are invisible to a ONE-SYMBOL case set, so
    # "the shipped oracle could not see them" is measured rather than asserted.
    #
    # FOUR of the five, not all five. `first0-refblind` ignores the reference
    # entirely, so it already differs on a one-symbol table whose symbol does
    # not match — which is exactly why the shipped case set already caught it,
    # and the kill above asserts that detection is RETAINED rather than newly
    # gained. Writing this control over all five was the first draft, and it
    # failed on that mutant: the claim was broader than the fact. Scoped here to
    # the four whose invisibility is the finding.
    class _Row:
        def __init__(self, ref, connector_type):
            self.ref = ref
            self.connector_type = connector_type

    class _Table:
        def __init__(self, rows):
            self.symbols = tuple(rows)

    single = [_Table(()), _Table((_Row("$ref:op", "database"),)),
              _Table((_Row("$ref:op", sorted(ep.LISTENER_CONNECTOR_TYPES)[0]),)),
              _Table((_Row("$ref:other", "database"),))]
    invisible = {k: v for k, v in mutants.items() if k != "first0-refblind"}
    assert len(invisible) == 4, sorted(invisible)
    for name, mutant in invisible.items():
        for table in single:
            for ref in ("$ref:op", "$ref:missing"):
                assert mutant(table, ref) == original(table, ref), (name, ref)
    # ...and the excluded one is excluded for a MEASURED reason, not by taste:
    # it differs from the real function on a one-symbol table.
    mismatch = [
        (tuple((r.ref, r.connector_type) for r in t.symbols), ref)
        for t in single
        for ref in ("$ref:op", "$ref:missing")
        if mutants["first0-refblind"](t, ref) != original(t, ref)
    ]
    assert mismatch, "first0-refblind is invisible to single-symbol tables after all"


def test_the_profile_case_set_keeps_both_arities_and_both_directions():
    """The structural guard, so a future row cannot re-open AR5-01 silently.

    Padding two rows fixes today's case set; it does not stop the next edit from
    reverting to a one-symbol-only probe set, which is how this defect arrived.
    So the property is asserted over the case set itself, derived from the rows
    that actually reach the lookup rather than from a list written here:

      * both arities stay represented — at least one single-symbol probe and at
        least one multi-symbol probe;
      * BOTH expected directions carry a multi-symbol table — a row whose value
        is `scheduled` and a row whose value is `listener` — because a mutant
        that flips only one direction is invisible to a case set that only
        exercises the other;
      * in every multi-symbol table the referenced symbol is not alone, and at
        least one decoy carries the opposite family class from the row's own
        answer, since a decoy of the same class discriminates nothing.
    """
    import boomi_mcp.compiler.process_ir.execution_profile as ep
    from boomi_mcp.authoring import contract as ct

    seen = []
    original = ep._operation_connector_family

    def _recording(symbols, ref):
        rows = tuple(symbols.symbols or ())
        seen.append((tuple((r.ref, r.connector_type) for r in rows), ref))
        return original(symbols, ref)

    try:
        ep._operation_connector_family = _recording
        oracle = ct._execution_profile_behaviour_oracle()
    finally:
        ep._operation_connector_family = original

    assert seen, "no probe reached the family lookup at all"
    arities = {len(rows) for rows, _ref in seen}
    assert any(a == 1 for a in arities), sorted(arities)
    assert any(a > 1 for a in arities), sorted(arities)

    listener_families = {f.strip().lower() for f in ep.LISTENER_CONNECTOR_TYPES}

    def _is_listener(value):
        return isinstance(value, str) and value.strip().lower() in listener_families

    multi = [(rows, ref) for rows, ref in seen if len(rows) > 1]
    directions = set()
    for rows, ref in multi:
        referenced = next((t for r, t in rows if r == ref), None)
        answer = "listener" if _is_listener(referenced) else "scheduled"
        decoys = [t for r, t in rows if r != ref]
        assert decoys, (rows, ref)
        # ...and at least one decoy is of the opposite class from the answer.
        assert any(_is_listener(t) != _is_listener(referenced) for t in decoys), (
            "a multi-symbol probe carries only same-class decoys, which "
            "discriminate nothing: %r" % (rows,)
        )
        directions.add(answer)
    assert directions == {"listener", "scheduled"}, sorted(directions)

    # ...and the rows' VALUES are what the directions above claim, so this guard
    # cannot pass over a case set whose answers have silently changed.
    assert oracle["cases"]["non-listener-family"] == "scheduled"
