"""#158 (M12.20): a canonical ProcessIR root is deployable, and a listener root verifies.

**What changed.** Before #158 ``orchestrate_deploy`` found a build's deploy target
only among ``spec.components`` and classified a listener only from the two LEGACY
config shapes. A canonical root — the only kind #153's direct apply creates —
lives in ``spec.processes``, so it was invisible to both: a canonical build had
"no process to deploy", and a canonical listener could be neither published nor
verified. #158 makes one selection rule (authored roots from BOTH namespaces;
reference-only process components excluded unless nothing is authored) and one
entry-recognition authority (``authoring.process_entry``: the compiler's entry
policy for a canonical root, the entry-shape normalizer for a legacy config).

**How every build here is made.** Through the public route a caller uses —
``build_integration_action`` ``plan`` → ``compile`` → ``apply`` — with only the
network boundary faked (``_execute_component``, ``create_component``,
``component_get_xml``, ``paginate_metadata``). The registry row orchestration
reads is therefore exactly the row apply wrote, never a hand-built dict. A
hand-seeded row carries whatever shape its author imagined — the existing
orchestration suites seed rows with no ``processes`` key at all, while apply
ALWAYS records one, in the container ``IntegrationSpecV1.model_dump()`` yields —
and a reader modelled on the imagined shape passes against it. Orchestration is
then driven through
``orchestrate_deploy_action`` (dry run, and a real run over a fake account whose
every router records its calls).

**The matrix** (``test_canonical_root_is_the_deploy_target``)::

    id                          spec.processes    spec.components processes     listener?
    scheduled-alone             scheduled root    (none)                        no
    scheduled-reference-child   scheduled root    reference-only child, own id  no
    listener-alone              listener root     (none)                        yes
    listener-reference-child    listener root     reference-only child, own id  yes

For every row the resolved target is the canonical root's APPLIED id (read from
the build's own results), and that exact id is what reaches
``_find_or_create_package(component_id=...)``, the SDK ``create_package`` call,
the ``deploy`` of the package created for it, and the runtime bindings. A
listener row plans and then completes ``listener_verify``; a scheduled row is
never classified a listener. The reference-only child neither replaces the root
nor makes the selection ambiguous, and its id reaches nothing.

**Fixture provenance** (CLAUDE.md clean-room rule — no fixture below is derived
from the code under test):

* scheduled root + REST connection/operation — ``_m12_11_support.APPLIABLE_*``,
  frozen before this slice's baseline;
* listener root — ``tests/fixtures/process_ir/listener/listener_rest_send.json``,
  whose shapes reproduce golden-000042 (``sync_pipeline_listener_send.xml``,
  legacy renderer, frozen 9711a9c); its ``$ref`` keys are READ from it below;
* WSS Listen operation config — the ``WssListenerOperationBuilder`` config
  contract (live capture renera op 601cf5a3, M6 #12), the same shape as
  ``test_integration_builder._asc_wss_op_comp`` (pre-baseline);
* API Service Component config — ``test_integration_builder._api_service_comp``
  (#133, pre-baseline);
* a REUSED root's stored process XML — two goldens frozen before the baseline:
  ``sync_pipeline_listener_send.xml`` (a Listen start on ``WSSOP-1``, 9711a9c) and
  ``canonical_process_envelope.xml`` (a ``noaction`` start, c48caa9).

Run: ``PYTHONPATH=src .venv/bin/python -m pytest tests/test_issue_158_listener_deployment.py``
"""

import ast
import contextlib
import copy
import inspect
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_HERE = Path(__file__).resolve().parent
_src = str(_HERE.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(_HERE))

from _m12_11_support import (  # noqa: E402
    APPLIABLE_CONN,
    APPLIABLE_IR_DOC,
    APPLIABLE_OP,
)
from boomi_mcp.authoring import process_entry  # noqa: E402
from boomi_mcp.authoring.process_entry import (  # noqa: E402
    ProcessEntryV1,
    canonical_root_entry,
    legacy_config_entry,
)
from boomi_mcp.categories import integration_builder  # noqa: E402
from boomi_mcp.categories.components.builders.connector_builder import (  # noqa: E402
    WssListenerOperationBuilder,
    wss_listen_action_from_config,
)
from boomi_mcp.categories.deployment import orchestration  # noqa: E402
from boomi_mcp.categories.integration_builder import (  # noqa: E402
    _BUILD_REGISTRY,
    build_integration_action,
)
from boomi_mcp.models.authoring_workflow import (  # noqa: E402
    AuthoringRequestV1,
    ProcessIRAuthoringIntentV1,
)
from boomi_mcp.models.process_component import (  # noqa: E402
    ProcessAuthoringUnitV1,
    ProcessComponentEnvelopeV1,
    ProcessConnectionOverrideV1,
    ProcessExtensionBindingsV1,
    ProcessOverrideFieldV1,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402

_EXECUTE = "boomi_mcp.categories.integration_builder._execute_component"
_CREATE = "boomi_mcp.categories.integration_builder.create_component"
_GET_XML = "boomi_mcp.categories.integration_builder.component_get_xml"
_SHARED_GET_XML = "boomi_mcp.categories.components._shared.component_get_xml"

_PROFILE = "qa_profile"
_ENV = "env-158"
_RUNTIME = "rt-158"

#: The id the fake account mints for a created canonical root. Distinct from
#: every other id in a build, so "the target is the root" cannot pass by accident.
_ROOT_ID = "canonical-root-cid-158"
#: The id of the EXISTING process a reference-only child names.
_CHILD_ID = "reference-child-cid-158"

_GOLDEN_XML = _HERE / "fixtures" / "golden_xml"
#: A Listen start on operation ``WSSOP-1`` (legacy renderer, frozen 9711a9c).
_LISTEN_START_XML = (_GOLDEN_XML / "sync_pipeline_listener_send.xml").read_text(encoding="utf-8")
_LISTEN_START_OPERATION_ID = "WSSOP-1"
#: A ``noaction`` (scheduled) start (frozen c48caa9).
_NOACTION_START_XML = (_GOLDEN_XML / "canonical_process_envelope.xml").read_text(encoding="utf-8")

#: Readback for a non-process component this file does not materialize.
_NON_PROCESS_READBACK = (
    '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
    'type="connector-settings"><bns:object><connection/></bns:object>'
    "</bns:Component>"
)

# ---------------------------------------------------------------------------
# The listener root and its supporting components
# ---------------------------------------------------------------------------

_LISTENER_DOC = json.loads(
    (_HERE / "fixtures" / "process_ir" / "listener" / "listener_rest_send.json").read_text(
        encoding="utf-8"
    )
)


def _ref_key(ref):
    assert ref.startswith("$ref:"), ref
    return ref[len("$ref:"):]


# READ from the anchored fixture, never restated: the components below are keyed
# by exactly the references the listener document makes.
_LISTEN_STEP, _TARGET_STEP = _LISTENER_DOC["body"]["steps"][:2]
assert _LISTEN_STEP["kind"] == "listener" and _TARGET_STEP["kind"] == "target"
_WSS_OP_KEY = _ref_key(_LISTEN_STEP["operation_ref"])
_REST_CONN_KEY = _ref_key(_TARGET_STEP["connection_ref"])
_REST_POST_KEY = _ref_key(_TARGET_STEP["operation_ref"])

_WSS_OBJECT_NAME = "orderIntake158"
_WSS_LISTEN_OP = {
    "key": _WSS_OP_KEY,
    "type": "connector-action",
    "name": "E158 order intake listener",
    "action": "create",
    "config": {
        "connector_type": "wss",
        "operation_mode": "listen",
        "component_name": "E158 order intake listener",
        "object_name": _WSS_OBJECT_NAME,
        "operation_type": "EXECUTE",
        "input_type": "singlejson",
        "output_type": "none",
    },
}

_REST_CONN = dict(copy.deepcopy(APPLIABLE_CONN), key=_REST_CONN_KEY)
_REST_POST_OP = copy.deepcopy(APPLIABLE_OP)
_REST_POST_OP.update(key=_REST_POST_KEY, name="E158 order post", depends_on=[_REST_CONN_KEY])
_REST_POST_OP["config"].update(
    component_name="E158 order post", connection_ref_key=_REST_CONN_KEY, method="POST"
)

_LISTENER_COMPONENTS = (_REST_CONN, _REST_POST_OP, _WSS_LISTEN_OP)
_SCHEDULED_COMPONENTS = (APPLIABLE_CONN, APPLIABLE_OP)


def _unit(doc, depends_on, *, key="root", name="E158 Root", **envelope_extra):
    return ProcessAuthoringUnitV1(
        envelope=ProcessComponentEnvelopeV1(
            component_key=key,
            name=name,
            action="create",
            depends_on=tuple(depends_on),
            **envelope_extra,
        ),
        process_ir=parse_process_ir_v1(doc),
    )


def _listener_unit(**kwargs):
    return _unit(_LISTENER_DOC, (_REST_CONN_KEY, _REST_POST_KEY, _WSS_OP_KEY), **kwargs)


def _scheduled_unit(**kwargs):
    return _unit(APPLIABLE_IR_DOC, ("conn", "op"), **kwargs)


def _request(units, components, **intent_extra):
    return AuthoringRequestV1(
        intent=ProcessIRAuthoringIntentV1(
            integration_name="E158 Integration",
            units=tuple(units),
            components=tuple(components),
            **intent_extra,
        )
    )


def _reference_child(key="child", component_id=_CHILD_ID, **config_extra):
    """A process the account already holds, which the build merely NAMES."""
    return {
        "key": key,
        "type": "process",
        "name": "E158 existing " + key,
        "action": "create",
        "config": {"reference_only": True, "component_id": component_id, **config_extra},
    }


def _asc(route_key, key="api_service"):
    """An API Service Component routing to ``route_key`` (#133 config shape)."""
    return {
        "key": key,
        "type": "webservice",
        "name": "E158 order API",
        "action": "create",
        "depends_on": [route_key],
        "config": {
            "component_type": "webservice",
            "component_name": "E158 order API",
            "routes": [{"process": "$ref:" + route_key}],
        },
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _registry_restored():
    saved = dict(_BUILD_REGISTRY)
    yield
    _BUILD_REGISTRY.clear()
    _BUILD_REGISTRY.update(saved)


@pytest.fixture(autouse=True)
def _no_live_metadata_queries(monkeypatch):
    # Not only hygiene: an unpatched metadata pager over a MagicMock client
    # never terminates (measured — a compile consumed 4.6 GB before it was killed).
    monkeypatch.setattr(integration_builder, "paginate_metadata", lambda *a, **k: [])


# ---------------------------------------------------------------------------
# The apply-side network boundary
# ---------------------------------------------------------------------------


class _ApplyBoundary:
    """``build_integration_action``'s network boundary — the only thing faked on apply.

    ``_execute_component`` answers a component create with ``applied-<key>`` (or
    the id ``ids`` names), ``create_component`` answers a canonical root's create
    with the next id of ``root_ids``, and ``component_get_xml`` reads back what
    was created — or, for a component the account already holds, ``stored``.
    """

    def __init__(self, *, root_ids=(_ROOT_ID,), ids=None, stored=None):
        self._root_ids = list(root_ids)
        self.ids = dict(ids or {})
        self.stored = dict(stored or {})
        self.executed = {}
        self.created = []
        self.planned = None

    def execute(self, *args, **kwargs):
        comp = kwargs["comp"]
        self.executed[comp.key] = copy.deepcopy(kwargs["config"])
        return {"_success": True, "component_id": self.ids.get(comp.key, "applied-" + comp.key)}

    def create(self, _client, _profile, payload):
        component_id = self._root_ids[len(self.created)]
        self.created.append(component_id)
        self.stored[component_id] = payload["xml"]
        return {"_success": True, "component_id": component_id}

    def get_xml(self, _client, component_id, *args, **kwargs):
        stored = self.stored.get(component_id)
        if isinstance(stored, dict):  # a typed readback: {"type": ..., "xml": ...}
            return dict(stored)
        if stored is not None:
            return {"type": "process", "xml": stored}
        return {"type": "connector-settings", "xml": _NON_PROCESS_READBACK}

    @contextlib.contextmanager
    def installed(self, existing=None):
        with contextlib.ExitStack() as stack:
            if existing is not None:
                stack.enter_context(
                    patch.object(integration_builder, "_resolve_existing_components", existing)
                )
            stack.enter_context(patch(_EXECUTE, side_effect=self.execute))
            stack.enter_context(patch(_CREATE, side_effect=self.create))
            stack.enter_context(patch(_GET_XML, side_effect=self.get_xml))
            # Apply's reused-connector reader imports its reader at call time from
            # the shared module, so the same boundary is installed there too.
            stack.enter_context(patch(_SHARED_GET_XML, side_effect=self.get_xml))
            yield self


def _cause_codes(result):
    """Every code an authoring or builder envelope carries, wherever it puts it."""
    codes = {result.get("error_code")}
    payload = result.get("authoring_result") or {}
    diagnostics = list(result.get("authoring_diagnostics") or ())
    for bucket in ("errors", "warnings", "diagnostics"):
        diagnostics.extend(payload.get(bucket) or ())
    for diagnostic in diagnostics:
        codes.add(diagnostic.get("code"))
        codes.update(diagnostic.get("cause_codes") or ())
    for step in list(result.get("steps") or ()) + list(result.get("unresolvable_steps") or ()):
        codes.add((step.get("validation_error") or {}).get("error_code"))
    return {code for code in codes if code}


def _typed_build(request, boundary=None, *, existing=None):
    """plan → compile → apply one typed request through the public dispatcher."""
    boundary = boundary or _ApplyBoundary()
    raw = request.model_dump(mode="json")
    with boundary.installed(existing):
        planned = build_integration_action(
            MagicMock(), _PROFILE, "plan", config={"authoring_request": raw}
        )
        assert planned["_success"] is True, planned.get("error")
        assert planned["authoring_result"]["validation_report"]["is_valid"] is True, (
            _cause_codes(planned)
        )
        boundary.planned = planned
        compiled = build_integration_action(
            MagicMock(), _PROFILE, "compile", config={"authoring_request": raw}
        )
        assert compiled["_success"] is True, _cause_codes(compiled)
        binding = compiled["authoring_result"]["revision_binding"]
        payload = dict(
            raw,
            expected_capability_revision=binding["capability_revision"],
            expected_compile_hash=binding["compile_hash"],
        )
        applied = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={"authoring_request": payload, "dry_run": False},
        )
    assert applied["_success"] is True, applied.get("error")
    return applied, boundary


def _legacy_build(spec, boundary=None):
    """plan → apply an ``integration_spec`` (the legacy root, which carries ``processes`` too)."""
    boundary = boundary or _ApplyBoundary()
    with boundary.installed():
        planned = build_integration_action(
            MagicMock(), _PROFILE, "plan", config={"integration_spec": spec}
        )
        assert planned["_success"] is True, planned.get("error")
        applied = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={"integration_spec": spec, "dry_run": False},
        )
    assert applied["_success"] is True, applied.get("error")
    return applied, boundary


# ---------------------------------------------------------------------------
# The orchestration-side account
# ---------------------------------------------------------------------------


class _ExplodingClient:
    """A client stand-in: every SDK path is patched, so any direct touch is a defect."""

    def __getattr__(self, name):
        raise AssertionError("unpatched SDK access during orchestrate_deploy: " + name)


class _FakeTime:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class _AccountSurface:
    """Every router a real ``orchestrate_deploy`` run reaches, faked and RECORDED.

    Nothing pre-exists: packages are created, deployed, and bound, so each id the
    run hands the account is visible in ``calls``. ``package_boundary`` records
    what ``_find_or_create_package`` itself was asked for — the package boundary
    the target id must reach.
    """

    def __init__(self, *, api_type="intermediate", stored=None):
        self.api_type = api_type
        self.stored = dict(stored or {})
        self.calls = []
        self.package_boundary = []
        self.probes = []
        self.reads = []
        self._record_polls = 0

    def install(self, monkeypatch):
        real_package = orchestration._find_or_create_package

        def package_spy(boomi_client, profile, **kwargs):
            self.package_boundary.append(dict(kwargs))
            return real_package(boomi_client, profile, **kwargs)

        monkeypatch.setattr(orchestration, "_find_or_create_package", package_spy)
        monkeypatch.setattr(
            orchestration, "manage_deployment_action", self._router("deployment", self._deployment)
        )
        monkeypatch.setattr(
            orchestration, "manage_environments_action", self._router("environments", self._env)
        )
        monkeypatch.setattr(
            orchestration, "manage_runtimes_action", self._router("runtimes", self._runtime)
        )
        monkeypatch.setattr(
            orchestration, "manage_schedules_action", self._router("schedules", self._never)
        )
        monkeypatch.setattr(
            orchestration,
            "manage_shared_resources_action",
            self._router("shared_resources", self._shared),
        )
        monkeypatch.setattr(
            orchestration, "monitor_platform_action", self._router("monitoring", self._monitor)
        )
        monkeypatch.setattr(
            orchestration, "execute_process_action", self._router("execution", self._never)
        )
        monkeypatch.setattr(orchestration, "_listener_probe", self._probe)
        monkeypatch.setattr(orchestration, "component_get_xml", self._read)
        monkeypatch.setattr(orchestration, "time", _FakeTime())
        return self

    # -- recording ----------------------------------------------------------

    def _router(self, name, answer):
        def call(sdk=None, profile=None, action=None, config_data=None, **kwargs):
            config = dict(config_data or {})
            self.calls.append((name, action, config))
            return answer(action, config)

        return call

    def actions(self, *names):
        return [config for _router, action, config in self.calls if action in names]

    # -- answers ------------------------------------------------------------

    @staticmethod
    def _never(action, config):
        raise AssertionError("router must not be reached: %s %r" % (action, config))

    @staticmethod
    def _deployment(action, config):
        if action == "list_packages":
            return {"_success": True, "packages": []}
        if action == "create_package":
            return {
                "_success": True,
                "package": {
                    "package_id": "pkg-" + config["component_id"],
                    "component_id": config["component_id"],
                    "component_type": config.get("component_type"),
                    "package_version": config["package_version"],
                },
            }
        if action == "list_deployments":
            return {"_success": True, "deployments": []}
        if action == "deploy":
            return {
                "_success": True,
                "deployment": {
                    "deployment_id": "dep-" + config["package_id"],
                    "active": True,
                    "version": 1,
                },
            }
        if action in ("list_process_environment_attachments", "list_process_atom_attachments"):
            return {"_success": True, "attachments": []}
        if action == "attach_process_environment":
            return {"_success": True, "attachment": {"id": "pe-158", **config}}
        if action == "attach_process_atom":
            return {"_success": True, "attachment": {"id": "pa-158", **config}}
        raise AssertionError("unexpected deployment action: %s" % action)

    @staticmethod
    def _env(action, config):
        assert action == "get", action
        return {"_success": True, "environment": {"id": _ENV}}

    @staticmethod
    def _runtime(action, config):
        if action == "get":
            return {"_success": True, "runtime": {"id": _RUNTIME}}
        if action == "list_attachments":
            return {
                "_success": True,
                "attachments": [{"id": "ea-158", "atom_id": _RUNTIME, "environment_id": _ENV}],
            }
        raise AssertionError("unexpected runtimes action: %s" % action)

    def _shared(self, action, config):
        assert action == "get_server_info", action
        return {
            "_success": True,
            "server_info": {
                "api_type": self.api_type,
                "auth": "none",
                "url": "http://atom.local:9090",
            },
        }

    def _monitor(self, action, config):
        assert action == "execution_records", action
        self._record_polls += 1
        if self._record_polls == 1:  # the pre-probe baseline
            return {"_success": True, "total_count": 0, "execution_records": []}
        return {
            "_success": True,
            "total_count": 1,
            "execution_records": [
                {"execution_id": "exec-158", "status": "COMPLETE", "execution_type": "exec_listener"}
            ],
        }

    def _probe(self, url, *, method, payload, headers, timeout_seconds):
        self.probes.append({"url": url, "method": method})
        return 200, None

    def _read(self, _client, component_id, **kwargs):
        self.reads.append(component_id)
        if component_id not in self.stored:
            raise RuntimeError("component %s could not be read" % component_id)
        return {"type": "process", "xml": self.stored[component_id]}


def _deploy(build_id, *, dry_run, **kwargs):
    return orchestration.orchestrate_deploy_action(
        boomi_client=None if dry_run else _ExplodingClient(),
        profile=_PROFILE,
        build_id=build_id,
        environment_id=_ENV,
        runtime_id=_RUNTIME,
        dry_run=dry_run,
        **kwargs,
    )


def _error_codes(result):
    return [error["code"] for error in result.get("errors") or ()]


def _recorded(build_id):
    """The registry row apply wrote, and the two spec namespaces it carries."""
    entry = _BUILD_REGISTRY[build_id]
    spec = entry["spec"]
    return entry, list(spec.get("components") or ()), list(spec.get("processes") or ())


# ---------------------------------------------------------------------------
# The matrix
# ---------------------------------------------------------------------------

_MATRIX = [
    pytest.param("scheduled", False, id="scheduled-alone"),
    pytest.param("scheduled", True, id="scheduled-reference-child"),
    pytest.param("listener", False, id="listener-alone"),
    pytest.param("listener", True, id="listener-reference-child"),
]


def _matrix_request(kind, with_child):
    if kind == "listener":
        unit, components = _listener_unit(), list(_LISTENER_COMPONENTS)
    else:
        unit, components = _scheduled_unit(), list(_SCHEDULED_COMPONENTS)
    if with_child:
        components.append(_reference_child())
    return _request([unit], components)


@pytest.mark.parametrize("kind,with_child", _MATRIX)
def test_canonical_root_is_the_deploy_target(kind, with_child, monkeypatch):
    applied, _boundary = _typed_build(_matrix_request(kind, with_child))
    build_id = applied["build_id"]
    root_id = applied["results"]["root"]["component_id"]

    # The id is the one the account minted for the ROOT'S create — not any other
    # id in the build — and the root lives ONLY in `spec.processes`.
    assert root_id == _ROOT_ID
    assert applied["results"]["root"]["status"] == "created"
    _entry, components, processes = _recorded(build_id)
    assert [unit["envelope"]["component_key"] for unit in processes] == ["root"]
    assert "root" not in [comp["key"] for comp in components]
    child = applied["results"].get("child")
    assert (child and (child["status"], child["component_id"])) == (
        ("reused", _CHILD_ID) if with_child else None
    ), child
    listener = kind == "listener"

    # 1. Selection: the canonical root, by its applied id — the child neither
    #    replaces it nor makes the selection ambiguous.
    target, error = orchestration._resolve_build_deployment_target(build_id)
    assert error is None, error
    assert target.process_key == "root"
    assert target.process_component_id == root_id
    assert target.process_status == "created"

    # 2. Classification, from the entry authority and nowhere else.
    meta = orchestration._resolve_listener_metadata(build_id, target)
    planned = _deploy(build_id, dry_run=True)
    assert planned["_success"] is True, planned.get("errors")
    assert _error_codes(planned) == []
    assert planned["target"]["process_component_id"] == root_id
    assert planned["package"]["component_id"] == root_id
    # Every row asserts every fact, the expectation chosen by the row: a branch
    # per kind would let a row that never reached its branch pass silently.
    # A listener's endpoint is derived from the build's OWN Listen operation, by
    # the live-settled M6 formula `/ws/simple/{lower(operationType)}{SentenceCase(objectName)}`.
    expected_meta = (
        {
            "object_name": _WSS_OBJECT_NAME,
            "publish_mode": "bare_wss",
            "endpoint_path": "/ws/simple/executeOrderIntake158",
        }
        if listener
        else None
    )
    assert (meta and {key: meta.get(key) for key in expected_meta or ()}) == expected_meta, meta
    assert planned["listener_verify"]["status"] == ("planned" if listener else "not_required")
    assert planned["listener_verify"].get("endpoint_path") == (meta or {}).get("endpoint_path")

    # 3. THE KEY ASSERTION: on a real run the root's applied id is exactly what
    #    reaches the package boundary, the SDK create, the deploy, the bindings.
    surface = _AccountSurface().install(monkeypatch)
    real = _deploy(build_id, dry_run=False)
    assert real["_success"] is True, (real.get("errors"), real.get("error"))
    assert surface.package_boundary == [{"component_id": root_id, "package_version": build_id}]
    assert [c["component_id"] for c in surface.actions("create_package")] == [root_id]
    assert [c["package_id"] for c in surface.actions("deploy")] == ["pkg-" + root_id]
    bound = surface.actions(
        "list_process_environment_attachments",
        "attach_process_environment",
        "list_process_atom_attachments",
        "attach_process_atom",
    )
    assert bound and {c["process_id"] for c in bound} == {root_id}
    assert real["package"]["component_id"] == root_id
    assert real["deployment"]["package_id"] == "pkg-" + root_id
    assert real["listener_verify"]["status"] == (
        "completed" if listener else "not_required"
    ), real["listener_verify"]
    assert [p["url"] for p in surface.probes] == (
        ["http://atom.local:9090" + expected_meta["endpoint_path"]] if listener else []
    )
    polled = surface.actions("execution_records")
    assert {c["process_id"] for c in polled} == ({root_id} if listener else set())
    assert bool(polled) is listener
    # A scheduled row never consults the shared web server.
    shared = [call for call in surface.calls if call[0] == "shared_resources"]
    assert listener or not shared, shared
    # ...and the child's id reaches nothing at all.
    assert _CHILD_ID not in json.dumps(surface.calls)


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------


_LEGACY_AUTHORED_PROCESS = {
    # An AUTHORED legacy process (test_integration_builder's #133 sync_pipeline
    # shape, literal ids): eligible exactly like a canonical root.
    "key": "legacy_proc",
    "type": "process",
    "name": "E158 legacy process",
    "action": "create",
    "config": {
        "process_kind": "sync_pipeline",
        "pipeline": {
            "stages": [
                {
                    "key": "fetch",
                    "kind": "fetch",
                    "config": {
                        "primitive": "rest_fetch",
                        "connection_id": "11111111-1111-1111-1111-111111111111",
                        "operation_id": "22222222-2222-2222-2222-222222222222",
                    },
                },
                {
                    "key": "send",
                    "kind": "send",
                    "config": {
                        "primitive": "rest_send",
                        "action_type": "POST",
                        "connection_id": "11111111-1111-1111-1111-111111111111",
                        "operation_id": "22222222-2222-2222-2222-222222222222",
                    },
                },
            ],
            "dependencies": [{"from_stage": "fetch", "to_stage": "send"}],
        },
    },
}


def _two_canonical_roots():
    applied, _ = _typed_build(
        _request(
            [_scheduled_unit(key="root_a", name="E158 Root A"),
             _scheduled_unit(key="root_b", name="E158 Root B")],
            _SCHEDULED_COMPONENTS,
        ),
        _ApplyBoundary(root_ids=("root-a-cid", "root-b-cid")),
    )
    return applied, ["root_a", "root_b"]


def _canonical_root_and_authored_component():
    applied, _ = _legacy_build(
        {
            "name": "E158 Mixed",
            "components": [APPLIABLE_CONN, APPLIABLE_OP, _LEGACY_AUTHORED_PROCESS],
            "processes": [_scheduled_unit().model_dump(mode="json")],
        }
    )
    return applied, ["legacy_proc", "root"]


@pytest.mark.parametrize(
    "make_build",
    [
        pytest.param(_two_canonical_roots, id="two-canonical-roots"),
        pytest.param(_canonical_root_and_authored_component, id="canonical-and-legacy-authored"),
    ],
)
def test_multiple_eligible_roots_refuse_before_package_deploy(make_build, monkeypatch):
    applied, expected_keys = make_build()
    build_id = applied["build_id"]

    planned = _deploy(build_id, dry_run=True)
    assert _error_codes(planned) == ["BUILD_MULTIPLE_PROCESS_COMPONENTS"]

    surface = _AccountSurface().install(monkeypatch)
    real = _deploy(build_id, dry_run=False)
    assert real["_success"] is False
    assert _error_codes(real) == ["BUILD_MULTIPLE_PROCESS_COMPONENTS"]
    assert sorted(real["errors"][0]["details"]["process_keys"]) == sorted(expected_keys)
    # Refused BEFORE the account: no package, no deploy, no call of any kind.
    assert surface.package_boundary == []
    assert surface.calls == []
    assert surface.reads == []


def test_reference_only_dependencies_do_not_make_a_root(monkeypatch):
    """Two reference-only process children beside ONE authored root."""
    applied, _ = _typed_build(
        _request(
            [_listener_unit()],
            list(_LISTENER_COMPONENTS)
            + [_reference_child("child_a", "child-a-cid"), _reference_child("child_b", "child-b-cid")],
        )
    )
    build_id = applied["build_id"]
    _entry, components, processes = _recorded(build_id)

    # NON-VACUITY: all three ARE process participants — the children are
    # excluded by the selection rule, not by never having been seen.
    roots = orchestration._process_roots(components, processes)
    assert {root.key: root.reference_only for root in roots} == {
        "child_a": True,
        "child_b": True,
        "root": False,
    }
    candidates = orchestration._deployment_root_candidates(roots)
    assert [(root.key, root.origin) for root in candidates] == [("root", "canonical_root")]

    target, error = orchestration._resolve_build_deployment_target(build_id)
    assert error is None, error
    assert (target.process_key, target.process_component_id) == ("root", _ROOT_ID)
    summary = {entry.key: entry for entry in target.component_summary.components}
    assert summary["root"].component_id == _ROOT_ID
    assert (summary["child_a"].status, summary["child_a"].component_id) == ("reused", "child-a-cid")
    assert (summary["child_b"].status, summary["child_b"].component_id) == ("reused", "child-b-cid")

    surface = _AccountSurface().install(monkeypatch)
    real = _deploy(build_id, dry_run=False)
    assert real["_success"] is True, real.get("errors")
    assert [call["component_id"] for call in surface.package_boundary] == [_ROOT_ID]
    assert "child-a-cid" not in json.dumps(surface.calls)
    assert "child-b-cid" not in json.dumps(surface.calls)


def test_reference_only_only_build_keeps_its_target(monkeypatch):
    """A build that authors NO root still deploys the process it references (pre-#158)."""
    applied, _ = _legacy_build({"name": "E158 Reference Only", "components": [_reference_child()]})
    build_id = applied["build_id"]
    assert applied["results"]["child"] == {
        "status": "reused",
        "component_id": _CHILD_ID,
        "type": "process",
        "name": "E158 existing child",
    }

    target, error = orchestration._resolve_build_deployment_target(build_id)
    assert error is None, error
    assert (target.process_key, target.process_component_id) == ("child", _CHILD_ID)
    assert target.process_status == "reused"
    assert orchestration._resolve_listener_metadata(build_id, target) is None

    surface = _AccountSurface().install(monkeypatch)
    real = _deploy(build_id, dry_run=False)
    assert real["_success"] is True, real.get("errors")
    assert surface.package_boundary == [{"component_id": _CHILD_ID, "package_version": build_id}]


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def _scheduled_with_wss_operation():
    return _typed_build(
        _request([_scheduled_unit()], list(_SCHEDULED_COMPONENTS) + [_WSS_LISTEN_OP])
    )[0], None


def _scheduled_with_listener_shaped_child():
    child = _reference_child(
        source={"connector_type": "wss", "action_type": "Listen", "operation_id": "$ref:" + _WSS_OP_KEY}
    )
    return _typed_build(
        _request([_scheduled_unit()], list(_SCHEDULED_COMPONENTS) + [_WSS_LISTEN_OP, child])
    )[0], child


@pytest.mark.parametrize(
    "make_build",
    [
        pytest.param(_scheduled_with_wss_operation, id="unrelated-wss-operation"),
        pytest.param(_scheduled_with_listener_shaped_child, id="listener-shaped-reference-child"),
    ],
)
def test_scheduled_root_with_wss_child_or_operation_stays_scheduled(make_build):
    applied, child = make_build()
    build_id = applied["build_id"]

    # NON-VACUITY: the decoys really are listener-shaped. The WSS component is a
    # genuine Listen operation, and the child's own config enters on it.
    assert wss_listen_action_from_config(_WSS_LISTEN_OP["config"]) == "Listen"
    # Exactly the child-bearing row carries a child, and that child listens.
    assert (child is None) is (make_build is _scheduled_with_wss_operation)
    assert child is None or legacy_config_entry(child["config"]).is_listener is True

    target, error = orchestration._resolve_build_deployment_target(build_id)
    assert error is None, error
    assert (target.process_key, target.process_component_id) == ("root", _ROOT_ID)
    assert orchestration._resolve_listener_metadata(build_id, target) is None
    assert orchestration._resolve_listener_metadata(build_id) is None

    planned = _deploy(build_id, dry_run=True, run_test=True)
    assert planned["_success"] is True, planned.get("errors")
    assert planned["listener_verify"]["status"] == "not_required"
    # ...so the Test-mode stage is NOT suppressed, as it would be for a listener.
    assert planned["execution"]["status"] == "planned"


_LISTENER_METADATA_BLOCK = {
    "object_name": "fromMetadata",
    "operation_type": "EXECUTE",
    "input_type": "singlejson",
    "output_type": "none",
    "http_method": "POST",
    "endpoint_path": "/ws/simple/executeFromMetadata",
}


def test_listener_metadata_cannot_reclassify_scheduled_root():
    """``validation_rules.listener`` is consulted only AFTER the entry classifies."""
    applied, _ = _legacy_build(
        {
            "name": "E158 Metadata Scheduled",
            "components": [APPLIABLE_CONN, APPLIABLE_OP],
            "processes": [_scheduled_unit().model_dump(mode="json")],
            "validation_rules": {"listener": dict(_LISTENER_METADATA_BLOCK)},
        }
    )
    build_id = applied["build_id"]
    entry, _components, _processes = _recorded(build_id)
    assert entry["spec"]["validation_rules"]["listener"] == _LISTENER_METADATA_BLOCK

    target, error = orchestration._resolve_build_deployment_target(build_id)
    assert error is None, error
    assert target.process_component_id == _ROOT_ID
    assert orchestration._resolve_listener_metadata(build_id, target) is None
    planned = _deploy(build_id, dry_run=True)
    assert planned["_success"] is True, planned.get("errors")
    assert planned["listener_verify"]["status"] == "not_required"

    # CONTROL: the same block on a root that DOES listen is used — so the block is
    # read, and the scheduled answer above is the classification refusing it.
    control, _ = _legacy_build(
        {
            "name": "E158 Metadata Listener",
            "components": list(_LISTENER_COMPONENTS),
            "processes": [_listener_unit().model_dump(mode="json")],
            "validation_rules": {"listener": dict(_LISTENER_METADATA_BLOCK)},
        }
    )
    control_target, control_error = orchestration._resolve_build_deployment_target(
        control["build_id"]
    )
    assert control_error is None, control_error
    meta = orchestration._resolve_listener_metadata(control["build_id"], control_target)
    assert meta is not None
    assert meta["endpoint_path"] == _LISTENER_METADATA_BLOCK["endpoint_path"]


# ---------------------------------------------------------------------------
# API Service Component routes
# ---------------------------------------------------------------------------


def test_canonical_asc_route_uses_selected_listener_and_resolved_id(monkeypatch):
    applied, boundary = _typed_build(
        _request([_listener_unit()], list(_LISTENER_COMPONENTS) + [_asc("root")])
    )
    # The plan ACCEPTED the route: a canonical listener root is a legal target.
    assert "API_SERVICE_ROUTE_PROCESS_NOT_LISTEN" not in _cause_codes(boundary.planned)
    build_id = applied["build_id"]
    asc_id = applied["results"]["api_service"]["component_id"]
    assert asc_id == "applied-api_service"
    assert applied["execution_order"].index("root") < applied["execution_order"].index(
        "api_service"
    )
    # Apply itself resolved the route's `$ref:root` to the canonical root's id.
    assert boundary.executed["api_service"]["routes"][0]["process"] == _ROOT_ID

    target, error = orchestration._resolve_build_deployment_target(build_id)
    assert error is None, error
    assert (target.process_key, target.process_component_id) == ("root", _ROOT_ID)

    entry, components, _processes = _recorded(build_id)
    binding = orchestration._resolve_asc_binding(
        entry, components, {"key": "root", "component_id": None}
    )
    assert binding is not None
    assert binding["key"] == "api_service"
    assert binding["route_process_ids"] == [_ROOT_ID]

    meta = orchestration._resolve_listener_metadata(build_id, target)
    assert meta is not None
    assert meta["publish_mode"] == "api_service"
    assert meta["api_service_component_id"] == asc_id
    assert meta["route_process_ids"] == [_ROOT_ID]

    planned = _deploy(build_id, dry_run=True)
    assert planned["listener_verify"]["status"] == "planned"
    assert planned["listener_verify"]["route_process_ids"] == [_ROOT_ID]

    # A real run on an advanced Shared Web Server publishes BOTH: the root under
    # its own id, then the ASC routing to it.
    surface = _AccountSurface(api_type="advanced").install(monkeypatch)
    real = _deploy(build_id, dry_run=False)
    assert real["_success"] is True, (real.get("errors"), real.get("error"))
    assert [call["component_id"] for call in surface.package_boundary] == [_ROOT_ID, asc_id]
    assert surface.package_boundary[1]["component_type"] == "webservice"
    assert real["listener_verify"]["status"] == "completed", real["listener_verify"]
    assert real["listener_verify"]["route_process_ids"] == [_ROOT_ID]


def test_canonical_scheduled_asc_route_is_refused():
    request = _request([_scheduled_unit()], list(_SCHEDULED_COMPONENTS) + [_asc("root")])
    raw = request.model_dump(mode="json")

    planned = build_integration_action(
        MagicMock(), _PROFILE, "plan", config={"authoring_request": raw}
    )
    assert planned["authoring_result"]["validation_report"]["is_valid"] is False
    refusals = [
        diagnostic
        for diagnostic in planned["authoring_result"]["errors"]
        if "API_SERVICE_ROUTE_PROCESS_NOT_LISTEN" in (diagnostic.get("cause_codes") or ())
    ]
    assert [d["path"] for d in refusals] == ["/components/api_service"]

    compiled = build_integration_action(
        MagicMock(), _PROFILE, "compile", config={"authoring_request": raw}
    )
    assert compiled["_success"] is False
    assert "API_SERVICE_ROUTE_PROCESS_NOT_LISTEN" in _cause_codes(compiled)

    # ...and an apply of the same spec refuses before ANY write.
    def _no_write(*_args, **_kwargs):
        raise AssertionError("a refused plan must not write")

    with patch(_EXECUTE, side_effect=_no_write), patch(_CREATE, side_effect=_no_write):
        refused = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={
                "integration_spec": {
                    "name": "E158 Scheduled ASC",
                    "components": list(_SCHEDULED_COMPONENTS) + [_asc("root")],
                    "processes": [_scheduled_unit().model_dump(mode="json")],
                },
                "dry_run": False,
            },
        )
    assert refused["_success"] is False
    (step,) = refused["unresolvable_steps"]
    assert step["key"] == "api_service"
    assert step["validation_error"]["error_code"] == "API_SERVICE_ROUTE_PROCESS_NOT_LISTEN"
    assert step["validation_error"]["details"] == {
        "ref_key": "root",
        "actual_role": "process_ir_root",
    }


# ---------------------------------------------------------------------------
# The listener's operation reference
# ---------------------------------------------------------------------------


def _wss_op_with_connection():
    op = copy.deepcopy(_WSS_LISTEN_OP)
    op["config"]["connection_ref_key"] = _REST_CONN_KEY
    op["depends_on"] = [_REST_CONN_KEY]
    return op


_LISTENER_OPERATION_INVALID = "PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID"


@pytest.mark.parametrize(
    "listener_patch,components,expected",
    [
        pytest.param(
            {"operation_ref": "$ref:" + _REST_POST_KEY},
            _LISTENER_COMPONENTS,
            _LISTENER_OPERATION_INVALID,
            id="non-wss-operation",
        ),
        pytest.param(
            {},
            (_REST_CONN, _REST_POST_OP, _wss_op_with_connection()),
            _LISTENER_OPERATION_INVALID,
            id="connection-bearing-wss-operation",
        ),
        pytest.param(
            # Role check: a CONNECTION is not an operation at all.
            {"operation_ref": "$ref:" + _REST_CONN_KEY},
            _LISTENER_COMPONENTS,
            "PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND",
            id="connection-component",
        ),
    ],
)
def test_wss_entry_ref_is_operation_only_and_role_checked(listener_patch, components, expected):
    planned, compiled = _plan_and_compile_patched_listener(listener_patch, components)

    assert expected in _cause_codes(planned), _cause_codes(planned)
    assert compiled["_success"] is False
    assert expected in _cause_codes(compiled), _cause_codes(compiled)
    # A reference refusal: plan REPORTS it (invalid), compile REFUSES, and both
    # blame the listener's own operation reference — the root's first step.
    assert planned["authoring_result"]["validation_report"]["is_valid"] is False
    assert compiled["error_code"] == "AUTHORING_COMPILE_BLOCKED"
    for diagnostics in (planned["authoring_result"]["errors"], compiled["authoring_diagnostics"]):
        blamed = sorted(
            {
                diagnostic["path"]
                for diagnostic in diagnostics
                if expected in ({diagnostic.get("code")} | set(diagnostic.get("cause_codes") or ()))
            }
        )
        assert blamed == ["/body/steps/0/operation_ref"], blamed

    # CONTROL: the unpatched listener over a genuine WSS Listen operation compiles.
    control = _request([_listener_unit()], _LISTENER_COMPONENTS).model_dump(mode="json")
    compiled = build_integration_action(
        MagicMock(), _PROFILE, "compile", config={"authoring_request": control}
    )
    assert compiled["_success"] is True, _cause_codes(compiled)


def test_a_listener_that_authors_a_connection_is_a_schema_refusal():
    """Operation-ONLY: a listener that authors a connection is refused by the
    schema, not ignored — the request never parses, so plan refuses outright."""
    expected = "PROCESS_IR_SCHEMA_LISTENER_CONNECTION_FORBIDDEN"
    planned, compiled = _plan_and_compile_patched_listener(
        {"connection_ref": "$ref:" + _REST_CONN_KEY}, _LISTENER_COMPONENTS
    )
    assert planned["_success"] is False
    assert expected in _cause_codes(planned), _cause_codes(planned)
    assert compiled["_success"] is False
    assert expected in _cause_codes(compiled), _cause_codes(compiled)


def _plan_and_compile_patched_listener(listener_patch, components):
    raw = _request([_listener_unit()], components).model_dump(mode="json")
    raw["intent"]["units"][0]["process_ir"]["body"]["steps"][0].update(listener_patch)

    def _no_write(*_args, **_kwargs):
        raise AssertionError("a refused listener must not write")

    with patch(_EXECUTE, side_effect=_no_write), patch(_CREATE, side_effect=_no_write):
        planned = build_integration_action(
            MagicMock(), _PROFILE, "plan", config={"authoring_request": raw}
        )
        compiled = build_integration_action(
            MagicMock(), _PROFILE, "compile", config={"authoring_request": raw}
        )
    return planned, compiled


# ---------------------------------------------------------------------------
# One entry-recognition authority
# ---------------------------------------------------------------------------

_LEGACY_SHAPES = {
    "pipeline-listener-stage": (
        {
            "process_kind": "sync_pipeline",
            "pipeline": {
                "stages": [
                    {"key": "listen", "kind": "listener",
                     "config": {"primitive": "wss_listen", "operation_id": "$ref:wss_op"}},
                    {"key": "send", "kind": "send",
                     "config": {"primitive": "rest_send", "action_type": "POST",
                                "connection_id": "C1", "operation_id": "O1"}},
                ],
            },
        },
        ProcessEntryV1("listener", "$ref:wss_op"),
    ),
    "pipeline-listener-stage-blank-operation": (
        {"pipeline": {"stages": [{"key": "listen", "kind": " Listener ",
                                  "config": {"operation_id": "  "}}]}},
        ProcessEntryV1("listener", None),
    ),
    "source-wss-listen": (
        {"source": {"connector_type": "wss", "action_type": "Listen", "operation_id": "WSSOP-LIT"}},
        ProcessEntryV1("listener", "WSSOP-LIT"),
    ),
    "source-web-services-server-alias": (
        {"source": {"connector_type": " Web_Services_Server ", "action_type": "Listen",
                    "operation_id": " $ref:wss_op "}},
        ProcessEntryV1("listener", "$ref:wss_op"),
    ),
    "source-wss-other-action": (
        {"source": {"connector_type": "wss", "action_type": "Get", "operation_id": "X"}},
        ProcessEntryV1("scheduled"),
    ),
    "source-non-wss-listen": (
        {"source": {"connector_type": "rest", "action_type": "Listen", "operation_id": "X"}},
        ProcessEntryV1("scheduled"),
    ),
    "unrelated-process-kind": ({"process_kind": "database_to_api_sync"}, ProcessEntryV1("scheduled")),
    "empty": ({}, ProcessEntryV1("scheduled")),
    "not-a-mapping": ("listener", ProcessEntryV1("scheduled")),
    "none": (None, ProcessEntryV1("scheduled")),
}


def _string_constants(module):
    tree = ast.parse(inspect.getsource(module))
    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }


def test_a_reused_operations_authored_endpoint_is_not_the_accounts(monkeypatch):
    """#158 ARCH-158-r1-01: a reuse binding names a component apply never writes,
    so its authored endpoint fields are not what the account serves. Reading them
    suppressed the account read and verified an endpoint the platform does not
    route: the deploy probed the authored object name while the stored operation
    kept its own. Only a component this build writes may describe itself."""
    account_id = _ACCOUNT_WSS_OP_ID
    reused = {
        "key": _WSS_OP_KEY, "type": "connector-action", "action": "create",
        "config": {"reference_only": True, "component_id": account_id,
                   "connector_type": "wss", "operation_mode": "listen",
                   "object_name": "wrongIntake158", "operation_type": "EXECUTE",
                   "input_type": "singlejson", "output_type": "none"},
    }
    entry = {"results": {_WSS_OP_KEY: {"status": "reused", "component_id": account_id}}}
    assert orchestration._listen_operation_facts_in_build(entry, [reused], account_id) is None
    # CONTROL: the same config as a CREATE describes the component it creates.
    created = {
        **reused,
        "config": {k: v for k, v in reused["config"].items() if k != "reference_only"},
    }
    facts = orchestration._listen_operation_facts_in_build(entry, [created], account_id)
    assert facts and facts["object_name"] == "wrongIntake158"


def test_a_stray_listener_stage_is_not_the_entry_of_another_process_kind():
    """#158 ARCH-158-r1-02: plan §3 requires the ACTIVE entry, not any listener
    stage. A `database_to_api_sync` config carrying a listener stage passes the
    builder's validation and emits its scheduled database start, yet the
    recognizer read the stage and reported a listener entry — the misclassified
    root would be treated as a listener for ASC routing and verification."""
    from boomi_mcp.categories.components.builders.process_flow_builder import (
        ProcessFlowBuilder,
    )

    stray = {"stages": [{"kind": "listener", "config": {"operation_id": "WSSOP"}}]}
    config = {
        "process_kind": "database_to_api_sync",
        "source": {"connector_type": "database", "connection_id": "00000000-0000-0000-0000-000000000001",
                   "operation_id": "00000000-0000-0000-0000-000000000002", "action_type": "Get"},
        "transform": {"mode": "passthrough"},
        "target": {"connector_type": "rest", "connection_id": "00000000-0000-0000-0000-000000000001",
                   "operation_id": "00000000-0000-0000-0000-000000000002", "action_type": "POST"},
        "reliability": {"retry_count": 0, "dlq": {"mode": "disabled"}},
        "pipeline": stray,
    }
    # The emitter accepts it and starts on the DATABASE source, with no Listen.
    assert ProcessFlowBuilder.validate_config(config) is None
    xml = ProcessFlowBuilder.build(config, name="P")
    assert 'actionType="Listen"' not in xml and 'connectorType="database"' in xml
    assert legacy_config_entry(config) == ProcessEntryV1("scheduled")
    assert integration_builder._process_config_has_wss_listen(config) is False
    # CONTROL 1: the pipeline builder's own kind still enters on its stage.
    assert legacy_config_entry({"process_kind": "sync_pipeline", "pipeline": stray}) == ProcessEntryV1(
        "listener", "WSSOP"
    )
    # CONTROL 2: an abbreviated shape naming no kind is read as before.
    assert legacy_config_entry({"pipeline": stray}) == ProcessEntryV1("listener", "WSSOP")


def test_listener_recognizers_share_compiler_entry_authority(monkeypatch):
    from boomi_mcp.compiler.process_ir.contracts import SymbolTableV1
    from boomi_mcp.compiler.process_ir.entry_policy import classify_entry
    from boomi_mcp.compiler.process_ir.execution_profile import (
        derive_process_execution_profile,
    )
    from boomi_mcp.compiler.process_ir.lowering import lower_process_ir_to_cfg

    # 1. The legacy shapes: the two recognizers agree with the authority on every one.
    for name, (config, expected) in _LEGACY_SHAPES.items():
        entry = legacy_config_entry(config)
        assert entry == expected, name
        assert integration_builder._process_config_has_wss_listen(config) is entry.is_listener, name
        assert orchestration._listener_operation_ref_from_process(config) == (
            entry.operation_ref if entry.is_listener else None
        ), name

    # 2. ...and they AGREE BECAUSE they delegate, not by coincidence: replace the
    #    authority and both follow it.
    with monkeypatch.context() as patched:
        patched.setattr(
            process_entry,
            "legacy_config_entry",
            lambda _config: ProcessEntryV1("listener", "$ref:sentinel"),
        )
        assert integration_builder._process_config_has_wss_listen({}) is True
        assert orchestration._listener_operation_ref_from_process({}) == "$ref:sentinel"
    assert integration_builder._process_config_has_wss_listen({}) is False

    # 3. A canonical root is classified by the COMPILER's entry policy — the same
    #    answer as the execution profile it was materialized with.
    for doc, expected_form, expected_ref in (
        (_LISTENER_DOC, "listener", _LISTEN_STEP["operation_ref"]),
        (APPLIABLE_IR_DOC, "scheduled", None),
    ):
        cfg = lower_process_ir_to_cfg(parse_process_ir_v1(doc))
        entry = canonical_root_entry(doc)
        assert (entry.form, entry.operation_ref) == (expected_form, expected_ref)
        assert classify_entry(cfg) == entry.form
        assert derive_process_execution_profile(cfg, SymbolTableV1(symbols=())) == entry.form

    # 4. Neither module keeps a private WSS alias set — by name, or by content.
    assert not hasattr(integration_builder, "_WSS_SOURCE_CONNECTOR_ALIASES")
    assert not hasattr(orchestration, "_WSS_CONNECTOR_ALIASES")
    for module in (integration_builder, orchestration, process_entry):
        leaked = _string_constants(module) & {"web_services", "web_services_server"}
        assert leaked == set(), (module.__name__, leaked)


# ---------------------------------------------------------------------------
# A REUSED listener root
# ---------------------------------------------------------------------------


_REUSED_ROOT_ID = "existing-root-cid-158"


def _existing_listener_root(_client, comp):
    """The account already holds a process named like the root; nothing else."""
    if getattr(comp, "type", None) == "process":
        return [{"component_id": _REUSED_ROOT_ID, "name": "E158 Root", "folder_name": "Home"}]
    return []


@pytest.mark.parametrize(
    "stored_xml,recorded_operation_id",
    [
        pytest.param(_NOACTION_START_XML, _LISTEN_START_OPERATION_ID, id="noaction-start"),
        pytest.param(_LISTEN_START_XML, "WSSOP-OTHER", id="listen-on-another-operation"),
        pytest.param(None, _LISTEN_START_OPERATION_ID, id="start-unreadable"),
    ],
)
def test_reused_listener_start_mismatch_refuses_before_package(
    stored_xml, recorded_operation_id, monkeypatch
):
    build_id, real, surface = _deploy_reused_listener(
        stored_xml, recorded_operation_id, monkeypatch
    )
    assert real["_success"] is False
    assert _error_codes(real) == ["BUILD_LISTENER_ENTRY_MISMATCH"]
    assert real["errors"][0]["details"]["process_key"] == "root"
    # Refused BEFORE anything is packaged: no router was reached at all.
    assert surface.package_boundary == []
    assert surface.calls == []


def test_reused_listener_matching_start_proceeds_to_package(monkeypatch):
    """CONTROL for the mismatch refusal: a stored Listen start on the build's own
    operation is confirmed, and the deploy proceeds to packaging and verify."""
    build_id, real, surface = _deploy_reused_listener(
        _LISTEN_START_XML, _LISTEN_START_OPERATION_ID, monkeypatch
    )
    assert real["_success"] is True, (real.get("errors"), real.get("error"))
    assert surface.package_boundary[0] == {
        "component_id": _REUSED_ROOT_ID,
        "package_version": build_id,
    }
    assert real["listener_verify"]["status"] == "completed", real["listener_verify"]


def _deploy_reused_listener(stored_xml, recorded_operation_id, monkeypatch):
    stored = {_REUSED_ROOT_ID: stored_xml} if stored_xml is not None else {}
    applied, _ = _typed_build(
        _request([_listener_unit()], _LISTENER_COMPONENTS, conflict_policy="reuse"),
        _ApplyBoundary(ids={_WSS_OP_KEY: recorded_operation_id}, stored=stored),
        existing=_existing_listener_root,
    )
    build_id = applied["build_id"]
    assert applied["results"]["root"]["status"] == "reused"
    assert applied["results"]["root"]["component_id"] == _REUSED_ROOT_ID
    assert applied["results"][_WSS_OP_KEY]["component_id"] == recorded_operation_id

    target, error = orchestration._resolve_build_deployment_target(build_id)
    assert error is None, error
    assert (target.process_component_id, target.process_status) == (_REUSED_ROOT_ID, "reused")
    assert orchestration._resolve_listener_metadata(build_id, target) is not None

    surface = _AccountSurface(stored=stored).install(monkeypatch)
    real = _deploy(build_id, dry_run=False)
    # The stored start is read in every case — that read is the whole check.
    assert surface.reads[:1] == [_REUSED_ROOT_ID]
    return build_id, real, surface


# ---------------------------------------------------------------------------
# A listener whose Listen operation is NOT in the build (QA-158-r1-02)
# ---------------------------------------------------------------------------

#: The id of the Listen operation the account already holds, which the build
#: reuses by reference — so its config carries no endpoint field at all.
_ACCOUNT_WSS_OP_ID = "existing-wss-op-158"
_ACCOUNT_WSS_OBJECT_NAME = "reusedIntake158"
#: What the account stores for it: the WSS builder's own bytes for the
#: live-captured config (renera op 601cf5a3, M6 #12) under this object name.
_ACCOUNT_WSS_OP_XML = WssListenerOperationBuilder().build(
    operation_mode="listen",
    component_name="E158 existing listener op",
    object_name=_ACCOUNT_WSS_OBJECT_NAME,
    operation_type="EXECUTE",
    input_type="singlejson",
    output_type="none",
)
_ACCOUNT_WSS_OP_READBACK = {"type": "connector-action", "xml": _ACCOUNT_WSS_OP_XML}
_REFERENCE_ONLY_WSS_OP = {
    "key": _WSS_OP_KEY,
    "type": "connector-action",
    "name": "E158 existing listener op",
    "action": "create",
    "config": {
        "reference_only": True,
        "connector_type": "wss",
        "component_id": _ACCOUNT_WSS_OP_ID,
        "operation_mode": "listen",
    },
}
#: The live-settled M6 formula over the ACCOUNT's operation, never the build's.
_ACCOUNT_ENDPOINT_PATH = "/ws/simple/executeReusedIntake158"


def _reference_only_listener_build(*extra, stored=None, **intent_extra):
    components = (_REST_CONN, _REST_POST_OP, _REFERENCE_ONLY_WSS_OP) + extra
    boundary = _ApplyBoundary(
        stored={_ACCOUNT_WSS_OP_ID: _ACCOUNT_WSS_OP_READBACK, **(stored or {})}
    )
    applied, boundary = _typed_build(
        _request([_listener_unit()], components, **intent_extra),
        boundary,
        existing=intent_extra.get("conflict_policy") and _existing_listener_root,
    )
    # NON-VACUITY: the build really does not carry the operation's endpoint.
    assert applied["results"][_WSS_OP_KEY]["component_id"] == _ACCOUNT_WSS_OP_ID
    _entry, components_recorded, _processes = _recorded(applied["build_id"])
    [op] = [c for c in components_recorded if c["key"] == _WSS_OP_KEY]
    assert "object_name" not in op["config"]
    return applied, boundary


def test_a_listener_whose_operation_is_not_in_the_build_is_still_a_listener(monkeypatch):
    """QA-158-r1-02: the classification follows the entry, never the endpoint fields.

    The root's Listen operation is reused by reference, so the build carries no
    object name. Orchestrate still treats the target as a listener: the plan
    says its endpoint is not known yet and suppresses Test mode, and the real
    run reads the operation from the ACCOUNT before anything is packaged and
    verifies the endpoint derived from what the account stores.
    """
    applied, _ = _reference_only_listener_build()
    build_id = applied["build_id"]

    planned = _deploy(build_id, dry_run=True, run_test=True)
    assert planned["_success"] is True, planned.get("errors")
    assert planned["listener_verify"]["status"] == "planned"
    assert planned["listener_verify"].get("endpoint_path") is None
    assert planned["execution"]["status"] == "not_required"
    assert [
        w for w in planned.get("warnings") or () if w.startswith("[LISTENER_ENDPOINT_FROM_ACCOUNT]")
    ], planned.get("warnings")

    surface = _AccountSurface(stored={_ACCOUNT_WSS_OP_ID: _ACCOUNT_WSS_OP_XML}).install(
        monkeypatch
    )
    real = _deploy(build_id, dry_run=False)
    assert real["_success"] is True, (real.get("errors"), real.get("error"))
    assert surface.reads == [_ACCOUNT_WSS_OP_ID]
    assert [call["component_id"] for call in surface.package_boundary] == [_ROOT_ID]
    assert real["listener_verify"]["status"] == "completed", real["listener_verify"]
    assert real["listener_verify"]["endpoint_path"] == _ACCOUNT_ENDPOINT_PATH
    assert [p["url"] for p in surface.probes] == ["http://atom.local:9090" + _ACCOUNT_ENDPOINT_PATH]


@pytest.mark.parametrize(
    "stored",
    [
        pytest.param({}, id="operation-unreadable"),
        pytest.param({_ACCOUNT_WSS_OP_ID: _NON_PROCESS_READBACK}, id="not-a-listen-operation"),
    ],
)
def test_a_listener_endpoint_the_account_cannot_supply_refuses_before_package(
    stored, monkeypatch
):
    """Fail CLOSED: a listener whose endpoint cannot be determined is neither
    verified nor published, and nothing is packaged for it."""
    applied, _ = _reference_only_listener_build()
    surface = _AccountSurface(stored=stored).install(monkeypatch)
    real = _deploy(applied["build_id"], dry_run=False)
    assert real["_success"] is False
    assert _error_codes(real) == ["LISTENER_ENDPOINT_UNRESOLVED"]
    assert real["errors"][0]["details"]["process_key"] == "root"
    assert surface.reads == [_ACCOUNT_WSS_OP_ID]
    assert surface.package_boundary == []
    assert surface.calls == []


def test_a_reused_root_is_checked_even_when_its_operation_is_not_in_the_build(monkeypatch):
    """The reused-root guard keys off the entry: a build that does not carry its
    operation's endpoint can no longer skip it (the bypass QA-158-r1-02 measured)."""
    mismatched = _LISTEN_START_XML  # starts on WSSOP-1, not the account operation
    applied, _ = _reference_only_listener_build(
        stored={_REUSED_ROOT_ID: mismatched}, conflict_policy="reuse"
    )
    assert applied["results"]["root"]["status"] == "reused"
    surface = _AccountSurface(
        stored={_REUSED_ROOT_ID: mismatched, _ACCOUNT_WSS_OP_ID: _ACCOUNT_WSS_OP_XML}
    ).install(monkeypatch)
    real = _deploy(applied["build_id"], dry_run=False)
    assert real["_success"] is False
    assert _error_codes(real) == ["BUILD_LISTENER_ENTRY_MISMATCH"]
    assert surface.package_boundary == []
    assert surface.calls == []

    # CONTROL: the same build over a stored start that listens on the account
    # operation proceeds, reading the start and then the operation.
    matching = _LISTEN_START_XML.replace(_LISTEN_START_OPERATION_ID, _ACCOUNT_WSS_OP_ID)
    assert matching != _LISTEN_START_XML
    applied, _ = _reference_only_listener_build(
        stored={_REUSED_ROOT_ID: matching}, conflict_policy="reuse"
    )
    surface = _AccountSurface(
        stored={_REUSED_ROOT_ID: matching, _ACCOUNT_WSS_OP_ID: _ACCOUNT_WSS_OP_XML}
    ).install(monkeypatch)
    real = _deploy(applied["build_id"], dry_run=False)
    assert real["_success"] is True, (real.get("errors"), real.get("error"))
    assert surface.reads[:2] == [_REUSED_ROOT_ID, _ACCOUNT_WSS_OP_ID]
    assert real["listener_verify"]["status"] == "completed", real["listener_verify"]


def test_an_asc_routed_to_a_listener_whose_operation_is_reused_is_published(monkeypatch):
    """The ASC publish no longer depends on the build carrying the operation's
    endpoint: the plan names the API Service publish mode, and the real run
    packages the ASC and probes the /ws/rest route derived from the account."""
    applied, _ = _reference_only_listener_build(_asc("root"))
    build_id = applied["build_id"]
    asc_id = applied["results"]["api_service"]["component_id"]

    planned = _deploy(build_id, dry_run=True)
    assert planned["listener_verify"]["status"] == "planned"
    assert planned["listener_verify"]["publish_mode"] == "api_service"
    assert planned["listener_verify"]["api_service_component_id"] == asc_id

    surface = _AccountSurface(
        api_type="advanced", stored={_ACCOUNT_WSS_OP_ID: _ACCOUNT_WSS_OP_XML}
    ).install(monkeypatch)
    real = _deploy(build_id, dry_run=False)
    assert real["_success"] is True, (real.get("errors"), real.get("error"))
    assert [call["component_id"] for call in surface.package_boundary] == [_ROOT_ID, asc_id]
    assert real["listener_verify"]["status"] == "completed", real["listener_verify"]
    assert real["listener_verify"]["endpoint_path"].endswith("/" + _ACCOUNT_WSS_OBJECT_NAME)
    assert real["listener_verify"]["endpoint_path"].startswith("/ws/rest/")


# ---------------------------------------------------------------------------
# The API Service route method follows the operation TYPE (QA-158-r2-01)
# ---------------------------------------------------------------------------


def _wss_op_with(**config):
    op = copy.deepcopy(_WSS_LISTEN_OP)
    op["config"].update(config)
    return op


def _account_wss_op_xml(**params):
    base = dict(
        operation_mode="listen", component_name="E158 existing listener op",
        object_name=_ACCOUNT_WSS_OBJECT_NAME, operation_type="EXECUTE",
        input_type="singlejson", output_type="none",
    )
    base.update(params)
    return WssListenerOperationBuilder().build(**base)


def test_an_asc_route_nothing_can_call_is_refused_before_package(monkeypatch):
    """A GET or QUERY operation behind an all-inherit API Service route is served on
    GET alone, and GET is refused for an operation expecting input (measured,
    evidence cap158-r3-wss-method-matrix). Such a route is refused before any
    package — in the plan too, when the build carries the facts."""
    wss_op = _wss_op_with(operation_type="QUERY", input_type="singlejson")
    applied, _ = _typed_build(_request(
        [_listener_unit()], (_REST_CONN, _REST_POST_OP, wss_op, _asc("root"))
    ))
    build_id = applied["build_id"]
    for dry_run in (True, False):
        surface = _AccountSurface(api_type="advanced").install(monkeypatch)
        result = _deploy(build_id, dry_run=dry_run)
        assert result["_success"] is False, dry_run
        assert _error_codes(result) == ["LISTENER_ASC_ROUTE_UNCALLABLE"], dry_run
        details = result["errors"][0]["details"]
        assert (details["http_method"], details["input_type"]) == ("GET", "singlejson")
        assert surface.package_boundary == [] and surface.calls == []

    # ...and when the operation is reused, the same facts arrive from the
    # ACCOUNT on the real run, and the same refusal follows that read.
    query_xml = _account_wss_op_xml(operation_type="QUERY", input_type="singlejson")
    applied, _ = _reference_only_listener_build(
        _asc("root"),
        stored={_ACCOUNT_WSS_OP_ID: {"type": "connector-action", "xml": query_xml}},
    )
    planned = _deploy(applied["build_id"], dry_run=True)
    assert planned["_success"] is True, planned.get("errors")
    surface = _AccountSurface(
        api_type="advanced", stored={_ACCOUNT_WSS_OP_ID: query_xml}
    ).install(monkeypatch)
    real = _deploy(applied["build_id"], dry_run=False)
    assert _error_codes(real) == ["LISTENER_ASC_ROUTE_UNCALLABLE"]
    assert surface.reads == [_ACCOUNT_WSS_OP_ID]
    assert surface.package_boundary == [] and surface.calls == []


@pytest.mark.parametrize(
    "operation_type,input_type,method",
    [
        pytest.param("QUERY", "none", "GET", id="query-without-input-is-GET"),
        pytest.param("UPDATE", "singlejson", "PUT", id="update-is-PUT"),
        pytest.param("DELETE", "singlejson", "DELETE", id="delete-is-DELETE"),
        pytest.param("UPSERT", "none", "POST", id="upsert-is-POST"),
    ],
)
def test_the_asc_probe_uses_the_method_the_operation_type_serves(
    operation_type, input_type, method, monkeypatch
):
    """CONTROL for the refusal, and the fix itself: every callable combination
    deploys, and verification probes the method the platform serves it on."""
    wss_op = _wss_op_with(operation_type=operation_type, input_type=input_type)
    applied, _ = _typed_build(_request(
        [_listener_unit()], (_REST_CONN, _REST_POST_OP, wss_op, _asc("root"))
    ))
    planned = _deploy(applied["build_id"], dry_run=True)
    assert planned["listener_verify"]["http_method"] == method
    surface = _AccountSurface(api_type="advanced").install(monkeypatch)
    real = _deploy(applied["build_id"], dry_run=False)
    assert real["_success"] is True, (real.get("errors"), real.get("error"))
    assert [p["method"] for p in surface.probes] == [method]
    assert real["listener_verify"]["status"] == "completed", real["listener_verify"]


# ---------------------------------------------------------------------------
# The empty-overrides guard
# ---------------------------------------------------------------------------


def _extension_unit(**kwargs):
    extensions = ProcessExtensionBindingsV1(
        connections=(
            ProcessConnectionOverrideV1(
                connection_id="$ref:conn",
                connector_type="rest",
                fields=(ProcessOverrideFieldV1(id="url", label="Base URL"),),
            ),
        )
    )
    return _scheduled_unit(process_extensions=extensions, **kwargs)


def test_canonical_root_extensions_block_empty_overrides(monkeypatch):
    applied, _ = _typed_build(_request([_extension_unit()], _SCHEDULED_COMPONENTS))
    build_id = applied["build_id"]
    _entry, _components, processes = _recorded(build_id)
    # The extension lives on the canonical root's ENVELOPE — there is no legacy
    # process component whose config could carry it.
    (unit,) = processes
    assert unit["envelope"]["process_extensions"]["connections"][0]["connection_id"] == "$ref:conn"
    assert orchestration._build_declares_process_extensions(build_id) is True

    planned = _deploy(build_id, dry_run=True, process_overrides={})
    assert planned["_success"] is False
    assert _error_codes(planned) == ["EMPTY_PROCESS_OVERRIDES_REJECTED"]

    surface = _AccountSurface().install(monkeypatch)
    real = _deploy(build_id, dry_run=False, process_overrides={})
    assert _error_codes(real) == ["EMPTY_PROCESS_OVERRIDES_REJECTED"]
    assert surface.calls == [] and surface.package_boundary == []

    # CONTROL 1: overrides NOT supplied — allowed, with the steering warning that
    # proves the extension was seen.
    omitted = _deploy(build_id, dry_run=True)
    assert omitted["_success"] is True, omitted.get("errors")
    assert any("PROCESS_OVERRIDES_NOT_SUPPLIED" in w for w in omitted.get("warnings") or ())

    # CONTROL 2: the same root WITHOUT extensions accepts an explicit empty set —
    # the guard is keyed to the envelope, not always on.
    bare, _ = _typed_build(_request([_scheduled_unit()], _SCHEDULED_COMPONENTS))
    assert orchestration._build_declares_process_extensions(bare["build_id"]) is False
    accepted = _deploy(bare["build_id"], dry_run=True, process_overrides={})
    assert accepted["_success"] is True, accepted.get("errors")
