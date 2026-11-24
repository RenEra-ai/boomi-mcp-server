"""The single immutable production registration tuple (issue #145 M12.10).

A literal tuple, not a package scan. ``patterns.registry`` scans its package at
import, which means the set of executable patterns depends on what happened to be
importable — and on import ORDER when two modules register the same name. For a
layer whose whole job is to make executable code provable, that is the wrong
default: a recipe exists here because a line below says so.

Three of the four entry kinds are represented here; the registry's ability to
TELL ALL FOUR APART is an acceptance criterion, and the fourth
(``compatibility_adapter``) is exercised through ``build_test_registry`` since
#159 S3 retired the production adapters:

* the six executable recipes (#159 S3): the typed fan-out 2.0.0 (the default,
  with a component-material channel), API->database 1.0.0, database->API 1.0.0,
  the two HTTP listeners and the Stop-only stub — each its id's default and each
  emitting one canonical process unit
* one constraint-only entry
* one advisory entry — the doctrine pointer, with no executor to call

#159 S3 retired fan-out 1.0.0, the slot-based sync presets (API->API and
API->database 0.1.0) and the three compatibility adapters. Their id constants
stay exported below only while isolated legacy modules (#160) still name them.
"""

from __future__ import annotations

from typing import Tuple

from .. import component_material as _component_material
from ...compiler.process_ir.connector_capabilities import (
    CONNECTOR_CALL_CAPABILITIES_V1 as _CONNECTOR_CALLS,
    REST_FAMILY as _REST_FAMILY,
)
from ..contracts import (
    RecipeCapabilityRequirementV1,
    RecipeConflictPolicyV1,
    RecipeExampleV1,
    RecipeRegistrationV1,
)
from . import http_listener_to_db as _listener_db
from . import http_listener_to_rest as _listener_rest
from .database_to_api_sync import (
    DatabaseToApiSyncInputV1,
    DatabaseToApiSyncMaterialV1,
    emit_database_to_api_sync,
    prepare_database_to_api_sync,
)
from .fanout import (
    ComposeDbRestFanoutInputV2,
    FanoutComponentMaterialV2,
    emit_db_rest_fanout_v2,
    prepare_db_rest_fanout_v2,
)
from .stub_minimal import StubMinimalInputV1, emit_stub_minimal
from .sync import (
    ApiToDatabaseSyncInputV1,
    ApiToDatabaseSyncMaterialV1,
    SyncRecipeInputV1,
    emit_api_to_database_sync_v1,
    prepare_api_to_database_sync_v1,
)

# Recipe ids. Namespaced by owner so a future third-party registration cannot
# collide with a built-in by accident.
RECIPE_DB_REST_FANOUT = "boomi.compose.db_rest_fanout"
#: #159 S3: retired (no registration). Still exported for the isolated legacy
#: archetype module until #160 deletes it.
RECIPE_API_TO_API_SYNC = "boomi.archetype.api_to_api_sync"
RECIPE_API_TO_DATABASE_SYNC = "boomi.archetype.api_to_database_sync"
RECIPE_DATABASE_TO_API_SYNC = "boomi.archetype.database_to_api_sync"
RECIPE_HTTP_LISTENER_TO_DB = "boomi.archetype.http_listener_to_db"
RECIPE_HTTP_LISTENER_TO_REST = "boomi.archetype.http_listener_to_rest"
RECIPE_STUB_MINIMAL = "boomi.archetype.stub_minimal"
RECIPE_CONSTRAINT_INBOUND_VALIDATE = "boomi.constraint.inbound_validate"
RECIPE_ADVISORY_INTEGRATION_DESIGN = "boomi.advisory.integration_design"
#: #159 S3: the three compatibility adapters are retired (no registrations). The
#: ids stay exported only while isolated legacy callers still import them (#160).
ADAPTER_COMPOSE_ARCHETYPES = "boomi.adapter.compose_archetypes"
ADAPTER_API_TO_API_SYNC = "boomi.adapter.api_to_api_sync"
ADAPTER_API_TO_DATABASE_SYNC = "boomi.adapter.api_to_database_sync"

#: #159 S1 registered 2.0.0 beside 1.0.0; #159 S3 retired 1.0.0 and promoted
#: this one to the default.
_FANOUT_V2_VERSION = "2.0.0"
#: #159 S2 registered the typed API->DB preset beside 0.1.0; #159 S3 retired
#: 0.1.0 and promoted this one to the default.
_API_DB_V1_VERSION = "1.0.0"
#: #159 S2: the typed presets with no earlier version; each is its id's default.
_PRESET_VERSION = "1.0.0"
_CONSTRAINT_VERSION = "1.0.0"
_ADVISORY_VERSION = "1.0.0"

# Merge rules must be canonically sorted — the contract enforces it, so the
# registry revision cannot change just because someone reordered a literal.
_SYNC_CONFLICT_POLICY = RecipeConflictPolicyV1(
    mode="error_unless_declared",
    merge_rules=("dedupe_identical_constraint",),
)

_FANOUT_CONFLICT_POLICY = RecipeConflictPolicyV1(
    mode="error_unless_declared",
    merge_rules=("append_root_terminal_leg", "dedupe_identical_constraint"),
)

_CONSTRAINT_CONFLICT_POLICY = RecipeConflictPolicyV1(
    mode="error_unless_declared",
    merge_rules=("dedupe_identical_constraint",),
)

_TARGET_EMITTER = RecipeCapabilityRequirementV1(
    authority="process_emitter",
    subject="connectoraction_target",
    required_state="supported",
)
_SOURCE_EMITTER = RecipeCapabilityRequirementV1(
    authority="process_emitter",
    subject="connectoraction_source",
    required_state="supported",
)
_MAP_EMITTER = RecipeCapabilityRequirementV1(
    authority="process_emitter", subject="map", required_state="supported"
)
_BRANCH_EMITTER = RecipeCapabilityRequirementV1(
    authority="process_emitter", subject="branch", required_state="supported"
)
_CACHE_LOAD_EMITTER = RecipeCapabilityRequirementV1(
    authority="process_emitter", subject="doccacheload", required_state="supported"
)
_CACHE_RETRIEVE_EMITTER = RecipeCapabilityRequirementV1(
    authority="process_emitter", subject="doccacheretrieve", required_state="supported"
)
_CATCH_EMITTER = RecipeCapabilityRequirementV1(
    authority="process_emitter", subject="catcherrors", required_state="supported"
)
_NOTIFY_EMITTER = RecipeCapabilityRequirementV1(
    authority="process_emitter", subject="notify", required_state="supported"
)
_LISTEN_EMITTER = RecipeCapabilityRequirementV1(
    authority="process_emitter", subject="start_listen", required_state="supported"
)


def _constraint_only_executor(inp):  # pragma: no cover - exercised via the registry
    """The constraint-only representative.

    Emits requirements and nothing else — a registry entry whose kind mechanically
    prevents it from contributing a patch or a component. It exists so the
    four-way entry-kind distinction has a tested member for every kind, not only
    for the two that materialize things.
    """
    from ...models.recipe_contributions import (
        ConstraintRequirementV1,
        RequireComponentV1,
    )

    return tuple(
        ConstraintRequirementV1(
            contribution_kind="constraint_requirement",
            version="1",
            requirement_id=f"req.inbound.{item.contribution_id}",
            requirement=RequireComponentV1(
                kind="component",
                component_key=item.component_key,
                component_type=item.component_type,
            ),
        )
        for item in inp.component_slots
    )


class InboundValidateInputV1(SyncRecipeInputV1):
    """Reuses the sync projection: this entry validates the SAME safe shape."""


# ---------------------------------------------------------------------------
# Served guidance (#159 S2): limitations, not_for, capability notes, examples
# ---------------------------------------------------------------------------
#
# Every example is a CONCRETE invocation the registry validates against the
# registration's own input (and component-material) model at construction.

_CAPABILITY_NOTE = (
    "Preflight checks the declared compiler capabilities; an unmet one fails with "
    "RECIPE_CAPABILITY_GATED before anything is emitted."
)
_LITERAL_QUERY_NOTE = (
    "Literal REST query parameter names are limited to filter, limit, mode and offset."
)


_FANOUT_V2_GUIDANCE = dict(
    limitations=(
        "Two to twenty-five REST targets behind one Branch; links, when given, must "
        "describe exactly the transform-to-target star.",
        "Keyed cache retrieval (doc_cache_index, cache_key_values) is refused with "
        "COMPOSITION_UNSUPPORTED_TOPOLOGY.",
        "Watermark query parameters are allowed on the first target only and must "
        "match watermark.query_parameter_refs; a watermark is recorded, never wired.",
        "Credential fields in component_material accept only opaque credential:// "
        "references.",
    ),
    not_for=(
        "Processes whose components already exist as a complete set: author the "
        "process directly (intent_kind=\"process_ir\") over those components.",
        "Listener-entry, API-to-API or API-to-database processes.",
    ),
    capability_notes=(
        "Connection settings, SQL, URLs and mapping travel in component_material, "
        "which only the trusted preparer reads; the recipe input carries identities "
        "and binding modes.",
        _CAPABILITY_NOTE,
    ),
    examples=(
        RecipeExampleV1(
            example_id="fanout.v2.flagship",
            summary=(
                "Create a database source, a shared JSON map and two REST targets from "
                "component material."
            ),
            is_template=False,
            raw_input={
                "naming": {"component_prefix": "M8DEMO"},
                "process_key": "main_process",
                "source": {
                    "auth_mode": "username_password",
                    "binding_mode": "create",
                    "part_key": "db",
                },
                "targets": [
                    {"binding_mode": "create", "label": "Orders", "method": "POST", "part_key": "orders"},
                    {"binding_mode": "create", "label": "Billing", "method": "POST", "part_key": "billing"},
                ],
                "transform": {"part_key": "shape"},
                "version": "2",
            },
            component_material={
                "payload_profile": {
                    "format": "json",
                    "root": {
                        "children": [
                            {"data_type": "character", "kind": "simple", "name": "target_a"},
                            {"data_type": "character", "kind": "simple", "name": "target_b"},
                        ],
                        "kind": "object",
                        "name": "Root",
                    },
                },
                "source": {
                    "connection": {
                        "credential_ref": "credential://example/orders-db",
                        "database": "AppDB",
                        "driver": "microsoft_jdbc",
                        "host": "db.example.invalid",
                        "mode": "create",
                        "username": "orders_reader",
                    },
                    "read": {
                        "result_fields": [
                            {"data_type": "character", "name": "source_a"},
                            {"data_type": "character", "name": "source_b"},
                        ],
                        "sql": "SELECT source_a, source_b FROM orders",
                    },
                },
                "targets": [
                    {
                        "connection": {"base_url": "https://orders.example.invalid", "mode": "create"},
                        "operation": {"path": "/v1/orders", "query_parameters": []},
                        "part_key": "orders",
                    },
                    {
                        "connection": {"base_url": "https://billing.example.invalid", "mode": "create"},
                        "operation": {"path": "/v1/billing", "query_parameters": []},
                        "part_key": "billing",
                    },
                ],
                "transform": {
                    "operations": [
                        {"operation_type": "direct", "source_field": "source_a", "target_path": "Root/target_a"},
                        {"operation_type": "direct", "source_field": "source_b", "target_path": "Root/target_b"},
                    ]
                },
                "version": "2",
            },
        ),
    ),
)

_API_TO_DB_V1_GUIDANCE = dict(
    limitations=(
        "One static REST GET source and one database Send target.",
        "A created REST source connection is unauthenticated; secured auth needs "
        "binding_mode reuse.",
        _LITERAL_QUERY_NOTE,
        "A watermark-sourced query parameter is refused (GOVERNANCE_WATERMARK_INCONSISTENT), "
        "and runtime-bound {token} path segments are refused.",
        "The write statement_type is one of the five confirmed variants.",
        "Transform operations are direct, side-effect-free map_function, or map_script "
        "by key to a script.mapping base component created in the same request from an "
        "inline create spec with a script body; a reference_only account script is "
        "refused.",
        "Every mandatory column and condition is mapped, each at most once.",
        "Pagination, retry, dead-letter, watermark and schedule activation are out of "
        "scope.",
    ),
    not_for=(
        "Database sources: use boomi.archetype.database_to_api_sync.",
        "REST targets.",
        "Runtime-bound path, query or header values.",
        "Upsert writes.",
    ),
    capability_notes=(
        "One typed unit: REST fetch, map, database write, stop.",
        "Supporting components are generated by the trusted preparer from closed "
        "component material.",
        "Projects the frozen fetch, transform and write rows and their endpoints.",
    ),
    examples=(
        RecipeExampleV1(
            example_id="sync.api_to_database.v1.create",
            summary=(
                "Create a REST source and write each response record into an existing "
                "database connection; the named connection (Customers DB) must already "
                "exist in the account."
            ),
            is_template=False,
            raw_input={
                "naming": {"component_prefix": "CustSync"},
                "process_key": "main_process",
                "source": {"binding_mode": "create"},
                "target": {"binding_mode": "reuse"},
                "version": "1",
            },
            component_material={
                "source": {
                    "connection": {"base_url": "https://api.example.invalid", "mode": "create"},
                    "request": {"path": "/v1/customers", "query_parameters": []},
                    "response_profile": {
                        "format": "json",
                        "root": {
                            "children": [
                                {"data_type": "character", "kind": "simple", "name": "id"},
                                {"data_type": "character", "kind": "simple", "name": "name"},
                            ],
                            "kind": "object",
                            "name": "Root",
                        },
                    },
                },
                "target": {
                    "connection": {"component_name": "Customers DB", "mode": "reuse"},
                    "write_profile": {
                        "fields": [
                            {"data_type": "character", "mandatory": True, "name": "id"},
                            {"data_type": "character", "name": "name"},
                        ],
                        "statement_type": "dynamicinsert",
                        "table_name": "customers",
                    },
                },
                "transform": {
                    "operations": [
                        {"operation_type": "direct", "source_field": "Root/id", "target_path": "Fields/id"},
                        {"operation_type": "direct", "source_field": "Root/name", "target_path": "Fields/name"},
                    ]
                },
                "version": "1",
            },
        ),
    ),
)

#: DERIVED from the connector-call registry (the authority the guarded send is
#: checked against) and the input's own method vocabulary, never hand-listed.
_GUARDED_REST_ACTIONS = {
    action.upper() for family, action in _CONNECTOR_CALLS if family == _REST_FAMILY
}
_DLQ_UNGUARDABLE_METHODS = tuple(
    method
    for method in _component_material._RestMethodV2.__args__
    if method not in _GUARDED_REST_ACTIONS
)
_DLQ_METHOD_NOTE = (
    "With a dead-letter cache the REST send runs inside a guarded connector call, "
    "which supports only the methods "
    + ", ".join(sorted(m for m in _component_material._RestMethodV2.__args__ if m in _GUARDED_REST_ACTIONS))
    + "; target.method "
    + ", ".join(_DLQ_UNGUARDABLE_METHODS)
    + " passes the input schema but is refused at compile with "
    "PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED (the same method without a "
    "dead-letter cache is accepted)."
)

_DB_TO_API_GUIDANCE = dict(
    limitations=(
        "reliability.retry.max_attempts accepts only 1; 2 to 6 are refused with "
        "DB_TO_API_RETRY_UNAVAILABLE, because a retried REST send needs an attested "
        "operation grant the public path does not have yet (issue #199). No "
        "idempotency field is accepted.",
        "Dead-letter routing takes only an existing, reference-only, unprofiled "
        "Document Cache (profileType profile.none) selected by base-component key "
        "(reliability.dlq.cache_key); a profiled or unreadable cache is refused at plan "
        "and compile (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH), and the cache is never "
        "created.",
        _DLQ_METHOD_NOTE,
        "With a dead-letter cache the process is two connector-scoped Try/Catch guards "
        "(database read, then REST send) with retry count 0; catch_notify is a log-only "
        "Notify on both catch legs and requires a dead-letter cache.",
        "A created REST connection is unauthenticated; secured REST auth requires a "
        "reused connection. A created database connection takes credentials only as an "
        "opaque credential:// reference.",
        _LITERAL_QUERY_NOTE,
        "Watermark-sourced query parameters are recorded on the watermark declaration "
        "and never emitted; the watermark is recorded intent and no watermark update is "
        "wired.",
        "A map script is a script.mapping base component created in the same request from "
        "an inline create spec with a script body and named by key; a reference_only "
        "account script is refused.",
    ),
    not_for=(
        "Error-subprocess dead-letter routing, or folder, topic or queue dead-letter "
        "intent.",
        "Per-document dynamic REST paths.",
        "Retried REST sends, caller-selected backoff, local error scopes or staged "
        "outcomes.",
        "Schedules, run metadata, runtime hints or error-classifier metadata.",
        "JDBC options, REST default headers, or database read fetch size, link element "
        "or bind-parameter SQL types.",
    ),
    capability_notes=(
        "One canonical process unit: database read, map, REST send; linear without a "
        "dead-letter cache, connector-scoped Try/Catch guards with one.",
        "Supporting components (database connection, read profile and operation, JSON "
        "target profile, map, REST connection and operation) are generated from closed "
        "component material; each side's connection may be created or reused.",
        "Per-environment extensions derive from the actual bindings: database "
        "credentials when the source is reused or created with username_password, REST "
        "credentials only when the target is reused, endpoint fields on opt-in.",
        "Projects the frozen extract, transform, send and reliability rows, and the "
        "watermark row when one is declared.",
    ),
    examples=(
        RecipeExampleV1(
            example_id="db_to_api.v1.create",
            summary=(
                "Create a database source and a REST target, map each row to a JSON "
                "payload and POST it."
            ),
            is_template=False,
            raw_input={
                "naming": {"component_prefix": "DEMO"},
                "process_key": "main_process",
                "reliability": {"retry": {"max_attempts": 1}},
                "source": {"auth_mode": "username_password", "binding_mode": "create"},
                "target": {"binding_mode": "create", "method": "POST"},
                "transform": {"map_label": "Map source to JSON payload"},
                "version": "1",
            },
            component_material={
                "payload_profile": {
                    "format": "json",
                    "root": {
                        "children": [
                            {"data_type": "character", "kind": "simple", "name": "target_a"}
                        ],
                        "kind": "object",
                        "name": "Root",
                    },
                },
                "source": {
                    "connection": {
                        "credential_ref": "credential://secrets/db/svc_sync",
                        "database": "AppDB",
                        "driver": "microsoft_jdbc",
                        "host": "db.example.invalid",
                        "mode": "create",
                        "username": "svc_sync",
                    },
                    "read": {
                        "result_fields": [{"data_type": "character", "name": "source_field_a"}],
                        "sql": "SELECT source_field_a FROM orders",
                    },
                },
                "target": {
                    "connection": {"base_url": "https://api.example.invalid", "mode": "create"},
                    "operation": {"path": "/v1/items", "query_parameters": []},
                },
                "transform": {
                    "operations": [
                        {
                            "operation_type": "direct",
                            "source_field": "source_field_a",
                            "target_path": "Root/target_a",
                        }
                    ]
                },
                "version": "1",
            },
        ),
    ),
)

_LISTENER_COMMON_LIMITATIONS = (
    "JSON input only (singlejson or multijson) with an ack-only response: HTTP 200 "
    "does not mean the process succeeded.",
    "Listener processes cannot run in Test mode.",
    "A bare listener serves basic and intermediate runtimes; an advanced runtime "
    "needs publish_mode api_service, and the API Service and the process deploy "
    "separately.",
    "Inbound validation is a build-time contract, not runtime payload validation.",
    "A map script must be a script.mapping base component created in the same request "
    "from an inline create spec with a script body and named by key (a reference_only "
    "account script is refused); map "
    "functions are limited to side-effect-free families.",
)
_LISTENER_COMMON_NOT_FOR = (
    "Scheduled or polling sources.",
    "Advanced runtimes without publish_mode api_service.",
    "Listener response mapping.",
    "Retry or dead-letter handling on the listener path.",
)
_LISTENER_COMMON_NOTES = (
    "One canonical unit: listener entry, map, target, stop; listener options come "
    "from the entry.",
    "publish_mode api_service adds an API Service component whose single route "
    "depends on the process.",
    "No process extensions are declared.",
)


def _listener_profile(*leaves: Tuple[str, str]) -> dict:
    return {
        "format": "json",
        "root": {
            "children": [
                {"data_type": data_type, "kind": "simple", "name": name}
                for name, data_type in leaves
            ],
            "kind": "object",
            "name": "Root",
        },
    }


_LISTENER_DB_GUIDANCE = dict(
    limitations=(
        *_LISTENER_COMMON_LIMITATIONS,
        "Database write statement types are limited to the write-profile builder's "
        "set; there is no upsert.",
    ),
    not_for=_LISTENER_COMMON_NOT_FOR,
    capability_notes=(
        *_LISTENER_COMMON_NOTES,
        "The generated request profile is the map source; the target is the database "
        "write profile (Fields/ and Conditions/ leaves).",
    ),
    examples=(
        RecipeExampleV1(
            example_id="listener_to_db.v1.api_service",
            summary=(
                "Publish an order-intake listener as an API Service and insert each "
                "request into an existing database connection; the named connection "
                "(Orders DB) must already exist in the account."
            ),
            is_template=False,
            raw_input={
                "listener": {"publish_mode": "api_service"},
                "naming": {"component_prefix": "OrderIntake"},
                "process_key": "main_process",
                "target": {"binding_mode": "reuse"},
                "version": "1",
            },
            component_material={
                "api_service": {},
                "listener": {
                    "object_name": "orderIntake",
                    "operation_type": "EXECUTE",
                    "request_profile": _listener_profile(("id", "character"), ("amount", "number")),
                },
                "target": {
                    "connection": {"component_name": "Orders DB", "mode": "reuse"},
                    "write_profile": {
                        "fields": [
                            {"data_type": "character", "mandatory": True, "name": "id"},
                            {"data_type": "number", "name": "amount"},
                        ],
                        "statement_type": "dynamicinsert",
                        "table_name": "orders",
                    },
                },
                "transform": {
                    "operations": [
                        {"operation_type": "direct", "source_field": "Root/id", "target_path": "Fields/id"},
                        {"operation_type": "direct", "source_field": "Root/amount", "target_path": "Fields/amount"},
                    ]
                },
                "version": "1",
            },
        ),
    ),
)

_LISTENER_REST_GUIDANCE = dict(
    limitations=(
        *_LISTENER_COMMON_LIMITATIONS,
        "The REST target path is static, a created connection is unauthenticated "
        "(secured auth requires reuse), and no request headers are sent.",
        _LITERAL_QUERY_NOTE,
    ),
    not_for=(*_LISTENER_COMMON_NOT_FOR, "Runtime-bound ({token}) REST paths."),
    capability_notes=(
        *_LISTENER_COMMON_NOTES,
        "The generated request profile is the map source; the target is the generated "
        "JSON payload profile.",
    ),
    examples=(
        RecipeExampleV1(
            example_id="listener_to_rest.v1.create",
            summary=(
                "Receive events on a listener, validate them against the request profile "
                "and POST each one to a new REST target."
            ),
            is_template=False,
            raw_input={
                "listener": {"inbound_validation": True},
                "naming": {"component_prefix": "EventRelay"},
                "process_key": "main_process",
                "target": {"binding_mode": "create", "method": "POST"},
                "version": "1",
            },
            component_material={
                "listener": {
                    "object_name": "eventRelay",
                    "operation_type": "EXECUTE",
                    "request_profile": _listener_profile(("event_id", "character"), ("detail", "character")),
                },
                "target": {
                    "connection": {"base_url": "https://api.example.invalid", "mode": "create"},
                    "operation": {"path": "/v1/events"},
                    "payload_profile": _listener_profile(("id", "character"), ("message", "character")),
                },
                "transform": {
                    "operations": [
                        {"operation_type": "direct", "source_field": "Root/event_id", "target_path": "Root/id"},
                        {"operation_type": "direct", "source_field": "Root/detail", "target_path": "Root/message"},
                    ]
                },
                "version": "1",
            },
        ),
    ),
)

_STUB_GUIDANCE = dict(
    limitations=(
        "One passthrough-then-stop process with zero supporting components and no "
        "component material.",
    ),
    not_for=("Real or production integrations.",),
    capability_notes=(
        "A smoke test of recipe planning, compilation and materialization with no "
        "components.",
    ),
    examples=(
        RecipeExampleV1(
            example_id="stub.v1.default",
            summary="A Stop-only process under the default STUB name prefix.",
            is_template=False,
            raw_input={"process_key": "main_process", "version": "1"},
        ),
    ),
)


PRODUCTION_REGISTRATIONS: Tuple[RecipeRegistrationV1, ...] = (
    # #159 S1: typed assembly. The executor emits one process unit; the trusted
    # preparer turns the closed component material into every supporting spec.
    RecipeRegistrationV1(
        recipe_id=RECIPE_DB_REST_FANOUT,
        recipe_version=_FANOUT_V2_VERSION,
        entry_kind="executable_recipe",
        # #159 S3: the default (fan-out 1.0.0 retired with the #153 lift).
        is_default=True,
        input_model=ComposeDbRestFanoutInputV2,
        executor=emit_db_rest_fanout_v2,
        output_types=(
            "constraint_requirement",
            "process_unit",
            "recorded_intent",
        ),
        capability_requirements=(
            _SOURCE_EMITTER,
            _TARGET_EMITTER,
            _MAP_EMITTER,
            _BRANCH_EMITTER,
            _CACHE_LOAD_EMITTER,
            _CACHE_RETRIEVE_EMITTER,
        ),
        conflict_policy=_FANOUT_CONFLICT_POLICY,
        component_material_model=FanoutComponentMaterialV2,
        component_preparer=prepare_db_rest_fanout_v2,
        implementation_dependency_modules=(_component_material.__name__,),
        **_FANOUT_V2_GUIDANCE,
    ),
    # #159 S2: the typed API->DB preset. #159 S3: the default and the only
    # API->DB version (0.1.0 retired with the #153 lift and its adapter).
    RecipeRegistrationV1(
        recipe_id=RECIPE_API_TO_DATABASE_SYNC,
        recipe_version=_API_DB_V1_VERSION,
        entry_kind="executable_recipe",
        is_default=True,
        input_model=ApiToDatabaseSyncInputV1,
        executor=emit_api_to_database_sync_v1,
        output_types=("constraint_requirement", "process_unit"),
        capability_requirements=(_SOURCE_EMITTER, _TARGET_EMITTER, _MAP_EMITTER),
        conflict_policy=_SYNC_CONFLICT_POLICY,
        component_material_model=ApiToDatabaseSyncMaterialV1,
        component_preparer=prepare_api_to_database_sync_v1,
        implementation_dependency_modules=(_component_material.__name__,),
        **_API_TO_DB_V1_GUIDANCE,
    ),
    # #159 S2: typed DB->API. The Try/Catch and Notify requirements cover the
    # dead-letter route; the executor states them per invocation as well.
    RecipeRegistrationV1(
        recipe_id=RECIPE_DATABASE_TO_API_SYNC,
        recipe_version=_PRESET_VERSION,
        entry_kind="executable_recipe",
        is_default=True,
        input_model=DatabaseToApiSyncInputV1,
        executor=emit_database_to_api_sync,
        output_types=("constraint_requirement", "process_unit", "recorded_intent"),
        capability_requirements=(
            _SOURCE_EMITTER,
            _TARGET_EMITTER,
            _MAP_EMITTER,
            _CACHE_LOAD_EMITTER,
            _CATCH_EMITTER,
            _NOTIFY_EMITTER,
        ),
        conflict_policy=_SYNC_CONFLICT_POLICY,
        component_material_model=DatabaseToApiSyncMaterialV1,
        component_preparer=prepare_database_to_api_sync,
        implementation_dependency_modules=(_component_material.__name__,),
        **_DB_TO_API_GUIDANCE,
    ),
    # #159 S2: the two HTTP listener presets.
    RecipeRegistrationV1(
        recipe_id=RECIPE_HTTP_LISTENER_TO_DB,
        recipe_version=_PRESET_VERSION,
        entry_kind="executable_recipe",
        is_default=True,
        input_model=_listener_db.HttpListenerToDbInputV1,
        executor=_listener_db.emit_http_listener_to_db,
        output_types=("constraint_requirement", "process_unit"),
        capability_requirements=(_LISTEN_EMITTER, _MAP_EMITTER, _TARGET_EMITTER),
        conflict_policy=_SYNC_CONFLICT_POLICY,
        component_material_model=_listener_db.HttpListenerToDbMaterialV1,
        component_preparer=_listener_db.prepare_http_listener_to_db,
        implementation_dependency_modules=(_component_material.__name__,),
        **_LISTENER_DB_GUIDANCE,
    ),
    RecipeRegistrationV1(
        recipe_id=RECIPE_HTTP_LISTENER_TO_REST,
        recipe_version=_PRESET_VERSION,
        entry_kind="executable_recipe",
        is_default=True,
        input_model=_listener_rest.HttpListenerToRestInputV1,
        executor=_listener_rest.emit_http_listener_to_rest,
        output_types=("constraint_requirement", "process_unit"),
        capability_requirements=(_LISTEN_EMITTER, _MAP_EMITTER, _TARGET_EMITTER),
        conflict_policy=_SYNC_CONFLICT_POLICY,
        component_material_model=_listener_rest.HttpListenerToRestMaterialV1,
        component_preparer=_listener_rest.prepare_http_listener_to_rest,
        # The REST listener delegates its listener half to the DB preset module.
        implementation_dependency_modules=(
            _component_material.__name__,
            _listener_db.__name__,
        ),
        **_LISTENER_REST_GUIDANCE,
    ),
    # #159 S2: the Stop-only stub; zero components, no material channel.
    RecipeRegistrationV1(
        recipe_id=RECIPE_STUB_MINIMAL,
        recipe_version=_PRESET_VERSION,
        entry_kind="executable_recipe",
        is_default=True,
        input_model=StubMinimalInputV1,
        executor=emit_stub_minimal,
        output_types=("constraint_requirement", "process_unit"),
        conflict_policy=_SYNC_CONFLICT_POLICY,
        **_STUB_GUIDANCE,
    ),
    RecipeRegistrationV1(
        recipe_id=RECIPE_CONSTRAINT_INBOUND_VALIDATE,
        recipe_version=_CONSTRAINT_VERSION,
        entry_kind="constraint_only",
        is_default=True,
        input_model=InboundValidateInputV1,
        executor=_constraint_only_executor,
        output_types=("constraint_requirement",),
        conflict_policy=_CONSTRAINT_CONFLICT_POLICY,
    ),
    # ADVISORY. No executor field is populated, and registry construction rejects
    # one — this is the structural reason doctrine can never become executable.
    RecipeRegistrationV1(
        recipe_id=RECIPE_ADVISORY_INTEGRATION_DESIGN,
        recipe_version=_ADVISORY_VERSION,
        entry_kind="advisory",
        is_default=True,
    ),
)

__all__ = [
    "ADAPTER_API_TO_API_SYNC",
    "ADAPTER_API_TO_DATABASE_SYNC",
    "ADAPTER_COMPOSE_ARCHETYPES",
    "PRODUCTION_REGISTRATIONS",
    "RECIPE_ADVISORY_INTEGRATION_DESIGN",
    "RECIPE_API_TO_API_SYNC",
    "RECIPE_API_TO_DATABASE_SYNC",
    "RECIPE_CONSTRAINT_INBOUND_VALIDATE",
    "RECIPE_DATABASE_TO_API_SYNC",
    "RECIPE_DB_REST_FANOUT",
    "RECIPE_HTTP_LISTENER_TO_DB",
    "RECIPE_HTTP_LISTENER_TO_REST",
    "RECIPE_STUB_MINIMAL",
]
