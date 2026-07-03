"""
Process-flow XML builder for structured process orchestration (issue #25).

Owns Boomi process Component XML emission for `process_kind` archetypes
that wire DB/REST/other connector-actions together. Today supports
`database_to_api_sync` (M2.5 vertical slice) — a deterministic
Start -> [optional transform] -> Target -> Stop flow whose shape XML
uses `shapetype="connectoraction"` (matches live Renera examples like
`DB Test`, `Rest Test GET`, `Rest Test PATCH`).

Issue #51 M3.R1a adds a verified Try/Catch + DLQ catch-path: for
`retry_count` 0..5 with `dlq.mode` in {`document_cache_ref`,
`error_subprocess_ref`}, the flow is wrapped in a `catcherrors` shape
(transcribed from live Boomi exports, not invented from docs) whose
catch leg routes to a `doccacheload` (DLQ cache) or `processcall` (error
subprocess). Issue #88 M4.5.3 un-gated retry 1..5 (docs-corroborated:
Boomi Try/Catch Retry Count is 0..5, platform-timed) — positive retry
requires a wired DLQ catch path; values outside 0..5 (or retry>0 without
a DLQ) still return PROCESS_RETRY_UNVERIFIED. Map and subprocess/cache
components are referenced by id/$ref only — their build is out of scope.

Issue #89 M4.5.4 adds an optional verified Notify step on the catch leg:
when `reliability.catch_notify` is set (a `level` + a `message_template`
that references the caught-error property), the catch leg becomes
`catch -> notify -> dlq route -> stop`. The Notify shape XML is transcribed
from a live `work`-profile export (notify shape, not invented from docs);
omitting `catch_notify` keeps the existing catch leg byte-for-byte
identical. Email/SMS notification channels and Notify outside catch paths
are out of scope.

Issue #90 M4.5.5 adds a verified standalone Process Call in the main flow
and the `WrapperSubprocessBuilder` (`process_kind="wrapper_subprocess"`): a
thin parent `start -> process call(s) -> stop` that invokes in-spec child
processes by `$ref:` or existing components by id. The standalone processcall
shape is transcribed from the live `work` wrapper exemplar (it continues past
a child failure: abort="false", wait="true"); the DLQ catch-leg processcall
(abort="true") is unchanged. integration_builder applies children first and
substitutes `$ref`->id; changing a child requires redeploying the parent.
"""

from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pydantic import ValidationError

from ....models.cache_property_models import PROPERTY_SOURCE_FIELD_CONTRACT
from ....models.pipeline_models import PipelineSpec, StageSpec
from ._preservation_policy import OwnedPath, PreservationPolicy
from .cache_property_lineage import validate_config_lineage
from .connector_builder import (
    BuilderValidationError,
    REST_CLIENT_SUBTYPE,
    SOAP_CLIENT_SUBTYPE,
    _escape_xml,
    _resolve_rest_connector_type,
    _resolve_soap_client_connector_type,
)


# REST HTTP methods supported by Boomi REST Client connector-action.
_REST_ACTION_TYPES = frozenset({
    "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "TRACE",
})

# Database SOURCE connector-action types wired into a process flow here. Get
# only: a read stage is a db_read Get source. The Send/write *target* is a
# separate slot (see _DB_TARGET_ACTION_TYPES) — broadening this set would wrongly
# accept a DB-source Send.
_DB_ACTION_TYPES = frozenset({"Get"})

# Database TARGET connector-action types (#74, M5.8). The api_to_database_sync
# preset wires a Send (write) step as the sync_pipeline target — the #32 Send/
# write *component* builders supply the connector-action; this lowers a `write`
# stage to a `database`/`Send` connector binding. Send only (Get is a source).
_DB_TARGET_ACTION_TYPES = frozenset({"Send"})

# SOAP Client connector-action types (#126). The SOAP Client exposes a single
# outbound EXECUTE action, used for both a soap_fetch source and a soap_send
# target — there is no GET/SEND/verb split like REST/DB.
_SOAP_ACTION_TYPES = frozenset({"EXECUTE"})

_SUPPORTED_TRANSFORM_MODES = frozenset(
    {"passthrough", "message", "map_ref", "dataprocess", "doccacheretrieve", "doccacheremove"}
)
_SUPPORTED_DLQ_MODES = frozenset({"disabled", "document_cache_ref", "error_subprocess_ref"})

# Issue #112 M10.8: Branch (N-way forward fan-out) shape. The optional ``branch``
# block carries an ``enabled`` flag and a ``targets`` list (legs 2..N — leg 1 is
# the top-level ``target``). Boomi's Branch step supports 2..25 paths (KB
# r-atm-Branch_shape_83d94692; invalid values default to 2), so the total leg
# count (1 + len(branch.targets)) must stay in that range. v1 fans the
# post-source document to N independent REST targets, each ending in its own Stop
# — forward-only, no join/merge, sequential per-path execution (see
# .codex/plans/issue-112-live-captures.md).
_BRANCH_ALLOWED_KEYS = frozenset({"enabled", "targets"})
_BRANCH_MAX_LEGS = 25

# Issue #111 M10.7: Flow Control (per-document batching) shape. The optional
# top-level ``flow_control`` block emits a Boomi Flow Control shape configured for
# the live-verified per-document batching mode (chunkStyle="threadOnly", chunks=0,
# forEachCount=N) — the document stream is processed in batches of N through the
# rest of the flow. Byte-exact to a live ``work``-profile capture (component
# 7ce0d74d-e71a-408b-9d59-a6f4498c64e2; see .codex/plans/issue-111.md). v1 ships
# ONLY this batching mode — true parallel chunks (chunks>0), multiProcess, and the
# combine variant stay design guidance. The shape sits right after the source
# (before any transform) so the whole downstream chain runs per batch; it does NOT
# compose with a Branch fan-out or a Decision route in v1 (each rejected by the
# composition guard — topology-changing follow-ups).
_FLOW_CONTROL_ALLOWED_KEYS = frozenset({"enabled", "for_each_count", "label"})

# Issue #113 M10.9: Decision (conditional two-path routing) shape. The optional
# ``decision`` block emits a Decision shape — Boomi's if/then — that routes the
# document down a labelled ``true`` or ``false`` dragpoint based on a value
# comparison (binary, both outcomes explicit — no fall-through). v1 ships the 7
# live comparison operators and the two most common operand sources (``track``
# DDP/DPP and ``static`` literal). The TRUE leg is the forward success path (the
# top-level ``target`` -> Stop). The FALSE leg is either forward (an optional
# ``false_notify`` Message before its own Stop, keeping the leg
# CONTROL_BRANCH_BARE_STOP-clean) OR a backward loop: ``false_next`` names an
# earlier shape, and the false dragpoint (or the false_notify Message's tail)
# wires back to it — the live ``shape31 false->shape32->shape27`` loop pattern.
# Transcribed from live work-profile decision XML (boomi_companion
# .../references/steps/decision_step.md; see .codex/plans/issue-113-live-captures.md).
_DECISION_COMPARISONS = frozenset(
    {"equals", "greaterthaneq", "lessthaneq", "greaterthan", "lessthan", "regex", "wildcard"}
)
_DECISION_VALUE_TYPES = frozenset({"track", "static"})  # v1 operand sources
_DECISION_ALLOWED_KEYS = frozenset(
    {"enabled", "comparison", "label", "left", "right", "false_notify", "false_next"}
)
_DECISION_OPERAND_ALLOWED_KEYS = frozenset(
    {"value_type", "property_id", "default_value", "property_name", "static_value"}
)
# Transform modes that insert a middle shape between the source and the Decision
# (so the pre-Decision chain is start -> source -> transform, length 3). A
# passthrough transform adds no shape (length 2). Used to derive the valid
# ``false_next`` loop-back targets identically in validate_config and build().
_DECISION_PRE_TRANSFORM_MODES = frozenset(
    {"message", "map_ref", "dataprocess", "doccacheretrieve", "doccacheremove"}
)

# Issue #106 M10.2 / #115 M10.2a: process-level Data Process shape. v1 ships the
# live-observed Custom Scripting (Groovy, processtype 12) operation plus the two
# profile-driven, cardinality-changing operations Split Documents (1->N,
# processtype 8) and Combine Documents (N->1, processtype 9). Split is captured
# from the live `work` account (see .codex/plans/issue-115-m10.2a-live-captures.md);
# the JSON-split and JSON/XML-combine variants are reconciled against the companion
# reference data_process_step.md. Every OTHER documented Data Process operation
# (Search/Replace, Zip, Unzip, Base64 encode/decode, character encoding) stays
# rejected with PROCESS_DATAPROCESS_OPERATION_UNSUPPORTED until each has its own
# byte-accurate live capture, so we never over-claim the operation union from docs
# alone. ``processtype``/``name`` are the exact Boomi step attributes (see the
# companion reference data_process_step.md); ``name`` MUST stay the standard
# operation name (a custom step name causes GUI display issues), descriptive text
# goes on the shape ``userlabel``.
_DATAPROCESS_OPERATIONS: Dict[str, Dict[str, str]] = {
    "custom_scripting": {"processtype": "12", "name": "Custom Scripting"},
    "split_documents": {"processtype": "8", "name": "Split Documents"},
    "combine_documents": {"processtype": "9", "name": "Combine Documents"},
}
# The only script engine the typed builder accepts/emits for Custom Scripting.
_DATAPROCESS_SCRIPT_LANGUAGE = "groovy2"
# Issue #115 M10.2a: the two profile-driven cardinality operations and the profile
# kinds they bind. ``json`` -> <JSONOptions>, ``xml`` -> <XMLOptions> (the
# live-observed child of <SplitOptions> for Split / of <dataprocesscombine> for
# Combine). The link-element + profile fields are caller-authored, opaque
# UI-captured tokens (linkElementKey/linkElementName/profileId) — no canned values.
_DATAPROCESS_PROFILE_TYPES = frozenset({"json", "xml"})
_DATAPROCESS_PROFILE_OPERATIONS = frozenset({"split_documents", "combine_documents"})
_DATAPROCESS_SPLIT_KEYS = frozenset(
    {"operation", "profile_type", "profile_id", "link_element_key", "link_element_name"}
)
_DATAPROCESS_COMBINE_KEYS = _DATAPROCESS_SPLIT_KEYS | {"combine_into_link_element_key"}

# Issue #107 M10.3: process-level Return Documents terminal shape. Live-captured
# from the `work` account (component 64e5397b-3583-42c9-8fe3-08ccefb0da6c, see
# .codex/plans/issue-107-live-captures.md): a Return Documents shape is placed at
# the END of a document path and returns the current documents to the calling
# source point (the parent process via a Process Call/Route, or a web-service
# client). It is TERMINAL (empty <dragpoints/>, no Stop after it — the verifier's
# RETURN_DOCS_STOP_EXCLUSIVE invariant), so when enabled it REPLACES the trailing
# Stop. The single optional `label` is the Boomi "custom label" that identifies
# the returned document type(s) (used for Process Call/Route return-path mapping);
# it is optional (empty in the live capture) and maps to both the shape userlabel
# and the inner <returndocuments label="..."> attribute.
_RETURN_DOCUMENTS_ALLOWED_KEYS = frozenset({"enabled", "label"})

# Issue #109 M10.5: process-level Document Cache Retrieve shape
# (transform.mode='doccacheretrieve'). Live-captured from the `work` account
# (component 64e5397b-3583-42c9-8fe3-08ccefb0da6c shape2; see
# .codex/plans/issue-109-live-captures.md): a linear NON-terminal shape placed
# between source and target that pulls documents from a Document Cache into the
# current flow — the read counterpart of the already-shipped doccacheload (Add
# to Cache, DLQ catch leg), completing Document Cache CRUD. v1 ships ONLY the
# live-observed all-document retrieve: loadAllDoc="true" with an empty
# <cacheKeyValues/> and the recommended "Stop document execution" empty-cache
# behavior (emptyCacheBehavior="stopprocess"). Keyed/index retrieval (a
# docCacheIndex + populated cacheKeyValues) and the backward-compat "Fail
# document with errors" behavior have no byte-accurate live capture, so each is
# rejected PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID until one — never
# over-claiming the wire shape from docs alone (mirrors the dataprocess
# operation gate). document_cache_id binds the Document Cache component id (a
# literal id or a $ref:KEY token in depends_on).
_DOCCACHE_RETRIEVE_ALLOWED_KEYS = frozenset(
    {"mode", "label", "document_cache_id", "empty_cache_behavior", "load_all_documents"}
)
# Only the live-verified "Stop document execution (recommended)" wire value.
_DOCCACHE_RETRIEVE_EMPTY_BEHAVIORS = frozenset({"stopprocess"})
_DOCCACHE_RETRIEVE_DEFAULT_EMPTY_BEHAVIOR = "stopprocess"

# Issue #110 M10.6: process-level Document Cache Remove shape
# (transform.mode='doccacheremove'). Live-captured from the `work` account
# (component 6e56df6a-1fc0-43f6-8db2-1b9e4eefa7a0 "[CDS] Initialize
# Caches" shapes 3-7; see .codex/plans/issue-110-live-captures.md): a Document
# Cache Remove shape clears documents from a Document Cache — the DELETE half of
# Document Cache CRUD, completing the set alongside Add to Cache (doccacheload,
# write) and Document Cache Retrieve (doccacheretrieve, read, #109). v1 ships
# ONLY the live-observed all-document remove: removeAllDocuments="true" with an
# empty <cacheKeyValues/>. The inner config element <doccacheremove> carries
# attribute order docCache, removeAllDocuments (NO emptyCacheBehavior, NO
# loadAllDoc — those are retrieve-only); the live capture shows two wire variants
# (self-closing and child-bearing <cacheKeyValues/>) — we emit the child-bearing
# form for byte-consistency with the #109 retrieve emitter. Keyed/index removal
# (a docCacheIndex + populated cacheKeyValues) has no byte-accurate live capture,
# so removeAllDocuments=False (and any keyed variant) is rejected
# PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID until one — never over-claiming the wire
# shape from docs alone (mirrors the #109 retrieve / dataprocess gates). The live
# remove shapes sit at branch-leg ends (empty <dragpoints/>); per #110 the
# builder shape is locked as a linear NON-terminal cache op (one forward
# dragpoint), mirroring doccacheretrieve. document_cache_id binds the Document
# Cache component id (a literal id or a $ref:KEY token in depends_on).
_DOCCACHE_REMOVE_ALLOWED_KEYS = frozenset(
    {"mode", "label", "document_cache_id", "remove_all_documents"}
)

# Issue #117 M10 follow-up: multi-control-shape composition via an ordered
# top-level ``flow_sequence``. Where the legacy single-slot blocks (transform /
# flow_control / branch / decision) each emit ONE shape per process and mutually
# exclude, a ``flow_sequence`` is an ordered list of step dicts that compose 2+
# M10 control/transform shapes in one process. Each step carries a ``kind``:
#   * LINEAR kinds insert one forward shape (the same shapes the legacy single
#     slots emit) and continue down the chain;
#   * CONTROL kinds (``decision`` / ``branch``) fan out and TERMINALIZE the
#     containing sequence (v1: each is the last step of its sequence — no
#     post-control join/continuation);
#   * the TERMINAL ``exception`` kind ends a path in a thrown error.
# ``flow_sequence`` is additive and opt-in: absent, the builder takes the exact
# pre-#117 single-shape path (byte-identical). Present, it routes to the composed
# sequencer and rejects ambiguous legacy siblings. The legacy single blocks stay
# single-shape and mutually exclusive (their guards/tests are unchanged); rich
# composition is expressed ONLY through ``flow_sequence``.
_FLOW_SEQUENCE_LINEAR_KINDS = frozenset(
    {
        "flow_control",
        "message",
        "map_ref",
        "dataprocess",
        "doccacheload",
        "doccacheretrieve",
        "doccacheremove",
        # Issue #121 M11.2 (epic #118): generic DDP/DPP Set Properties steps.
        # Both lower to the same documentproperties shape the REST dynamic-path
        # helper emits, via the shared generic emitters below.
        "set_ddp",
        "set_dpp",
        # Issue #122 M11.3 (epic #118): authored cache vocabulary. cache_put is
        # the success-path Add to Cache write (lowers to the byte-locked
        # doccacheload emitter); cache_get retrieves (lowers to the all-document
        # doccacheretrieve emitter — keyed mode is gated pending a live keyed
        # capture, #119 census Outcome B).
        "cache_put",
        "cache_get",
    }
)
_FLOW_SEQUENCE_CONTROL_KINDS = frozenset({"decision", "branch"})
_FLOW_SEQUENCE_TERMINAL_KINDS = frozenset({"exception"})
_FLOW_SEQUENCE_ALLOWED_KINDS = (
    _FLOW_SEQUENCE_LINEAR_KINDS | _FLOW_SEQUENCE_CONTROL_KINDS | _FLOW_SEQUENCE_TERMINAL_KINDS
)
# Per-kind allowed step keys (strict — an unknown key is a typo, never silently
# dropped; mirrors the dataprocess/return_documents/catch_exception strictness).
_FLOW_SEQUENCE_STEP_COMMON_KEYS = frozenset({"kind", "label"})
_FLOW_SEQUENCE_STEP_KEYS: Dict[str, frozenset] = {
    "flow_control": _FLOW_SEQUENCE_STEP_COMMON_KEYS | {"for_each_count"},
    "message": _FLOW_SEQUENCE_STEP_COMMON_KEYS | {"message_text"},
    "map_ref": _FLOW_SEQUENCE_STEP_COMMON_KEYS | {"map_ref"},
    "dataprocess": _FLOW_SEQUENCE_STEP_COMMON_KEYS | {"steps"},
    "doccacheload": _FLOW_SEQUENCE_STEP_COMMON_KEYS | {"document_cache_id"},
    "doccacheretrieve": _FLOW_SEQUENCE_STEP_COMMON_KEYS
    | {"document_cache_id", "empty_cache_behavior", "load_all_documents"},
    "doccacheremove": _FLOW_SEQUENCE_STEP_COMMON_KEYS
    | {"document_cache_id", "remove_all_documents"},
    # Issue #121 M11.2: DDP/DPP Set Properties steps. `name` is the bare
    # property name (no dynamicdocument./process. prefix); `source_values` is
    # the ordered value-source list (#120 PropertySourceValue contract);
    # `persist` (DPP only) persists the value at atom level.
    "set_ddp": _FLOW_SEQUENCE_STEP_COMMON_KEYS | {"name", "source_values"},
    "set_dpp": _FLOW_SEQUENCE_STEP_COMMON_KEYS | {"name", "source_values", "persist"},
    # Issue #122 M11.3: authored cache steps. The keyed-retrieval keys
    # (doc_cache_index / cache_key_values / load_all_documents) are allow-listed
    # so the validator can reject them with the NAMED gated error instead of a
    # generic unknown-key message.
    "cache_put": _FLOW_SEQUENCE_STEP_COMMON_KEYS | {"document_cache_id"},
    "cache_get": _FLOW_SEQUENCE_STEP_COMMON_KEYS
    | {"document_cache_id", "empty_cache_behavior", "doc_cache_index", "cache_key_values", "load_all_documents", "external_writer"},
    "decision": frozenset(
        {"kind", "label", "comparison", "left", "right", "true_steps", "false_steps"}
    ),
    "branch": _FLOW_SEQUENCE_STEP_COMMON_KEYS | {"legs"},
    "exception": frozenset(
        {"kind", "title", "message_template", "stop_single_document", "parameter_source"}
    ),
}
# Branch leg shape: {"steps"?: [...linear...], "target": {REST binding}}.
_FLOW_SEQUENCE_BRANCH_LEG_KEYS = frozenset({"steps", "target"})

# Issue #51 M3.R1a / #88 M4.5.3: DLQ modes that emit a verified Try/Catch
# wrapper + DLQ catch-path (every supported mode except "disabled"). The catch
# leg is structural, so positive retry_count is only emittable WITH one of
# these modes wired.
_TRY_CATCH_DLQ_MODES = frozenset({"document_cache_ref", "error_subprocess_ref"})

# Issue #99 G1 — Try/Catch placement scope. "process" (default) wraps the whole
# source -> [transform] -> target chain in one Try/Catch (the pre-#99 shape).
# "connector" emits a Try/Catch per connector (source retry 0, target retry N)
# so a target retry does not re-execute the source read.
_SUPPORTED_TRY_CATCH_SCOPES = frozenset({"process", "connector"})

# Per the Boomi Try/Catch shape docs, Retry Count ranges 0..5 (count 1 retries
# immediately; 2..5 use the platform's built-in escalating wait schedule). The
# platform offers no caller-selected backoff. Issue #88 un-gated 1..5 (with a
# wired catch path); values outside 0..5 fail with PROCESS_RETRY_UNVERIFIED.
_MAX_RETRY_COUNT = 5

# Issue #89 M4.5.4 — optional Notify step on the Try/Catch catch leg.
# Boomi Notify message levels are INFO / WARNING / ERROR (the Notify-step
# docs list "Information, Warning, or Error"; the live notify shape emits the
# token "INFO"). The catch-path Notify is log-only (no platform email event),
# so email/SMS channels are out of scope and any extra config key is rejected.
_SUPPORTED_NOTIFY_LEVELS = frozenset({"INFO", "WARNING", "ERROR"})
_CATCH_NOTIFY_ALLOWED_KEYS = frozenset({"level", "message_template"})
# The runtime property holding the caught Try/Catch error message. Boomi binds
# it via a numbered placeholder + a notify track parameter (verified live), not
# by embedding the path in the message text, so the builder substitutes this
# token for the {1} placeholder and emits the matching track-parameter binding.
_NOTIFY_CAUGHT_ERROR_TOKEN = "meta.base.catcherrorsmessage"

# Issue #108 M10.4 — optional Exception (Throw) terminal on the Try/Catch catch
# leg. A ``reliability.catch_exception`` block makes the catcherrors Catch leg
# terminate in a deliberate Exception throw (a user-defined error message
# reported on the Process Reporting page) INSTEAD of a bare catch-row Stop —
# which also keeps the leg CONTROL_BRANCH_BARE_STOP-clean (a catcherrors ->
# exception edge is not a catcherrors -> stop edge). The Boomi docs are explicit:
# a Stop is a *successful* conclusion; an error path uses an Exception instead.
# Live-captured from the ``work`` account (component
# 1139079f-fff5-434c-aedc-d2758cc20525 shape10 + the decision-terminal exceptions
# in b34d3812-...; see .codex/plans/issue-108-live-captures.md). The Exception is
# TERMINAL (empty <dragpoints/>) and composes optionally with catch_notify and/or
# a DLQ route: ``[notify ->] [dlq route ->] exception``.
#
# parameter_source binds the single ``{1}`` placeholder in the message:
#   * caught_error     -> a track binding to meta.base.catcherrorsmessage (the
#                         platform caught-error message — same token Notify uses);
#   * current_document -> valueType="current" (the live default — the current doc);
#   * none             -> no <exParameters> (a static message, no {1}).
_SUPPORTED_EXCEPTION_PARAMETER_SOURCES = frozenset(
    {"caught_error", "current_document", "none"}
)
_CATCH_EXCEPTION_ALLOWED_KEYS = frozenset(
    {"title", "message_template", "stop_single_document", "parameter_source"}
)

# Issue #92 M4.5.7 — environment-extension declarations for connection fields.
# A deployed process exposes a connection/operation field as an environment
# override point ONLY when the process DECLARES it as an extension. The typed
# builders previously emitted an empty `<bns:processOverrides/>`, so
# manage_environments(get_extensions) returned zero override points and
# update_extensions silently no-op'd (live-proven 2026-06-12/13). Declaring the
# DB connection fields here makes them overrideable per environment without
# embedding a credential in the connection component.
#
# Field ids / labels / xpaths are `live_verified` — transcribed from the
# `work`-profile main-sync exemplar (component ab040894-...). Runtime
# availability through get_extensions / update_extensions is `live_QA_required`
# (unit tests verify only the emitted declaration shape; boomi-qa-tester proves
# the deploy -> override -> run path).
#
# CREATE-only: `<bns:processOverrides>` is deliberately UNOWNED by
# PRESERVATION_POLICY (see the policy comment near the bottom of this module),
# so a structured UPDATE preserves the LIVE per-environment override VALUES that
# Boomi populates via the UI rather than overwriting them with this declaration.
# The declaration therefore lands on the initial CREATE — the deploy path the
# acceptance criteria exercise.
DB_CONNECTION_EXTENSION_FIELDS_CREDENTIAL: Tuple[Dict[str, str], ...] = (
    {"id": "username", "label": "User", "xpath": "DatabaseConnectionSettings/@username"},
    {"id": "password", "label": "Password", "xpath": "DatabaseConnectionSettings/@password"},
)
DB_CONNECTION_EXTENSION_FIELDS_ENDPOINT: Tuple[Dict[str, str], ...] = (
    {"id": "host", "label": "Host", "xpath": "DatabaseConnectionSettings/@host"},
    {"id": "port", "label": "Port", "xpath": "DatabaseConnectionSettings/@port"},
)

# REST Client connection-field extensions (#102 B1). live_verified from the
# `renera` ``Rest Example`` process export (ConnectionOverride id
# 5a2c4949-...): a REST Client override keys PURELY by field id with NO xpath
# attribute — unlike the DB connector. ``url`` is the Base URL field; ``username``
# / ``password`` are the basic-auth credential fields.
REST_CONNECTION_EXTENSION_FIELDS_CREDENTIAL: Tuple[Dict[str, str], ...] = (
    {"id": "username", "label": "User"},
    {"id": "password", "label": "Password"},
)
REST_CONNECTION_EXTENSION_FIELDS_ENDPOINT: Tuple[Dict[str, str], ...] = (
    {"id": "url", "label": "Base URL"},
)

# Visual layout. Geometry is decorative only — process correctness is
# driven by toShape wiring. Numbers approximate the live Renera examples
# so the rendered diagram stays readable.
_SHAPE_Y = 96.0
_START_SHAPE_X = 96.0
_START_SHAPE_Y = 94.0
_SHAPE_X_STEP = 160.0
_DRAGPOINT_X_OFFSET = 144.0
_DRAGPOINT_Y = 104.0
# Catch-path row sits below the Try row. Geometry is decorative; the verified
# live Try/Catch (work component dff0bf83-d525-4781-b572-c93d285bb788) places
# the catch leg on a separate lower y. Issue #51 M3.R1a.
_CATCH_SHAPE_Y = 456.0
_CATCH_DRAGPOINT_Y = 464.0


def _shape_x(index: int) -> float:
    # index is 1-based.
    return _START_SHAPE_X + (index - 1) * _SHAPE_X_STEP


def _dragpoint_x(shape_index: int) -> float:
    return _shape_x(shape_index) + _DRAGPOINT_X_OFFSET


class ProcessFlowBuilder:
    """Builder for structured process components (process_kind dispatched).

    Public surface mirrors the database / REST builders so that
    integration_builder._build_plan and _apply_plan can treat all
    structured builders uniformly:

      - scan_forbidden_secret_fields(config) -> Optional[BuilderValidationError]
      - validate_config(config, *, depends_on) -> Optional[BuilderValidationError]
      - build(config, *, name, folder_name=None) -> str  # Component XML
    """

    PROCESS_KIND = "database_to_api_sync"

    # ------------------------------------------------------------------
    # Plan-time validation
    # ------------------------------------------------------------------

    # Substrings that mark a dict key as carrying a secret. Matching is
    # case-insensitive — every key is lowercased before the substring
    # check. This deliberately catches variants the connector contract
    # doesn't enforce (apiKey, db_password, AUTH_TOKEN, customerSecret,
    # etc.) because process configs are freeform user-provided JSON.
    #
    # `credential_ref` and similar `*_ref` keys do NOT contain any
    # forbidden substring — they carry URI references (credential://...),
    # not the secrets themselves.
    FORBIDDEN_SECRET_FIELDS: Tuple[str, ...] = (
        "password",
        "passcode",
        "secret",
        "private_key",
        "api_key",
        "apikey",
        "api-key",
        "auth_token",
        "access_token",
        "client_secret",
        "token",
        "authorization",
        "bearer",
        "credentials",
    )

    @classmethod
    def _key_matches_forbidden(cls, key: Any) -> Optional[str]:
        """Return the matched forbidden substring, or None.

        Case-insensitive substring scan — catches camelCase (apiKey),
        snake-prefixed (db_password), screaming-case (AUTH_TOKEN), and
        compound names (customerSecret). Codex review r4 P1 — exact-key
        membership was too narrow for freeform process configs.
        """
        if not isinstance(key, str):
            return None
        lowered = key.lower()
        for forbidden in cls.FORBIDDEN_SECRET_FIELDS:
            if forbidden in lowered:
                return forbidden
        return None

    @classmethod
    def scan_forbidden_secret_fields(
        cls, config: Any, _path_prefix: str = ""
    ) -> Optional[BuilderValidationError]:
        """Detect plaintext secret-shaped keys at any depth.

        At each dict level: case-insensitive substring match every key
        against FORBIDDEN_SECRET_FIELDS. A match flags the entire value
        — string non-empty (the obvious case) AND any dict / list
        container (`authorization: {"value": "..."}` style). Then
        recurse into non-matching subtrees in case a deeper key matches.

        Codex review r4 P1: the previous r3 exact-key scanner missed
        variant key names (apiKey, db_password) and container-shape
        secrets that the pre-r3 substring scanner caught.
        """
        if isinstance(config, dict):
            for key, value in config.items():
                matched = cls._key_matches_forbidden(key)
                if matched is not None:
                    path = f"{_path_prefix}{key}" if _path_prefix else key
                    # Reject both string leaves (the obvious case) AND
                    # container shapes where the secret is one level
                    # deeper. Empty strings still skip (matches the
                    # explicit "value and value" convention used by the
                    # DB builder for the same reason — empty defaults
                    # are not secrets).
                    if isinstance(value, str):
                        if value:
                            return cls._secret_rejection(path)
                    elif isinstance(value, (dict, list)):
                        return cls._secret_rejection(path)
                    # Scalars (None / bool / int) at a forbidden key
                    # carry no plaintext to leak — skip.
                    continue
                nested = cls.scan_forbidden_secret_fields(
                    value, _path_prefix=f"{_path_prefix}{key}."
                )
                if nested is not None:
                    return nested
        elif isinstance(config, list):
            for i, item in enumerate(config):
                nested = cls.scan_forbidden_secret_fields(
                    item, _path_prefix=f"{_path_prefix}[{i}]."
                )
                if nested is not None:
                    return nested
        # Scalars / None: no keys to scan.
        return None

    @classmethod
    def _secret_rejection(cls, path: str) -> BuilderValidationError:
        return BuilderValidationError(
            f"Plaintext secret-shaped field {path!r} is not allowed in "
            f"process config; reference connector secrets via a "
            f"connection_id / $ref:KEY token instead.",
            error_code="PLAINTEXT_SECRET_REJECTED",
            field=path,
            hint=(
                "Move credentials onto the connector-settings component "
                "and reference its connection_id from source/target."
            ),
        )

    @classmethod
    def redact_forbidden_secret_fields_in_place(cls, config: Any) -> None:
        """Recursively replace any FORBIDDEN_SECRET_FIELDS-keyed values
        with '[REDACTED]'.

        Matches the scan: at each dict level, any case-insensitive
        substring-matching key has its WHOLE value (string, dict, or
        list) replaced with `"[REDACTED]"`. Container-shape secrets
        (`{"password": {"plaintext": "..."}}`) are obliterated
        wholesale, mirroring DatabaseConnectorBuilder.redact's behavior.
        Codex review r4 P1.
        """
        if isinstance(config, dict):
            for key in list(config.keys()):
                if cls._key_matches_forbidden(key) is not None:
                    config[key] = "[REDACTED]"
                else:
                    cls.redact_forbidden_secret_fields_in_place(config[key])
        elif isinstance(config, list):
            for item in config:
                cls.redact_forbidden_secret_fields_in_place(item)
        # Scalars / None: no-op.

    @classmethod
    def validate_config(
        cls,
        config: Dict[str, Any],
        *,
        depends_on: Optional[Iterable[str]] = None,
        allow_rest_source: bool = False,
        allow_db_target: bool = False,
        allow_soap_source: bool = False,
        allow_soap_target: bool = False,
    ) -> Optional[BuilderValidationError]:
        """Validate structured process config; return error or None.

        ``allow_rest_source`` is False for the base database_to_api_sync protocol
        (DB source only); SyncPipelineBuilder passes True so a lowered rest_fetch
        fetch stage's REST GET source is accepted (M5.4 #72). A hand-authored
        database_to_api_sync with a REST source therefore stays rejected.

        ``allow_db_target`` is False for the base protocol (REST target only);
        SyncPipelineBuilder passes True so a lowered ``write`` stage's database
        Send target is accepted (#74 M5.8). A hand-authored database_to_api_sync
        with a database target therefore stays rejected.

        ``allow_soap_source`` / ``allow_soap_target`` are False for the base
        protocol; SyncPipelineBuilder passes True so a lowered ``soap_fetch``
        source / ``soap_send`` target's SOAP Client EXECUTE binding is accepted
        (#126). A hand-authored database_to_api_sync with a SOAP source/target
        therefore stays rejected.

        Validation order is intentional — surface the most-specific
        actionable error first:

          1. process_kind known
          2. source/target connector bindings well-formed
          3. transform mode supported
          4. reliability gating (retry/DLQ still unverified)
          5. $ref tokens reachable via depends_on
        """
        # str() coercion so non-string inputs (e.g. process_kind=123) fall
        # out as a clean structured PROCESS_KIND_UNSUPPORTED error instead
        # of raising AttributeError on .strip(). Codex review L1.
        process_kind = str(config.get("process_kind") or config.get("process_type") or "").strip()
        if process_kind != cls.PROCESS_KIND:
            return BuilderValidationError(
                f"process_kind {process_kind!r} is not supported.",
                error_code="PROCESS_KIND_UNSUPPORTED",
                field="process_kind",
                hint=(
                    f"Use process_kind={cls.PROCESS_KIND!r} for the M2.5 "
                    "database_to_api_sync builder. Other archetypes are "
                    "tracked by follow-up issues."
                ),
            )

        # Issue #117 M10 follow-up: a top-level ``flow_sequence`` routes to the
        # composed multi-shape validator/emitter BEFORE the legacy source-binding +
        # single-slot validators. _validate_flow_sequence_config owns the WHOLE
        # validation order for a composed config (source/target bindings included)
        # so its source.dynamic_path-not-supported guard fires before the generic
        # dynamic_path-shape check — and so validate_config + build()'s
        # _build_composed_process_flow run the IDENTICAL checks (no parity drift; QA
        # #142). It rejects ambiguous legacy siblings (flow_control / branch /
        # decision / non-passthrough transform / Try-Catch reliability), so the
        # legacy validators never run for it. Absent flow_sequence, this returns
        # immediately and the existing single-shape validation order is byte-for-byte
        # unchanged. Reachability still runs last (the SAME $ref walker the legacy
        # path uses), covering every nested ref.
        if _flow_sequence_enabled(config):
            seq_err = _validate_flow_sequence_config(config)
            if seq_err is not None:
                return seq_err
            # Issue #123 M11.4: cache/property lineage runs AFTER structural
            # validation (well-formed steps only) and only on the composed
            # path — the authored M11 kinds are the opt-in surface; legacy
            # single-slot configs stay exempt for backward compatibility.
            lineage_err = validate_config_lineage(config)
            if lineage_err is not None:
                return lineage_err
            try:
                _extract_process_extension_connections(config)
            except BuilderValidationError as exc:
                return exc
            return _validate_ref_reachability(config, set(depends_on or []))

        source_err = _validate_source_binding(
            config.get("source"),
            allow_rest_source=allow_rest_source,
            allow_soap_source=allow_soap_source,
        )
        if source_err is not None:
            return source_err

        # Issue #111 M10.7: validate the optional Flow Control (per-document
        # batching) block AFTER the source binding and BEFORE Branch/Decision.
        # _validate_flow_control_config owns a single fixed precedence (block
        # structure -> unknown key -> non-bool enabled -> unsupported v1
        # composition (branch/decision) -> for_each_count -> label). build()
        # funnels through the SAME validator, so the two paths cannot diverge. A
        # non-flow_control config returns immediately, keeping the existing
        # validation order byte-for-byte unchanged.
        flow_control_err = _validate_flow_control_config(config)
        if flow_control_err is not None:
            return flow_control_err

        # Issue #112 M10.8 (review): validate the optional Branch (N-way fan-out)
        # block BEFORE the top-level target binding. _validate_branch_config owns a
        # single fixed precedence — branch-block structure -> BRANCH_OUTPUT_UNSET
        # (missing/empty/non-list targets) -> leg count -> unsupported v1
        # composition (dynamic_path / Try-Catch reliability / return_documents
        # alongside Branch) -> leg bindings. Running it ahead of
        # _validate_target_binding lets its composition guard report a
        # target.dynamic_path + Branch as PROCESS_BRANCH_CONFIG_INVALID (the
        # persistent blocker) instead of the binding-stage
        # PROCESS_PATH_REPLACEMENT_INVALID. build() funnels through the SAME
        # validator, so validate_config and build() cannot diverge on which
        # structured error a malformed branch yields. Non-branch configs return
        # from this validator immediately (branch absent), keeping the
        # single-target validation order byte-for-byte unchanged.
        branch_err = _validate_branch_config(config)
        if branch_err is not None:
            return branch_err

        # Issue #113 M10.9: validate the optional Decision (conditional two-path
        # routing) block alongside Branch, BEFORE the top-level target binding —
        # _validate_decision_config owns a single fixed precedence (block structure
        # -> unsupported v1 composition -> comparison -> operands -> false_notify ->
        # false_next loop target). build() funnels through the SAME validator, so
        # the two paths cannot diverge. A non-decision config returns immediately.
        decision_err = _validate_decision_config(config)
        if decision_err is not None:
            return decision_err

        target_err = _validate_target_binding(
            config.get("target"),
            allow_db_target=allow_db_target,
            allow_soap_target=allow_soap_target,
        )
        if target_err is not None:
            return target_err

        transform_err = _validate_transform(config.get("transform"))
        if transform_err is not None:
            return transform_err

        reliability_err = _validate_reliability(config.get("reliability"))
        if reliability_err is not None:
            return reliability_err

        # Issue #96 review: a source dynamic_path cannot compose with a
        # connector-scoped Try/Catch (the emitter assumes the source connector is
        # the first post-start shape). Reject after both are individually valid.
        source_scope_err = _source_dynamic_path_connector_scope_error(config)
        if source_scope_err is not None:
            return source_scope_err

        # Issue #107 M10.3: validate the optional Return Documents terminal block.
        return_documents_err = _validate_return_documents(config.get("return_documents"))
        if return_documents_err is not None:
            return return_documents_err

        # Issue #92 M4.5.7: validate the optional process_extensions declaration
        # shape at plan time. build() calls the same helper (which raises) so a
        # validate_config-bypass path stays total; here we surface the error.
        try:
            _extract_process_extension_connections(config)
        except BuilderValidationError as exc:
            return exc

        # Dependency reachability: every $ref:KEY token in the config tree
        # must appear in depends_on (shared helper — see _validate_ref_reachability).
        return _validate_ref_reachability(config, set(depends_on or []))

    @classmethod
    def _should_emit_try_catch(cls, reliability: Any) -> bool:
        """True when the config should emit a verified Try/Catch wrapper.

        Issue #51 M3.R1a + #88 M4.5.3: retry_count 0..5 with a supported DLQ
        mode (document_cache_ref / error_subprocess_ref) is un-gated. Issue #108
        M10.4: a ``catch_exception`` block ALSO wires a Try/Catch — its catch leg
        throws a deliberate Exception (the live "fail/halt" shape is a bare
        catcherrors -> exception, no DLQ required), and retry_count > 0 is valid
        with it too (the catch leg exists). Values outside 0..5, the wrong type,
        or retry_count > 0 without EITHER a supported DLQ mode or a catch_exception
        stay gated (PROCESS_RETRY_UNVERIFIED) and never reach this path because
        validate_config rejects them first. This guard mirrors that boundary so a
        direct build() call is also total.
        """
        if not isinstance(reliability, dict):
            return False
        retry_count = reliability.get("retry_count", 0)
        if (
            not isinstance(retry_count, int)
            or isinstance(retry_count, bool)
            or not (0 <= retry_count <= _MAX_RETRY_COUNT)
        ):
            return False
        dlq = reliability.get("dlq")
        dlq_ok = (
            isinstance(dlq, dict)
            and str(dlq.get("mode") or "").strip().lower() in _TRY_CATCH_DLQ_MODES
        )
        exception_ok = isinstance(reliability.get("catch_exception"), dict)
        return dlq_ok or exception_ok

    # ------------------------------------------------------------------
    # Apply-time XML emission
    # ------------------------------------------------------------------

    @classmethod
    def build(
        cls,
        config: Dict[str, Any],
        *,
        name: str,
        folder_name: Optional[str] = None,
    ) -> str:
        """Emit the full Boomi Component XML for the process.

        Assumes validate_config has already passed and that $ref tokens
        in source/target/transform have been substituted with real
        component IDs by the integration builder. The internal
        parse-back roundtrip guards against silent XML malformation
        (PROCESS_XML_VALIDATION_FAILED).
        """
        # Coerce string-like metadata fields. validate_config does not
        # type-check these, so a non-string description/folder_name/name
        # would crash _escape_xml's .replace() with AttributeError at
        # build time. str() coercion keeps build() total. Codex review
        # r2 Q4.
        name = str(name) if name is not None else ""
        if not name or not name.strip():
            raise BuilderValidationError(
                "Process component name is required.",
                error_code="PROCESS_XML_VALIDATION_FAILED",
                field="name",
                hint="Pass a non-empty name via the IntegrationComponentSpec.name field.",
            )

        # Issue #117 M10 follow-up: a top-level ``flow_sequence`` composes 2+ M10
        # control/transform shapes in one process. Dispatch to the composed
        # sequencer; the existing single-shape path below is reached unchanged (and
        # byte-identical) when no flow_sequence is present.
        if _flow_sequence_enabled(config):
            return _build_composed_process_flow(config, name=name, folder_name=folder_name)

        source = config.get("source") or {}
        target = config.get("target") or {}
        transform = config.get("transform") or {"mode": "passthrough"}
        # str() coercion guards against non-string mode values reaching
        # build() in any code path that bypasses validate_config. Codex L1.
        transform_mode = str(transform.get("mode") or "passthrough").strip().lower()
        description = str(config.get("description") or "")

        # Build shapes in deterministic flow order: start, source,
        # [transform], target, stop. transform is omitted entirely when
        # mode=passthrough.
        flow: List[Tuple[str, Dict[str, Any]]] = []
        flow.append(("start_noaction", {}))
        # Canonicalize the source connector subtype + verb before emission. Boomi
        # expects exact case on both sides. A DATABASE source is `database` /
        # `Get` — the validator accepts case-insensitive input, so lowercase the
        # subtype (Codex review r6 P2.2). A REST fetch source (M5.4 #72) keeps the
        # canonical mixed-case REST Client subtype (officialboomi-X3979C-rest-prod —
        # an unconditional .lower() would corrupt the uppercase 'X') and the
        # uppercased verb, matching the REST target emission convention.
        source_canonical_type = _canonical_connector_type(source.get("connector_type"))
        source_action_raw = str(source.get("action_type") or "").strip()
        source_is_rest = _resolve_rest_connector_type(source.get("connector_type")) is not None
        if source_is_rest:
            source_connector_type = source_canonical_type
            source_action_type = source_action_raw.upper()
        else:
            source_connector_type = source_canonical_type.lower()
            source_action_type = source_action_raw
        # Issue #96 M5.4a: a REST source carrying a dynamic_path block (lowered from
        # a rest_fetch path runtime_binding) gets a Set Properties shape building the
        # path DDP BEFORE the source connector, and the source connectoraction emits
        # the matching "Path" dynamic operation property — the same proven mechanism
        # the #100 target path uses, applied to the source connectoraction (which
        # otherwise emits empty <parameters/><dynamicProperties/>). Absent dynamic_
        # path, the source flow is byte-for-byte unchanged.
        source_dynamic_path = (
            source.get("dynamic_path") if isinstance(source.get("dynamic_path"), dict) else None
        )
        if source_is_rest and source_dynamic_path:
            flow.append((
                "setproperties",
                {
                    "ddp_name": str(source_dynamic_path.get("ddp_name") or "").strip(),
                    "request_profile_id": str(source_dynamic_path.get("request_profile_id") or "").strip(),
                    "profile_type": str(source_dynamic_path.get("profile_type") or "profile.json").strip(),
                    "segments": source_dynamic_path.get("segments") or [],
                },
            ))
        flow.append((
            "connectoraction_source",
            {
                "connector_type": source_connector_type,
                "action_type": source_action_type,
                "connection_id": str(source.get("connection_id") or "").strip(),
                "operation_id": str(source.get("operation_id") or "").strip(),
                "userlabel": str(source.get("label") or ""),
                "dynamic_path": source_dynamic_path if source_is_rest else None,
            },
        ))
        # Issue #111 M10.7: Flow Control (per-document batching) shape. Run the
        # SAME validator validate_config uses, UNCONDITIONALLY (it returns None
        # when flow_control is absent or disabled), so a malformed flow_control —
        # or an unsupported branch/decision composition — raises here too
        # (totality on a validate_config bypass), BEFORE any branch/decision
        # emission path is taken. When enabled, insert the batching shape right
        # after the source and before the transform so the whole downstream chain
        # runs per batch. _flow_control_enabled() then only picks the emission.
        flow_control_err = _validate_flow_control_config(config)
        if flow_control_err is not None:
            raise flow_control_err
        if _flow_control_enabled(config):
            flow_control_cfg = config.get("flow_control") or {}
            flow.append((
                "flowcontrol",
                {
                    "for_each_count": flow_control_cfg.get("for_each_count"),
                    "userlabel": str(flow_control_cfg.get("label") or ""),
                },
            ))
        if transform_mode == "message":
            flow.append((
                "message",
                {
                    "text": str(transform.get("message_text") or ""),
                    "userlabel": str(transform.get("label") or ""),
                },
            ))
        elif transform_mode == "map_ref":
            flow.append((
                "map",
                {
                    # Strip whitespace so a padded literal map ID
                    # ("  ABC-MAP-123  ") becomes canonical before
                    # emission. Padded $ref tokens are already rejected
                    # at validate_config (r7 P2.2). Codex review r8 F3.
                    "map_id": str(transform.get("map_ref") or transform.get("map_id") or "").strip(),
                    "userlabel": str(transform.get("label") or ""),
                },
            ))
        elif transform_mode == "dataprocess":
            # Issue #106 M10.2: a process-level Data Process shape carrying one or
            # more ordered operation steps (v1: Custom Scripting). It sits in the
            # same middle-transform slot as message/map_ref. validate_config has
            # already proven the steps list; build() re-reads it total.
            flow.append((
                "dataprocess",
                {
                    "steps": transform.get("steps") or [],
                    "userlabel": str(transform.get("label") or ""),
                },
            ))
        elif transform_mode == "doccacheretrieve":
            # Issue #109 M10.5: a process-level Document Cache Retrieve shape that
            # pulls documents from a Document Cache into the current flow (the read
            # half of Document Cache CRUD). It sits in the same middle-transform
            # slot as message/map_ref/dataprocess. validate_config has already
            # proven the config; build() re-reads it total.
            flow.append((
                "doccacheretrieve",
                {
                    "document_cache_id": str(transform.get("document_cache_id") or "").strip(),
                    "empty_cache_behavior": str(
                        transform.get("empty_cache_behavior")
                        or _DOCCACHE_RETRIEVE_DEFAULT_EMPTY_BEHAVIOR
                    ).strip(),
                    "load_all_documents": transform.get("load_all_documents", True),
                    "userlabel": str(transform.get("label") or ""),
                },
            ))
        elif transform_mode == "doccacheremove":
            # Issue #110 M10.6: a process-level Document Cache Remove shape that
            # clears documents from a Document Cache (the delete half of Document
            # Cache CRUD). It sits in the same middle-transform slot as
            # message/map_ref/dataprocess/doccacheretrieve. validate_config has
            # already proven the config; build() re-reads it total.
            flow.append((
                "doccacheremove",
                {
                    "document_cache_id": str(transform.get("document_cache_id") or "").strip(),
                    "remove_all_documents": transform.get("remove_all_documents", True),
                    "userlabel": str(transform.get("label") or ""),
                },
            ))
        # Issue #112 M10.8: a Branch (N-way fan-out) block replaces the single
        # target + terminal with a forward fan-out — the post-source document is
        # branched to N independent REST-target -> Stop legs (leg 1 = top-level
        # target, legs 2..N = branch.targets). At this point ``flow`` holds only
        # the shared pre-branch chain (start -> source -> [transform]). build()
        # stays total on a validate_config-bypass by funnelling through the SAME
        # _validate_branch_config validator validate_config uses. Run it
        # UNCONDITIONALLY (it returns None when ``branch`` is absent or disabled),
        # NOT behind _branch_enabled(): a malformed branch block that makes
        # _branch_enabled() false (a non-dict ``branch=1``, or a non-bool
        # ``enabled``) must still raise PROCESS_BRANCH_CONFIG_INVALID here — exactly
        # as validate_config does — rather than being silently dropped while build()
        # emits the linear flow. _branch_enabled() then only picks the emission
        # path. A non-list/empty targets raises BRANCH_OUTPUT_UNSET (no degenerate
        # 1-leg fan-out); >25 legs / malformed leg bindings / an unsupported v1
        # composition (dynamic_path / Try-Catch / Return Documents) all raise too.
        branch_err = _validate_branch_config(config)
        if branch_err is not None:
            raise branch_err
        # Issue #113 M10.9: same totality contract for the Decision block — run the
        # SAME validator validate_config uses, unconditionally, so a malformed
        # decision raises here too (the composition guard also rejects branch +
        # decision together, so at most one of the two emission paths below fires).
        decision_err = _validate_decision_config(config)
        if decision_err is not None:
            raise decision_err
        if _branch_enabled(config):
            legs: List[List[Tuple[str, Dict[str, Any]]]] = []
            for leg_target in _branch_leg_targets(config):
                legs.append([
                    ("connectoraction_target", _branch_target_params(leg_target)),
                    ("stop", {"continue_": True}),
                ])
            shape_xml_parts: List[str] = _emit_branch_shapes(flow, legs)
        elif _decision_enabled(config):
            # The TRUE leg is the forward success path: the top-level target then a
            # Stop. The FALSE leg is forward (an optional false_notify Message then
            # its own Stop) or a backward loop (false_next names an earlier shape;
            # the Message tail — or, with no false_notify, the false dragpoint —
            # wires back to it). Decision rejects dynamic_path (composition guard),
            # so the true target carries dynamic_path=None.
            decision = config.get("decision") or {}
            true_leg: List[Tuple[str, Dict[str, Any]]] = [
                (
                    "connectoraction_target",
                    {
                        "connector_type": _canonical_connector_type(target.get("connector_type")),
                        "action_type": str(target.get("action_type") or "").strip().upper(),
                        "connection_id": str(target.get("connection_id") or "").strip(),
                        "operation_id": str(target.get("operation_id") or "").strip(),
                        "userlabel": str(target.get("label") or ""),
                        "dynamic_path": None,
                    },
                ),
                ("stop", {"continue_": True}),
            ]
            false_next = decision.get("false_next")
            false_loop_to = (
                false_next.strip()
                if isinstance(false_next, str) and false_next.strip()
                else None
            )
            false_leg: List[Tuple[str, Dict[str, Any]]] = []
            false_notify = decision.get("false_notify")
            if false_notify:
                false_leg.append(("message", {"text": str(false_notify), "userlabel": ""}))
            if false_loop_to is None:
                false_leg.append(("stop", {"continue_": True}))
            shape_xml_parts = _emit_decision_shapes(
                flow, decision, true_leg, false_leg, false_loop_to
            )
        else:
            # Issue #100 G2: when the target carries a dynamic_path block, insert a
            # Set Properties (documentproperties) shape AFTER the transform and
            # BEFORE the target connector so the path DPP is built per-document, and
            # pass dynamic_path into the connector step so it emits the matching
            # "Path" dynamic operation property. Absent dynamic_path, no shape is
            # added and the flow is byte-for-byte the pre-#100 chain.
            # Canonicalize the target connector subtype + verb before emission,
            # mirroring the source branch. A REST target keeps the canonical
            # mixed-case REST Client subtype and an uppercased HTTP verb. A
            # DATABASE write target (#74 M5.8) lowercases the subtype to
            # `database` and keeps the mixed-case verb `Send` (Boomi's DB
            # connectoraction emits actionType="Send", like the DB source's "Get"
            # — an unconditional .upper() would corrupt it to "SEND"). A DB target
            # carries no dynamic_path (REST path-binding only).
            target_canonical_type = _canonical_connector_type(target.get("connector_type"))
            target_is_rest = _resolve_rest_connector_type(target.get("connector_type")) is not None
            if target_is_rest:
                target_connector_type = target_canonical_type
                target_action_type = str(target.get("action_type") or "").strip().upper()
            else:
                target_connector_type = target_canonical_type.lower()
                target_action_type = str(target.get("action_type") or "").strip()
            dynamic_path = (
                target.get("dynamic_path")
                if (target_is_rest and isinstance(target.get("dynamic_path"), dict))
                else None
            )
            if dynamic_path:
                flow.append((
                    "setproperties",
                    {
                        "ddp_name": str(dynamic_path.get("ddp_name") or "").strip(),
                        "request_profile_id": str(dynamic_path.get("request_profile_id") or "").strip(),
                        "profile_type": str(dynamic_path.get("profile_type") or "profile.json").strip(),
                        "segments": dynamic_path.get("segments") or [],
                    },
                ))
            flow.append((
                "connectoraction_target",
                {
                    "connector_type": target_connector_type,
                    "action_type": target_action_type,
                    # Strip ID whitespace so whitespace-padded refs don't leak
                    # into emitted XML. Codex review r6 P2.2.
                    "connection_id": str(target.get("connection_id") or "").strip(),
                    "operation_id": str(target.get("operation_id") or "").strip(),
                    "userlabel": str(target.get("label") or ""),
                    "dynamic_path": dynamic_path,
                },
            ))
            # Issue #107 M10.3: the terminal is normally a Stop, but a
            # return_documents.enabled=True block swaps it for a Return Documents
            # shape (subprocess return value) — no Stop is appended after it.
            flow.append(_terminal_flow_entry(config))

            # Issue #51 M3.R1a + #88 M4.5.3: when retry_count is 0..5 and a supported
            # DLQ mode is set, wrap the linear flow in the verified Try/Catch
            # (catcherrors) shape with a DLQ catch path, emitting the validated
            # retry count. Otherwise emit the unchanged linear flow so existing
            # non-DLQ process XML is byte-for-byte identical.
            reliability_cfg = config.get("reliability")
            # Issue #96 review: totality on a validate_config bypass — a source
            # dynamic_path + connector-scoped Try/Catch would mis-wrap the source
            # Set Properties shape; raise rather than emit broken XML.
            source_scope_err = _source_dynamic_path_connector_scope_error(config)
            if source_scope_err is not None:
                raise source_scope_err
            if cls._should_emit_try_catch(reliability_cfg):
                # _should_emit_try_catch proved retry_count is a valid int 0..5 and
                # EITHER reliability_cfg["dlq"] is a Try/Catch DLQ mode OR
                # reliability_cfg["catch_exception"] is a dict (issue #108 M10.4 — a
                # bare catcherrors -> exception leg needs no DLQ). So dlq may be absent
                # for a catch_exception-only leg; default it to {} (the emitter skips
                # the DLQ route when the mode is not a supported DLQ mode).
                #
                # Issue #99 G1: reliability.try_catch_scope selects the wrapper
                # layout. "process" (default — absent key keeps the pre-#99 output
                # byte-for-byte identical) wraps the whole chain in ONE Try/Catch;
                # "connector" emits a Try/Catch per connector (source retry 0, target
                # retry N) so a target retry does not re-run the source read.
                try_catch_scope = str(
                    reliability_cfg.get("try_catch_scope") or "process"
                ).strip().lower()
                emitter = (
                    _emit_connector_scoped_try_catch_shapes
                    if try_catch_scope == "connector"
                    else _emit_try_catch_shapes
                )
                shape_xml_parts = emitter(
                    flow,
                    reliability_cfg.get("dlq") or {},
                    retry_count=int(reliability_cfg.get("retry_count", 0)),
                    catch_notify=reliability_cfg.get("catch_notify"),
                    catch_exception=reliability_cfg.get("catch_exception"),
                )
            else:
                # build() stays total on the validate_config-bypass path: a present
                # catch_notify cannot be honored without a Try/Catch catch leg, so
                # raise rather than silently dropping it (issue #89; mirrors the DLQ
                # binding guard inside _emit_try_catch_shapes). validate_config
                # already rejects this combination on the normal path.
                if isinstance(reliability_cfg, dict) and reliability_cfg.get("catch_notify") is not None:
                    raise BuilderValidationError(
                        "reliability.catch_notify requires a wired Try/Catch catch path.",
                        error_code="PROCESS_NOTIFY_CONFIG_INVALID",
                        field="reliability.catch_notify",
                        hint=(
                            "Notify is emitted only on a catch leg. Set "
                            "reliability.dlq.mode to document_cache_ref or "
                            "error_subprocess_ref."
                        ),
                    )
                # Issue #108 M10.4: same totality guard for catch_exception — a present
                # (but malformed, so _should_emit_try_catch rejected it) catch_exception
                # cannot be honored without a wired Try/Catch leg. A WELL-FORMED
                # catch_exception always makes _should_emit_try_catch True, so this only
                # fires on the bypass path with an invalid block; validate_config
                # rejects it with PROCESS_EXCEPTION_CONFIG_INVALID on the normal path.
                if isinstance(reliability_cfg, dict) and reliability_cfg.get("catch_exception") is not None:
                    raise BuilderValidationError(
                        "reliability.catch_exception requires a wired Try/Catch catch path.",
                        error_code="PROCESS_EXCEPTION_CONFIG_INVALID",
                        field="reliability.catch_exception",
                        hint=(
                            "The Exception throw is emitted only on a catch leg. Provide "
                            "a well-formed reliability.catch_exception "
                            '(message_template required).'
                        ),
                    )
                shape_xml_parts = _emit_linear_shapes(flow)

        # Issue #92 M4.5.7: emit a non-empty <bns:processOverrides> declaring
        # connection-field environment extensions when the config carries a
        # process_extensions block; otherwise _assemble emits the empty override
        # element byte-for-byte as before. validate_config already proved the
        # shape; _extract stays defensive so build() is total on a bypass path.
        # Connection ids inside process_extensions resolve via the same $ref
        # walker as connector shapes (apply-time substitution + reachability),
        # so no resolver change is needed. The forbidden-secret scanner matches
        # dict KEYS only (id/label/xpath are not secret-shaped, and the field id
        # value "password" sits under key "id"), so the declaration is clean.
        process_overrides_xml = ""
        connections = _extract_process_extension_connections(config)
        if connections:
            process_overrides_xml = _emit_process_overrides(connections)

        return _assemble_process_component_xml(
            shape_xml_parts,
            name=name,
            description=description,
            folder_name=folder_name,
            process_overrides_xml=process_overrides_xml,
        )


def _extract_process_extension_connections(
    config: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Read + normalize ``config["process_extensions"]["connections"]``.

    Returns ``[]`` when the block is absent/empty. Otherwise returns a list of
    ``{"connection_id": <resolved id or $ref token>, "connector_type"?: str,
    "fields": [{"id","label","xpath"}, ...]}`` with field order preserved.

    Raises ``BuilderValidationError(error_code="PROCESS_EXTENSIONS_INVALID")``
    on any malformed shape so both validate_config (which catches it) and
    build() (which lets it raise) share one source of truth for the contract.
    """
    process_extensions = config.get("process_extensions")
    if process_extensions in (None, {}, []):
        return []
    if not isinstance(process_extensions, dict):
        raise BuilderValidationError(
            "process_extensions must be a JSON object with a 'connections' list.",
            error_code="PROCESS_EXTENSIONS_INVALID",
            field="process_extensions",
            hint='Shape: {"connections": [{"connection_id": "...", "fields": [...]}]}.',
        )
    # A present, non-empty process_extensions block MUST carry a 'connections'
    # key. A missing/misspelled key (e.g. "connection") or a null value would
    # otherwise silently drop the caller's override declaration — reject it so
    # the documented PROCESS_EXTENSIONS_INVALID contract holds. An absent/empty
    # block (handled above) or an explicitly empty connections list is a no-op.
    if "connections" not in process_extensions:
        raise BuilderValidationError(
            "process_extensions must contain a 'connections' list.",
            error_code="PROCESS_EXTENSIONS_INVALID",
            field="process_extensions.connections",
            hint=(
                'Shape: {"connections": [{"connection_id": "...", "fields": '
                '[...]}]}. (Did you mean "connections"?)'
            ),
        )
    raw_connections = process_extensions.get("connections")
    if raw_connections is None:
        raise BuilderValidationError(
            "process_extensions.connections must be a list, not null.",
            error_code="PROCESS_EXTENSIONS_INVALID",
            field="process_extensions.connections",
            hint='Provide a list: {"connections": [{"connection_id": "...", "fields": [...]}]}.',
        )
    if not isinstance(raw_connections, list):
        raise BuilderValidationError(
            "process_extensions.connections must be a list of connection-override "
            "declarations.",
            error_code="PROCESS_EXTENSIONS_INVALID",
            field="process_extensions.connections",
            hint='Each entry: {"connection_id": "...", "fields": [{"id","label","xpath"}]}.',
        )
    if not raw_connections:
        # Explicitly empty connections list — nothing to declare, valid no-op.
        return []

    normalized: List[Dict[str, Any]] = []
    for i, entry in enumerate(raw_connections):
        loc = f"process_extensions.connections[{i}]"
        if not isinstance(entry, dict):
            raise BuilderValidationError(
                f"{loc} must be a JSON object.",
                error_code="PROCESS_EXTENSIONS_INVALID",
                field=loc,
                hint='Each entry: {"connection_id": "...", "fields": [{"id","label","xpath"}]}.',
            )
        conn_id = entry.get("connection_id")
        if not isinstance(conn_id, str) or not conn_id.strip():
            raise BuilderValidationError(
                f"{loc}.connection_id is required and must be a non-empty string.",
                error_code="PROCESS_EXTENSIONS_INVALID",
                field=f"{loc}.connection_id",
                hint=(
                    "Use the same connection id / $ref:KEY token the connector "
                    "shapes bind to, so the override declaration resolves to the "
                    "same component."
                ),
            )
        raw_fields = entry.get("fields")
        if not isinstance(raw_fields, list) or not raw_fields:
            raise BuilderValidationError(
                f"{loc}.fields must be a non-empty list of field declarations.",
                error_code="PROCESS_EXTENSIONS_INVALID",
                field=f"{loc}.fields",
                hint='Each field: {"id": "password", "label": "Password", "xpath": "..."}.',
            )
        fields: List[Dict[str, str]] = []
        for j, raw_field in enumerate(raw_fields):
            floc = f"{loc}.fields[{j}]"
            if not isinstance(raw_field, dict):
                raise BuilderValidationError(
                    f"{floc} must be a JSON object with id, label, and xpath.",
                    error_code="PROCESS_EXTENSIONS_INVALID",
                    field=floc,
                    hint='Each field: {"id": "...", "label": "...", "xpath": "..."}.',
                )
            normalized_field: Dict[str, str] = {}
            for key in ("id", "label"):
                value = raw_field.get(key)
                if not isinstance(value, str) or not value.strip():
                    raise BuilderValidationError(
                        f"{floc}.{key} is required and must be a non-empty string.",
                        error_code="PROCESS_EXTENSIONS_INVALID",
                        field=f"{floc}.{key}",
                        hint="Field declarations carry an id and a label (xpath optional).",
                    )
                # label intentionally not stripped — a leading/trailing space is
                # cosmetic and the value is escaped on emission anyway. id is
                # structural, so canonicalize it.
                normalized_field[key] = value.strip() if key == "id" else value
            # xpath is REQUIRED only for an EXPLICIT DB override entry
            # (connector_type='database'), which is xpath-keyed (e.g.
            # DatabaseConnectionSettings/@username) — a missing xpath there emits a
            # declaration that never maps to the DB field. A no-xpath field is the
            # id-keyed (REST) form, valid by ITSELF without requiring connector_type
            # to be set (Codex review: a hand-authored REST override that omits
            # connector_type must still build). So only an explicitly-DB entry
            # mandates xpath; REST aliases / omitted / unknown connector_type leave
            # it optional.
            connector_type = entry.get("connector_type")
            entry_is_db = (
                isinstance(connector_type, str)
                and connector_type.strip().lower() == "database"
            )
            xpath = raw_field.get("xpath")
            if xpath is None:
                if entry_is_db:
                    raise BuilderValidationError(
                        f"{floc}.xpath is required for a database connection override.",
                        error_code="PROCESS_EXTENSIONS_INVALID",
                        field=f"{floc}.xpath",
                        hint=(
                            "DB overrides are xpath-keyed; set xpath (e.g. "
                            "'DatabaseConnectionSettings/@username'). REST overrides "
                            "are id-keyed and omit xpath."
                        ),
                    )
            elif not isinstance(xpath, str) or not xpath.strip():
                raise BuilderValidationError(
                    f"{floc}.xpath must be a non-empty string when present.",
                    error_code="PROCESS_EXTENSIONS_INVALID",
                    field=f"{floc}.xpath",
                    hint="Omit xpath entirely for id-keyed (REST) overrides.",
                )
            else:
                normalized_field["xpath"] = xpath.strip()
            fields.append(normalized_field)
        normalized_entry: Dict[str, Any] = {
            "connection_id": conn_id.strip(),
            "fields": fields,
        }
        connector_type = entry.get("connector_type")
        if isinstance(connector_type, str) and connector_type.strip():
            normalized_entry["connector_type"] = connector_type.strip()
        normalized.append(normalized_entry)
    return normalized


def _emit_process_overrides(connections: List[Dict[str, Any]]) -> str:
    """Emit a non-empty ``<bns:processOverrides>`` declaring connection-field
    environment extensions (issue #92 M4.5.7).

    ``connections`` is the normalized list from
    :func:`_extract_process_extension_connections`. The container shape and
    sibling order are ``live_verified`` from the ``work``-profile main-sync
    exemplar; field order is preserved from the input. All attribute values are
    XML-escaped. ``connector_type`` is carried in config for downstream tooling
    but is not part of the emitted declaration (Boomi keys overrides by the
    connection id + field id, not by connector type).
    """
    connection_parts: List[str] = []
    for conn in connections:
        field_parts: List[str] = []
        for field in conn["fields"]:
            # xpath is emitted only when present (#102 B1): DB overrides carry an
            # xpath; live REST Client overrides key purely by field id and emit
            # none — matching the live `Rest Example` process export.
            xpath = field.get("xpath")
            xpath_attr = f' xpath="{_escape_xml(str(xpath))}"' if xpath else ""
            field_parts.append(
                f'<field id="{_escape_xml(str(field["id"]))}" '
                f'label="{_escape_xml(str(field["label"]))}" '
                f'overrideable="true"{xpath_attr}/>'
            )
        connection_parts.append(
            f'<ConnectionOverride id="{_escape_xml(str(conn["connection_id"]))}">'
            f"{''.join(field_parts)}"
            '</ConnectionOverride>'
        )
    return (
        '<bns:processOverrides>'
        '<Overrides xmlns="">'
        f"<Connections>{''.join(connection_parts)}</Connections>"
        '<Operations/>'
        '<PartnerOverrides/>'
        '<Properties/>'
        '<Extensions>'
        '<ObjectDefinitions><unusedProfiles/></ObjectDefinitions>'
        '<DataMaps><unusedMaps/></DataMaps>'
        '</Extensions>'
        '<CrossReferenceOverrides/>'
        '<PGPOverrides/>'
        '<DefinedProcessPropertyOverrides/>'
        '</Overrides>'
        '</bns:processOverrides>'
    )


def _assemble_process_component_xml(
    shape_xml_parts: List[str],
    *,
    name: str,
    description: str = "",
    folder_name: Optional[str] = None,
    process_overrides_xml: str = "",
) -> str:
    """Wrap emitted shapes in the ``<process>`` / ``<bns:Component>`` envelope.

    Shared by ProcessFlowBuilder and WrapperSubprocessBuilder (issue #90). Coerces
    and requires a non-empty name, emits ``folderName`` when set, and round-trips
    the result through ElementTree (PROCESS_XML_VALIDATION_FAILED on malformation).

    folderName is the writable folder attribute on Component create/update;
    folderFullPath is response-only metadata Boomi ignores on writes. Other
    builders emit folderName for placement — match them (Codex review r8 F2).
    """
    name = str(name) if name is not None else ""
    if not name or not name.strip():
        raise BuilderValidationError(
            "Process component name is required.",
            error_code="PROCESS_XML_VALIDATION_FAILED",
            field="name",
            hint="Pass a non-empty name via the IntegrationComponentSpec.name field.",
        )
    process_inner = (
        '<process xmlns="" '
        'allowSimultaneous="false" '
        'enableUserLog="false" '
        'processLogOnErrorOnly="false" '
        'purgeDataImmediately="false" '
        'stopProcessingIfZeroDocuments="true" '
        'updateRunDates="true" '
        'workload="general">'
        '<shapes>'
        f"{''.join(shape_xml_parts)}"
        '</shapes>'
        '</process>'
    )
    folder_attr = (
        f' folderName="{_escape_xml(str(folder_name))}"' if folder_name else ""
    )
    component_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<bns:Component '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xmlns:bns="http://api.platform.boomi.com/" '
        f'type="process" name="{_escape_xml(name)}"'
        f"{folder_attr}>"
        '<bns:encryptedValues/>'
        f'<bns:description>{_escape_xml(description)}</bns:description>'
        '<bns:object>'
        f"{process_inner}"
        '</bns:object>'
        # Issue #92 M4.5.7: a connection-field extension declaration when one was
        # emitted, else the empty element (byte-for-byte unchanged for all
        # existing process XML, including wrapper_subprocess).
        f"{process_overrides_xml or '<bns:processOverrides/>'}"
        '</bns:Component>'
    )

    # Internal invariant: the XML we just produced must round-trip through
    # ElementTree without raising. Catches stray unescaped chars or malformed
    # manual concatenation early — surfaces as PROCESS_XML_VALIDATION_FAILED
    # rather than a confusing Boomi API error at apply time.
    try:
        ET.fromstring(component_xml)
    except ET.ParseError as exc:  # pragma: no cover — defensive
        raise BuilderValidationError(
            f"Generated process Component XML did not round-trip: {exc}",
            error_code="PROCESS_XML_VALIDATION_FAILED",
            field="config",
            hint="Internal builder bug — please report.",
        ) from exc

    return component_xml


# ----------------------------------------------------------------------
# Field-level validators (split out so error messages can be specific)
# ----------------------------------------------------------------------

def _validate_source_binding(
    source: Any, *, allow_rest_source: bool = False, allow_soap_source: bool = False
) -> Optional[BuilderValidationError]:
    if not isinstance(source, dict):
        return BuilderValidationError(
            "source binding must be a JSON object with connector_type, "
            "connection_id, operation_id, and action_type.",
            error_code="PROCESS_CONNECTOR_BINDING_INVALID",
            field="source",
            hint="See get_schema_template(resource_type='process', operation='create', protocol='database_to_api_sync').",
        )
    # The source connector is always a database (db_read, action_type='Get') for the
    # base database_to_api_sync protocol. A REST Client GET source is ONLY valid
    # through the sync_pipeline fetch lowering (M5.4 #72), which passes
    # allow_rest_source=True — so a hand-authored database_to_api_sync stays
    # DB-source-only, exactly as the #72 plan intends. A SOAP Client EXECUTE
    # source is likewise valid only through the sync_pipeline soap_fetch lowering
    # (#126), gated by allow_soap_source.
    raw_connector_type = source.get("connector_type")
    connector_type = str(raw_connector_type or "").strip().lower()
    rest_source = allow_rest_source and _resolve_rest_connector_type(raw_connector_type) is not None
    soap_source = allow_soap_source and _resolve_soap_client_connector_type(raw_connector_type) is not None
    if connector_type != "database" and not rest_source and not soap_source:
        if allow_rest_source or allow_soap_source:
            return BuilderValidationError(
                f"source.connector_type must be 'database' (db_read), a REST Client "
                f"connector (rest_fetch), or a SOAP Client connector (soap_fetch); "
                f"got {connector_type!r}.",
                error_code="PROCESS_CONNECTOR_BINDING_INVALID",
                field="source.connector_type",
                hint="Database (db_read Get), REST Client (rest_fetch GET), and SOAP Client (soap_fetch EXECUTE) are the supported source connectors.",
            )
        return BuilderValidationError(
            f"source.connector_type must be 'database' for "
            f"database_to_api_sync; got {connector_type!r}.",
            error_code="PROCESS_CONNECTOR_BINDING_INVALID",
            field="source.connector_type",
            hint="Database is the only source connector for database_to_api_sync; a REST source is expressed via a sync_pipeline fetch stage (rest_fetch).",
        )
    action_type = str(source.get("action_type") or "").strip()
    if soap_source:
        # SOAP Client is EXECUTE-only (#126) — reject any other action.
        if action_type.upper() != "EXECUTE":
            return BuilderValidationError(
                f"source.action_type must be 'EXECUTE' for a SOAP fetch source; "
                f"got {action_type!r}.",
                error_code="PROCESS_CONNECTOR_BINDING_INVALID",
                field="source.action_type",
                hint="soap_fetch is EXECUTE-only (#126); the SOAP Client exposes a single EXECUTE action.",
            )
    elif rest_source:
        # REST fetch is GET-only in M5.4 — reject any other verb so a source-side
        # write can never be modeled here.
        if action_type.upper() != "GET":
            return BuilderValidationError(
                f"source.action_type must be 'GET' for a REST fetch source; "
                f"got {action_type!r}.",
                error_code="PROCESS_CONNECTOR_BINDING_INVALID",
                field="source.action_type",
                hint="rest_fetch is GET-only in M5.4 (#72); other verbs are out of scope.",
            )
    elif action_type not in _DB_ACTION_TYPES:
        return BuilderValidationError(
            f"source.action_type must be 'Get' for database source; "
            f"got {action_type!r}.",
            error_code="PROCESS_CONNECTOR_BINDING_INVALID",
            field="source.action_type",
            hint="Issue #32 will cover Send/Upsert write paths.",
        )
    for required in ("connection_id", "operation_id"):
        value = source.get(required)
        if not isinstance(value, str) or not value.strip():
            return BuilderValidationError(
                f"source.{required} is required.",
                error_code="PROCESS_CONNECTOR_BINDING_INVALID",
                field=f"source.{required}",
                hint=(
                    "Pass the component_id of the already-built "
                    "connector-settings / connector-action, or a "
                    "$ref:KEY token pointing at it (and add KEY to "
                    "depends_on)."
                ),
            )
    # Issue #96 M5.4a: a REST source may carry a lowered dynamic_path (a path
    # runtime binding); validate it with the same shape contract the target uses,
    # and gate any raw runtime_bindings block (query/header/DDP/DPP) the builder
    # does not emit as process XML.
    dyn_err = _validate_dynamic_path(source.get("dynamic_path"), field_prefix="source")
    if dyn_err is not None:
        return dyn_err
    return _validate_runtime_bindings_gate(source, "source")


def _validate_db_target_binding(
    target: Dict[str, Any], field_prefix: str
) -> Optional[BuilderValidationError]:
    """Validate a database Send (write) target binding (#74 M5.8).

    A `write` stage lowers to a `database`/`Send` target. The action_type is
    NOT uppercased for membership (Boomi's DB connectoraction emits the mixed-case
    verb ``Send``, mirroring the DB source's ``Get``). connection_id/operation_id
    are required; a DB target carries no dynamic_path/runtime_bindings (those are
    REST path-binding only), so the runtime-binding gate still applies.
    """
    action_type = str(target.get("action_type") or "").strip()
    if action_type not in _DB_TARGET_ACTION_TYPES:
        return BuilderValidationError(
            f"{field_prefix}.action_type must be one of {sorted(_DB_TARGET_ACTION_TYPES)} "
            f"for a database target; got {target.get('action_type')!r}.",
            error_code="PROCESS_CONNECTOR_BINDING_INVALID",
            field=f"{field_prefix}.action_type",
            hint="A database write target is a Send operation (db_write); Get is a source.",
        )
    for required in ("connection_id", "operation_id"):
        value = target.get(required)
        if not isinstance(value, str) or not value.strip():
            return BuilderValidationError(
                f"{field_prefix}.{required} is required.",
                error_code="PROCESS_CONNECTOR_BINDING_INVALID",
                field=f"{field_prefix}.{required}",
                hint=(
                    "Pass the component_id of the already-built database "
                    "connector-settings / connector-action Send, or a "
                    "$ref:KEY token pointing at it (and add KEY to depends_on)."
                ),
            )
    dyn_err = _validate_dynamic_path(target.get("dynamic_path"), field_prefix=field_prefix)
    if dyn_err is not None:
        return dyn_err
    return _validate_runtime_bindings_gate(target, field_prefix)


def _validate_soap_target_binding(
    target: Dict[str, Any], field_prefix: str
) -> Optional[BuilderValidationError]:
    """Validate a SOAP Client EXECUTE target binding (#126).

    A ``soap_send`` stage lowers to a ``wssoapclientsdk``/``EXECUTE`` target. The
    action_type must be EXECUTE (the SOAP Client's only action). connection_id /
    operation_id are required; a SOAP target carries no dynamic_path (REST
    path-binding only), so the runtime-binding gate still applies.
    """
    action_type = str(target.get("action_type") or "").strip().upper()
    if action_type not in _SOAP_ACTION_TYPES:
        return BuilderValidationError(
            f"{field_prefix}.action_type must be 'EXECUTE' for a SOAP target; "
            f"got {target.get('action_type')!r}.",
            error_code="PROCESS_CONNECTOR_BINDING_INVALID",
            field=f"{field_prefix}.action_type",
            hint="A SOAP Client target is a single EXECUTE operation (soap_send).",
        )
    for required in ("connection_id", "operation_id"):
        value = target.get(required)
        if not isinstance(value, str) or not value.strip():
            return BuilderValidationError(
                f"{field_prefix}.{required} is required.",
                error_code="PROCESS_CONNECTOR_BINDING_INVALID",
                field=f"{field_prefix}.{required}",
                hint=(
                    "Pass the component_id of the already-built SOAP "
                    "connector-settings / connector-action EXECUTE, or a "
                    "$ref:KEY token pointing at it (and add KEY to depends_on)."
                ),
            )
    return _validate_runtime_bindings_gate(target, field_prefix)


def _validate_target_binding(
    target: Any,
    field_prefix: str = "target",
    *,
    allow_db_target: bool = False,
    allow_soap_target: bool = False,
) -> Optional[BuilderValidationError]:
    """Validate one connector target binding (REST, or DB Send / SOAP when allowed).

    ``field_prefix`` scopes every emitted ``field``/message to the binding's
    location in the config tree — ``"target"`` for the top-level target (leg 1)
    and ``"branch.targets[i]"`` for Branch legs 2..N (issue #112 M10.8). The
    contract is identical regardless of prefix, so both share this one validator.

    ``allow_db_target`` (#74 M5.8) accepts a ``database``/``Send`` target in
    addition to REST — only the sync_pipeline path (api_to_database_sync) opts in,
    so a hand-authored database_to_api_sync target and every Branch leg stay
    REST-only (the default). ``allow_soap_target`` (#126) likewise accepts a
    ``wssoapclientsdk``/``EXECUTE`` SOAP target only through the sync_pipeline
    soap_send lowering.
    """
    if not isinstance(target, dict):
        return BuilderValidationError(
            f"{field_prefix} binding must be a JSON object with connector_type, "
            "connection_id, operation_id, and action_type.",
            error_code="PROCESS_CONNECTOR_BINDING_INVALID",
            field=field_prefix,
            hint="See get_schema_template(resource_type='process', operation='create', protocol='database_to_api_sync').",
        )
    raw_connector_type = target.get("connector_type")
    canonical = _resolve_rest_connector_type(raw_connector_type)
    if canonical is None:
        # #74 M5.8: a sync_pipeline write target lowers to a database Send binding.
        if (
            allow_db_target
            and isinstance(raw_connector_type, str)
            and raw_connector_type.strip().lower() == "database"
        ):
            return _validate_db_target_binding(target, field_prefix)
        # #126: a sync_pipeline soap_send target lowers to a SOAP EXECUTE binding.
        if (
            allow_soap_target
            and _resolve_soap_client_connector_type(raw_connector_type) is not None
        ):
            return _validate_soap_target_binding(target, field_prefix)
        return BuilderValidationError(
            f"{field_prefix}.connector_type must be 'rest', 'rest_client', or "
            f"{REST_CLIENT_SUBTYPE!r}; got {raw_connector_type!r}.",
            error_code="PROCESS_CONNECTOR_BINDING_INVALID",
            field=f"{field_prefix}.connector_type",
            hint="REST Client is the only supported target connector in M2.5.",
        )
    action_type = str(target.get("action_type") or "").strip().upper()
    if action_type not in _REST_ACTION_TYPES:
        return BuilderValidationError(
            f"{field_prefix}.action_type must be one of {sorted(_REST_ACTION_TYPES)}; "
            f"got {target.get('action_type')!r}.",
            error_code="PROCESS_CONNECTOR_BINDING_INVALID",
            field=f"{field_prefix}.action_type",
            hint="REST Client supports standard HTTP verbs.",
        )
    for required in ("connection_id", "operation_id"):
        value = target.get(required)
        if not isinstance(value, str) or not value.strip():
            return BuilderValidationError(
                f"{field_prefix}.{required} is required.",
                error_code="PROCESS_CONNECTOR_BINDING_INVALID",
                field=f"{field_prefix}.{required}",
                hint=(
                    "Pass the component_id of the already-built REST "
                    "connector-settings / connector-action, or a "
                    "$ref:KEY token pointing at it (and add KEY to "
                    "depends_on)."
                ),
            )
    dyn_err = _validate_dynamic_path(target.get("dynamic_path"), field_prefix=field_prefix)
    if dyn_err is not None:
        return dyn_err
    return _validate_runtime_bindings_gate(target, field_prefix)


def _validate_runtime_bindings_gate(
    binding: Any, field_prefix: str
) -> Optional[BuilderValidationError]:
    """Reject a raw ``runtime_bindings`` block in a source/target binding (#96 M5.4a).

    The builder emits a PATH runtime binding only via a lowered ``dynamic_path``
    block (Set Properties DDP + connector-step "Path" — the live-proven #100/#96
    mechanism); the rest_fetch/rest_send primitive performs that lowering. The
    builder itself does NOT lower a raw ``runtime_bindings`` block, so a
    ``runtime_bindings`` key reaching the builder is a hand-authored spec that
    bypassed the primitive; gate it as ``PROCESS_RUNTIME_BINDING_UNVERIFIED`` rather
    than silently dropping it. Absent / empty is a no-op (the normal lowered path).
    """
    if not isinstance(binding, dict):
        return None
    if binding.get("runtime_bindings"):
        return BuilderValidationError(
            f"{field_prefix}.runtime_bindings is not emitted as process XML.",
            error_code="PROCESS_RUNTIME_BINDING_UNVERIFIED",
            field=f"{field_prefix}.runtime_bindings",
            hint=(
                "A path runtime binding is emitted via a lowered dynamic_path "
                "block. Express it through the rest_fetch/rest_send primitive so it "
                "lowers to dynamic_path (the builder does not lower a raw "
                "runtime_bindings block)."
            ),
        )
    return None


def _source_dynamic_path_connector_scope_error(
    config: Dict[str, Any]
) -> Optional[BuilderValidationError]:
    """Reject a source dynamic_path combined with a connector-scoped Try/Catch (#96 review).

    A source dynamic_path (a rest_fetch path runtime binding) inserts a Set
    Properties shape BEFORE the source connector. The connector-scoped Try/Catch
    emitter (#99 G1) assumes ``flow[1]`` is the source connector, so it would wrap
    the Set Properties step instead of the source connector — leaving source
    failures uncaught/un-retried. v1 rejects this composition (the whole-process
    Try/Catch wraps the entire chain and is unaffected). Shared by validate_config
    (surfaces it) and build() (raises it for totality on a validate_config bypass).
    """
    source = config.get("source")
    if not (isinstance(source, dict) and source.get("dynamic_path") is not None):
        return None
    reliability = config.get("reliability")
    if not ProcessFlowBuilder._should_emit_try_catch(reliability):
        return None
    scope = str((reliability or {}).get("try_catch_scope") or "process").strip().lower()
    if scope != "connector":
        return None
    return BuilderValidationError(
        "source.dynamic_path is not supported together with a connector-scoped "
        "Try/Catch (reliability.try_catch_scope='connector') in v1.",
        error_code="PROCESS_RUNTIME_BINDING_UNVERIFIED",
        field="source.dynamic_path",
        hint=(
            "Use try_catch_scope='process' (the whole-process Try/Catch wraps the "
            "source path Set Properties + source connector together), or drop the "
            "source runtime path binding."
        ),
    )


def _validate_dynamic_path(
    dynamic_path: Any, field_prefix: str = "target"
) -> Optional[BuilderValidationError]:
    """Validate the optional ``<target>.dynamic_path`` block (issue #100 G2 / #96).

    Absent (None) is valid — the path stays static. When present it must carry a
    non-blank ``ddp_name`` and a non-empty ``segments`` list of well-formed
    static/profile/ddp/dpp entries with at least one DYNAMIC segment (profile / ddp
    / dpp — an all-static path would not be dynamic). ``request_profile_id`` is
    required ONLY when a ``profile`` segment is present (it feeds the single
    ``<profileelement profileId=...>``); a ddp/dpp-only dynamic path carries no
    profile and leaves it ``None`` (#96 §H). Errors never echo segment values (path
    strings / element / property names can carry caller-specific identifiers).
    ``field_prefix`` scopes the error to the owning target (issue #112 M10.8).
    """
    if dynamic_path is None:
        return None
    err = BuilderValidationError(
        f"{field_prefix}.dynamic_path is malformed.",
        error_code="PROCESS_PATH_REPLACEMENT_INVALID",
        field=f"{field_prefix}.dynamic_path",
        hint=(
            "dynamic_path needs a non-blank ddp_name and a non-empty segments list "
            "(static {type,value} / profile {type,element_id,element_name} / "
            "ddp|dpp {type,property_name}) with at least one dynamic (profile/ddp/dpp) "
            "segment; a profile segment also requires a non-blank request_profile_id. "
            "It is emitted by the rest_fetch/rest_send primitive from path "
            "runtime_bindings (or the database_to_api_sync archetype from "
            "target.send_request.path_replacements)."
        ),
    )
    if not isinstance(dynamic_path, dict):
        return err
    ddp_name = dynamic_path.get("ddp_name")
    if not isinstance(ddp_name, str) or not ddp_name.strip():
        return err
    segments = dynamic_path.get("segments")
    if not isinstance(segments, list) or not segments:
        return err
    profile_segments = 0
    dynamic_seg_count = 0
    for seg in segments:
        if not isinstance(seg, dict):
            return err
        seg_type = seg.get("type")
        if seg_type == "static":
            if not isinstance(seg.get("value"), str):
                return err
        elif seg_type == "profile":
            element_id = seg.get("element_id")
            if isinstance(element_id, bool) or not isinstance(element_id, (int, str)):
                return err
            if isinstance(element_id, str) and not element_id.strip():
                return err
            if not isinstance(seg.get("element_name"), str) or not seg.get("element_name").strip():
                return err
            profile_segments += 1
            dynamic_seg_count += 1
        elif seg_type in ("ddp", "dpp"):
            name = seg.get("property_name")
            if not isinstance(name, str) or not name.strip():
                return err
            dynamic_seg_count += 1
        else:
            return err
    if dynamic_seg_count == 0:
        return err
    # request_profile_id is consumed only by profile segments (one shared profile).
    rpid = dynamic_path.get("request_profile_id")
    if profile_segments:
        if not isinstance(rpid, str) or not rpid.strip():
            return err
    else:
        # A ddp/dpp-only path carries no profile, so request_profile_id must be
        # None/absent (the lowered shape rest_fetch/rest_send produce). Any present,
        # non-blank value is contradictory — a stray UUID string OR a malformed
        # non-string (123/true) — and would (absent the emitter guard) bind a
        # parameter-profile with no matching <profileelement>. Reject it at plan time.
        if rpid is not None and str(rpid).strip():
            return err
    return None


# ----------------------------------------------------------------------
# Issue #112 M10.8 — Branch (N-way forward fan-out)
# ----------------------------------------------------------------------


def _flow_control_enabled(config: Dict[str, Any]) -> bool:
    """True when the config carries an enabled ``flow_control`` batching block.

    Absent block, a non-dict block, or ``enabled: False`` all keep the process on
    the pre-#111 chain (byte-for-byte) with no Flow Control shape.
    """
    flow_control = config.get("flow_control")
    if not isinstance(flow_control, dict):
        return False
    return flow_control.get("enabled", True) is True


def _flow_control_unsupported_combo_error(
    config: Dict[str, Any]
) -> Optional[BuilderValidationError]:
    """v1 Flow Control composition guard (issue #111 M10.7).

    Flow Control v1 is a single per-document batching shape inserted into the
    linear (optionally Try/Catch-wrapped) chain right after the source. It does
    NOT yet compose with a Branch fan-out or a Decision route — both change the
    shape graph's topology (and the Decision shifts the index-dependent
    pre-Decision chain), so each is a clean ``PROCESS_FLOW_CONTROL_CONFIG_INVALID``
    rejection (follow-up work), never a silent drop. Shared by ``validate_config``
    (surfaces it) and ``build()`` (raises it for totality on a validate_config
    bypass), so the two paths cannot diverge.
    """
    if _branch_enabled(config):
        return BuilderValidationError(
            "flow_control is not supported together with a branch fan-out in v1.",
            error_code="PROCESS_FLOW_CONTROL_CONFIG_INVALID",
            field="flow_control",
            hint="Drop the branch block to batch, or drop flow_control to fan out.",
        )
    if _decision_enabled(config):
        return BuilderValidationError(
            "flow_control is not supported together with a decision in v1.",
            error_code="PROCESS_FLOW_CONTROL_CONFIG_INVALID",
            field="flow_control",
            hint="Drop the decision block to batch, or drop flow_control to route.",
        )
    return None


def _validate_flow_control_config(
    config: Dict[str, Any]
) -> Optional[BuilderValidationError]:
    """Validate the optional ``flow_control`` per-document batching block (issue #111 M10.7).

    Absent, or ``enabled: False``, is valid — no Flow Control shape is emitted.
    When enabled the precedence (mirroring Branch/Decision) is: block structure ->
    unknown key -> non-bool ``enabled`` -> unsupported v1 composition
    (branch / decision) -> ``for_each_count`` (a positive int) -> optional
    ``label`` (a string). ``build()`` funnels through this SAME validator, so the
    two paths cannot diverge on which structured error a malformed flow_control
    yields.
    """
    flow_control = config.get("flow_control")
    if flow_control is None:
        return None
    if not isinstance(flow_control, dict):
        return BuilderValidationError(
            "flow_control must be a JSON object with an optional 'enabled' flag and a 'for_each_count'.",
            error_code="PROCESS_FLOW_CONTROL_CONFIG_INVALID",
            field="flow_control",
            hint='Shape: {"enabled": true, "for_each_count": 10, "label": "optional"}.',
        )
    unknown = set(flow_control) - _FLOW_CONTROL_ALLOWED_KEYS
    if unknown:
        return BuilderValidationError(
            f"flow_control has unsupported key(s): {sorted(unknown)}.",
            error_code="PROCESS_FLOW_CONTROL_CONFIG_INVALID",
            field="flow_control",
            hint=f"Supported flow_control keys: {sorted(_FLOW_CONTROL_ALLOWED_KEYS)}.",
        )
    enabled = flow_control.get("enabled", True)
    if not isinstance(enabled, bool):
        return BuilderValidationError(
            "flow_control.enabled must be a boolean.",
            error_code="PROCESS_FLOW_CONTROL_CONFIG_INVALID",
            field="flow_control.enabled",
            hint="Use true to batch, or omit/false to keep the unbatched flow.",
        )
    if not enabled:
        return None
    combo_err = _flow_control_unsupported_combo_error(config)
    if combo_err is not None:
        return combo_err
    for_each_count = flow_control.get("for_each_count")
    if (
        not isinstance(for_each_count, int)
        or isinstance(for_each_count, bool)
        or for_each_count <= 0
    ):
        return BuilderValidationError(
            "flow_control.for_each_count must be a positive integer (documents per batch).",
            error_code="PROCESS_FLOW_CONTROL_CONFIG_INVALID",
            field="flow_control.for_each_count",
            hint="v1 supports per-document batching: set for_each_count to the batch size, e.g. 10.",
        )
    label = flow_control.get("label")
    if label is not None and not isinstance(label, str):
        return BuilderValidationError(
            "flow_control.label must be a string when provided.",
            error_code="PROCESS_FLOW_CONTROL_CONFIG_INVALID",
            field="flow_control.label",
            hint="Use a short display label, or omit it.",
        )
    return None


def _branch_enabled(config: Dict[str, Any]) -> bool:
    """True when the config carries an enabled ``branch`` fan-out block.

    Absent block, a non-dict block, or ``enabled: False`` all keep the process on
    the single-target linear/Try-Catch path (byte-for-byte the pre-#112 output).
    """
    branch = config.get("branch")
    if not isinstance(branch, dict):
        return False
    return branch.get("enabled", True) is True


def _branch_leg_targets(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Ordered Branch leg targets: leg 1 = top-level ``target``, legs 2..N =
    ``branch.targets[]`` (issue #112 M10.8)."""
    branch = config.get("branch") or {}
    legs: List[Dict[str, Any]] = [config.get("target") or {}]
    extra = branch.get("targets")
    if isinstance(extra, list):
        legs.extend(t for t in extra if isinstance(t, dict))
    return legs


def _validate_branch_config(config: Dict[str, Any]) -> Optional[BuilderValidationError]:
    """Validate the optional ``branch`` N-way fan-out block (issue #112 M10.8).

    Absent, or ``enabled: False``, is valid — the process stays single-target.
    When enabled:

      * ``branch.targets`` must be a non-empty list (legs 2..N; leg 1 is the
        top-level ``target``) → else ``BRANCH_OUTPUT_UNSET`` (the verifier's hard
        error for an unset branch output, reused at build time);
      * the total leg count ``1 + len(targets)`` must stay in Boomi's 2..25 Branch
        range;
      * each leg is a well-formed REST connector binding (shared
        ``_validate_target_binding`` with a ``branch.targets[i]`` field prefix).

    v1 does NOT compose Branch with per-target ``dynamic_path``, Try/Catch
    ``reliability`` (retry/DLQ/notify/exception), or a Return Documents terminal —
    each is a clean ``PROCESS_BRANCH_CONFIG_INVALID`` rejection (follow-up work),
    never a silent drop.
    """
    branch = config.get("branch")
    if branch is None:
        return None
    if not isinstance(branch, dict):
        return BuilderValidationError(
            "branch must be a JSON object with an optional 'enabled' flag and a 'targets' list.",
            error_code="PROCESS_BRANCH_CONFIG_INVALID",
            field="branch",
            hint='Shape: {"enabled": true, "targets": [{connector_type, connection_id, operation_id, action_type}, ...]}.',
        )
    unknown = set(branch) - _BRANCH_ALLOWED_KEYS
    if unknown:
        return BuilderValidationError(
            f"branch has unsupported key(s): {sorted(unknown)}.",
            error_code="PROCESS_BRANCH_CONFIG_INVALID",
            field="branch",
            hint=f"Supported branch keys: {sorted(_BRANCH_ALLOWED_KEYS)}.",
        )
    enabled = branch.get("enabled", True)
    if not isinstance(enabled, bool):
        return BuilderValidationError(
            "branch.enabled must be a boolean.",
            error_code="PROCESS_BRANCH_CONFIG_INVALID",
            field="branch.enabled",
            hint="Use true to fan out, or omit/false to keep the single-target flow.",
        )
    if not enabled:
        return None
    targets = branch.get("targets")
    if not isinstance(targets, list) or not targets:
        return BuilderValidationError(
            "branch.targets must be a non-empty list when branch is enabled "
            "(legs 2..N; leg 1 is the top-level target).",
            error_code="BRANCH_OUTPUT_UNSET",
            field="branch.targets",
            hint=(
                "Provide at least one additional REST target binding; a Branch "
                "needs at least 2 paths total (the top-level target plus one "
                "branch.targets entry)."
            ),
        )
    num_branches = 1 + len(targets)
    if num_branches > _BRANCH_MAX_LEGS:
        return BuilderValidationError(
            f"branch fan-out supports 2..{_BRANCH_MAX_LEGS} legs; got {num_branches}.",
            error_code="PROCESS_BRANCH_CONFIG_INVALID",
            field="branch.targets",
            hint=(
                f"Boomi Branch supports up to {_BRANCH_MAX_LEGS} paths "
                f"(the top-level target plus up to {_BRANCH_MAX_LEGS - 1} branch.targets)."
            ),
        )
    # Unsupported v1 composition (dynamic_path / Try-Catch reliability / return
    # documents alongside Branch) is rejected here — AFTER the branch-block
    # structure + BRANCH_OUTPUT_UNSET + leg-count checks (a missing/empty branch
    # output is the more fundamental error, reported first), and BEFORE the leg
    # binding loop (so a leg carrying a malformed dynamic_path is reported as the
    # composition error, not the binding-stage PROCESS_PATH_REPLACEMENT_INVALID).
    # Both validate_config and build() funnel through this one validator, so the
    # two paths cannot diverge on which structured error a malformed branch yields.
    combo_err = _branch_unsupported_combo_error(config)
    if combo_err is not None:
        return combo_err
    for i, leg in enumerate(targets):
        leg_err = _validate_target_binding(leg, field_prefix=f"branch.targets[{i}]")
        if leg_err is not None:
            return leg_err
    return None


def _runtime_binding_composition_error(
    config: Dict[str, Any], *, error_code: str, feature: str
) -> Optional[BuilderValidationError]:
    """Reject #96 runtime bindings / source dynamic path under a branch/decision.

    v1 does not compose a runtime binding (a source ``dynamic_path``, or any
    source/target/leg ``runtime_bindings`` block) with a Branch fan-out or a
    Decision route — each changes the shape graph and needs its own live capture.
    ``feature`` names the composing block (``'a branch fan-out'`` / ``'a decision'``)
    for the message; ``error_code`` is the owning block's config-invalid code.
    Shared by ``validate_config`` and ``build()`` so the two cannot diverge.
    """
    source = config.get("source")
    if isinstance(source, dict) and source.get("dynamic_path") is not None:
        return BuilderValidationError(
            f"source.dynamic_path is not supported together with {feature} in v1.",
            error_code=error_code,
            field="source.dynamic_path",
            hint="Remove the source runtime path binding, or drop the composing block.",
        )
    for loc in ("source", "target"):
        binding = config.get(loc)
        if isinstance(binding, dict) and binding.get("runtime_bindings"):
            return BuilderValidationError(
                f"{loc}.runtime_bindings is not supported together with {feature} in v1.",
                error_code=error_code,
                field=f"{loc}.runtime_bindings",
                hint="Remove the runtime_bindings, or drop the composing block.",
            )
    branch = config.get("branch") if isinstance(config.get("branch"), dict) else {}
    legs = branch.get("targets")
    if isinstance(legs, list):
        for i, leg in enumerate(legs):
            if isinstance(leg, dict) and leg.get("runtime_bindings"):
                return BuilderValidationError(
                    f"branch.targets[{i}].runtime_bindings is not supported in "
                    f"{feature} in v1.",
                    error_code=error_code,
                    field=f"branch.targets[{i}].runtime_bindings",
                    hint="Branch legs are plain REST targets in v1; remove the per-leg runtime_bindings.",
                )
    return None


def _branch_unsupported_combo_error(
    config: Dict[str, Any]
) -> Optional[BuilderValidationError]:
    """v1 Branch composition guard (issue #112 M10.8).

    Branch fan-out v1 emits plain ``target -> Stop`` legs. It does not yet compose
    with a dynamic REST path, a Try/Catch reliability wrapper, a Return Documents
    terminal, or a #96 runtime binding — each would change the per-leg sub-flow
    shape and needs its own live capture + tests. Returning a structured error here
    (rather than silently ignoring the combination) is shared by ``validate_config``
    (surfaces it) and ``build()`` (raises it for totality on a validate_config bypass).
    """
    rb_err = _runtime_binding_composition_error(
        config, error_code="PROCESS_BRANCH_CONFIG_INVALID", feature="a branch fan-out"
    )
    if rb_err is not None:
        return rb_err
    target = config.get("target")
    if isinstance(target, dict) and target.get("dynamic_path") is not None:
        return BuilderValidationError(
            "target.dynamic_path is not supported together with a branch fan-out in v1.",
            error_code="PROCESS_BRANCH_CONFIG_INVALID",
            field="target.dynamic_path",
            hint="Remove dynamic_path, or drop the branch block to keep the dynamic-path single target.",
        )
    # NOTE: this guard now runs (via validate_config) BEFORE _validate_branch_config
    # has checked that branch.targets is a list, so it must stay total on a
    # malformed targets value. Only iterate when it is actually a list — a non-list
    # targets (e.g. a scalar 1/true, which is truthy but not iterable) is reported
    # as BRANCH_OUTPUT_UNSET by _validate_branch_config, not crashed on here.
    branch = config.get("branch") if isinstance(config.get("branch"), dict) else {}
    branch_targets = branch.get("targets")
    if isinstance(branch_targets, list):
        for i, leg in enumerate(branch_targets):
            if isinstance(leg, dict) and leg.get("dynamic_path") is not None:
                return BuilderValidationError(
                    f"branch.targets[{i}].dynamic_path is not supported in a branch fan-out in v1.",
                    error_code="PROCESS_BRANCH_CONFIG_INVALID",
                    field=f"branch.targets[{i}].dynamic_path",
                    hint="Branch legs are plain REST targets in v1; remove the per-leg dynamic_path.",
                )
    reliability = config.get("reliability")
    if isinstance(reliability, dict) and _reliability_requests_try_catch(reliability):
        return BuilderValidationError(
            "reliability (Try/Catch retry/DLQ/notify/exception) is not supported "
            "together with a branch fan-out in v1.",
            error_code="PROCESS_BRANCH_CONFIG_INVALID",
            field="reliability",
            hint="Drop the branch block to use a Try/Catch, or drop reliability to fan out.",
        )
    rd = config.get("return_documents")
    if isinstance(rd, dict) and rd.get("enabled") is True:
        return BuilderValidationError(
            "return_documents is not supported together with a branch fan-out in v1.",
            error_code="PROCESS_BRANCH_CONFIG_INVALID",
            field="return_documents",
            hint="Each branch leg ends in its own Stop; a Return Documents terminal is a follow-up.",
        )
    return None


def _reliability_requests_try_catch(reliability: Dict[str, Any]) -> bool:
    """True when a reliability block would emit a Try/Catch wrapper.

    Used by the Branch v1 composition guard. Delegates to
    ``ProcessFlowBuilder._should_emit_try_catch`` so Branch rejects exactly the
    reliability configs that would otherwise produce a catcherrors wrapper — a
    no-op ``{retry_count: 0, dlq: {mode: "disabled"}}`` block emits nothing and is
    harmless alongside Branch (it is the default reliability shape), so it is NOT
    rejected.
    """
    return ProcessFlowBuilder._should_emit_try_catch(reliability)


def _branch_target_params(leg_target: Dict[str, Any]) -> Dict[str, Any]:
    """Build the ``connectoraction_target`` emitter params for one Branch leg.

    Mirrors the top-level target coercion in ``build()`` (canonical REST subtype,
    uppercased HTTP verb, whitespace-stripped ids) minus ``dynamic_path`` — v1
    Branch legs are plain REST targets (see ``_branch_unsupported_combo_error``).
    """
    return {
        "connector_type": _canonical_connector_type(leg_target.get("connector_type")),
        "action_type": str(leg_target.get("action_type") or "").strip().upper(),
        "connection_id": str(leg_target.get("connection_id") or "").strip(),
        "operation_id": str(leg_target.get("operation_id") or "").strip(),
        "userlabel": str(leg_target.get("label") or ""),
        "dynamic_path": None,
    }


# ----------------------------------------------------------------------
# Issue #113 M10.9 — Decision (conditional two-path routing) + loop-back
# ----------------------------------------------------------------------


def _decision_enabled(config: Dict[str, Any]) -> bool:
    """True when the config carries an enabled ``decision`` block.

    Absent block, a non-dict block, or ``enabled: False`` all keep the process on
    the single-target linear/Try-Catch path (byte-for-byte the pre-#113 output).
    """
    decision = config.get("decision")
    if not isinstance(decision, dict):
        return False
    return decision.get("enabled", True) is True


def _decision_pre_shape_names(config: Dict[str, Any]) -> List[str]:
    """Shape names of the shared pre-Decision chain (``start -> source ->
    [transform]``), in order — the valid backward targets for a ``false_next``
    loop edge (issue #113 M10.9).

    The Decision sits after this chain, so a loop-back must point at one of these
    earlier shapes. Both ``validate_config`` (via ``_validate_decision_config``)
    and ``build()`` derive the set the SAME way, so a ``false_next`` that names a
    non-existent / non-earlier shape is rejected identically on both paths.
    """
    transform = config.get("transform")
    if isinstance(transform, dict):
        mode = str(transform.get("mode") or "passthrough").strip().lower()
    else:
        # A non-dict transform (e.g. ``transform: 1``) is rejected later by
        # _validate_transform; stay total here (the branch validators' "stay total
        # on malformed input" doctrine) rather than raising AttributeError on
        # ``.get`` — treat it as no pre-Decision transform shape.
        mode = "passthrough"
    has_transform = mode in _DECISION_PRE_TRANSFORM_MODES
    count = 2 + (1 if has_transform else 0)  # start(1) + source(2) + [transform(3)]
    return [f"shape{i}" for i in range(1, count + 1)]


def _validate_decision_operand(
    operand: Any, field: str
) -> Optional[BuilderValidationError]:
    """Validate one Decision operand (``left`` / ``right``) — issue #113 M10.9.

    v1 supports two operand sources: ``track`` (a DDP/DPP, requiring a non-blank
    ``property_id``) and ``static`` (a literal, requiring a string ``static_value``
    which MAY be empty — the live "is empty" check compares a track value against
    an empty static).
    """
    if not isinstance(operand, dict):
        return BuilderValidationError(
            f"{field} must be a JSON object with a 'value_type' and its operand fields.",
            error_code="PROCESS_DECISION_CONFIG_INVALID",
            field=field,
            hint='Shape: {"value_type": "track", "property_id": "dynamicdocument.DDP_X"} '
            'or {"value_type": "static", "static_value": "..."}.',
        )
    unknown = set(operand) - _DECISION_OPERAND_ALLOWED_KEYS
    if unknown:
        return BuilderValidationError(
            f"{field} has unsupported key(s): {sorted(unknown)}.",
            error_code="PROCESS_DECISION_CONFIG_INVALID",
            field=field,
            hint=f"Supported operand keys: {sorted(_DECISION_OPERAND_ALLOWED_KEYS)}.",
        )
    value_type = operand.get("value_type")
    if not isinstance(value_type, str) or value_type.strip() not in _DECISION_VALUE_TYPES:
        return BuilderValidationError(
            f"{field}.value_type must be one of {sorted(_DECISION_VALUE_TYPES)}.",
            error_code="PROCESS_DECISION_CONFIG_INVALID",
            field=f"{field}.value_type",
            hint="v1 supports 'track' (a DDP/DPP) and 'static' (a literal value).",
        )
    vtype = value_type.strip()
    if vtype == "track":
        property_id = operand.get("property_id")
        if not isinstance(property_id, str) or not property_id.strip():
            return BuilderValidationError(
                f"{field}.property_id is required (non-blank) for a track operand.",
                error_code="PROCESS_DECISION_CONFIG_INVALID",
                field=f"{field}.property_id",
                hint="Provide the tracked property id, e.g. 'dynamicdocument.DDP_STATUS'.",
            )
    else:  # static
        static_value = operand.get("static_value")
        if not isinstance(static_value, str):
            return BuilderValidationError(
                f"{field}.static_value is required (a string, may be empty) for a static operand.",
                error_code="PROCESS_DECISION_CONFIG_INVALID",
                field=f"{field}.static_value",
                hint="Use an empty string to compare against an empty value (the 'is empty' check).",
            )
    return None


def _decision_unsupported_combo_error(
    config: Dict[str, Any]
) -> Optional[BuilderValidationError]:
    """v1 Decision composition guard (issue #113 M10.9).

    Decision v1 emits a forward ``target -> Stop`` true leg plus a forward/looping
    false leg. It does not yet compose with a Branch fan-out, a Try/Catch
    reliability wrapper, a dynamic REST path, or a Return Documents terminal — each
    would change the emitted shape graph and needs its own live capture + tests.
    Returning a structured error here (rather than silently dropping the
    combination, or silently taking the Branch path in ``build()``) is shared by
    ``validate_config`` and ``build()`` so the two cannot diverge.
    """
    if _branch_enabled(config):
        return BuilderValidationError(
            "a branch fan-out is not supported together with a decision in v1.",
            error_code="PROCESS_DECISION_CONFIG_INVALID",
            field="branch",
            hint="Drop the branch block to use a Decision, or drop the decision block to fan out.",
        )
    rb_err = _runtime_binding_composition_error(
        config, error_code="PROCESS_DECISION_CONFIG_INVALID", feature="a decision"
    )
    if rb_err is not None:
        return rb_err
    target = config.get("target")
    if isinstance(target, dict) and target.get("dynamic_path") is not None:
        return BuilderValidationError(
            "target.dynamic_path is not supported together with a decision in v1.",
            error_code="PROCESS_DECISION_CONFIG_INVALID",
            field="target.dynamic_path",
            hint="Remove dynamic_path, or drop the decision block to keep the dynamic-path single target.",
        )
    reliability = config.get("reliability")
    if isinstance(reliability, dict) and _reliability_requests_try_catch(reliability):
        return BuilderValidationError(
            "reliability (Try/Catch retry/DLQ/notify/exception) is not supported "
            "together with a decision in v1.",
            error_code="PROCESS_DECISION_CONFIG_INVALID",
            field="reliability",
            hint="Drop the decision block to use a Try/Catch, or drop reliability to route.",
        )
    rd = config.get("return_documents")
    if isinstance(rd, dict) and rd.get("enabled") is True:
        return BuilderValidationError(
            "return_documents is not supported together with a decision in v1.",
            error_code="PROCESS_DECISION_CONFIG_INVALID",
            field="return_documents",
            hint="The decision true leg ends in its own Stop; a Return Documents terminal is a follow-up.",
        )
    return None


def _validate_decision_config(config: Dict[str, Any]) -> Optional[BuilderValidationError]:
    """Validate the optional ``decision`` conditional-routing block (issue #113 M10.9).

    Absent, or ``enabled: False``, is valid — the process stays single-target. When
    enabled the precedence (mirroring Branch) is: block structure -> unknown key ->
    non-bool ``enabled`` -> unsupported v1 composition (branch / Try-Catch
    reliability / dynamic_path / return_documents) -> ``comparison`` enum ->
    ``left``/``right`` operands -> ``false_notify`` -> ``false_next`` loop target.
    ``build()`` funnels through this SAME validator, so the two paths cannot diverge
    on which structured error a malformed decision yields.
    """
    decision = config.get("decision")
    if decision is None:
        return None
    if not isinstance(decision, dict):
        return BuilderValidationError(
            "decision must be a JSON object with a comparison and two operands.",
            error_code="PROCESS_DECISION_CONFIG_INVALID",
            field="decision",
            hint='Shape: {"comparison": "equals", "left": {...}, "right": {...}, '
            '"false_notify"?: "...", "false_next"?: "shapeN"}.',
        )
    unknown = set(decision) - _DECISION_ALLOWED_KEYS
    if unknown:
        return BuilderValidationError(
            f"decision has unsupported key(s): {sorted(unknown)}.",
            error_code="PROCESS_DECISION_CONFIG_INVALID",
            field="decision",
            hint=f"Supported decision keys: {sorted(_DECISION_ALLOWED_KEYS)}.",
        )
    enabled = decision.get("enabled", True)
    if not isinstance(enabled, bool):
        return BuilderValidationError(
            "decision.enabled must be a boolean.",
            error_code="PROCESS_DECISION_CONFIG_INVALID",
            field="decision.enabled",
            hint="Use true to route, or omit/false to keep the single-target flow.",
        )
    if not enabled:
        return None
    combo_err = _decision_unsupported_combo_error(config)
    if combo_err is not None:
        return combo_err
    comparison = decision.get("comparison")
    if not isinstance(comparison, str) or comparison.strip() not in _DECISION_COMPARISONS:
        return BuilderValidationError(
            f"decision.comparison must be one of {sorted(_DECISION_COMPARISONS)}.",
            error_code="PROCESS_DECISION_CONFIG_INVALID",
            field="decision.comparison",
            hint="Boomi Decision operators: equals / greaterthaneq / lessthaneq / "
            "greaterthan / lessthan / regex / wildcard.",
        )
    for side in ("left", "right"):
        operand_err = _validate_decision_operand(decision.get(side), f"decision.{side}")
        if operand_err is not None:
            return operand_err
    false_notify = decision.get("false_notify")
    if false_notify is not None and (
        not isinstance(false_notify, str) or not false_notify.strip()
    ):
        return BuilderValidationError(
            "decision.false_notify must be a non-empty string when present.",
            error_code="PROCESS_DECISION_CONFIG_INVALID",
            field="decision.false_notify",
            hint="false_notify is the Message text shown on the false path before its Stop/loop.",
        )
    false_next = decision.get("false_next")
    if false_next is not None:
        if not isinstance(false_next, str) or not false_next.strip():
            return BuilderValidationError(
                "decision.false_next must be a non-empty earlier-shape name when present.",
                error_code="PROCESS_DECISION_CONFIG_INVALID",
                field="decision.false_next",
                hint="Name an earlier shape (e.g. 'shape2') so the false path loops back to it.",
            )
        valid_targets = _decision_pre_shape_names(config)
        if false_next.strip() not in valid_targets:
            return BuilderValidationError(
                f"decision.false_next must reference an earlier shape; got "
                f"{false_next.strip()!r}, expected one of {valid_targets}.",
                error_code="PROCESS_DECISION_CONFIG_INVALID",
                field="decision.false_next",
                hint="A loop-back edge points at a shape BEFORE the Decision "
                "(start/source/[transform]).",
            )
    return None


def _validate_transform(transform: Any) -> Optional[BuilderValidationError]:
    if transform is None:
        return None
    if not isinstance(transform, dict):
        return BuilderValidationError(
            "transform must be a JSON object with a 'mode' field.",
            error_code="PROCESS_SHAPE_UNSUPPORTED",
            field="transform",
            hint=f"Supported modes: {sorted(_SUPPORTED_TRANSFORM_MODES)}.",
        )
    mode = str(transform.get("mode") or "passthrough").strip().lower()
    if mode not in _SUPPORTED_TRANSFORM_MODES:
        return BuilderValidationError(
            f"transform.mode {mode!r} is not supported.",
            error_code="PROCESS_SHAPE_UNSUPPORTED",
            field="transform.mode",
            hint=f"Supported modes: {sorted(_SUPPORTED_TRANSFORM_MODES)}.",
        )
    if mode == "message":
        text = transform.get("message_text")
        if not isinstance(text, str) or not text:
            return BuilderValidationError(
                "transform.message_text is required when mode='message'.",
                error_code="PROCESS_SHAPE_UNSUPPORTED",
                field="transform.message_text",
                hint="Provide the message body to emit on the Message shape.",
            )
    if mode == "map_ref":
        ref = transform.get("map_ref") or transform.get("map_id")
        if not isinstance(ref, str) or not ref.strip():
            return BuilderValidationError(
                "transform.map_ref is required when mode='map_ref'.",
                error_code="PROCESS_SHAPE_UNSUPPORTED",
                field="transform.map_ref",
                hint=(
                    "Pass a map component_id or a $ref:KEY token "
                    "(map component creation is issue #26 scope)."
                ),
            )
    if mode == "dataprocess":
        return _validate_dataprocess_transform(transform)
    if mode == "doccacheretrieve":
        return _validate_doccacheretrieve_transform(transform)
    if mode == "doccacheremove":
        return _validate_doccacheremove_transform(transform)
    return None


def _validate_dataprocess_transform(
    transform: Dict[str, Any],
) -> Optional[BuilderValidationError]:
    """Validate a ``transform.mode='dataprocess'`` config (issue #106 / #115).

    The shape is a non-empty ordered list of operation ``steps``. v1 supports
    ``custom_scripting`` (Groovy, ``language='groovy2'``) and the two
    profile-driven cardinality operations ``split_documents`` /
    ``combine_documents`` (issue #115 M10.2a); any other operation — including the
    docs-only Search/Replace, Zip/Unzip, Base64, and character encoding — is
    rejected ``PROCESS_DATAPROCESS_OPERATION_UNSUPPORTED`` until it has a
    byte-accurate live capture. Malformed step config is
    ``PROCESS_DATAPROCESS_CONFIG_INVALID``.
    """
    # Reject unknown top-level transform keys so a typo isn't silently dropped.
    # Mirrors the step-level strictness below and the DataProcessPrimitive's
    # extra="forbid" parameter model. (mode is the discriminator.)
    extra = set(transform) - {"mode", "label", "steps"}
    if extra:
        return BuilderValidationError(
            f"transform has unsupported keys for mode='dataprocess': {sorted(extra)}.",
            error_code="PROCESS_DATAPROCESS_CONFIG_INVALID",
            field="transform",
            hint="Allowed keys: mode, label, steps.",
        )
    steps = transform.get("steps")
    if not isinstance(steps, list) or not steps:
        return BuilderValidationError(
            "transform.steps must be a non-empty list when mode='dataprocess'.",
            error_code="PROCESS_DATAPROCESS_CONFIG_INVALID",
            field="transform.steps",
            hint="Provide at least one Data Process operation step.",
        )
    for i, step in enumerate(steps):
        field = f"transform.steps[{i}]"
        if not isinstance(step, dict):
            return BuilderValidationError(
                f"{field} must be a JSON object.",
                error_code="PROCESS_DATAPROCESS_CONFIG_INVALID",
                field=field,
                hint="Each step is {operation: ..., ...}.",
            )
        operation = str(step.get("operation") or "").strip()
        if operation not in _DATAPROCESS_OPERATIONS:
            return BuilderValidationError(
                f"{field}.operation {operation!r} is not supported.",
                error_code="PROCESS_DATAPROCESS_OPERATION_UNSUPPORTED",
                field=f"{field}.operation",
                hint=(
                    "Supported: custom_scripting, split_documents, "
                    "combine_documents. Other documented Data Process operations "
                    "are deferred pending a live capture; see "
                    f"supported: {sorted(_DATAPROCESS_OPERATIONS)}."
                ),
            )
        if operation == "custom_scripting":
            allowed_keys = {"operation", "script", "language", "use_cache"}
            extra = set(step) - allowed_keys
            if extra:
                return BuilderValidationError(
                    f"{field} has unsupported keys: {sorted(extra)}.",
                    error_code="PROCESS_DATAPROCESS_CONFIG_INVALID",
                    field=field,
                    hint=f"Allowed keys for custom_scripting: {sorted(allowed_keys)}.",
                )
            script = step.get("script")
            if not isinstance(script, str) or not script.strip():
                return BuilderValidationError(
                    f"{field}.script is required and must be a non-empty string.",
                    error_code="PROCESS_DATAPROCESS_CONFIG_INVALID",
                    field=f"{field}.script",
                    hint="Provide the Custom Scripting body.",
                )
            language = step.get("language", _DATAPROCESS_SCRIPT_LANGUAGE)
            if language != _DATAPROCESS_SCRIPT_LANGUAGE:
                return BuilderValidationError(
                    f"{field}.language must be '{_DATAPROCESS_SCRIPT_LANGUAGE}'.",
                    error_code="PROCESS_DATAPROCESS_CONFIG_INVALID",
                    field=f"{field}.language",
                    hint=f"Only '{_DATAPROCESS_SCRIPT_LANGUAGE}' is accepted.",
                )
            use_cache = step.get("use_cache", True)
            if use_cache is not True:
                return BuilderValidationError(
                    f"{field}.use_cache must be true.",
                    error_code="PROCESS_DATAPROCESS_CONFIG_INVALID",
                    field=f"{field}.use_cache",
                    hint="Script compilation caching is required (use_cache=true).",
                )
        elif operation in _DATAPROCESS_PROFILE_OPERATIONS:
            err = _validate_dataprocess_profile_step(step, operation, field)
            if err is not None:
                return err
    return None


def _validate_dataprocess_profile_step(
    step: Dict[str, Any], operation: str, field: str
) -> Optional[BuilderValidationError]:
    """Validate one Split/Combine Documents Data Process step (issue #115 M10.2a).

    Both operations bind a JSON/XML profile component plus a link element on it.
    The profile/link values (``profile_id`` / ``link_element_key`` /
    ``link_element_name`` and the Combine-only ``combine_into_link_element_key``)
    are caller-authored opaque tokens captured from the Boomi UI — validated here
    only as present, non-blank strings (no canned/templated values). The
    ``profile_id`` $ref's *type* (json vs xml profile component) is checked at plan
    time in ``integration_builder._check_process_flow_ref_types``.
    """
    allowed_keys = (
        _DATAPROCESS_COMBINE_KEYS
        if operation == "combine_documents"
        else _DATAPROCESS_SPLIT_KEYS
    )
    extra = set(step) - allowed_keys
    if extra:
        return BuilderValidationError(
            f"{field} has unsupported keys: {sorted(extra)}.",
            error_code="PROCESS_DATAPROCESS_CONFIG_INVALID",
            field=field,
            hint=f"Allowed keys for {operation}: {sorted(allowed_keys)}.",
        )
    if step.get("profile_type") not in _DATAPROCESS_PROFILE_TYPES:
        return BuilderValidationError(
            f"{field}.profile_type must be one of "
            f"{sorted(_DATAPROCESS_PROFILE_TYPES)}.",
            error_code="PROCESS_DATAPROCESS_CONFIG_INVALID",
            field=f"{field}.profile_type",
            hint="Split/Combine bind a 'json' or 'xml' profile component.",
        )
    for key in ("profile_id", "link_element_key", "link_element_name"):
        value = step.get(key)
        if not isinstance(value, str) or not value.strip():
            return BuilderValidationError(
                f"{field}.{key} is required and must be a non-empty string.",
                error_code="PROCESS_DATAPROCESS_CONFIG_INVALID",
                field=f"{field}.{key}",
                hint=(
                    "Provide the caller-authored profile reference / link element "
                    "captured from the profile (no canned values)."
                ),
            )
    if operation == "combine_documents":
        # Optional; defaults to the literal 'null' (combine into the document
        # root). When supplied it must be a non-blank string element key.
        combine_into = step.get("combine_into_link_element_key", "null")
        if not isinstance(combine_into, str) or not combine_into.strip():
            return BuilderValidationError(
                f"{field}.combine_into_link_element_key must be a non-empty string.",
                error_code="PROCESS_DATAPROCESS_CONFIG_INVALID",
                field=f"{field}.combine_into_link_element_key",
                hint=(
                    "Defaults to the literal 'null' (combine into root); set a "
                    "parent element key to nest."
                ),
            )
    return None


def _validate_doccacheretrieve_transform(
    transform: Dict[str, Any],
) -> Optional[BuilderValidationError]:
    """Validate a ``transform.mode='doccacheretrieve'`` config (issue #109 M10.5).

    The Document Cache Retrieve shape pulls documents from a Document Cache into
    the current flow — the read half of Document Cache CRUD (the write half,
    Add to Cache / ``doccacheload``, already ships on the DLQ catch leg). v1
    accepts ONLY the live-observed all-document retrieve:

      * ``document_cache_id`` (required) — the Document Cache component id, a
        literal id or a ``$ref:KEY`` token (reachability is enforced generically
        by ``_validate_ref_reachability``).
      * ``empty_cache_behavior`` (optional, default ``stopprocess``) — the only
        live-verified "If cache is empty" wire value (Stop document execution).
        The docs-listed backward-compat "Fail document with errors" option has
        no live capture and is deferred.
      * ``load_all_documents`` (optional, default ``True``) — must be ``True``;
        keyed/index retrieval (a docCacheIndex + populated cacheKeyValues) is
        deferred pending a byte-accurate live capture.
      * ``label`` (optional) — the shape display label.

    Unknown keys and unsupported values all return
    ``PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID`` (mirrors the dataprocess gate).
    """
    extra = set(transform) - _DOCCACHE_RETRIEVE_ALLOWED_KEYS
    if extra:
        return BuilderValidationError(
            f"transform has unsupported keys for mode='doccacheretrieve': {sorted(extra)}.",
            error_code="PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID",
            field="transform",
            hint=f"Allowed keys: {sorted(_DOCCACHE_RETRIEVE_ALLOWED_KEYS)}.",
        )
    doc_cache_id = transform.get("document_cache_id")
    if not isinstance(doc_cache_id, str) or not doc_cache_id.strip():
        return BuilderValidationError(
            "transform.document_cache_id is required when mode='doccacheretrieve'.",
            error_code="PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID",
            field="transform.document_cache_id",
            hint=(
                "Pass the Document Cache component id (a literal id or a $ref:KEY "
                "token in depends_on) to retrieve documents from."
            ),
        )
    behavior = transform.get(
        "empty_cache_behavior", _DOCCACHE_RETRIEVE_DEFAULT_EMPTY_BEHAVIOR
    )
    if behavior not in _DOCCACHE_RETRIEVE_EMPTY_BEHAVIORS:
        return BuilderValidationError(
            f"transform.empty_cache_behavior {behavior!r} is not supported.",
            error_code="PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID",
            field="transform.empty_cache_behavior",
            hint=(
                "v1 supports only 'stopprocess' (Stop document execution, the "
                "recommended default); the backward-compat 'fail document with "
                "errors' behavior is deferred pending a live capture."
            ),
        )
    load_all = transform.get("load_all_documents", True)
    if load_all is not True:
        return BuilderValidationError(
            "transform.load_all_documents must be true when mode='doccacheretrieve'.",
            error_code="PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID",
            field="transform.load_all_documents",
            hint=(
                "v1 retrieves ALL cached documents (loadAllDoc=true, empty "
                "cacheKeyValues). Keyed/index retrieval is deferred pending a "
                "byte-accurate live capture."
            ),
        )
    label = transform.get("label")
    if label is not None and not isinstance(label, str):
        return BuilderValidationError(
            "transform.label must be a string.",
            error_code="PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID",
            field="transform.label",
            hint="Use a string display label for the Document Cache Retrieve shape.",
        )
    return None


def _validate_doccacheremove_transform(
    transform: Dict[str, Any],
) -> Optional[BuilderValidationError]:
    """Validate a ``transform.mode='doccacheremove'`` config (issue #110 M10.6).

    The Document Cache Remove shape clears documents from a Document Cache — the
    DELETE half of Document Cache CRUD (the read half, Document Cache Retrieve /
    ``doccacheretrieve``, ships in #109; the write half, Add to Cache /
    ``doccacheload``, ships on the DLQ catch leg). v1 accepts ONLY the
    live-observed all-document remove:

      * ``document_cache_id`` (required) — the Document Cache component id, a
        literal id or a ``$ref:KEY`` token (reachability is enforced generically
        by ``_validate_ref_reachability``).
      * ``remove_all_documents`` (optional, default ``True``) — must be ``True``;
        keyed/index removal (a docCacheIndex + populated cacheKeyValues) is
        deferred pending a byte-accurate live capture.
      * ``label`` (optional) — the shape display label.

    Unlike retrieve, the remove shape carries NO ``empty_cache_behavior`` /
    ``load_all_documents`` (those are retrieve-only wire attributes). Unknown
    keys and unsupported values all return
    ``PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID`` (mirrors the #109 retrieve gate).
    """
    extra = set(transform) - _DOCCACHE_REMOVE_ALLOWED_KEYS
    if extra:
        return BuilderValidationError(
            f"transform has unsupported keys for mode='doccacheremove': {sorted(extra)}.",
            error_code="PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID",
            field="transform",
            hint=f"Allowed keys: {sorted(_DOCCACHE_REMOVE_ALLOWED_KEYS)}.",
        )
    doc_cache_id = transform.get("document_cache_id")
    if not isinstance(doc_cache_id, str) or not doc_cache_id.strip():
        return BuilderValidationError(
            "transform.document_cache_id is required when mode='doccacheremove'.",
            error_code="PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID",
            field="transform.document_cache_id",
            hint=(
                "Pass the Document Cache component id (a literal id or a $ref:KEY "
                "token in depends_on) to remove documents from."
            ),
        )
    remove_all = transform.get("remove_all_documents", True)
    if remove_all is not True:
        return BuilderValidationError(
            "transform.remove_all_documents must be true when mode='doccacheremove'.",
            error_code="PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID",
            field="transform.remove_all_documents",
            hint=(
                "v1 removes ALL cached documents (removeAllDocuments=true, empty "
                "cacheKeyValues). Keyed/index removal is deferred pending a "
                "byte-accurate live capture."
            ),
        )
    label = transform.get("label")
    if label is not None and not isinstance(label, str):
        return BuilderValidationError(
            "transform.label must be a string.",
            error_code="PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID",
            field="transform.label",
            hint="Use a string display label for the Document Cache Remove shape.",
        )
    return None


def _validate_return_documents(value: Any) -> Optional[BuilderValidationError]:
    """Validate the optional ``return_documents`` terminal block (issue #107 M10.3).

    ``None``/absent is valid (the default terminal stays a Stop). When present it
    must be an object with a bool ``enabled`` and an optional string ``label``;
    unknown keys are rejected so a typo is not silently dropped (mirrors the
    dataprocess strictness and the ReturnDocumentsPrimitive's ``extra='forbid'``
    model). A Return Documents terminal carries no Stop after it (the verifier's
    RETURN_DOCS_STOP_EXCLUSIVE invariant); that is structural — build() replaces
    the Stop rather than appending one after Return Documents.
    """
    if value is None:
        return None
    if not isinstance(value, dict):
        return BuilderValidationError(
            "return_documents must be a JSON object.",
            error_code="PROCESS_RETURN_DOCUMENTS_CONFIG_INVALID",
            field="return_documents",
            hint='Use {"enabled": true, "label": "..."} to end the flow in a Return Documents terminal.',
        )
    extra = set(value) - _RETURN_DOCUMENTS_ALLOWED_KEYS
    if extra:
        return BuilderValidationError(
            f"return_documents has unsupported keys: {sorted(extra)}.",
            error_code="PROCESS_RETURN_DOCUMENTS_CONFIG_INVALID",
            field="return_documents",
            hint=f"Allowed keys: {sorted(_RETURN_DOCUMENTS_ALLOWED_KEYS)}.",
        )
    enabled = value.get("enabled")
    if not isinstance(enabled, bool):
        return BuilderValidationError(
            "return_documents.enabled must be a boolean.",
            error_code="PROCESS_RETURN_DOCUMENTS_CONFIG_INVALID",
            field="return_documents.enabled",
            hint="Set enabled=true to end the flow in a Return Documents terminal.",
        )
    label = value.get("label")
    if label is not None and not isinstance(label, str):
        return BuilderValidationError(
            "return_documents.label must be a string.",
            error_code="PROCESS_RETURN_DOCUMENTS_CONFIG_INVALID",
            field="return_documents.label",
            hint="The label is the Return Documents custom label identifying the returned document type(s).",
        )
    return None


def _terminal_flow_entry(config: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """Return the terminal flow shape for a process (issue #107 M10.3).

    Default is a Stop (``continue=true``) — byte-for-byte the pre-#107 output.
    When ``config['return_documents'].enabled`` is True the terminal is a Return
    Documents shape instead, and NO Stop is appended after it (the verifier's
    RETURN_DOCS_STOP_EXCLUSIVE invariant requires a Return Documents path to never
    reach a Stop). Stays total on the validate_config-bypass path: a malformed or
    disabled return_documents block falls back to the default Stop (validate_config
    rejects malformed blocks on the normal path with
    PROCESS_RETURN_DOCUMENTS_CONFIG_INVALID).
    """
    rd = config.get("return_documents")
    if isinstance(rd, dict) and rd.get("enabled") is True:
        return ("returndocuments", {"label": str(rd.get("label") or "")})
    return ("stop", {"continue_": True})


def _validate_reliability(reliability: Any) -> Optional[BuilderValidationError]:
    if reliability is None:
        return None
    if not isinstance(reliability, dict):
        return BuilderValidationError(
            "reliability must be a JSON object.",
            error_code="PROCESS_RETRY_UNVERIFIED",
            field="reliability",
            hint="See get_schema_template for the reliability surface.",
        )
    retry_count = reliability.get("retry_count", 0)
    if not isinstance(retry_count, int) or isinstance(retry_count, bool):
        return BuilderValidationError(
            "reliability.retry_count must be an integer 0..5.",
            error_code="PROCESS_RETRY_UNVERIFIED",
            field="reliability.retry_count",
            hint="Use a plain integer in 0..5.",
        )
    if retry_count < 0 or retry_count > _MAX_RETRY_COUNT:
        return BuilderValidationError(
            f"reliability.retry_count must be 0..{_MAX_RETRY_COUNT}; "
            f"got {retry_count}.",
            error_code="PROCESS_RETRY_UNVERIFIED",
            field="reliability.retry_count",
            hint=f"Boomi Try/Catch retry range is 0..{_MAX_RETRY_COUNT}.",
        )
    # Issue #99 G1: optional Try/Catch placement scope. Absent -> "process"
    # (the pre-#99 whole-chain wrapper). "connector" emits a Try/Catch per
    # connector. Validated here so a bad value fails at plan time rather than
    # silently falling back to the process scope in build().
    scope = reliability.get("try_catch_scope")
    if scope is not None:
        if not isinstance(scope, str) or scope.strip().lower() not in _SUPPORTED_TRY_CATCH_SCOPES:
            return BuilderValidationError(
                "reliability.try_catch_scope must be 'process' or 'connector'.",
                error_code="PROCESS_RETRY_UNVERIFIED",
                field="reliability.try_catch_scope",
                hint=f"Supported scopes: {sorted(_SUPPORTED_TRY_CATCH_SCOPES)}.",
            )

    # Issue #88 (M4.5.3): retry_count 0..5 is un-gated. The Try/Catch Retry
    # Count range (0..5) and its built-in platform wait schedule (count 1
    # retries immediately; 2..5 use escalating built-in waits) are
    # docs-corroborated; the platform offers no caller-selected backoff.
    # Positive retry is only emittable inside a Try/Catch whose catch leg
    # routes to a DLQ (the catcherrors shape always carries a catch leg), so
    # retry_count > 0 requires a supported Try/Catch DLQ mode (checked below).
    dlq = reliability.get("dlq")
    dlq_mode = "disabled"
    if dlq is not None:
        if not isinstance(dlq, dict):
            # Shape error → caller mistake → PROCESS_DLQ_BINDING_INVALID
            # (Codex review A3). Reserve PROCESS_RETRY_UNVERIFIED for
            # known-but-deferred modes only.
            return BuilderValidationError(
                "reliability.dlq must be a JSON object with a 'mode' field.",
                error_code="PROCESS_DLQ_BINDING_INVALID",
                field="reliability.dlq",
                hint=f"Supported dlq modes: {sorted(_SUPPORTED_DLQ_MODES)}.",
            )
        # str() coercion: non-string mode (e.g. 1) becomes "1" and falls
        # out of the enum membership check below. Codex review L1.
        dlq_mode = str(dlq.get("mode") or "disabled").strip().lower()
        if dlq_mode not in _SUPPORTED_DLQ_MODES:
            # Unknown enum value → caller typo → PROCESS_DLQ_BINDING_INVALID.
            return BuilderValidationError(
                f"reliability.dlq.mode {dlq_mode!r} is not supported.",
                error_code="PROCESS_DLQ_BINDING_INVALID",
                field="reliability.dlq.mode",
                hint=f"Supported dlq modes: {sorted(_SUPPORTED_DLQ_MODES)}.",
            )
        if dlq_mode in _TRY_CATCH_DLQ_MODES:
            # The verified Try/Catch wrapper + DLQ catch-path is emitted (issue
            # #51 M3.R1a for retry_count=0; issue #88 for 1..5). Require a
            # resolvable catch-leg binding (literal id or $ref:KEY token).
            binding_err = _validate_dlq_binding(dlq, dlq_mode)
            if binding_err is not None:
                return binding_err
        # dlq_mode == "disabled" → no Try/Catch; nothing else to validate.
    # Issue #108 M10.4: validate the optional Exception (Throw) catch-leg terminal
    # before the retry/notify gates so its own shape errors surface first. A
    # catch_exception wires a Try/Catch catch leg even WITHOUT a DLQ (the bare
    # catcherrors -> exception "fail/halt" shape), so it relaxes both gates below.
    catch_exception = reliability.get("catch_exception")
    catch_exception_err = _validate_catch_exception(catch_exception)
    if catch_exception_err is not None:
        return catch_exception_err
    has_catch_exception = isinstance(catch_exception, dict)
    if (
        retry_count > 0
        and dlq_mode not in _TRY_CATCH_DLQ_MODES
        and not has_catch_exception
    ):
        return BuilderValidationError(
            "reliability.retry_count > 0 requires a wired Try/Catch catch path.",
            error_code="PROCESS_RETRY_UNVERIFIED",
            field="reliability.retry_count",
            hint=(
                "Positive retry is emitted only inside a Try/Catch whose catch "
                "leg routes to a DLQ or throws an Exception. Set "
                "reliability.dlq.mode to document_cache_ref or "
                "error_subprocess_ref, add reliability.catch_exception, or use "
                "retry_count=0."
            ),
        )
    # Issue #89 (M4.5.4): optional Notify on the catch leg. Validated after
    # dlq_mode is finalized (Notify only exists on a wired catch path) and
    # after the retry gate (retry/DLQ shape errors surface first). Issue #108
    # M10.4: a catch_exception also wires the catch leg, so Notify is allowed
    # without a DLQ when the leg ends in an Exception throw.
    notify_err = _validate_catch_notify(
        reliability.get("catch_notify"), dlq_mode, has_catch_exception=has_catch_exception
    )
    if notify_err is not None:
        return notify_err
    return None


def _validate_catch_notify(
    catch_notify: Any, dlq_mode: str, has_catch_exception: bool = False
) -> Optional[BuilderValidationError]:
    """Validate the optional ``reliability.catch_notify`` config (issue #89).

    Returns ``None`` when absent (Notify is opt-in) or valid; otherwise a
    ``PROCESS_NOTIFY_CONFIG_INVALID`` error. Notify is emitted only at the head
    of a wired Try/Catch catch leg, so it requires ``dlq_mode`` in
    ``_TRY_CATCH_DLQ_MODES`` OR a ``catch_exception`` terminal (issue #108 M10.4 —
    a bare catcherrors -> notify -> exception leg has no DLQ). The message must
    reference the caught-error property so the emitted Notify logs the real error.
    Email/SMS/channel keys are out of scope and rejected (extra keys).
    """
    if catch_notify is None:
        return None
    if not isinstance(catch_notify, dict):
        return BuilderValidationError(
            "reliability.catch_notify must be a JSON object.",
            error_code="PROCESS_NOTIFY_CONFIG_INVALID",
            field="reliability.catch_notify",
            hint="See get_schema_template for the catch_notify surface.",
        )
    extra = set(catch_notify) - _CATCH_NOTIFY_ALLOWED_KEYS
    if extra:
        return BuilderValidationError(
            f"reliability.catch_notify has unsupported keys: {sorted(extra)}.",
            error_code="PROCESS_NOTIFY_CONFIG_INVALID",
            field="reliability.catch_notify",
            hint=(
                "Only 'level' and 'message_template' are supported. Email/SMS "
                "notification channels are out of scope (#14/M4.5.5)."
            ),
        )
    template = catch_notify.get("message_template")
    if not isinstance(template, str) or not template.strip():
        return BuilderValidationError(
            "reliability.catch_notify.message_template is required and must be "
            "a non-empty string.",
            error_code="PROCESS_NOTIFY_CONFIG_INVALID",
            field="reliability.catch_notify.message_template",
            hint=(
                "Provide the notify message text and reference the caught error "
                f"via the {_NOTIFY_CAUGHT_ERROR_TOKEN} property token."
            ),
        )
    if _NOTIFY_CAUGHT_ERROR_TOKEN not in template:
        return BuilderValidationError(
            "reliability.catch_notify.message_template must reference the "
            "caught-error property.",
            error_code="PROCESS_NOTIFY_CONFIG_INVALID",
            field="reliability.catch_notify.message_template",
            hint=(
                f"Include the {_NOTIFY_CAUGHT_ERROR_TOKEN} token so the emitted "
                "Notify logs the caught error."
            ),
        )
    level = catch_notify.get("level")
    if not isinstance(level, str) or level.strip().upper() not in _SUPPORTED_NOTIFY_LEVELS:
        return BuilderValidationError(
            f"reliability.catch_notify.level must be one of "
            f"{sorted(_SUPPORTED_NOTIFY_LEVELS)}.",
            error_code="PROCESS_NOTIFY_CONFIG_INVALID",
            field="reliability.catch_notify.level",
            hint="Boomi Notify message levels are INFO, WARNING, ERROR.",
        )
    if dlq_mode not in _TRY_CATCH_DLQ_MODES and not has_catch_exception:
        return BuilderValidationError(
            "reliability.catch_notify requires a wired Try/Catch catch path.",
            error_code="PROCESS_NOTIFY_CONFIG_INVALID",
            field="reliability.catch_notify",
            hint=(
                "Notify is emitted only on a catch leg. Set reliability.dlq.mode "
                "to document_cache_ref or error_subprocess_ref, or add "
                "reliability.catch_exception."
            ),
        )
    return None


def _validate_catch_exception(value: Any) -> Optional[BuilderValidationError]:
    """Validate the optional ``reliability.catch_exception`` block (issue #108 M10.4).

    ``None``/absent is valid. When present it must be an object whose only keys are
    in ``_CATCH_EXCEPTION_ALLOWED_KEYS``; a non-empty ``message_template`` string is
    required; the optional ``title`` is a string; ``stop_single_document`` is a bool
    (default ``False``); ``parameter_source`` is one of
    ``_SUPPORTED_EXCEPTION_PARAMETER_SOURCES`` (default ``caught_error``). When the
    parameter source binds a value (``caught_error`` / ``current_document``) the
    message must carry the ``{1}`` placeholder so the bound value actually renders
    (mirrors the catch_notify token requirement); ``none`` carries a static message
    with no parameters. Unknown keys are rejected so a typo is not silently dropped
    (matches the dataprocess / return_documents strictness and the
    ThrowExceptionPrimitive's ``extra='forbid'`` model). A catch_exception is the
    terminal throw on the catch leg — no Stop follows it.
    """
    if value is None:
        return None
    if not isinstance(value, dict):
        return BuilderValidationError(
            "reliability.catch_exception must be a JSON object.",
            error_code="PROCESS_EXCEPTION_CONFIG_INVALID",
            field="reliability.catch_exception",
            hint='Use {"message_template": "{1}", "parameter_source": "caught_error"} to end the catch leg in an Exception throw.',
        )
    extra = set(value) - _CATCH_EXCEPTION_ALLOWED_KEYS
    if extra:
        return BuilderValidationError(
            f"reliability.catch_exception has unsupported keys: {sorted(extra)}.",
            error_code="PROCESS_EXCEPTION_CONFIG_INVALID",
            field="reliability.catch_exception",
            hint=f"Allowed keys: {sorted(_CATCH_EXCEPTION_ALLOWED_KEYS)}.",
        )
    template = value.get("message_template")
    if not isinstance(template, str) or not template.strip():
        return BuilderValidationError(
            "reliability.catch_exception.message_template is required and must be "
            "a non-empty string.",
            error_code="PROCESS_EXCEPTION_CONFIG_INVALID",
            field="reliability.catch_exception.message_template",
            hint="Provide the error message; use the {1} placeholder for the bound parameter_source value.",
        )
    title = value.get("title")
    if title is not None and not isinstance(title, str):
        return BuilderValidationError(
            "reliability.catch_exception.title must be a string.",
            error_code="PROCESS_EXCEPTION_CONFIG_INVALID",
            field="reliability.catch_exception.title",
            hint="The title is the Exception alert subject / process-log title.",
        )
    stop_single = value.get("stop_single_document")
    if stop_single is not None and not isinstance(stop_single, bool):
        return BuilderValidationError(
            "reliability.catch_exception.stop_single_document must be a boolean.",
            error_code="PROCESS_EXCEPTION_CONFIG_INVALID",
            field="reliability.catch_exception.stop_single_document",
            hint="true fails only the reaching document; false (default) halts the whole process.",
        )
    parameter_source = value.get("parameter_source")
    if parameter_source is not None and (
        not isinstance(parameter_source, str)
        or parameter_source not in _SUPPORTED_EXCEPTION_PARAMETER_SOURCES
    ):
        return BuilderValidationError(
            "reliability.catch_exception.parameter_source must be one of "
            f"{sorted(_SUPPORTED_EXCEPTION_PARAMETER_SOURCES)}.",
            error_code="PROCESS_EXCEPTION_CONFIG_INVALID",
            field="reliability.catch_exception.parameter_source",
            hint="caught_error binds the Try/Catch error; current_document binds the current document; none omits parameters.",
        )
    source = str(parameter_source or "caught_error")
    if source != "none" and "{1}" not in template:
        return BuilderValidationError(
            "reliability.catch_exception.message_template must contain the {1} "
            "placeholder when parameter_source binds a value.",
            error_code="PROCESS_EXCEPTION_CONFIG_INVALID",
            field="reliability.catch_exception.message_template",
            hint="Include {1} so the bound parameter_source value renders, or set parameter_source='none'.",
        )
    return None


def _validate_dlq_binding(
    dlq: Dict[str, Any], mode: str
) -> Optional[BuilderValidationError]:
    """Validate the DLQ catch-leg binding for a supported Try/Catch mode.

    The process builder resolves component references via literal ids or
    ``$ref:KEY`` tokens (substituted by integration_builder before build()).
    The dlq_writer primitive's bare ``*_ref_key`` mechanism is NOT resolvable
    on this build path, so the binding must use the ``*_id`` field — a literal
    Boomi component id, or a ``$ref:KEY`` token whose KEY is in depends_on
    (the existing $ref-reachability walk in validate_config covers it). Issue
    #51 M3.R1a.
    """
    if mode == "document_cache_ref":
        id_field, ref_field, target = (
            "document_cache_id", "document_cache_ref_key", "Document Cache",
        )
    else:  # error_subprocess_ref
        id_field, ref_field, target = (
            "process_id", "process_ref_key", "error subprocess",
        )

    id_value = dlq.get(id_field)
    has_id = isinstance(id_value, str) and id_value.strip() != ""
    ref_value = dlq.get(ref_field)
    has_ref = isinstance(ref_value, str) and ref_value.strip() != ""

    bind_hint = (
        f"Set {id_field} to a literal Boomi component id, or a '$ref:KEY' "
        f"token whose KEY is in the process component's depends_on."
    )
    if has_id and has_ref:
        return BuilderValidationError(
            f"reliability.dlq for mode {mode!r} must set exactly one of "
            f"{id_field!r} or {ref_field!r}, not both.",
            error_code="PROCESS_DLQ_BINDING_INVALID",
            field=f"reliability.dlq.{id_field}",
            hint=f"Provide only {id_field!r}. {bind_hint}",
        )
    if not has_id:
        if has_ref:
            return BuilderValidationError(
                f"reliability.dlq.{ref_field} is not resolvable by the "
                f"process builder; bind the {target} via {id_field!r} "
                f"instead.",
                error_code="PROCESS_DLQ_BINDING_INVALID",
                field=f"reliability.dlq.{ref_field}",
                hint=bind_hint,
            )
        return BuilderValidationError(
            f"reliability.dlq.mode={mode!r} requires {id_field!r} to bind "
            f"the {target} catch path.",
            error_code="PROCESS_DLQ_BINDING_INVALID",
            field=f"reliability.dlq.{id_field}",
            hint=bind_hint,
        )
    return None


# ----------------------------------------------------------------------
# Shape emitters
# ----------------------------------------------------------------------

def _emit_start_noaction(
    shape_name: str, next_name: Optional[str], shape_index: int
) -> str:
    dragpoints = _emit_dragpoints([next_name], shape_index)
    return (
        f'<shape image="start" name="{shape_name}" shapetype="start" '
        f'userlabel="" x="{_START_SHAPE_X}" y="{_START_SHAPE_Y}">'
        '<configuration><noaction/></configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def _emit_connectoraction(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    dragpoints = _emit_dragpoints([next_name], shape_index)
    userlabel = _escape_xml(params.get("userlabel") or "")
    connector_type = _escape_xml(params["connector_type"])
    action_type = _escape_xml(params["action_type"])
    connection_id = _escape_xml(params["connection_id"])
    operation_id = _escape_xml(params["operation_id"])

    # Issue #100 G2: when the target carries a dynamic_path block, emit the
    # connector step's "Path" dynamic operation property sourcing the Dynamic
    # Document Property a preceding Set Properties shape sets (valueType="track"
    # references a tracked document property). A DOCUMENT property is used so each
    # document in a multi-record run carries its own path (Codex review P1). The
    # REST Client operation itself declares no URL path parameter (that is
    # HTTP-Client only); the path is supplied here at process time. Transcribed
    # from the live REST Client capture (issue #100). Absent dynamic_path, the
    # body is byte-for-byte the pre-#100 empty <parameters/><dynamicProperties/>
    # with no parameter-profile attribute.
    dynamic_path = params.get("dynamic_path")
    if isinstance(dynamic_path, dict) and dynamic_path:
        ddp_name = _escape_xml(str(dynamic_path.get("ddp_name") or "").strip())
        profile_id = _escape_xml(str(dynamic_path.get("request_profile_id") or "").strip())
        # parameter-profile binds the connector step to the request/path profile, and
        # is meaningful only when a <profileelement> segment exists. A ddp/dpp-only
        # path (#96 §H) carries no profile, so omit the attribute rather than emit an
        # empty parameter-profile="" (no request_profile_id) or a bogus binding with
        # no matching profile segment (validate_config bypass). Mirrors absent
        # dynamic_path. validate_config already rejects a stray request_profile_id on
        # a profile-less path; this keeps build() total if that gate is bypassed.
        has_profile_segment = any(
            isinstance(seg, dict) and seg.get("type") == "profile"
            for seg in (dynamic_path.get("segments") or [])
        )
        parameter_profile_attr = (
            f' parameter-profile="{profile_id}"' if (profile_id and has_profile_segment) else ''
        )
        inner = (
            '<parameters/>'
            '<dynamicProperties>'
            '<propertyvalue childKey="" key="path" name="Path" valueType="track">'
            f'<trackparameter defaultValue="" propertyId="dynamicdocument.{ddp_name}" '
            f'propertyName="Dynamic Document Property - {ddp_name}"/>'
            '</propertyvalue>'
            '</dynamicProperties>'
        )
    else:
        parameter_profile_attr = ''
        inner = '<parameters/><dynamicProperties/>'

    return (
        f'<shape image="connectoraction_icon" name="{shape_name}" '
        f'shapetype="connectoraction" userlabel="{userlabel}" '
        f'x="{_shape_x(shape_index)}" y="{_SHAPE_Y}">'
        '<configuration>'
        f'<connectoraction actionType="{action_type}" '
        'allowDynamicCredentials="NONE" '
        f'connectionId="{connection_id}" '
        f'connectorType="{connector_type}" '
        'hideSettings="false" '
        f'operationId="{operation_id}"{parameter_profile_attr}>'
        f'{inner}'
        '</connectoraction>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def _looks_like_json(text: str) -> bool:
    """True when ``text`` is a JSON object/array literal.

    Used to decide whether a Message/Notify body needs MessageFormat single-quote
    wrapping (its ``{ }`` braces would otherwise be read as ``{N}`` placeholders).
    """
    stripped = text.strip()
    if not stripped or stripped[0] not in "{[":
        return False
    try:
        parsed = json.loads(stripped)
    except (ValueError, TypeError):
        return False
    return isinstance(parsed, (dict, list))


def _escape_message_format_text(text: str) -> str:
    """Escape free text for a Boomi Message/Notify MessageFormat field (#102 C3).

    Boomi MessageFormat (Message step, Notify step, Business Rules step — see
    help.boomi.com Message step docs) treats the single quote ``'`` as an escape
    character and ``{N}`` as a numbered variable placeholder:

      * a lone single quote is stripped — ``today's`` renders as ``todays``;
      * two single quotes render as one — ``today''s`` renders as ``today's``;
      * an unmatched single quote escapes the rest of the message, suppressing
        ``{N}`` substitution from there on;
      * JSON content must be wrapped in single quotes so its ``{ }`` braces are
        not interpreted as variable placeholders.

    So this doubles every apostrophe and, when the body is a JSON object/array,
    wraps the doubled result in single quotes. Emission owns this escaping — the
    #1 most-common Message/Notify bug — so callers pass raw text/JSON and never
    hand-author the quoting.
    """
    doubled = text.replace("'", "''")
    if _looks_like_json(text):
        return f"'{doubled}'"
    return doubled


def _emit_message(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    dragpoints = _emit_dragpoints([next_name], shape_index)
    userlabel = _escape_xml(params.get("userlabel") or "")
    text = _escape_xml(_escape_message_format_text(params.get("text") or ""))
    return (
        f'<shape image="message_icon" name="{shape_name}" shapetype="message" '
        f'userlabel="{userlabel}" x="{_shape_x(shape_index)}" y="{_SHAPE_Y}">'
        '<configuration>'
        '<message combined="false">'
        f'<msgTxt>{text}</msgTxt>'
        '<msgParameters/>'
        '</message>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def _emit_map(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    dragpoints = _emit_dragpoints([next_name], shape_index)
    userlabel = _escape_xml(params.get("userlabel") or "")
    map_id = _escape_xml(params.get("map_id") or "")
    return (
        f'<shape image="map_icon" name="{shape_name}" shapetype="map" '
        f'userlabel="{userlabel}" x="{_shape_x(shape_index)}" y="{_SHAPE_Y}">'
        '<configuration>'
        f'<map mapId="{map_id}"/>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def _emit_flowcontrol(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    """Emit a process-level Flow Control (per-document batching) shape (issue #111 M10.7).

    Byte-accurate to the live ``work``-account capture (component
    7ce0d74d-e71a-408b-9d59-a6f4498c64e2; see .codex/plans/issue-111.md): the
    supported v1 mode is per-document batching —
    ``<flowcontrol chunkStyle="threadOnly" chunks="0" forEachCount="N"/>`` with no
    userdefoptions. Linear shape — one forward dragpoint to ``next_name``.
    """
    for_each_count = params.get("for_each_count")
    if (
        not isinstance(for_each_count, int)
        or isinstance(for_each_count, bool)
        or for_each_count <= 0
    ):
        # build() stays total on the validate_config-bypass path: a non-positive
        # forEachCount would emit a semantically broken batch size (well-formed
        # XML, so the parse-back guard would not catch it), so raise rather than
        # emit it — mirrors the _emit_dataprocess empty-steps guard. validate_config
        # already rejects this on the normal path.
        raise BuilderValidationError(
            "flow_control.for_each_count must be a positive integer (documents per batch).",
            error_code="PROCESS_FLOW_CONTROL_CONFIG_INVALID",
            field="flow_control.for_each_count",
            hint="v1 supports per-document batching: set for_each_count to the batch size, e.g. 10.",
        )
    userlabel = _escape_xml(params.get("userlabel") or "")
    dragpoints = _emit_dragpoints([next_name], shape_index)
    return (
        f'<shape image="flowcontrol_icon" name="{shape_name}" '
        f'shapetype="flowcontrol" userlabel="{userlabel}" '
        f'x="{_shape_x(shape_index)}" y="{_SHAPE_Y}">'
        '<configuration>'
        f'<flowcontrol chunkStyle="threadOnly" chunks="0" forEachCount="{for_each_count}"/>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def _emit_dataprocess(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    """Emit a process-level Data Process shape (issue #106 M10.2).

    Renders the ordered ``steps`` into ``<dataprocess><step .../></dataprocess>``
    with sequential 1-based ``index``/``key``, dispatching each step to its
    per-operation renderer. Byte-accurate to the live ``work``-account capture
    (see ``.codex/plans/issue-106-live-captures.md``): the step ``name`` is the
    standard Boomi operation name (a custom step name causes GUI display issues)
    and descriptive text lives on the shape ``userlabel``. Linear shape — one
    forward dragpoint to ``next_name``.
    """
    dragpoints = _emit_dragpoints([next_name], shape_index)
    userlabel = _escape_xml(params.get("userlabel") or "")
    steps = params.get("steps") or []
    if not steps:
        # build() stays total on the validate_config-bypass path: an empty step
        # list would emit a semantically broken <dataprocess/> with no <step>
        # (well-formed XML, so the parse-back guard would not catch it), so raise
        # rather than emit it — mirrors the catch_notify / unsupported-operation
        # bypass guards. validate_config already rejects this on the normal path.
        raise BuilderValidationError(
            "transform.steps must be a non-empty list when mode='dataprocess'.",
            error_code="PROCESS_DATAPROCESS_CONFIG_INVALID",
            field="transform.steps",
            hint="Provide at least one Data Process operation step.",
        )
    step_parts = [_emit_dataprocess_step(step, i) for i, step in enumerate(steps, start=1)]
    return (
        f'<shape image="dataprocess_icon" name="{shape_name}" '
        f'shapetype="dataprocess" userlabel="{userlabel}" '
        f'x="{_shape_x(shape_index)}" y="{_SHAPE_Y}">'
        '<configuration>'
        f'<dataprocess>{"".join(step_parts)}</dataprocess>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def _emit_dataprocess_step(step: Dict[str, Any], index: int) -> str:
    """Render one Data Process ``<step>`` (issue #106 / #115 M10.2a).

    Dispatches per operation: ``custom_scripting`` -> <dataprocessscript>,
    ``split_documents`` -> <documentsplit><SplitOptions>…, ``combine_documents``
    -> <dataprocesscombine>…. Keeps build() total on the validate_config-bypass
    path: an operation not in the supported set raises
    ``PROCESS_DATAPROCESS_OPERATION_UNSUPPORTED`` rather than emitting a malformed
    step.
    """
    operation = str(step.get("operation") or "").strip()
    meta = _DATAPROCESS_OPERATIONS.get(operation)
    if meta is None:
        raise BuilderValidationError(
            f"Unsupported Data Process operation {operation!r}.",
            error_code="PROCESS_DATAPROCESS_OPERATION_UNSUPPORTED",
            field="transform.steps[].operation",
            hint=f"Supported: {sorted(_DATAPROCESS_OPERATIONS)}.",
        )
    open_tag = (
        f'<step index="{index}" key="{index}" '
        f'name="{meta["name"]}" processtype="{meta["processtype"]}">'
    )
    if operation == "custom_scripting":
        script = _escape_xml(str(step.get("script") or ""))
        body = (
            f'<dataprocessscript language="{_DATAPROCESS_SCRIPT_LANGUAGE}" useCache="true">'
            f'<script>{script}</script>'
            '</dataprocessscript>'
        )
    elif operation == "split_documents":
        body = _emit_dataprocess_split_body(step)
    elif operation == "combine_documents":
        body = _emit_dataprocess_combine_body(step)
    else:  # pragma: no cover - registry/emitter dispatch kept in lockstep
        raise BuilderValidationError(
            f"Unsupported Data Process operation {operation!r}.",
            error_code="PROCESS_DATAPROCESS_OPERATION_UNSUPPORTED",
            field="transform.steps[].operation",
            hint=f"Supported: {sorted(_DATAPROCESS_OPERATIONS)}.",
        )
    return f"{open_tag}{body}</step>"


def _dataprocess_option_tag(step: Dict[str, Any]) -> str:
    """Return the option element tag for a Split/Combine profile step (issue #115).

    json -> ``JSONOptions``, xml -> ``XMLOptions`` (the live-observed child of
    <SplitOptions> / <dataprocesscombine>). Raises so build() stays total on the
    validate_config-bypass path when an unknown profile_type slips through.
    """
    profile_type = str(step.get("profile_type") or "").strip().lower()
    if profile_type == "json":
        return "JSONOptions"
    if profile_type == "xml":
        return "XMLOptions"
    raise BuilderValidationError(
        f"Unsupported Data Process profile_type {profile_type!r}.",
        error_code="PROCESS_DATAPROCESS_CONFIG_INVALID",
        field="transform.steps[].profile_type",
        hint=f"Allowed: {sorted(_DATAPROCESS_PROFILE_TYPES)}.",
    )


def _emit_dataprocess_split_body(step: Dict[str, Any]) -> str:
    """Render a Split Documents body (issue #115): <documentsplit><SplitOptions>….

    Attribute order linkElementKey, linkElementName, profileId — byte-exact to the
    live `work`-account capture (XML split) and the companion reference (JSON split).
    """
    tag = _dataprocess_option_tag(step)
    profile_type = _escape_xml(str(step.get("profile_type") or ""))
    link_key = _escape_xml(str(step.get("link_element_key") or ""))
    link_name = _escape_xml(str(step.get("link_element_name") or ""))
    profile_id = _escape_xml(str(step.get("profile_id") or ""))
    return (
        f'<documentsplit profileType="{profile_type}"><SplitOptions>'
        f'<{tag} linkElementKey="{link_key}" linkElementName="{link_name}" '
        f'profileId="{profile_id}"/>'
        '</SplitOptions></documentsplit>'
    )


def _emit_dataprocess_combine_body(step: Dict[str, Any]) -> str:
    """Render a Combine Documents body (issue #115): <dataprocesscombine>….

    Attribute order combineIntoLinkElementKey, linkElementKey, linkElementName,
    profileId — per the companion reference. ``combine_into_link_element_key``
    defaults to the literal 'null' (combine into root).
    """
    tag = _dataprocess_option_tag(step)
    profile_type = _escape_xml(str(step.get("profile_type") or ""))
    combine_into = _escape_xml(str(step.get("combine_into_link_element_key") or "null"))
    link_key = _escape_xml(str(step.get("link_element_key") or ""))
    link_name = _escape_xml(str(step.get("link_element_name") or ""))
    profile_id = _escape_xml(str(step.get("profile_id") or ""))
    return (
        f'<dataprocesscombine profileType="{profile_type}">'
        f'<{tag} combineIntoLinkElementKey="{combine_into}" '
        f'linkElementKey="{link_key}" linkElementName="{link_name}" '
        f'profileId="{profile_id}"/>'
        '</dataprocesscombine>'
    )


def _emit_doccacheretrieve(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    """Emit a process-level Document Cache Retrieve shape (issue #109 M10.5).

    Byte-accurate to the live ``work``-account capture (component
    64e5397b-3583-42c9-8fe3-08ccefb0da6c shape2; see
    ``.codex/plans/issue-109-live-captures.md``):
    ``<shape image="doccacheretrieve_icon" ... shapetype="doccacheretrieve"
    userlabel="..."><configuration><doccacheretrieve docCache="..."
    emptyCacheBehavior="stopprocess" loadAllDoc="true"><cacheKeyValues/>
    </doccacheretrieve></configuration><dragpoints>...</dragpoints></shape>``.

    The shape pulls documents from a Document Cache into the current flow — the
    read counterpart of the already-shipped ``doccacheload`` (Add to Cache). It
    is a normal linear NON-terminal step: one forward dragpoint to ``next_name``.
    v1 emits only the all-document retrieve form (``loadAllDoc="true"`` with an
    empty ``<cacheKeyValues/>``); the attribute order — docCache,
    emptyCacheBehavior, loadAllDoc — matches the live XML byte-for-byte. build()
    stays total on a validate_config-bypass: it re-guards the three invariants
    _validate_doccacheretrieve_transform enforces (non-empty ``document_cache_id``,
    ``empty_cache_behavior`` in the supported set, ``load_all_documents`` True) and
    raises rather than serialize a semantically broken / unsupported variant —
    ``docCache=""``, ``emptyCacheBehavior="returnerror"``, or ``loadAllDoc="false"``
    with an empty ``<cacheKeyValues/>`` (a broken keyed retrieve). All are
    well-formed XML the parse-back guard would not catch, so mirror the
    _emit_dataprocess empty-steps guard and raise here.
    """
    doc_cache_id = _escape_xml(str(params.get("document_cache_id") or "").strip())
    if not doc_cache_id:
        raise BuilderValidationError(
            "transform.document_cache_id is required when mode='doccacheretrieve'.",
            error_code="PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID",
            field="transform.document_cache_id",
            hint=(
                "Pass the Document Cache component id (a literal id or a $ref:KEY "
                "token in depends_on) to retrieve documents from."
            ),
        )
    empty_cache_behavior = str(
        params.get("empty_cache_behavior") or _DOCCACHE_RETRIEVE_DEFAULT_EMPTY_BEHAVIOR
    ).strip()
    if empty_cache_behavior not in _DOCCACHE_RETRIEVE_EMPTY_BEHAVIORS:
        raise BuilderValidationError(
            f"transform.empty_cache_behavior {empty_cache_behavior!r} is not supported.",
            error_code="PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID",
            field="transform.empty_cache_behavior",
            hint=(
                "v1 supports only 'stopprocess' (Stop document execution); the "
                "backward-compat 'fail document with errors' behavior is deferred."
            ),
        )
    if params.get("load_all_documents", True) is not True:
        raise BuilderValidationError(
            "transform.load_all_documents must be true when mode='doccacheretrieve'.",
            error_code="PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID",
            field="transform.load_all_documents",
            hint=(
                "v1 retrieves ALL cached documents (loadAllDoc=true, empty "
                "cacheKeyValues). Keyed/index retrieval is deferred."
            ),
        )
    dragpoints = _emit_dragpoints([next_name], shape_index)
    userlabel = _escape_xml(params.get("userlabel") or "")
    empty_cache_behavior_xml = _escape_xml(empty_cache_behavior)
    return (
        f'<shape image="doccacheretrieve_icon" name="{shape_name}" '
        f'shapetype="doccacheretrieve" userlabel="{userlabel}" '
        f'x="{_shape_x(shape_index)}" y="{_SHAPE_Y}">'
        '<configuration>'
        f'<doccacheretrieve docCache="{doc_cache_id}" '
        f'emptyCacheBehavior="{empty_cache_behavior_xml}" loadAllDoc="true">'
        '<cacheKeyValues/>'
        '</doccacheretrieve>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def _emit_doccacheremove(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    """Emit a process-level Document Cache Remove shape (issue #110 M10.6).

    Byte-accurate to the live ``work``-account capture (component
    6e56df6a-1fc0-43f6-8db2-1b9e4eefa7a0 "[CDS] Initialize Caches" shapes
    3-7; see ``.codex/plans/issue-110-live-captures.md``):
    ``<shape image="doccacheremove_icon" ... shapetype="doccacheremove"
    userlabel="..."><configuration><doccacheremove docCache="..."
    removeAllDocuments="true"><cacheKeyValues/></doccacheremove></configuration>
    <dragpoints>...</dragpoints></shape>``.

    The shape clears documents from a Document Cache — the DELETE counterpart of
    the already-shipped ``doccacheload`` (Add to Cache, write) and ``doccacheretrieve``
    (Document Cache Retrieve, read, #109), completing Document Cache CRUD. The live
    remove shapes sit at branch-leg ends (empty ``<dragpoints/>``); per #110 the
    builder shape is a normal linear NON-terminal step: one forward dragpoint to
    ``next_name`` (mirroring doccacheretrieve). v1 emits only the all-document
    remove form (``removeAllDocuments="true"`` with an empty ``<cacheKeyValues/>``);
    the attribute order — docCache, removeAllDocuments — matches the live XML
    byte-for-byte (there is no emptyCacheBehavior/loadAllDoc on a remove). The live
    capture shows two wire variants (self-closing and child-bearing); we emit the
    child-bearing ``<cacheKeyValues/>`` form for byte-consistency with the #109
    retrieve emitter. build() stays total on a validate_config-bypass: it re-guards
    the two invariants _validate_doccacheremove_transform enforces (non-empty
    ``document_cache_id``, ``remove_all_documents`` True) and raises rather than
    serialize a semantically broken / unsupported variant — ``docCache=""`` or
    ``removeAllDocuments="false"`` with an empty ``<cacheKeyValues/>`` (a broken
    keyed remove). Both are well-formed XML the parse-back guard would not catch,
    so mirror the _emit_doccacheretrieve guards and raise here.
    """
    doc_cache_id = _escape_xml(str(params.get("document_cache_id") or "").strip())
    if not doc_cache_id:
        raise BuilderValidationError(
            "transform.document_cache_id is required when mode='doccacheremove'.",
            error_code="PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID",
            field="transform.document_cache_id",
            hint=(
                "Pass the Document Cache component id (a literal id or a $ref:KEY "
                "token in depends_on) to remove documents from."
            ),
        )
    if params.get("remove_all_documents", True) is not True:
        raise BuilderValidationError(
            "transform.remove_all_documents must be true when mode='doccacheremove'.",
            error_code="PROCESS_DOCCACHE_REMOVE_CONFIG_INVALID",
            field="transform.remove_all_documents",
            hint=(
                "v1 removes ALL cached documents (removeAllDocuments=true, empty "
                "cacheKeyValues). Keyed/index removal is deferred."
            ),
        )
    dragpoints = _emit_dragpoints([next_name], shape_index)
    userlabel = _escape_xml(params.get("userlabel") or "")
    return (
        f'<shape image="doccacheremove_icon" name="{shape_name}" '
        f'shapetype="doccacheremove" userlabel="{userlabel}" '
        f'x="{_shape_x(shape_index)}" y="{_SHAPE_Y}">'
        '<configuration>'
        f'<doccacheremove docCache="{doc_cache_id}" removeAllDocuments="true">'
        '<cacheKeyValues/>'
        '</doccacheremove>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def _emit_returndocuments(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    """Emit a process-level Return Documents terminal shape (issue #107 M10.3).

    Byte-accurate to the live ``work``-account capture (component
    64e5397b-3583-42c9-8fe3-08ccefb0da6c; see
    ``.codex/plans/issue-107-live-captures.md``):
    ``<shape image="returndocuments_icon" ... shapetype="returndocuments"
    userlabel=""><configuration><returndocuments label=""/></configuration>
    <dragpoints/></shape>``. The shape is TERMINAL — it returns the current
    documents to the calling source point (parent process / web-service client) —
    so it carries an empty ``<dragpoints/>`` and ``next_name`` is ignored (like
    ``_emit_stop``). The single optional ``label`` is the Boomi "custom label"
    identifying the returned document type(s) (used for Process Call/Route
    return-path mapping); it maps to BOTH the shape ``userlabel`` and the inner
    ``<returndocuments label="...">`` attribute. Empty in the live capture, so the
    common subprocess-return case is byte-identical to the export.
    """
    label = _escape_xml(str(params.get("label") or ""))
    return (
        f'<shape image="returndocuments_icon" name="{shape_name}" '
        f'shapetype="returndocuments" userlabel="{label}" '
        f'x="{_shape_x(shape_index)}" y="{_SHAPE_Y}">'
        '<configuration>'
        f'<returndocuments label="{label}"/>'
        '</configuration>'
        '<dragpoints/>'
        '</shape>'
    )


def _emit_property_source_value(key: int, source: Dict[str, Any]) -> str:
    """Emit one ``<parametervalue>`` for a property assignment (issue #121).

    ``static`` / ``profile`` / ``ddp`` (track) / ``dpp`` (process) are
    byte-identical to the pre-#121 dynamic-path segment emission (#100/#96
    live captures); ``current`` is transcribed from the #119 fixtures
    (``process_doccacheretrieve_loadalldoc_variant.xml`` DDP_ORIGINAL_DOCUMENT).
    ``definedparameter`` is rejected by validation before emission (gated —
    no verified wire shape; #119 census Outcome B).
    """
    value_type = str(source.get("value_type") or "").strip()
    if value_type == "static":
        value = _escape_xml(str(source.get("value") or ""))
        return (
            f'<parametervalue key="{key}" usesEncryption="false" valueType="static">'
            f'<staticparameter staticproperty="{value}"/>'
            '</parametervalue>'
        )
    if value_type == "current":
        # Current-document content source (#119 live capture: bare element).
        return f'<parametervalue key="{key}" usesEncryption="false" valueType="current"/>'
    if value_type == "profile":
        element_id = _escape_xml(str(source.get("element_id") or ""))
        element_name = _escape_xml(str(source.get("element_name") or ""))
        profile_id = _escape_xml(str(source.get("profile_id") or "").strip())
        profile_type = _escape_xml(str(source.get("profile_type") or "profile.json").strip())
        return (
            f'<parametervalue key="{key}" usesEncryption="false" valueType="profile">'
            f'<profileelement elementId="{element_id}" '
            f'elementName="{element_name}" '
            f'profileId="{profile_id}" profileType="{profile_type}"/>'
            '</parametervalue>'
        )
    if value_type == "ddp":
        # Dynamic Document Property value source (#96 §H). SAME valueType="track"
        # + <trackparameter> shape as the connector-step Path reference (§C1).
        name = _escape_xml(str(source.get("property_name") or "").strip())
        default = _escape_xml(str(source.get("default_value") or ""))
        return (
            f'<parametervalue key="{key}" usesEncryption="false" valueType="track">'
            f'<trackparameter defaultValue="{default}" propertyId="dynamicdocument.{name}" '
            f'propertyName="Dynamic Document Property - {name}"/>'
            '</parametervalue>'
        )
    if value_type == "dpp":
        # Dynamic Process Property value source (#96 §H live capture).
        name = _escape_xml(str(source.get("property_name") or "").strip())
        default = _escape_xml(str(source.get("default_value") or ""))
        return (
            f'<parametervalue key="{key}" usesEncryption="false" valueType="process">'
            f'<processparameter processproperty="{name}" processpropertydefaultvalue="{default}"/>'
            '</parametervalue>'
        )
    raise BuilderValidationError(  # pragma: no cover — validators reject first
        f"Unknown property source value_type {value_type!r}.",
        error_code="PROCESS_PROPERTY_SOURCE_INVALID",
        field="source_values",
        hint="Internal builder bug — please report.",
    )


def _emit_documentproperty_assignment(
    scope: str, name: str, persist: bool, sourcevalues_xml: str
) -> str:
    """Emit one ``<documentproperty>`` assignment (issue #121).

    DDP: ``propertyId="dynamicdocument.<name>"``, always ``persist="false"``.
    DPP: ``propertyId="process.<name>"``, ``persist`` caller-controlled. The
    attribute set/order is the #100 live-capture shape, cross-checked against
    the #119 fixtures (both DDP and DPP writes).
    """
    esc = _escape_xml(str(name or "").strip())
    if scope == "ddp":
        display = f"Dynamic Document Property - {esc}"
        property_id = f"dynamicdocument.{esc}"
        persist_text = "false"
    else:
        display = f"Dynamic Process Property - {esc}"
        property_id = f"process.{esc}"
        persist_text = "true" if persist else "false"
    return (
        '<documentproperty defaultValue="" isDynamicCredential="false" '
        f'isTradingPartner="false" name="{display}" '
        f'persist="{persist_text}" propertyId="{property_id}" shouldEncrypt="false">'
        f'<sourcevalues>{sourcevalues_xml}</sourcevalues>'
        '</documentproperty>'
    )


def _emit_setproperties_shape(
    shape_name: str,
    properties_xml: str,
    next_name: Optional[str],
    shape_index: int,
    userlabel: str = "",
) -> str:
    """Emit the ``documentproperties`` shape wrapper around assignment(s)."""
    dragpoints = _emit_dragpoints([next_name], shape_index)
    return (
        f'<shape image="documentproperties_icon" name="{shape_name}" '
        f'shapetype="documentproperties" userlabel="{_escape_xml(userlabel)}" '
        f'x="{_shape_x(shape_index)}" y="{_SHAPE_Y}">'
        f'<configuration><documentproperties>{properties_xml}'
        '</documentproperties></configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def _emit_setproperties_step(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    """Emit a generic ``set_ddp`` / ``set_dpp`` flow-sequence step (issue #121)."""
    sources = params.get("source_values") or []
    sourcevalues = "".join(
        _emit_property_source_value(i, src) for i, src in enumerate(sources, start=1)
    )
    prop = _emit_documentproperty_assignment(
        str(params.get("scope") or "ddp"),
        str(params.get("name") or ""),
        bool(params.get("persist", False)),
        sourcevalues,
    )
    return _emit_setproperties_shape(
        shape_name,
        prop,
        next_name,
        shape_index,
        userlabel=str(params.get("userlabel") or ""),
    )


def _emit_setproperties(
    shape_name: str,
    params: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    """Emit a Set Properties (``documentproperties``) shape that builds the REST
    dynamic path into a Dynamic Document Property.

    Issue #100 G2: concatenates the ordered ``segments`` (static literals + mapped
    profile elements) into ``dynamicdocument.<ddp_name>``; the following connector
    step's "Path" dynamic operation property then sources that DDP. A Dynamic
    DOCUMENT Property (not a process property) is used so each document in a
    multi-record run carries its own path (Codex review P1). Transcribed verbatim
    from the live REST Client export (see ``.codex/plans/issue-100-live-captures.md``)
    — the ``<profileelement>`` ``elementId`` / ``elementName`` come from the same
    JSON profile field index the map uses, so they match the generated profile.

    Since #121 this is a thin adapter over the generic property emitters above;
    its output stays byte-identical to the pre-#121 inline emission.
    """
    ddp_name = str(params.get("ddp_name") or "").strip()
    profile_id = str(params.get("request_profile_id") or "").strip()
    profile_type = str(params.get("profile_type") or "profile.json").strip()
    segments = params.get("segments") or []

    sources: List[Dict[str, Any]] = []
    for seg in segments:
        seg_type = str(seg.get("type") or "").strip()
        if seg_type == "static":
            sources.append({"value_type": "static", "value": str(seg.get("value") or "")})
        elif seg_type == "profile":
            sources.append(
                {
                    "value_type": "profile",
                    "element_id": str(seg.get("element_id") or ""),
                    "element_name": str(seg.get("element_name") or ""),
                    "profile_id": profile_id,
                    "profile_type": profile_type,
                }
            )
        elif seg_type == "ddp":
            sources.append(
                {"value_type": "ddp", "property_name": str(seg.get("property_name") or "").strip()}
            )
        elif seg_type == "dpp":
            sources.append(
                {"value_type": "dpp", "property_name": str(seg.get("property_name") or "").strip()}
            )
        else:  # pragma: no cover — _validate_dynamic_path rejects other types
            raise BuilderValidationError(
                f"Unknown dynamic_path segment type {seg_type!r}.",
                error_code="PROCESS_XML_VALIDATION_FAILED",
                field="target.dynamic_path.segments",
                hint="Internal builder bug — please report.",
            )

    sourcevalues = "".join(
        _emit_property_source_value(i, src) for i, src in enumerate(sources, start=1)
    )
    prop = _emit_documentproperty_assignment("ddp", ddp_name, False, sourcevalues)
    return _emit_setproperties_shape(shape_name, prop, next_name, shape_index)


def _emit_stop(shape_name: str, params: Dict[str, Any], y: float = _SHAPE_Y) -> str:
    cont = "true" if params.get("continue_", True) else "false"
    # Stop x position == last index but we don't know it here; the caller
    # passes shape_index implicitly through shape_name's numeric suffix.
    # ``y`` defaults to the Try-row y; the issue #89 catch-leg Stop (after a
    # Notify + DLQ route) passes ``_CATCH_SHAPE_Y`` to sit on the catch row.
    shape_index = int(re.sub(r"\D", "", shape_name) or "1")
    return (
        f'<shape image="stop_icon" name="{shape_name}" shapetype="stop" '
        f'x="{_shape_x(shape_index)}" y="{y}">'
        f'<configuration><stop continue="{cont}"/></configuration>'
        '<dragpoints/>'
        '</shape>'
    )


def _emit_dragpoints(
    next_names: List[Optional[str]], shape_index: int, y: float = _DRAGPOINT_Y
) -> str:
    """Emit <dragpoint .../> children for a shape.

    Each non-None entry in next_names produces one dragpoint with name
    "<shape>.dragpoint<N>" and toShape set. None entries are skipped
    (used by Stop, which has no outgoing edge). ``y`` defaults to the Try-row
    dragpoint y; catch-row shapes (issue #89 Notify / chained DLQ route) pass
    ``_CATCH_DRAGPOINT_Y`` so their outgoing edges sit on the catch row.
    """
    parts: List[str] = []
    point_index = 0
    for to_shape in next_names:
        if to_shape is None:
            continue
        point_index += 1
        parts.append(
            f'<dragpoint name="shape{shape_index}.dragpoint{point_index}" '
            f'toShape="{_escape_xml(to_shape)}" '
            f'x="{_dragpoint_x(shape_index)}" y="{y}"/>'
        )
    return "".join(parts)


# ----------------------------------------------------------------------
# Issue #51 M3.R1a / #89 M4.5.4 — Try/Catch + DLQ + Notify catch-path emission
#
# Shapes below are transcribed verbatim from verified live `work`-profile
# exports (no XML invented from docs):
#   * catcherrors  — component dff0bf83-d525-4781-b572-c93d285bb788 (shape4)
#   * doccacheload — same component (shape80), terminal catch leg
#   * processcall  — component 7b19baeb-ed62-4fac-9962-44fc0ed87f07 (shape34,
#                    on a catcherrors error branch), terminal catch leg
#   * notify       — component 1139079f-fff5-434c-aedc-d2758cc20525 (shape5),
#                    a notify on an error-handling path: notifyMessage with
#                    {N} placeholders, notifyMessageLevel, and a notifyParameters
#                    track binding of meta.base.catcherrorsmessage (issue #89)
# ----------------------------------------------------------------------

# Shape "kinds" produced by build()'s flow list (mirrors the dispatch order).
def _emit_flow_shape(
    kind: str,
    params: Dict[str, Any],
    shape_name: str,
    next_name: Optional[str],
    shape_index: int,
) -> str:
    """Emit one linear-flow shape with an outgoing edge to next_name."""
    if kind == "start_noaction":
        return _emit_start_noaction(shape_name, next_name, shape_index)
    if kind in ("connectoraction_source", "connectoraction_target"):
        return _emit_connectoraction(shape_name, params, next_name, shape_index)
    if kind == "message":
        return _emit_message(shape_name, params, next_name, shape_index)
    if kind == "map":
        return _emit_map(shape_name, params, next_name, shape_index)
    if kind == "flowcontrol":
        return _emit_flowcontrol(shape_name, params, next_name, shape_index)
    if kind == "dataprocess":
        return _emit_dataprocess(shape_name, params, next_name, shape_index)
    if kind == "doccacheretrieve":
        return _emit_doccacheretrieve(shape_name, params, next_name, shape_index)
    if kind == "doccacheremove":
        return _emit_doccacheremove(shape_name, params, next_name, shape_index)
    if kind == "setproperties":
        return _emit_setproperties(shape_name, params, next_name, shape_index)
    if kind == "setproperties_step":
        # Issue #121 M11.2: generic set_ddp / set_dpp flow-sequence step.
        return _emit_setproperties_step(shape_name, params, next_name, shape_index)
    if kind == "processcall":
        # Standalone main-flow Process Call (issue #90 wrapper_subprocess):
        # main-flow geometry, abort defaults False (parent continues past a
        # child failure — the live-observed wrapper value), forward dragpoint
        # to the next shape.
        return _emit_processcall(
            shape_name,
            str(params.get("process_id") or "").strip(),
            shape_index,
            next_name,
            wait=bool(params.get("wait", True)),
            abort=bool(params.get("abort", False)),
            y=_SHAPE_Y,
            dragpoint_y=_DRAGPOINT_Y,
            userlabel=str(params.get("userlabel") or ""),
        )
    if kind == "returndocuments":
        # Issue #107 M10.3: terminal Return Documents shape (replaces the Stop
        # when return_documents.enabled). next_name is ignored — it is terminal.
        return _emit_returndocuments(shape_name, params, next_name, shape_index)
    if kind == "stop":
        return _emit_stop(shape_name, params)
    raise BuilderValidationError(  # pragma: no cover — defensive
        f"Unknown shape kind {kind!r} produced by builder.",
        error_code="PROCESS_XML_VALIDATION_FAILED",
        field="shapes",
        hint="Internal builder bug — please report.",
    )


def _emit_linear_shapes(flow: List[Tuple[str, Dict[str, Any]]]) -> List[str]:
    """Emit the unwrapped Start -> ... -> Stop chain (pre-#51 behavior)."""
    total = len(flow)
    parts: List[str] = []
    for i, (kind, params) in enumerate(flow):
        shape_index = i + 1  # shape1..N
        shape_name = f"shape{shape_index}"
        next_name = f"shape{shape_index + 1}" if shape_index < total else None
        parts.append(_emit_flow_shape(kind, params, shape_name, next_name, shape_index))
    return parts


def _emit_branch(
    shape_name: str, leg_first_names: List[str], shape_index: int, *, userlabel: str = ""
) -> str:
    """Emit a Branch (N-way forward fan-out) shape (issue #112 M10.8).

    Transcribed from a live ``work``-profile export (component b34d3812 shape53,
    see .codex/plans/issue-112-live-captures.md): a
    ``<branch numBranches="N"/>`` configuration plus N ``<dragpoint>`` children
    with 1-based integer ``identifier``/``text`` labels, each wiring to a leg's
    first shape via ``toShape`` (forward only — no back edges, no join/merge).
    ``numBranches`` equals the dragpoint count (the verifier's
    ``BRANCH_NUM_BRANCHES_MISMATCH`` invariant); every dragpoint carries a
    non-empty ``toShape`` (``BRANCH_OUTPUT_UNSET``). Branch dragpoints are built
    inline here — like ``_emit_catcherrors`` — because the plain
    ``_emit_dragpoints`` helper emits no ``identifier``/``text`` (those are
    label-bearing edges).

    ``userlabel`` defaults to ``""`` so the legacy single-shape branch fan-out
    (``_emit_branch_shapes``) stays byte-for-byte identical; the #117 composed
    sequencer threads a branch step's ``label`` through so it is not silently
    dropped (every other composed step kind emits its label).
    """
    dragpoints = "".join(
        f'<dragpoint identifier="{i}" name="{shape_name}.dragpoint{i}" '
        f'text="{i}" toShape="{_escape_xml(to_shape)}" '
        f'x="{_dragpoint_x(shape_index)}" y="{_DRAGPOINT_Y}"/>'
        for i, to_shape in enumerate(leg_first_names, start=1)
    )
    return (
        f'<shape image="branch_icon" name="{shape_name}" shapetype="branch" '
        f'userlabel="{_escape_xml(userlabel)}" x="{_shape_x(shape_index)}" y="{_SHAPE_Y}">'
        f'<configuration><branch numBranches="{len(leg_first_names)}"/></configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def _emit_branch_shapes(
    pre_branch: List[Tuple[str, Dict[str, Any]]],
    legs: List[List[Tuple[str, Dict[str, Any]]]],
) -> List[str]:
    """Emit ``start -> ... -> branch -> N independent legs`` (issue #112 M10.8).

    ``pre_branch`` is the shared chain before the fan-out (start -> source ->
    [transform]); its last shape forwards to the Branch shape. ``legs`` is the
    ordered list of leg sub-flows (each a ``[target, terminal]`` chain); each
    Branch dragpoint targets its leg's first shape and every leg runs forward to
    its own terminal with no merge. Shape indices are assigned positionally
    (pre-branch 1..p, branch p+1, then each leg in turn) so names never collide —
    the same index-walk ``_emit_catch_leg`` uses for Try/Catch legs.
    """
    parts: List[str] = []
    p = len(pre_branch)
    branch_index = p + 1
    branch_name = f"shape{branch_index}"

    # Pre-branch chain: forward each shape to the next; the last pre-branch shape
    # forwards to the Branch shape (index p+1).
    for i, (kind, params) in enumerate(pre_branch):
        shape_index = i + 1
        shape_name = f"shape{shape_index}"
        next_name = f"shape{shape_index + 1}"  # i == p-1 -> branch_name
        parts.append(_emit_flow_shape(kind, params, shape_name, next_name, shape_index))

    # Allocate leg shape indices and record each leg's first shape name (the
    # Branch dragpoint target). Legs are laid out one after another, after the
    # Branch shape.
    leg_first_names: List[str] = []
    leg_layouts: List[Tuple[int, List[Tuple[str, Dict[str, Any]]]]] = []
    idx = branch_index + 1
    for leg in legs:
        leg_first_names.append(f"shape{idx}")
        leg_layouts.append((idx, leg))
        idx += len(leg)

    parts.append(_emit_branch(branch_name, leg_first_names, branch_index))

    # Each leg is an independent forward sub-flow ending in its own terminal.
    for start_index, leg in leg_layouts:
        m = len(leg)
        for j, (kind, params) in enumerate(leg):
            shape_index = start_index + j
            shape_name = f"shape{shape_index}"
            next_name = None if j == m - 1 else f"shape{shape_index + 1}"
            parts.append(_emit_flow_shape(kind, params, shape_name, next_name, shape_index))
    return parts


def _emit_decisionvalue(operand: Dict[str, Any], field: str) -> str:
    """Emit one ``<decisionvalue>`` operand (issue #113 M10.9).

    Transcribed from live work-profile decision XML (boomi_companion
    .../references/steps/decision_step.md): a ``track`` operand emits a
    ``<trackparameter defaultValue=.. propertyId=.. propertyName=..>`` (attributes
    in the live alphabetical order), a ``static`` operand a
    ``<staticparameter staticproperty=..>``. Re-guards the operand (raising
    ``PROCESS_DECISION_CONFIG_INVALID``) so a ``build()`` call that bypassed
    ``validate_config`` stays total — the same pattern as ``_emit_dataprocess``.
    """
    value_type = str(operand.get("value_type") or "").strip()
    if value_type == "track":
        property_id = str(operand.get("property_id") or "").strip()
        if not property_id:
            raise BuilderValidationError(
                f"{field}.property_id is required (non-blank) for a track operand.",
                error_code="PROCESS_DECISION_CONFIG_INVALID",
                field=f"{field}.property_id",
                hint="Provide the tracked property id, e.g. 'dynamicdocument.DDP_STATUS'.",
            )
        default_value = _escape_xml(str(operand.get("default_value") or ""))
        property_name = _escape_xml(str(operand.get("property_name") or ""))
        return (
            '<decisionvalue valueType="track">'
            f'<trackparameter defaultValue="{default_value}" '
            f'propertyId="{_escape_xml(property_id)}" '
            f'propertyName="{property_name}"/>'
            '</decisionvalue>'
        )
    if value_type == "static":
        static_value = operand.get("static_value")
        if not isinstance(static_value, str):
            raise BuilderValidationError(
                f"{field}.static_value is required (a string, may be empty) for a static operand.",
                error_code="PROCESS_DECISION_CONFIG_INVALID",
                field=f"{field}.static_value",
                hint="Use an empty string to compare against an empty value (the 'is empty' check).",
            )
        return (
            '<decisionvalue valueType="static">'
            f'<staticparameter staticproperty="{_escape_xml(static_value)}"/>'
            '</decisionvalue>'
        )
    raise BuilderValidationError(
        f"{field}.value_type must be one of {sorted(_DECISION_VALUE_TYPES)}.",
        error_code="PROCESS_DECISION_CONFIG_INVALID",
        field=f"{field}.value_type",
        hint="v1 supports 'track' (a DDP/DPP) and 'static' (a literal value).",
    )


def _emit_decision(
    shape_name: str,
    decision_config: Dict[str, Any],
    true_to: str,
    false_to: str,
    shape_index: int,
) -> str:
    """Emit a Decision (conditional two-path routing) shape (issue #113 M10.9).

    Transcribed from a live ``work``-profile export (see
    boomi_companion .../references/steps/decision_step.md and
    .codex/plans/issue-113-live-captures.md): a ``<decision comparison=..>``
    configuration with two ``<decisionvalue>`` operands plus exactly two
    dragpoints — ``identifier="true"`` (dragpoint1, ``text="True"``) and
    ``identifier="false"`` (dragpoint2, ``text="False"``) — each wiring to a leg's
    first shape (or, for the false leg, backward to an earlier shape for a loop).
    Both ``userlabel`` (on ``<shape>``) and ``name`` (on ``<decision>``) carry the
    same label so the display name renders. Dragpoints are built inline (like
    ``_emit_branch``) because they carry ``identifier``/``text`` labels the plain
    ``_emit_dragpoints`` helper does not emit.
    """
    label = _escape_xml(str(decision_config.get("label") or ""))
    # Strip the comparison to the canonical operator token: _validate_decision_config
    # accepts it via comparison.strip() membership, so emit the trimmed form too —
    # a padded " equals " must serialize as the supported "equals" token, never the
    # whitespaced value (mirrors _emit_decisionvalue stripping property_id).
    comparison = _escape_xml(str(decision_config.get("comparison") or "").strip())
    left = _emit_decisionvalue(decision_config.get("left") or {}, "decision.left")
    right = _emit_decisionvalue(decision_config.get("right") or {}, "decision.right")
    return (
        f'<shape image="decision_icon" name="{shape_name}" shapetype="decision" '
        f'userlabel="{label}" x="{_shape_x(shape_index)}" y="{_SHAPE_Y}">'
        f'<configuration><decision comparison="{comparison}" name="{label}">'
        f'{left}{right}</decision></configuration>'
        '<dragpoints>'
        f'<dragpoint identifier="true" name="{shape_name}.dragpoint1" text="True" '
        f'toShape="{_escape_xml(true_to)}" x="{_dragpoint_x(shape_index)}" y="{_DRAGPOINT_Y}"/>'
        f'<dragpoint identifier="false" name="{shape_name}.dragpoint2" text="False" '
        f'toShape="{_escape_xml(false_to)}" x="{_dragpoint_x(shape_index)}" y="{_CATCH_DRAGPOINT_Y}"/>'
        '</dragpoints>'
        '</shape>'
    )


def _emit_decision_leg(
    leg: List[Tuple[str, Dict[str, Any]]], start_index: int, tail_to: Optional[str]
) -> List[str]:
    """Emit one Decision leg as a forward shape chain (issue #113 M10.9).

    Each shape forwards to the next; the LAST shape forwards to ``tail_to`` — a
    backward shape name for a loop leg, or ``None`` when the leg already ends in a
    terminal Stop (whose own dragpoints are empty). The same index walk
    ``_emit_branch_shapes`` uses for fan-out legs.
    """
    parts: List[str] = []
    m = len(leg)
    for j, (kind, params) in enumerate(leg):
        shape_index = start_index + j
        shape_name = f"shape{shape_index}"
        next_name = tail_to if j == m - 1 else f"shape{shape_index + 1}"
        parts.append(_emit_flow_shape(kind, params, shape_name, next_name, shape_index))
    return parts


def _emit_decision_shapes(
    pre_decision: List[Tuple[str, Dict[str, Any]]],
    decision_config: Dict[str, Any],
    true_leg: List[Tuple[str, Dict[str, Any]]],
    false_leg: List[Tuple[str, Dict[str, Any]]],
    false_loop_to: Optional[str],
) -> List[str]:
    """Emit ``start -> ... -> decision -> {true leg, false leg}`` (issue #113 M10.9).

    ``pre_decision`` is the shared chain before the Decision (start -> source ->
    [transform]); its last shape forwards to the Decision. The TRUE leg is the
    forward success path (``target -> Stop``). The FALSE leg is either forward (an
    optional Message then its own Stop) or a backward loop: when ``false_loop_to``
    is set, the false leg's last processing shape wires back to that earlier shape
    (or, when ``false_leg`` is empty, the Decision's false dragpoint targets it
    directly). Shape indices are positional (pre-decision 1..p, decision p+1, then
    the true leg, then the false leg) so names never collide — the same index-walk
    ``_emit_branch_shapes`` uses.
    """
    parts: List[str] = []
    p = len(pre_decision)
    decision_index = p + 1
    decision_name = f"shape{decision_index}"

    # Pre-decision chain: forward each shape to the next; the last pre-decision
    # shape forwards to the Decision shape (index p+1).
    for i, (kind, params) in enumerate(pre_decision):
        shape_index = i + 1
        shape_name = f"shape{shape_index}"
        next_name = f"shape{shape_index + 1}"  # i == p-1 -> decision_name
        parts.append(_emit_flow_shape(kind, params, shape_name, next_name, shape_index))

    # Lay out the true leg right after the Decision, then the false leg.
    true_start = decision_index + 1
    false_start = true_start + len(true_leg)

    true_to = f"shape{true_start}"
    if false_leg:
        false_to = f"shape{false_start}"
    else:
        # An empty false leg only happens on a bare loop (no false_notify); the
        # false dragpoint then targets the backward shape directly.
        false_to = false_loop_to if false_loop_to is not None else f"shape{false_start}"

    parts.append(_emit_decision(decision_name, decision_config, true_to, false_to, decision_index))

    # True leg: forward chain ending in its own terminal Stop (tail None).
    parts.extend(_emit_decision_leg(true_leg, true_start, tail_to=None))
    # False leg: forward (tail None -> own Stop) or looping (tail -> earlier shape).
    parts.extend(_emit_decision_leg(false_leg, false_start, tail_to=false_loop_to))
    return parts


# ----------------------------------------------------------------------
# Issue #117 M10 follow-up — multi-control-shape composition (flow_sequence)
#
# An ordered ``flow_sequence`` composes 2+ M10 control/transform shapes in one
# process. Validation + emission reuse the existing single-shape validators and
# emitters (so each composed shape stays byte-accurate); only the wiring /
# index allocation is new. v1 control/terminal kinds (decision / branch /
# exception) terminalize the containing sequence — no post-control join yet.
# Branch legs are linear sub-flows + a target; decision legs may end in a nested
# branch or exception (one level), never a nested decision.
# ----------------------------------------------------------------------


def _flow_sequence_enabled(config: Dict[str, Any]) -> bool:
    """True when the config carries a ``flow_sequence`` (presence, any value).

    Presence — not well-formedness — routes to the composed path so a malformed
    flow_sequence (empty list, non-list) is REJECTED there rather than silently
    dropped while the legacy single-shape path emits a plain process. Absent (None)
    keeps the pre-#117 single-shape behavior byte-for-byte.
    """
    return config.get("flow_sequence") is not None


def _sequence_sibling_error(block: str) -> BuilderValidationError:
    """Reject a legacy single-slot block present alongside a ``flow_sequence``."""
    return BuilderValidationError(
        f"{block} cannot be combined with a top-level flow_sequence in v1; express "
        f"it as a flow_sequence step instead.",
        error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
        field=block,
        hint=(
            "flow_sequence is the single composition surface — remove the legacy "
            "single-slot block (flow_control / branch / decision / non-passthrough "
            "transform / Try-Catch reliability) and add an equivalent flow_sequence step."
        ),
    )


def _validate_flow_sequence_config(config: Dict[str, Any]) -> Optional[BuilderValidationError]:
    """Validate the optional top-level ``flow_sequence`` (issue #117).

    Structural validation only — reachability is run separately by validate_config
    (the SAME ``$ref`` walker the legacy path uses), so build() can re-run this for
    totality on a validate_config bypass without depends_on. Precedence: non-empty
    list -> no ambiguous legacy sibling -> source/target dynamic_path gate -> target
    binding -> return_documents gate -> recursive step validation.
    """
    seq = config.get("flow_sequence")
    if not isinstance(seq, list) or not seq:
        return BuilderValidationError(
            "flow_sequence must be a non-empty list of step objects.",
            error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
            field="flow_sequence",
            hint=(
                'Each step is {"kind": ..., ...}; a control kind (decision/branch) or '
                "the exception terminal must be the LAST step of its sequence."
            ),
        )
    # Ambiguous legacy siblings — rich composition is expressed ONLY via
    # flow_sequence. Reject ANY co-present legacy single-slot block by PRESENCE (not
    # just when "enabled"): the composed emitter ignores branch/decision/flow_control
    # /transform entirely, so a present block — enabled, disabled, OR a malformed
    # scalar like ``branch=1`` (which the *_enabled predicates read as disabled) —
    # would be silently dropped. Rejecting on presence keeps validate_config and
    # build()'s _build_composed_process_flow parity-total and never silently drops a
    # caller-specified block (Codex #117). A passthrough transform (or no transform)
    # is the harmless default and stays allowed; any other transform value (a
    # non-passthrough dict OR a malformed non-dict) is rejected.
    for sibling in ("flow_control", "branch", "decision"):
        if config.get(sibling) is not None:
            return _sequence_sibling_error(sibling)
    transform = config.get("transform")
    if transform is not None:
        is_passthrough = (
            isinstance(transform, dict)
            and str(transform.get("mode") or "passthrough").strip().lower() == "passthrough"
        )
        if not is_passthrough:
            return _sequence_sibling_error("transform")
    reliability = config.get("reliability")
    if isinstance(reliability, dict) and _reliability_requests_try_catch(reliability):
        return _sequence_sibling_error("reliability")
    # A reliability block that does NOT request a Try/Catch is not consumed by the
    # composed path (no wrapper is emitted), but a MALFORMED one must still be
    # rejected rather than silently dropped — validate it with the SAME checker the
    # legacy path uses (a no-op default {retry_count:0, dlq:{mode:disabled}} passes
    # and is harmlessly ignored; retry_count out of range, a bad dlq/notify shape,
    # or catch_notify without a catch path is rejected with its own code). Keeps
    # validate_config and build() parity-total on the composed path (Codex #117).
    reliability_err = _validate_reliability(reliability)
    if reliability_err is not None:
        return reliability_err
    # v1 rejects a source/target dynamic_path under a flow_sequence — the composed
    # sequencer emits plain connectors (a runtime path binding is a follow-up). The
    # source.dynamic_path presence guard runs BEFORE _validate_source_binding so the
    # composition rejection (not the generic dynamic_path-shape check) is reported,
    # identically on the validate_config and build() paths (QA #142).
    source = config.get("source")
    if isinstance(source, dict) and source.get("dynamic_path") is not None:
        return BuilderValidationError(
            "source.dynamic_path is not supported together with flow_sequence in v1.",
            error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
            field="source.dynamic_path",
            hint="Remove the source runtime path binding to compose a flow_sequence.",
        )
    # flow_sequence is database_to_api_sync-only (DB source); validate the source
    # binding here so build()'s composed path is total on a validate_config bypass.
    source_err = _validate_source_binding(source)
    if source_err is not None:
        return source_err
    target = config.get("target")
    if isinstance(target, dict) and target.get("dynamic_path") is not None:
        return BuilderValidationError(
            "target.dynamic_path is not supported together with flow_sequence in v1.",
            error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
            field="target.dynamic_path",
            hint="Remove the target runtime path binding to compose a flow_sequence.",
        )
    # The top-level target is the default success terminal (used by a linear main
    # line and by a decision TRUE leg); required even when every path self-terminates.
    target_err = _validate_target_binding(target)
    if target_err is not None:
        return target_err
    # return_documents terminal is allowed ONLY for a purely linear sequence — a
    # control fan-out / exception already terminates every path.
    rd = config.get("return_documents")
    rd_err = _validate_return_documents(rd)
    if rd_err is not None:
        return rd_err
    if isinstance(rd, dict) and rd.get("enabled") is True and not _sequence_is_linear_only(seq):
        return BuilderValidationError(
            "return_documents is not supported together with a branch/decision/exception "
            "flow_sequence in v1.",
            error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
            field="return_documents",
            hint=(
                "A control/terminal step already terminates each path; use "
                "return_documents only on a purely linear flow_sequence."
            ),
        )
    steps_err = _validate_flow_sequence_steps(
        seq,
        "flow_sequence",
        allowed_terminal_controls=_FLOW_SEQUENCE_CONTROL_KINDS | _FLOW_SEQUENCE_TERMINAL_KINDS,
        allow_empty=False,
    )
    if steps_err is not None:
        return steps_err
    last_step = seq[-1]
    last_kind = (
        str(last_step.get("kind") or "").strip() if isinstance(last_step, dict) else ""
    )
    if last_kind in ("cache_put", "doccacheload"):
        # Companion review P1 (+ scoped re-review): the top-level sequence
        # falls through to the target connector, which would receive an empty
        # document stream after an Add to Cache (it consumes the documents it
        # stores) — both the authored cache_put and the legacy doccacheload
        # kind emit the same shape. Runs AFTER the per-step validators so the
        # more specific body errors (e.g. a missing document_cache_id) win.
        return BuilderValidationError(
            f"flow_sequence must not END in a {last_kind} — the top-level "
            "target would receive an empty document stream (Add to Cache "
            "consumes the documents).",
            error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
            field=f"flow_sequence[{len(seq) - 1}].kind",
            hint=(
                "Follow the cache_put with cache_get/doccacheretrieve, or "
                "stage inside a target-less branch leg instead."
            ),
        )
    return None


def _validate_flow_sequence_steps(
    steps: Any,
    field: str,
    *,
    allowed_terminal_controls: frozenset,
    allow_empty: bool,
) -> Optional[BuilderValidationError]:
    """Validate an ordered list of flow-sequence steps.

    Only the LAST step may be a control (decision/branch) or terminal (exception)
    kind, and only when its kind is in ``allowed_terminal_controls`` (top-level
    allows decision/branch/exception; a decision leg allows branch/exception; a
    branch leg is linear-only). Every step body is validated by
    ``_validate_flow_sequence_step``.
    """
    if not isinstance(steps, list):
        return BuilderValidationError(
            f"{field} must be a list of step objects.",
            error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
            field=field,
            hint="Provide an ordered list of {kind: ...} step objects.",
        )
    if not steps:
        if allow_empty:
            return None
        return BuilderValidationError(
            f"{field} must be a non-empty list.",
            error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
            field=field,
            hint="Provide at least one step.",
        )
    last = len(steps) - 1
    control_or_terminal = _FLOW_SEQUENCE_CONTROL_KINDS | _FLOW_SEQUENCE_TERMINAL_KINDS
    for i, step in enumerate(steps):
        kind = str((step.get("kind") if isinstance(step, dict) else "") or "").strip()
        if kind in control_or_terminal:
            if i != last:
                return BuilderValidationError(
                    f"{field}[{i}] is a {kind} step, which must be the LAST step in its "
                    f"sequence (it terminalizes the path in v1).",
                    error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
                    field=f"{field}[{i}]",
                    hint=(
                        "Move the decision/branch/exception to the end, or push the "
                        "trailing steps into its legs."
                    ),
                )
            if kind not in allowed_terminal_controls:
                return BuilderValidationError(
                    f"{field}[{i}] kind {kind!r} is not allowed here; allowed terminal "
                    f"control kinds: {sorted(allowed_terminal_controls)}.",
                    error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
                    field=f"{field}[{i}].kind",
                    hint="A branch leg is linear in v1; a nested decision is a follow-up.",
                )
        # Companion review P1 (#122 follow-up): Add to Cache CONSUMES the
        # documents it stores, so a cache_put may only be followed (same
        # path) by a stream-REPLACING retrieve — anything else would run on
        # an empty document stream while validation reported success.
        consuming_kind = (
            str(step.get("kind") or "").strip() if isinstance(step, dict) else ""
        )
        if consuming_kind in ("cache_put", "doccacheload"):
            if i < len(steps) - 1:
                nxt = steps[i + 1]
                next_kind = (
                    str(nxt.get("kind") or "").strip() if isinstance(nxt, dict) else ""
                )
                if next_kind not in ("cache_get", "doccacheretrieve"):
                    return BuilderValidationError(
                        f"{field}[{i}] {consuming_kind} consumes the documents "
                        f"it stores — the following step ({next_kind!r}) would "
                        "receive an empty stream.",
                        error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
                        field=f"{field}[{i + 1}].kind",
                        hint=(
                            "Follow a cache_put with cache_get/doccacheretrieve "
                            "(which refills the stream from the cache), or make "
                            "the cache_put the terminal step of a target-less "
                            "branch leg (the live staging pattern)."
                        ),
                    )
        step_err = _validate_flow_sequence_step(step, f"{field}[{i}]")
        if step_err is not None:
            return step_err
    return None


def _validate_flow_sequence_step(step: Any, field: str) -> Optional[BuilderValidationError]:
    """Validate one flow-sequence step (kind -> per-kind body validation)."""
    if not isinstance(step, dict):
        return BuilderValidationError(
            f"{field} must be a JSON object with a 'kind'.",
            error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
            field=field,
            hint='Each step is {"kind": "...", ...}.',
        )
    kind = str(step.get("kind") or "").strip()
    if kind not in _FLOW_SEQUENCE_ALLOWED_KINDS:
        return BuilderValidationError(
            f"{field}.kind {kind!r} is not supported.",
            error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
            field=f"{field}.kind",
            hint=f"Supported kinds: {sorted(_FLOW_SEQUENCE_ALLOWED_KINDS)}.",
        )
    allowed_keys = _FLOW_SEQUENCE_STEP_KEYS[kind]
    extra = set(step) - allowed_keys
    if extra:
        return BuilderValidationError(
            f"{field} has unsupported key(s) for kind={kind!r}: {sorted(extra)}.",
            error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
            field=field,
            hint=f"Allowed keys for {kind!r}: {sorted(allowed_keys)}.",
        )
    if "label" in allowed_keys:
        label = step.get("label")
        if label is not None and not isinstance(label, str):
            return BuilderValidationError(
                f"{field}.label must be a string when provided.",
                error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
                field=f"{field}.label",
                hint="Use a short display label, or omit it.",
            )
    if kind == "doccacheload":
        return _validate_sequence_doccacheload_step(step, field)
    if kind in ("set_ddp", "set_dpp"):
        return _validate_sequence_set_properties_step(step, field, kind)
    if kind == "cache_put":
        return _validate_sequence_cache_put_step(step, field)
    if kind == "cache_get":
        return _validate_sequence_cache_get_step(step, field)
    if kind in _FLOW_SEQUENCE_LINEAR_KINDS:
        return _validate_sequence_linear_step(step, field, kind)
    if kind == "decision":
        return _validate_sequence_decision_step(step, field)
    if kind == "branch":
        return _validate_sequence_branch_step(step, field)
    if kind == "exception":
        return _validate_sequence_exception_step(step, field)
    return None  # pragma: no cover — kind already validated above


def _validate_sequence_linear_step(
    step: Dict[str, Any], field: str, kind: str
) -> Optional[BuilderValidationError]:
    """Validate a linear flow-sequence step by normalizing it into the equivalent
    legacy single-slot shape and delegating to that shape's existing validator.

    The delegated validator returns the SAME specific structured error code the
    legacy single-slot shape returns (PROCESS_SHAPE_UNSUPPORTED for message/map,
    PROCESS_DATAPROCESS_* / PROCESS_DOCCACHE_* / PROCESS_FLOW_CONTROL_*), so a
    malformed step body is reported with the exact per-shape contract.
    """
    label = step.get("label")
    if kind == "flow_control":
        fc: Dict[str, Any] = {"enabled": True, "for_each_count": step.get("for_each_count")}
        if label is not None:
            fc["label"] = label
        return _validate_flow_control_config({"flow_control": fc})
    if kind == "message":
        transform: Dict[str, Any] = {"mode": "message", "message_text": step.get("message_text")}
    elif kind == "map_ref":
        transform = {"mode": "map_ref", "map_ref": step.get("map_ref")}
    elif kind == "dataprocess":
        transform = {"mode": "dataprocess", "steps": step.get("steps")}
    elif kind == "doccacheretrieve":
        transform = {"mode": "doccacheretrieve", "document_cache_id": step.get("document_cache_id")}
        if "empty_cache_behavior" in step:
            transform["empty_cache_behavior"] = step.get("empty_cache_behavior")
        if "load_all_documents" in step:
            transform["load_all_documents"] = step.get("load_all_documents")
    elif kind == "doccacheremove":
        transform = {"mode": "doccacheremove", "document_cache_id": step.get("document_cache_id")}
        if "remove_all_documents" in step:
            transform["remove_all_documents"] = step.get("remove_all_documents")
    else:  # pragma: no cover — kind already validated
        return None
    if label is not None:
        transform["label"] = label
    return _validate_transform(transform)


def _validate_sequence_doccacheload_step(
    step: Dict[str, Any], field: str
) -> Optional[BuilderValidationError]:
    """Validate a ``doccacheload`` (Add to Cache) flow-sequence step (issue #117).

    The Add-to-Cache shape ships today only on the DLQ catch leg; as a main-row
    sequence step it carries the single required ``document_cache_id`` (a literal id
    or a $ref:KEY token in depends_on) plus an optional ``label``.
    """
    doc_cache_id = step.get("document_cache_id")
    if not isinstance(doc_cache_id, str) or not doc_cache_id.strip():
        return BuilderValidationError(
            f"{field}.document_cache_id is required for a doccacheload (Add to Cache) step.",
            error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
            field=f"{field}.document_cache_id",
            hint=(
                "Pass the Document Cache component id (a literal id or a $ref:KEY token "
                "in depends_on) to add the current documents to."
            ),
        )
    return None


# Issue #121 M11.2: property names are bare — the emitter owns the wire prefix
# and the display-name convention, so a caller-supplied prefix would double up.
_PROPERTY_NAME_FORBIDDEN_PREFIXES = (
    "dynamicdocument.",
    "process.",
    "document.dynamic.userdefined.",
)


def _validate_property_source_value(
    source: Any, field: str
) -> Optional[BuilderValidationError]:
    """Validate one ``source_values[]`` entry against the #120 contract.

    ``definedparameter`` (read a Process Property component field) is
    vocabulary but GATED: the #119 census found no live Set Properties capture
    of its wire shape (companion-only), so it is rejected here until a
    verified capture exists.
    """
    if not isinstance(source, dict):
        return BuilderValidationError(
            f"{field} must be a JSON object with a 'value_type'.",
            error_code="PROCESS_PROPERTY_SOURCE_INVALID",
            field=field,
            hint='Each source value is {"value_type": "static|current|profile|ddp|dpp", ...}.',
        )
    value_type = str(source.get("value_type") or "").strip()
    if value_type == "definedparameter":
        return BuilderValidationError(
            f"{field}.value_type 'definedparameter' has no verified wire shape yet.",
            error_code="PROCESS_PROPERTY_SOURCE_INVALID",
            field=f"{field}.value_type",
            hint=(
                "Reading a Process Property component inside Set Properties is "
                "gated (companion-documented only; #119 census Outcome B). Read "
                "it via the defined_process_property_get map function instead."
            ),
        )
    if value_type not in PROPERTY_SOURCE_FIELD_CONTRACT:
        supported = sorted(set(PROPERTY_SOURCE_FIELD_CONTRACT) - {"definedparameter"})
        return BuilderValidationError(
            f"{field}.value_type {value_type!r} is not supported.",
            error_code="PROCESS_PROPERTY_SOURCE_INVALID",
            field=f"{field}.value_type",
            hint=f"Supported source value types: {supported}.",
        )
    required, optional = PROPERTY_SOURCE_FIELD_CONTRACT[value_type]
    allowed = {"value_type", *required, *optional}
    extra = set(source) - allowed
    if extra:
        return BuilderValidationError(
            f"{field} has unsupported key(s) for value_type={value_type!r}: {sorted(extra)}.",
            error_code="PROCESS_PROPERTY_SOURCE_INVALID",
            field=field,
            hint=f"Allowed keys for {value_type!r}: {sorted(allowed)}.",
        )
    for key in required:
        value = source.get(key)
        blank_ok = value_type == "static" and key == "value"
        if not isinstance(value, str) or (not blank_ok and not value.strip()):
            return BuilderValidationError(
                f"{field}.{key} is required (a string) for value_type={value_type!r}.",
                error_code="PROCESS_PROPERTY_SOURCE_INVALID",
                field=f"{field}.{key}",
                hint=f"value_type={value_type!r} requires: {sorted(required)}.",
            )
    for key in optional:
        value = source.get(key)
        if value is not None and not isinstance(value, str):
            return BuilderValidationError(
                f"{field}.{key} must be a string when provided.",
                error_code="PROCESS_PROPERTY_SOURCE_INVALID",
                field=f"{field}.{key}",
                hint="Pass a string value, or omit the key.",
            )
    return None


def _validate_sequence_set_properties_step(
    step: Dict[str, Any], field: str, kind: str
) -> Optional[BuilderValidationError]:
    """Validate a ``set_ddp`` / ``set_dpp`` flow-sequence step (issue #121).

    ``name`` is the bare property name (the emitter owns the
    ``dynamicdocument.`` / ``process.`` prefix); ``source_values`` is the
    ordered value-source list (concatenated by Boomi at runtime); ``persist``
    (DPP only — enforced by the per-kind key allow-list) persists the value at
    atom level.
    """
    name = step.get("name")
    if not isinstance(name, str) or not name.strip():
        return BuilderValidationError(
            f"{field}.name is required for a {kind} step.",
            error_code="PROCESS_PROPERTY_NAME_INVALID",
            field=f"{field}.name",
            hint="Pass the bare property name (no dynamicdocument./process. prefix).",
        )
    stripped = name.strip()
    for prefix in _PROPERTY_NAME_FORBIDDEN_PREFIXES:
        if stripped.startswith(prefix):
            return BuilderValidationError(
                f"{field}.name must not carry the {prefix!r} prefix — the emitter owns it.",
                error_code="PROCESS_PROPERTY_NAME_INVALID",
                field=f"{field}.name",
                hint=f"Use the bare name, e.g. {stripped[len(prefix):]!r}.",
            )
    if any(ch.isspace() for ch in stripped):
        return BuilderValidationError(
            f"{field}.name must not contain whitespace.",
            error_code="PROCESS_PROPERTY_NAME_INVALID",
            field=f"{field}.name",
            hint="Use UPPER_SNAKE names like DDP_ORDER_ID / DPP_RUN_FLAG.",
        )
    source_values = step.get("source_values")
    if not isinstance(source_values, list) or not source_values:
        return BuilderValidationError(
            f"{field}.source_values must be a non-empty list for a {kind} step.",
            error_code="PROCESS_SET_PROPERTIES_CONFIG_INVALID",
            field=f"{field}.source_values",
            hint="Boomi concatenates the ordered source values into the property.",
        )
    for j, source in enumerate(source_values):
        source_err = _validate_property_source_value(
            source, f"{field}.source_values[{j}]"
        )
        if source_err is not None:
            return source_err
    if kind == "set_dpp":
        persist = step.get("persist")
        if persist is not None and not isinstance(persist, bool):
            return BuilderValidationError(
                f"{field}.persist must be a boolean when provided.",
                error_code="PROCESS_SET_PROPERTIES_CONFIG_INVALID",
                field=f"{field}.persist",
                hint="true persists the DPP at atom level; default false.",
            )
    return None


def _validate_sequence_cache_put_step(
    step: Dict[str, Any], field: str
) -> Optional[BuilderValidationError]:
    """Validate a ``cache_put`` step (issue #122) — the authored success-path
    Add to Cache write. Same contract as ``doccacheload``: one required
    ``document_cache_id`` (literal id or $ref:KEY token in depends_on)."""
    doc_cache_id = step.get("document_cache_id")
    if not isinstance(doc_cache_id, str) or not doc_cache_id.strip():
        return BuilderValidationError(
            f"{field}.document_cache_id is required for a cache_put step.",
            error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
            field=f"{field}.document_cache_id",
            hint=(
                "Pass the Document Cache component id (a literal id or a "
                "$ref:KEY token in depends_on) to write the current documents to."
            ),
        )
    return None


def _validate_sequence_cache_get_step(
    step: Dict[str, Any], field: str
) -> Optional[BuilderValidationError]:
    """Validate a ``cache_get`` step (issue #122) — authored retrieve.

    v1 supports the all-document form only (the byte-locked M10 retrieve).
    Keyed/index retrieval keys are recognized but rejected with the NAMED
    gated error: the #119 census found no live capture of a populated
    ``cacheKeyValues`` wire shape (Outcome B), so emitting one would be
    invented XML.
    """
    for gated_key in ("doc_cache_index", "cache_key_values"):
        if gated_key in step:
            return BuilderValidationError(
                f"{field}.{gated_key} is gated — keyed cache retrieval has no "
                "live-captured wire shape (#119 census).",
                error_code="PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID",
                field=f"{field}.{gated_key}",
                hint=(
                    "v1 cache_get retrieves ALL cached documents. The keyed "
                    "form unlocks after a disposable-account round-trip "
                    "captures the populated cacheKeyValues shape."
                ),
            )
    if "load_all_documents" in step and step.get("load_all_documents") is not True:
        return BuilderValidationError(
            f"{field}.load_all_documents supports only true in v1 (keyed "
            "retrieval is gated, #119 census).",
            error_code="PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID",
            field=f"{field}.load_all_documents",
            hint="Omit the key or pass true; keyed mode needs a live capture.",
        )
    external_writer = step.get("external_writer")
    if external_writer is not None and not isinstance(external_writer, bool):
        return BuilderValidationError(
            f"{field}.external_writer must be a boolean when provided.",
            error_code="PROCESS_DOCCACHE_RETRIEVE_CONFIG_INVALID",
            field=f"{field}.external_writer",
            hint=(
                "true declares the cache is populated outside this process "
                "(e.g. by the parent execution) — the #123 lineage check "
                "then accepts the read without an in-process writer."
            ),
        )
    transform: Dict[str, Any] = {
        "mode": "doccacheretrieve",
        "document_cache_id": step.get("document_cache_id"),
    }
    if "empty_cache_behavior" in step:
        transform["empty_cache_behavior"] = step.get("empty_cache_behavior")
    label = step.get("label")
    if label is not None:
        transform["label"] = label
    return _validate_transform(transform)


def _validate_sequence_decision_step(
    step: Dict[str, Any], field: str
) -> Optional[BuilderValidationError]:
    """Validate a ``decision`` flow-sequence step (issue #117).

    Reuses the legacy comparison enum + operand validators (same
    PROCESS_DECISION_CONFIG_INVALID body contract). The TRUE leg (``true_steps``,
    may be empty -> continue to the top-level target) and FALSE leg
    (``false_steps``, required non-empty so the reject path is never a bare Stop —
    keeps the verifier CONTROL_BRANCH_BARE_STOP-clean) each allow a nested branch or
    exception as their LAST step. Sequence decisions have no false_next loop in v1.
    """
    comparison = step.get("comparison")
    if not isinstance(comparison, str) or comparison.strip() not in _DECISION_COMPARISONS:
        return BuilderValidationError(
            f"{field}.comparison must be one of {sorted(_DECISION_COMPARISONS)}.",
            error_code="PROCESS_DECISION_CONFIG_INVALID",
            field=f"{field}.comparison",
            hint="Boomi Decision operators: equals / greaterthaneq / lessthaneq / "
            "greaterthan / lessthan / regex / wildcard.",
        )
    for side in ("left", "right"):
        operand_err = _validate_decision_operand(step.get(side), f"{field}.{side}")
        if operand_err is not None:
            return operand_err
    true_err = _validate_flow_sequence_steps(
        step.get("true_steps") if step.get("true_steps") is not None else [],
        f"{field}.true_steps",
        allowed_terminal_controls=frozenset({"branch"}) | _FLOW_SEQUENCE_TERMINAL_KINDS,
        allow_empty=True,
    )
    if true_err is not None:
        return true_err
    true_steps = step.get("true_steps") or []
    true_last_kind = (
        str(true_steps[-1].get("kind") or "").strip()
        if isinstance(true_steps, list)
        and true_steps
        and isinstance(true_steps[-1], dict)
        else ""
    )
    if true_last_kind in ("cache_put", "doccacheload"):
        # Companion review P1: the TRUE leg falls through to the top-level
        # target, which would starve after an Add to Cache. (The FALSE leg
        # falls through to a Stop, so a trailing cache write is harmless there.)
        return BuilderValidationError(
            f"{field}.true_steps must not end in a {true_last_kind} — the leg "
            "falls through to the target, which would receive an empty stream.",
            error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
            field=f"{field}.true_steps[{len(true_steps) - 1}].kind",
            hint="Follow it with cache_get, or stage in a target-less branch leg.",
        )
    return _validate_flow_sequence_steps(
        step.get("false_steps"),
        f"{field}.false_steps",
        allowed_terminal_controls=frozenset({"branch"}) | _FLOW_SEQUENCE_TERMINAL_KINDS,
        allow_empty=False,
    )


def _validate_sequence_branch_step(
    step: Dict[str, Any], field: str
) -> Optional[BuilderValidationError]:
    """Validate a ``branch`` flow-sequence step (issue #117).

    ``legs`` is a 2..25-length list; each leg is ``{steps?: [...linear...],
    target: {REST binding}}``. The leg target reuses ``_validate_target_binding``
    (PROCESS_CONNECTOR_BINDING_INVALID body contract); leg steps are linear-only.
    """
    legs = step.get("legs")
    if not isinstance(legs, list) or len(legs) < 2:
        return BuilderValidationError(
            f"{field}.legs must be a list of at least 2 branch legs.",
            error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
            field=f"{field}.legs",
            hint="A Branch needs 2..25 legs; each leg is {steps?: [...], target: {REST binding}}.",
        )
    if len(legs) > _BRANCH_MAX_LEGS:
        return BuilderValidationError(
            f"{field}.legs supports 2..{_BRANCH_MAX_LEGS} legs; got {len(legs)}.",
            error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
            field=f"{field}.legs",
            hint=f"Boomi Branch supports up to {_BRANCH_MAX_LEGS} paths.",
        )
    for i, leg in enumerate(legs):
        leg_field = f"{field}.legs[{i}]"
        if not isinstance(leg, dict):
            return BuilderValidationError(
                f"{leg_field} must be a JSON object.",
                error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
                field=leg_field,
                hint="Each leg is {steps?: [...], target: {REST binding}}.",
            )
        extra = set(leg) - _FLOW_SEQUENCE_BRANCH_LEG_KEYS
        if extra:
            return BuilderValidationError(
                f"{leg_field} has unsupported key(s): {sorted(extra)}.",
                error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
                field=leg_field,
                hint=f"Allowed leg keys: {sorted(_FLOW_SEQUENCE_BRANCH_LEG_KEYS)}.",
            )
        leg_target = leg.get("target")
        leg_steps_list = leg.get("steps") or []
        leg_last_kind = (
            str(leg_steps_list[-1].get("kind") or "").strip()
            if isinstance(leg_steps_list, list)
            and leg_steps_list
            and isinstance(leg_steps_list[-1], dict)
            else ""
        )
        if leg_last_kind in ("cache_put", "doccacheload"):
            # Companion review P1: a staging leg ends AT the Add to Cache
            # (the live-captured terminal pattern) — a leg target after it
            # would receive an empty stream, so it must be omitted. Applies
            # to the legacy doccacheload kind too (same emitted shape).
            if leg_target is not None:
                return BuilderValidationError(
                    f"{leg_field}.target must be omitted when the leg ends in "
                    f"a {leg_last_kind} — Add to Cache consumes the documents, "
                    "so a leg target after it would receive an empty stream.",
                    error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
                    field=f"{leg_field}.target",
                    hint=(
                        "Drop the target (the staging leg terminates at the "
                        "cache write), or follow the cache_put with cache_get."
                    ),
                )
        else:
            if isinstance(leg_target, dict) and leg_target.get("dynamic_path") is not None:
                return BuilderValidationError(
                    f"{leg_field}.target.dynamic_path is not supported in a flow_sequence "
                    "branch leg in v1.",
                    error_code="PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
                    field=f"{leg_field}.target.dynamic_path",
                    hint="Branch legs are plain REST targets in v1; remove the per-leg dynamic_path.",
                )
            target_err = _validate_target_binding(leg_target, field_prefix=f"{leg_field}.target")
            if target_err is not None:
                return target_err
        steps_err = _validate_flow_sequence_steps(
            leg.get("steps") if leg.get("steps") is not None else [],
            f"{leg_field}.steps",
            allowed_terminal_controls=frozenset(),
            allow_empty=True,
        )
        if steps_err is not None:
            return steps_err
    return None


def _validate_sequence_exception_step(
    step: Dict[str, Any], field: str
) -> Optional[BuilderValidationError]:
    """Validate an ``exception`` flow-sequence terminal step (issue #117).

    Reuses ``_validate_catch_exception`` (same keys: title / message_template /
    stop_single_document / parameter_source) so the throw contract is identical to
    the Try/Catch catch-leg exception — PROCESS_EXCEPTION_CONFIG_INVALID body code.
    """
    return _validate_catch_exception({k: v for k, v in step.items() if k != "kind"})


def _sequence_is_linear_only(steps: Any) -> bool:
    """True when every step kind in ``steps`` is a linear (non-control/terminal) kind."""
    if not isinstance(steps, list):
        return False
    for step in steps:
        kind = str((step.get("kind") if isinstance(step, dict) else "") or "").strip()
        if kind not in _FLOW_SEQUENCE_LINEAR_KINDS:
            return False
    return True


# --- composed emission --------------------------------------------------------


def _seq_step_to_flow_entry(step: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """Map a LINEAR flow-sequence step to an ``(emit_kind, params)`` flow entry.

    ``emit_kind`` is the shape kind the emitter dispatches on (``flowcontrol`` /
    ``message`` / ``map`` / ``dataprocess`` / ``doccacheload`` / ``doccacheretrieve``
    / ``doccacheremove``); ``params`` matches the keys the corresponding emitter
    reads, so each composed shape reuses its byte-accurate single-shape emitter.
    """
    kind = str(step.get("kind") or "").strip()
    label = str(step.get("label") or "")
    if kind == "flow_control":
        return ("flowcontrol", {"for_each_count": step.get("for_each_count"), "userlabel": label})
    if kind == "message":
        return ("message", {"text": str(step.get("message_text") or ""), "userlabel": label})
    if kind == "map_ref":
        return ("map", {"map_id": str(step.get("map_ref") or "").strip(), "userlabel": label})
    if kind == "dataprocess":
        return ("dataprocess", {"steps": step.get("steps") or [], "userlabel": label})
    if kind == "doccacheload":
        return (
            "doccacheload",
            {"document_cache_id": str(step.get("document_cache_id") or "").strip(), "userlabel": label},
        )
    if kind == "doccacheretrieve":
        return (
            "doccacheretrieve",
            {
                "document_cache_id": str(step.get("document_cache_id") or "").strip(),
                "empty_cache_behavior": str(
                    step.get("empty_cache_behavior") or _DOCCACHE_RETRIEVE_DEFAULT_EMPTY_BEHAVIOR
                ).strip(),
                "load_all_documents": step.get("load_all_documents", True),
                "userlabel": label,
            },
        )
    if kind == "doccacheremove":
        return (
            "doccacheremove",
            {
                "document_cache_id": str(step.get("document_cache_id") or "").strip(),
                "remove_all_documents": step.get("remove_all_documents", True),
                "userlabel": label,
            },
        )
    if kind == "cache_put":
        # Issue #122 M11.3: authored alias over the byte-locked Add to Cache
        # (doccacheload) emitter.
        return (
            "doccacheload",
            {"document_cache_id": str(step.get("document_cache_id") or "").strip(), "userlabel": label},
        )
    if kind == "cache_get":
        # Issue #122 M11.3: authored alias over the all-document retrieve.
        return (
            "doccacheretrieve",
            {
                "document_cache_id": str(step.get("document_cache_id") or "").strip(),
                "empty_cache_behavior": str(
                    step.get("empty_cache_behavior") or _DOCCACHE_RETRIEVE_DEFAULT_EMPTY_BEHAVIOR
                ).strip(),
                "load_all_documents": True,
                "userlabel": label,
            },
        )
    if kind in ("set_ddp", "set_dpp"):
        # Issue #121 M11.2: generic DDP/DPP Set Properties step.
        return (
            "setproperties_step",
            {
                "scope": "ddp" if kind == "set_ddp" else "dpp",
                "name": str(step.get("name") or "").strip(),
                "source_values": step.get("source_values") or [],
                "persist": bool(step.get("persist", False)),
                "userlabel": label,
            },
        )
    raise BuilderValidationError(  # pragma: no cover — defensive
        f"Unknown linear flow_sequence step kind {kind!r}.",
        error_code="PROCESS_XML_VALIDATION_FAILED",
        field="flow_sequence",
        hint="Internal builder bug — please report.",
    )


def _seq_exception_params(step: Dict[str, Any]) -> Dict[str, Any]:
    """Map an ``exception`` step to the params ``_emit_exception`` reads."""
    return {
        "title": step.get("title"),
        "message_template": step.get("message_template"),
        "stop_single_document": step.get("stop_single_document", False),
        "parameter_source": step.get("parameter_source"),
    }


def _source_prefix_flow_entries(config: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    """Build the shared ``start -> source`` prefix flow entries (issue #117).

    Reuses build()'s source canonicalization (DB source for the base protocol;
    REST source handled defensively for symmetry) minus dynamic_path — a
    flow_sequence rejects a source dynamic_path, so the source connector is plain.
    """
    source = config.get("source") or {}
    source_canonical_type = _canonical_connector_type(source.get("connector_type"))
    source_action_raw = str(source.get("action_type") or "").strip()
    source_is_rest = _resolve_rest_connector_type(source.get("connector_type")) is not None
    if source_is_rest:
        source_connector_type = source_canonical_type
        source_action_type = source_action_raw.upper()
    else:
        source_connector_type = source_canonical_type.lower()
        source_action_type = source_action_raw
    return [
        ("start_noaction", {}),
        (
            "connectoraction_source",
            {
                "connector_type": source_connector_type,
                "action_type": source_action_type,
                "connection_id": str(source.get("connection_id") or "").strip(),
                "operation_id": str(source.get("operation_id") or "").strip(),
                "userlabel": str(source.get("label") or ""),
                "dynamic_path": None,
            },
        ),
    ]


def _target_terminal_entries(config: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    """The default success terminal flow entries: ``target -> Stop`` (or a single
    ``Return Documents`` terminal when return_documents.enabled on a linear sequence)."""
    rd = config.get("return_documents")
    if isinstance(rd, dict) and rd.get("enabled") is True:
        return [("returndocuments", {"label": str(rd.get("label") or "")})]
    target = config.get("target") or {}
    return [
        (
            "connectoraction_target",
            {
                "connector_type": _canonical_connector_type(target.get("connector_type")),
                "action_type": str(target.get("action_type") or "").strip().upper(),
                "connection_id": str(target.get("connection_id") or "").strip(),
                "operation_id": str(target.get("operation_id") or "").strip(),
                "userlabel": str(target.get("label") or ""),
                "dynamic_path": None,
            },
        ),
        ("stop", {"continue_": True}),
    ]


def _emit_seq_linear(
    emit_kind: str,
    params: Dict[str, Any],
    shape_name: str,
    next_name: Optional[str],
    shape_index: int,
) -> str:
    """Emit one LINEAR composed-sequence shape forwarding to ``next_name``.

    ``doccacheload`` is emitted via ``_emit_doccacheload`` directly (it is not a
    ``_emit_flow_shape`` dispatch kind — it ships as a catch-leg terminal today),
    placing it on the main row (``_SHAPE_Y``). Every other linear kind goes through
    the existing ``_emit_flow_shape`` dispatch.
    """
    if emit_kind == "doccacheload":
        return _emit_doccacheload(
            shape_name,
            str(params.get("document_cache_id") or "").strip(),
            shape_index,
            next_name=next_name,
            y=_SHAPE_Y,
            dragpoint_y=_DRAGPOINT_Y,
            userlabel=str(params.get("userlabel") or ""),
        )
    return _emit_flow_shape(emit_kind, params, shape_name, next_name, shape_index)


def _append_linear_entries(
    parts: List[str], entries: List[Tuple[str, Dict[str, Any]]], start_index: int
) -> int:
    """Emit ``entries`` as a forward chain (the last entry terminal). Returns next index."""
    idx = start_index
    m = len(entries)
    for j, (kind, params) in enumerate(entries):
        name = f"shape{idx}"
        nxt = None if j == m - 1 else f"shape{idx + 1}"
        parts.append(_emit_flow_shape(kind, params, name, nxt, idx))
        idx += 1
    return idx


def _append_path(
    parts: List[str],
    steps: List[Dict[str, Any]],
    start_index: int,
    *,
    fallthrough: List[Tuple[str, Dict[str, Any]]],
    config: Dict[str, Any],
) -> int:
    """Emit one path = linear prefix + a terminal, starting at ``start_index``.

    The terminal is the last step when it is a control (decision/branch) or terminal
    (exception) kind, otherwise the ``fallthrough`` entries (a linear continuation
    ending in Stop / Return Documents). The shape before the terminal forwards into
    the terminal's first shape. Returns the next free shape index. Index allocation
    is depth-first and matches the legacy branch/decision emitters' positional walk.
    """
    terminal_step: Optional[Dict[str, Any]] = None
    linear_prefix: List[Dict[str, Any]] = steps
    if steps:
        last_kind = str((steps[-1].get("kind") if isinstance(steps[-1], dict) else "") or "").strip()
        if last_kind in (_FLOW_SEQUENCE_CONTROL_KINDS | _FLOW_SEQUENCE_TERMINAL_KINDS):
            linear_prefix = steps[:-1]
            terminal_step = steps[-1]

    idx = start_index
    has_continuation = terminal_step is not None or bool(fallthrough)
    for j, step in enumerate(linear_prefix):
        emit_kind, params = _seq_step_to_flow_entry(step)
        name = f"shape{idx}"
        # Last linear shape forwards into the first terminal/fallthrough shape;
        # with NO continuation (a target-less staging leg, companion review P1)
        # it is emitted terminal — the live doccacheload pattern.
        is_last = j == len(linear_prefix) - 1
        nxt = None if (is_last and not has_continuation) else f"shape{idx + 1}"
        parts.append(_emit_seq_linear(emit_kind, params, name, nxt, idx))
        idx += 1

    if terminal_step is None:
        return _append_linear_entries(parts, fallthrough, idx)
    kind = str(terminal_step.get("kind") or "").strip()
    if kind == "exception":
        parts.append(_emit_exception(f"shape{idx}", _seq_exception_params(terminal_step), idx, y=_SHAPE_Y))
        return idx + 1
    if kind == "decision":
        return _append_decision(parts, terminal_step, idx, config=config)
    if kind == "branch":
        return _append_branch(parts, terminal_step, idx, config=config)
    raise BuilderValidationError(  # pragma: no cover — kind already validated
        f"Unknown terminal flow_sequence step kind {kind!r}.",
        error_code="PROCESS_XML_VALIDATION_FAILED",
        field="flow_sequence",
        hint="Internal builder bug — please report.",
    )


def _append_decision(
    parts: List[str],
    decision_step: Dict[str, Any],
    decision_index: int,
    *,
    config: Dict[str, Any],
) -> int:
    """Emit a composed Decision + its true/false legs (issue #117).

    The TRUE leg falls through to the top-level success terminal (target -> Stop /
    Return Documents); the FALSE (reject) leg falls through to its own Stop. Either
    leg may itself end in a nested branch or exception. Emits ``decision -> true leg
    -> false leg`` (the same shape order the legacy ``_emit_decision_shapes`` uses).
    """
    success = _target_terminal_entries(config)
    true_start = decision_index + 1
    true_parts: List[str] = []
    false_start = _append_path(
        true_parts,
        decision_step.get("true_steps") or [],
        true_start,
        fallthrough=success,
        config=config,
    )
    false_parts: List[str] = []
    end_index = _append_path(
        false_parts,
        decision_step.get("false_steps") or [],
        false_start,
        fallthrough=[("stop", {"continue_": True})],
        config=config,
    )
    parts.append(
        _emit_decision(
            f"shape{decision_index}",
            decision_step,
            f"shape{true_start}",
            f"shape{false_start}",
            decision_index,
        )
    )
    parts.extend(true_parts)
    parts.extend(false_parts)
    return end_index


def _append_branch(
    parts: List[str],
    branch_step: Dict[str, Any],
    branch_index: int,
    *,
    config: Dict[str, Any],
) -> int:
    """Emit a composed Branch + N independent legs (issue #117).

    Each leg is its own linear sub-flow (``leg.steps``) ending in the leg's own
    ``target -> Stop`` — forward-only, no join/merge (the legacy fan-out contract).
    Emits ``branch -> leg1 -> leg2 -> ...`` (the same shape order as
    ``_emit_branch_shapes``).
    """
    legs = branch_step.get("legs") or []
    leg_first_names: List[str] = []
    leg_parts: List[str] = []
    cur = branch_index + 1
    for leg in legs:
        leg_first_names.append(f"shape{cur}")
        if leg.get("target") is None:
            # Companion review P1: a staging leg (ends in cache_put) has no
            # target — the path terminates at the cache write.
            fallthrough: List[Tuple[str, Dict[str, Any]]] = []
        else:
            fallthrough = [
                ("connectoraction_target", _branch_target_params(leg.get("target") or {})),
                ("stop", {"continue_": True}),
            ]
        cur = _append_path(
            leg_parts, leg.get("steps") or [], cur, fallthrough=fallthrough, config=config
        )
    parts.append(
        _emit_branch(
            f"shape{branch_index}",
            leg_first_names,
            branch_index,
            userlabel=str(branch_step.get("label") or ""),
        )
    )
    parts.extend(leg_parts)
    return cur


def _emit_composed_flow_shapes(
    prefix: List[Tuple[str, Dict[str, Any]]],
    steps: List[Dict[str, Any]],
    config: Dict[str, Any],
) -> List[str]:
    """Emit ``start -> source -> <composed flow_sequence>`` (issue #117).

    The prefix shapes (start, source) chain forward; the last prefix shape forwards
    to the first sequence shape (index ``len(prefix)+1``). The sequence is laid out
    by ``_append_path`` with the top-level success terminal as its fallthrough.
    """
    parts: List[str] = []
    p = len(prefix)
    for i, (kind, params) in enumerate(prefix):
        idx = i + 1
        parts.append(_emit_flow_shape(kind, params, f"shape{idx}", f"shape{idx + 1}", idx))
    _append_path(parts, steps, p + 1, fallthrough=_target_terminal_entries(config), config=config)
    return parts


def _build_composed_process_flow(
    config: Dict[str, Any], *, name: str, folder_name: Optional[str] = None
) -> str:
    """Emit the full Component XML for a composed ``flow_sequence`` process (issue #117).

    Re-runs the structural validator for totality on a validate_config bypass
    (mirrors how build() funnels through the legacy validators), reuses the same
    process_extensions / envelope assembly as the single-shape path.
    """
    seq_err = _validate_flow_sequence_config(config)
    if seq_err is not None:
        raise seq_err
    lineage_err = validate_config_lineage(config)
    if lineage_err is not None:
        raise lineage_err
    prefix = _source_prefix_flow_entries(config)
    shape_xml_parts = _emit_composed_flow_shapes(prefix, config.get("flow_sequence") or [], config)
    process_overrides_xml = ""
    connections = _extract_process_extension_connections(config)
    if connections:
        process_overrides_xml = _emit_process_overrides(connections)
    return _assemble_process_component_xml(
        shape_xml_parts,
        name=name,
        description=str(config.get("description") or ""),
        folder_name=folder_name,
        process_overrides_xml=process_overrides_xml,
    )


def _emit_try_catch_shapes(
    flow: List[Tuple[str, Dict[str, Any]]],
    dlq: Dict[str, Any],
    retry_count: int = 0,
    catch_notify: Optional[Dict[str, Any]] = None,
    catch_exception: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """Wrap the linear flow in a verified catcherrors Try/Catch + DLQ catch leg.

    Layout (shape names are positional, like the linear path):
        shape1  start        -> shape2 (catcherrors)
        shape2  catcherrors  Try(default) -> shape3 ; Catch(error) -> catch leg
        shape3..K  source -> [transform] -> target -> stop  (the normal chain)
        shape{K+1}.. catch leg

    Without ``catch_notify`` the catch leg is a single terminal
    doccacheload|processcall (byte-for-byte the issue #51/#88 output). With
    ``catch_notify`` (issue #89) the catch leg becomes
    ``notify -> dlq route -> catch stop``: the catcherrors Catch dragpoint
    targets the Notify, the Notify routes to the DLQ shape, and the DLQ shape
    routes to a catch-row Stop. ``retry_count`` is a validated 0..5 value; for
    counts > 0 the platform applies its built-in wait schedule before each
    retry, then routes the failed documents down the catch leg on exhaust
    (issue #88).
    """
    normal = flow[1:]  # source, [transform], target, stop
    n = len(normal)
    catcherrors_index = 2
    catcherrors_name = f"shape{catcherrors_index}"
    first_try_index = 3
    first_try_name = f"shape{first_try_index}"
    stop_index = catcherrors_index + n  # last normal (Try-path stop) shape index

    # Catch leg shapes follow the Try chain (indices stop_index+1..). The catch
    # target name (catcherrors Catch dragpoint) is the leg's first shape.
    mode = str(dlq.get("mode") or "").strip().lower()
    catch_parts, catch_target_name, _next = _emit_catch_leg(
        stop_index + 1, dlq, mode, catch_notify, catch_exception
    )

    parts: List[str] = []
    # Start keeps its noaction config; only its outgoing edge moves to catcherrors.
    parts.append(_emit_start_noaction("shape1", catcherrors_name, 1))
    parts.append(
        _emit_catcherrors(
            catcherrors_name, first_try_name, catch_target_name, catcherrors_index, retry_count
        )
    )
    # Normal Try chain, shifted to indices 3..stop_index.
    for j, (kind, params) in enumerate(normal):
        shape_index = first_try_index + j
        shape_name = f"shape{shape_index}"
        is_last = j == n - 1
        next_name = None if is_last else f"shape{shape_index + 1}"
        parts.append(_emit_flow_shape(kind, params, shape_name, next_name, shape_index))
    parts.extend(catch_parts)
    return parts


def _emit_catch_leg(
    start_index: int,
    dlq: Dict[str, Any],
    mode: str,
    catch_notify: Optional[Dict[str, Any]],
    catch_exception: Optional[Dict[str, Any]] = None,
) -> Tuple[List[str], str, int]:
    """Emit one Try/Catch catch leg and return ``(parts, first_name, next_index)``.

    The catch leg is ``[notify ->] [dlq route ->] terminal``:

      * ``catch_notify`` (issue #89) prepends a Notify at the HEAD of the leg;
      * a supported ``mode`` (``_TRY_CATCH_DLQ_MODES``) emits the DLQ route
        (``doccacheload`` / ``processcall``);
      * ``catch_exception`` (issue #108 M10.4) makes the leg END in a deliberate
        Exception throw instead of a catch-row Stop; without it the pre-#108
        notify path ends in a catch-row Stop and the bare DLQ-only leg is itself
        terminal.

    Supported compositions (all preserve the pre-#108 output byte-for-byte when
    ``catch_exception`` is absent)::

        dlq                          (pre-#89 — dlq route is terminal)
        notify -> dlq -> stop        (pre-#108, issue #89)
        exception                    (#108 — bare throw, no DLQ, no notify)
        notify -> exception          (#108)
        dlq -> exception             (#108)
        notify -> dlq -> exception   (#108)

    ``first_name`` is the shape the catcherrors Catch dragpoint targets;
    ``next_index`` is the first free shape index after this leg (so a second leg —
    issue #99 G1 connector scope — lays out without colliding).

    Bindings are normally validated by ``_validate_dlq_binding`` /
    ``_validate_catch_notify`` / ``_validate_catch_exception``; ids are literals or
    ``$ref:KEY`` already resolved by integration_builder before ``build()``. This
    stays total on the validate_config-bypass path: it raises on a
    missing/invalid binding rather than emitting broken XML.
    """
    # A present-but-empty/invalid catch_notify/catch_exception still counts as
    # "intended" so the validate_config-bypass path rejects it consistently
    # (matches the validators, which treat only None as "absent").
    notify_present = catch_notify is not None
    exception_present = catch_exception is not None
    dlq_present = mode in _TRY_CATCH_DLQ_MODES
    if exception_present:
        # Totality on the validate_config-bypass path (mirrors the notify/DLQ
        # binding checks below): a malformed catch_exception that slipped past
        # validate_config — including a non-dict reaching here alongside a valid
        # DLQ mode — must raise PROCESS_EXCEPTION_CONFIG_INVALID rather than emit
        # broken XML (empty <exMessage>, a bound parameter without {1}) or
        # AttributeError in _emit_exception. _should_emit_try_catch only requires
        # catch_exception to be a dict, not a VALID one, so re-validate here.
        exception_err = _validate_catch_exception(catch_exception)
        if exception_err is not None:
            raise exception_err
    if not dlq_present and not exception_present:
        # No DLQ route and no Exception terminal: the leg has no body. build()
        # never reaches here (_should_emit_try_catch requires a DLQ mode or a
        # catch_exception); only a validate_config-bypass call with an unsupported
        # mode and no catch_exception can. Raise rather than emit broken XML
        # (mirrors the pre-#108 unsupported-mode guard).
        raise BuilderValidationError(
            f"Unsupported DLQ mode {mode!r} reached the Try/Catch emitter without "
            "a catch_exception terminal.",
            error_code="PROCESS_XML_VALIDATION_FAILED",
            field="reliability.dlq.mode",
            hint="Set a supported dlq.mode or a reliability.catch_exception terminal.",
        )

    # Lay out the leg shapes in order, assigning positional indices. The trailing
    # terminal is an Exception throw (catch_exception) or — for the pre-#108 notify
    # path without an exception — a catch-row Stop; the bare DLQ-only leg has no
    # trailing terminal (the DLQ route is itself terminal, pre-#89).
    idx = start_index
    notify_index = notify_name = None
    if notify_present:
        notify_index = idx
        notify_name = f"shape{idx}"
        idx += 1
    dlq_index = dlq_name = None
    if dlq_present:
        dlq_index = idx
        dlq_name = f"shape{idx}"
        idx += 1
    terminal_kind: Optional[str] = None
    if exception_present:
        terminal_kind = "exception"
    elif notify_present:
        terminal_kind = "stop"
    terminal_index = terminal_name = None
    if terminal_kind is not None:
        terminal_index = idx
        terminal_name = f"shape{idx}"
        idx += 1
    next_index = idx

    # The catcherrors Catch dragpoint targets the first shape of the leg.
    if notify_present:
        first_name = notify_name
    elif dlq_present:
        first_name = dlq_name
    else:  # exception-only leg (bare throw)
        first_name = terminal_name

    parts: List[str] = []
    if notify_present:
        notify_err = _validate_catch_notify(
            catch_notify, mode, has_catch_exception=exception_present
        )
        if notify_err is not None:
            raise notify_err
        notify_next = dlq_name if dlq_present else terminal_name
        parts.append(_emit_notify(notify_name, catch_notify, notify_next, notify_index))
    if dlq_present:
        # The DLQ route points at the trailing terminal (Exception/Stop) when one
        # exists, else it is itself terminal (next_name=None).
        dlq_next_name = terminal_name
        if mode == "document_cache_ref":
            cache_id = str(dlq.get("document_cache_id") or "").strip()
            if not cache_id:
                raise BuilderValidationError(
                    "reliability.dlq.mode='document_cache_ref' requires a non-empty "
                    "document_cache_id to emit the DLQ catch leg.",
                    error_code="PROCESS_DLQ_BINDING_INVALID",
                    field="reliability.dlq.document_cache_id",
                    hint="Set document_cache_id to a literal id or a resolved $ref:KEY.",
                )
            parts.append(_emit_doccacheload(dlq_name, cache_id, dlq_index, next_name=dlq_next_name))
        else:  # error_subprocess_ref — the only other _TRY_CATCH_DLQ_MODES member
            process_id = str(dlq.get("process_id") or "").strip()
            if not process_id:
                raise BuilderValidationError(
                    "reliability.dlq.mode='error_subprocess_ref' requires a non-empty "
                    "process_id to emit the DLQ catch leg.",
                    error_code="PROCESS_DLQ_BINDING_INVALID",
                    field="reliability.dlq.process_id",
                    hint="Set process_id to a literal id or a resolved $ref:KEY.",
                )
            parts.append(_emit_processcall(dlq_name, process_id, dlq_index, next_name=dlq_next_name))
    if terminal_kind == "exception":
        parts.append(_emit_exception(terminal_name, catch_exception, terminal_index))
    elif terminal_kind == "stop":
        parts.append(_emit_stop(terminal_name, {"continue_": True}, y=_CATCH_SHAPE_Y))
    return parts, first_name, next_index


def _emit_connector_scoped_try_catch_shapes(
    flow: List[Tuple[str, Dict[str, Any]]],
    dlq: Dict[str, Any],
    retry_count: int = 0,
    catch_notify: Optional[Dict[str, Any]] = None,
    catch_exception: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """Connector-scoped Try/Catch: one Try/Catch per connector (issue #99 G1).

    The whole-process variant (:func:`_emit_try_catch_shapes`) wraps the entire
    ``source -> [transform] -> target`` chain in ONE catcherrors, so a target
    (REST) retry re-executes the source (DB) read on every attempt — live-proven
    a problem in #91 Scenario 2. This variant instead emits a SEPARATE Try/Catch
    around each connector:

      * the SOURCE connector in its own catcherrors with ``retryCount=0`` (a
        re-read on transient is rarely safe and the source read is not the
        retriable unit here);
      * the TARGET connector in its own catcherrors with the configured
        ``retryCount`` (the retriable send).

    Per the Boomi Try/Catch docs ("two Try/Catch steps separated by other steps
    — each behaves according to its own Failure Trigger setting"), the two
    catcherrors are SEPARATED by the source connector (and the optional
    transform), so each scopes its own failures independently: a target retry
    no longer re-runs the source read. Each catch leg routes to the same DLQ
    (and optional Notify) as the whole-process variant.

    Layout (``m`` = number of transform shapes, 0 or 1)::

        shape1  start            -> shape2
        shape2  catcherrors(src)  Try -> shape3 ; Catch -> src catch leg   (retry 0)
        shape3  source           -> shape4 (transform) | catcherrors(tgt)
        shape4  [transform]      -> catcherrors(tgt)                       (if m)
        shapeK  catcherrors(tgt)  Try -> target ; Catch -> tgt catch leg   (retry N)
        target                   -> stop
        stop
        <src catch leg> ; <tgt catch leg>
    """
    source = flow[1]            # ("connectoraction_source", {...})
    middle = flow[2:-2]         # [] | [map] | [map, setproperties] | [setproperties]
    target = flow[-2]           # ("connectoraction_target", {...})
    stop = flow[-1]             # ("stop", {...})

    # Issue #100 G2: a Set Properties (documentproperties) shape that builds the
    # per-document REST path must execute INSIDE the target Try/Catch retry unit
    # (so each retry re-applies it before re-sending), while the map/message
    # stays OUTSIDE as the separator between the two catcherrors. Split middle
    # accordingly. With no setproperties shape (inside == []), the indices and
    # output collapse to the pre-#100 connector-scoped layout byte-for-byte.
    outside = [s for s in middle if s[0] != "setproperties"]
    inside = [s for s in middle if s[0] == "setproperties"]
    o = len(outside)
    ins = len(inside)

    ce_src_index = 2
    source_index = 3
    first_outside_index = 4
    ce_tgt_index = 4 + o
    first_inside_index = ce_tgt_index + 1
    target_index = ce_tgt_index + ins + 1
    stop_index = target_index + 1

    ce_src_name = f"shape{ce_src_index}"
    source_name = f"shape{source_index}"
    ce_tgt_name = f"shape{ce_tgt_index}"
    target_name = f"shape{target_index}"
    stop_name = f"shape{stop_index}"

    mode = str(dlq.get("mode") or "").strip().lower()
    # Two catch legs laid out after the Try-row stop, source leg first. Both
    # route to the same DLQ + (optional) Notify config.
    src_leg, src_catch_first, after_src = _emit_catch_leg(
        stop_index + 1, dlq, mode, catch_notify, catch_exception
    )
    tgt_leg, tgt_catch_first, _after_tgt = _emit_catch_leg(
        after_src, dlq, mode, catch_notify, catch_exception
    )

    parts: List[str] = []
    parts.append(_emit_start_noaction("shape1", ce_src_name, 1))
    # Source Try/Catch (retry 0).
    parts.append(
        _emit_catcherrors(ce_src_name, source_name, src_catch_first, ce_src_index, 0)
    )
    # Source connector -> first outside (map) shape if any, else the target
    # Try/Catch.
    source_next_name = f"shape{first_outside_index}" if o else ce_tgt_name
    parts.append(
        _emit_flow_shape(source[0], source[1], source_name, source_next_name, source_index)
    )
    # Outside (map/message) separator shapes between the two catcherrors.
    for i, (kind, params) in enumerate(outside):
        idx = first_outside_index + i
        nxt = f"shape{idx + 1}" if i < o - 1 else ce_tgt_name
        parts.append(_emit_flow_shape(kind, params, f"shape{idx}", nxt, idx))
    # Target Try/Catch (configured retry); its Try branch enters the retry unit
    # at the first inside (setproperties) shape if any, else the target connector.
    target_try_first = f"shape{first_inside_index}" if ins else target_name
    parts.append(
        _emit_catcherrors(ce_tgt_name, target_try_first, tgt_catch_first, ce_tgt_index, retry_count)
    )
    # Inside (setproperties) shapes — part of the target retry unit, before target.
    for i, (kind, params) in enumerate(inside):
        idx = first_inside_index + i
        nxt = f"shape{idx + 1}" if i < ins - 1 else target_name
        parts.append(_emit_flow_shape(kind, params, f"shape{idx}", nxt, idx))
    parts.append(
        _emit_flow_shape(target[0], target[1], target_name, stop_name, target_index)
    )
    parts.append(_emit_flow_shape(stop[0], stop[1], stop_name, None, stop_index))
    parts.extend(src_leg)
    parts.extend(tgt_leg)
    return parts


def _emit_catcherrors(
    shape_name: str, try_to: str, catch_to: str, shape_index: int, retry_count: int = 0
) -> str:
    """Emit the verified catcherrors Try/Catch shape (catchAll, bounded retry).

    Dragpoints carry the verified identifier/text pair: Try=`default`,
    Catch=`error` (live component dff0bf83-... shape4). ``retry_count`` is a
    validated 0..5 value; for retry_count=0 the emitted XML and userlabel are
    byte-identical to the M3.R1a output (issue #88).
    """
    retry_label = "no retry" if retry_count == 0 else f"retry {retry_count}"
    dragpoints = (
        f'<dragpoint identifier="default" name="{shape_name}.dragpoint1" '
        f'text="Try" toShape="{_escape_xml(try_to)}" '
        f'x="{_dragpoint_x(shape_index)}" y="{_DRAGPOINT_Y}"/>'
        f'<dragpoint identifier="error" name="{shape_name}.dragpoint2" '
        f'text="Catch" toShape="{_escape_xml(catch_to)}" '
        f'x="{_dragpoint_x(shape_index)}" y="{_CATCH_DRAGPOINT_Y}"/>'
    )
    return (
        f'<shape image="catcherrors_icon" name="{shape_name}" '
        f'shapetype="catcherrors" '
        f'userlabel="Try/Catch all errors ({retry_label}) - route caught documents to the failure handler" '
        f'x="{_shape_x(shape_index)}" y="{_SHAPE_Y}">'
        f'<configuration><catcherrors catchAll="true" retryCount="{retry_count}"/></configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def _emit_notify(
    shape_name: str,
    catch_notify: Dict[str, Any],
    next_name: Optional[str],
    shape_index: int,
) -> str:
    """Emit the verified Notify catch-leg step (issue #89).

    Live shape: ``notify`` with ``disableEvent="true"`` (log-only — no platform
    email event, so email/SMS channels stay out of scope), ``enableUserLog`` /
    ``perExecution`` false, a ``<notifyMessage>`` using ``{N}`` placeholders, a
    ``<notifyMessageLevel>``, and a ``<notifyParameters>`` track binding (live
    ``work`` component 1139079f-... shape5, a notify on a catch path).

    Boomi binds runtime properties via numbered placeholders + a notify track
    parameter, not by embedding the property path in the message text. The
    validated ``message_template`` references the caught-error property by its
    token; here that token is substituted for the ``{1}`` placeholder and bound
    as the single track parameter, so the emitted Notify logs the real caught
    error at runtime.
    """
    level = str(catch_notify.get("level") or "").strip().upper()
    template = str(catch_notify.get("message_template") or "")
    # Boomi Notify message text uses Java-MessageFormat quoting: a single quote
    # is an escape char, so an unmatched apostrophe (e.g. "couldn't") would quote
    # the rest of the message and stop the {1} placeholder from expanding — the
    # caught error would silently not appear. Route through the shared
    # MessageFormat escaper (#102 C3) so Message and Notify share one quoting
    # source of truth, THEN insert the {1} placeholder for the caught-error token.
    message = _escape_message_format_text(template).replace(_NOTIFY_CAUGHT_ERROR_TOKEN, "{1}")
    dragpoints = _emit_dragpoints([next_name], shape_index, y=_CATCH_DRAGPOINT_Y)
    return (
        f'<shape image="notify_icon" name="{shape_name}" shapetype="notify" '
        f'userlabel="Notify caught error to the process log" '
        f'x="{_shape_x(shape_index)}" y="{_CATCH_SHAPE_Y}">'
        '<configuration>'
        '<notify disableEvent="true" enableUserLog="false" perExecution="false" '
        'title="Catch path notification">'
        f'<notifyMessage>{_escape_xml(message)}</notifyMessage>'
        f'<notifyMessageLevel>{_escape_xml(level)}</notifyMessageLevel>'
        '<notifyParameters>'
        '<parametervalue key="0" valueType="track">'
        f'<trackparameter defaultValue="" propertyId="{_escape_xml(_NOTIFY_CAUGHT_ERROR_TOKEN)}" '
        'propertyName="Base - Try/Catch Message"/>'
        '</parametervalue>'
        '</notifyParameters>'
        '</notify>'
        '</configuration>'
        f'<dragpoints>{dragpoints}</dragpoints>'
        '</shape>'
    )


def _emit_exception_parameters(parameter_source: str) -> str:
    """Emit the ``<exParameters>`` binding for the single ``{1}`` placeholder
    (issue #108 M10.4).

    ``none`` emits no element (a static message); ``current_document`` binds the
    current document (the live default); ``caught_error`` binds the platform
    caught-error message via the same track parameter the Notify catch step uses
    (``meta.base.catcherrorsmessage`` / "Base - Try/Catch Message"). The
    ``usesEncryption`` attribute is omitted to match the live captures — every
    live exception/notify ``parametervalue`` omits it (the companion doc's
    hand-written ``usesEncryption="false"`` is idealized).
    """
    if parameter_source == "none":
        return ""
    if parameter_source == "current_document":
        return '<exParameters><parametervalue key="0" valueType="current"/></exParameters>'
    # caught_error (default) — track binding to the caught Try/Catch error message.
    return (
        '<exParameters>'
        '<parametervalue key="0" valueType="track">'
        f'<trackparameter defaultValue="" propertyId="{_escape_xml(_NOTIFY_CAUGHT_ERROR_TOKEN)}" '
        'propertyName="Base - Try/Catch Message"/>'
        '</parametervalue>'
        '</exParameters>'
    )


def _emit_exception(
    shape_name: str,
    catch_exception: Dict[str, Any],
    shape_index: int,
    *,
    y: float = _CATCH_SHAPE_Y,
) -> str:
    """Emit a deliberate Exception (Throw) terminal on the Try/Catch catch leg
    (issue #108 M10.4) or a composed flow-sequence path (issue #117).

    ``y`` defaults to the catch-row y so the existing catch-leg call site stays
    byte-for-byte identical; the #117 composed sequencer passes ``y=_SHAPE_Y`` to
    place the Exception as a main-row path terminal.

    Byte-accurate to the live ``work``-account capture (component
    1139079f-fff5-434c-aedc-d2758cc20525 shape10 + the decision-terminal
    exceptions in b34d3812-...; see ``.codex/plans/issue-108-live-captures.md``):
    ``<shape image="exception_icon" ... shapetype="exception" userlabel="<title>">
    <configuration><exception stopProcessReturnSingleDoc="false"
    stopsingledoc="<bool>" title="<title>"><exMessage>...</exMessage>
    [<exParameters>...</exParameters>]</exception></configuration>
    <dragpoints/></shape>``.

    The Exception is TERMINAL — it ends the catch leg with a user-defined error
    (the Process Reporting alert + initial process-log message), so it carries an
    empty ``<dragpoints/>`` and no Stop follows it (the Boomi docs: a Stop is a
    *successful* conclusion; an error path uses an Exception instead). ``title``
    maps to BOTH the shape ``userlabel`` and the inner ``<exception title>``.
    ``stop_single_document`` -> ``stopsingledoc`` ("Continue executing other
    documents" = true vs "Stop executing the entire process" = false).
    ``stopProcessReturnSingleDoc="false"`` is always emitted (observed in every
    capture; runtime behavior unconfirmed) and not exposed.

    The message uses Java-MessageFormat ``{1}`` placeholder substitution (same as
    Message/Notify), so route the template through the shared MessageFormat escaper
    THEN ``_escape_xml`` — the ``{1}`` placeholder is preserved and the bound
    ``parameter_source`` value renders at runtime.
    """
    title_attr = _escape_xml(str(catch_exception.get("title") or ""))
    stop_single = str(bool(catch_exception.get("stop_single_document", False))).lower()
    template = str(catch_exception.get("message_template") or "")
    message = _escape_xml(_escape_message_format_text(template))
    source = str(catch_exception.get("parameter_source") or "caught_error").strip().lower()
    return (
        f'<shape image="exception_icon" name="{shape_name}" shapetype="exception" '
        f'userlabel="{title_attr}" x="{_shape_x(shape_index)}" y="{y}">'
        '<configuration>'
        f'<exception stopProcessReturnSingleDoc="false" stopsingledoc="{stop_single}" '
        f'title="{title_attr}">'
        f'<exMessage>{message}</exMessage>'
        f'{_emit_exception_parameters(source)}'
        '</exception>'
        '</configuration>'
        '<dragpoints/>'
        '</shape>'
    )


def _emit_doccacheload(
    shape_name: str,
    doc_cache_id: str,
    shape_index: int,
    next_name: Optional[str] = None,
    *,
    y: float = _CATCH_SHAPE_Y,
    dragpoint_y: float = _CATCH_DRAGPOINT_Y,
    userlabel: str = "Route caught errors to DLQ cache",
) -> str:
    """Emit the verified document-cache Add-to-Cache shape.

    Live shape: doccacheload with a docCache id (live component dff0bf83-...
    shape80). Terminal (empty dragpoints) by default; when ``next_name`` is set it
    routes forward (issue #89 catch-leg notify path → catch Stop; issue #117
    composed main-row Add-to-Cache → next shape). The ``y`` / ``dragpoint_y`` /
    ``userlabel`` keyword params default to the catch-leg values so every existing
    DLQ catch-leg call site stays byte-for-byte identical; the #117 composed
    sequencer overrides them for a main-row cache write (``y=_SHAPE_Y``,
    ``dragpoint_y=_DRAGPOINT_Y``, a step ``userlabel``).
    """
    dragpoints_xml = (
        f'<dragpoints>{_emit_dragpoints([next_name], shape_index, y=dragpoint_y)}</dragpoints>'
        if next_name
        else '<dragpoints/>'
    )
    return (
        f'<shape image="doccacheload_icon" name="{shape_name}" '
        f'shapetype="doccacheload" userlabel="{_escape_xml(userlabel)}" '
        f'x="{_shape_x(shape_index)}" y="{y}">'
        f'<configuration><doccacheload docCache="{_escape_xml(doc_cache_id)}"/></configuration>'
        f'{dragpoints_xml}'
        '</shape>'
    )


def _emit_processcall(
    shape_name: str,
    process_id: str,
    shape_index: int,
    next_name: Optional[str] = None,
    *,
    wait: bool = True,
    abort: bool = True,
    y: float = _CATCH_SHAPE_Y,
    dragpoint_y: float = _CATCH_DRAGPOINT_Y,
    userlabel: str = "Route caught errors to error subprocess",
) -> str:
    """Emit a verified ``processcall`` shape (two live-grounded call sites).

    * **DLQ catch leg** (``error_subprocess_ref``) — the issue #51 default:
      ``abort="true" wait="true"``, catch-row geometry, terminal empty
      dragpoints by default (live component 7b19baeb-... shape34). When
      ``next_name`` is set (issue #89 notify path) it routes to the catch Stop.
    * **Standalone main/try-flow Process Call** (issue #90 ``wrapper_subprocess``)
      — ``abort="false"`` so the parent continues past a child failure, main-flow
      geometry, and a forward dragpoint to the next shape. Boomi runs the child
      as a separate process and waits for it (``wait="true"``). Transcribed from
      the live ``work`` wrapper parent 6a432a0b-... (a processcall calling the
      main-logic subprocess 57a5822c-...): abort="false", wait="true",
      empty parameters/returnpaths.

    Defaults reproduce the catch-leg shape byte-for-byte; the standalone caller
    overrides ``abort``/``y``/``dragpoint_y``/``userlabel``.
    """
    wait_s = "true" if wait else "false"
    abort_s = "true" if abort else "false"
    dragpoints_xml = (
        f'<dragpoints>{_emit_dragpoints([next_name], shape_index, y=dragpoint_y)}</dragpoints>'
        if next_name
        else '<dragpoints/>'
    )
    return (
        f'<shape image="processcall_icon" name="{shape_name}" '
        f'shapetype="processcall" userlabel="{_escape_xml(userlabel)}" '
        f'x="{_shape_x(shape_index)}" y="{y}">'
        '<configuration>'
        f'<processcall abort="{abort_s}" processId="{_escape_xml(process_id)}" wait="{wait_s}">'
        '<parameters/><returnpaths/>'
        '</processcall>'
        '</configuration>'
        f'{dragpoints_xml}'
        '</shape>'
    )


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _canonical_connector_type(value: Optional[str]) -> str:
    """Resolve REST/SOAP aliases to their canonical subtype; pass others through.

    REST Client's three spellings (rest, rest_client, canonical) map to the
    canonical REST Client subtype; SOAP Client's spellings (soap_client,
    web_services_soap_client, wssoapclientsdk) map to `wssoapclientsdk` (#126).
    Database and any future connector types are emitted verbatim.
    """
    if not isinstance(value, str):
        return ""
    canonical = _resolve_rest_connector_type(value)
    if canonical is not None:
        return canonical
    soap_canonical = _resolve_soap_client_connector_type(value)
    if soap_canonical is not None:
        return soap_canonical
    return value.strip()


def _walk_scalars(value: Any, _path: Tuple[str, ...] = ()) -> Iterable[Tuple[Tuple[str, ...], Any]]:
    """Yield (path, scalar) pairs for every leaf in the value tree.

    Mirrors _resolve_dependency_tokens' traversal so secret/$ref scans
    cover the same surface area.
    """
    if isinstance(value, dict):
        for k, v in value.items():
            yield from _walk_scalars(v, _path + (str(k),))
    elif isinstance(value, list):
        for i, item in enumerate(value):
            yield from _walk_scalars(item, _path + (f"[{i}]",))
    else:
        yield _path, value


def _validate_ref_reachability(
    config: Any, declared: set
) -> Optional[BuilderValidationError]:
    """Every ``$ref:KEY`` token in the config tree must appear in ``declared``.

    Matches integration_builder's ``_resolve_dependency_tokens`` contract —
    apply-time substitution walks the same tree, so an undeclared or
    whitespace-padded ref would survive as a literal ``"$ref:KEY"`` string in
    the emitted XML. Shared by ProcessFlowBuilder and WrapperSubprocessBuilder.
    """
    for path, value in _walk_scalars(config):
        if not isinstance(value, str):
            continue
        stripped = value.strip()
        if not stripped.startswith("$ref:"):
            continue
        # Codex review r7 P2.2: a padded value like " $ref:foo " is not
        # recognized as a ref by _resolve_dependency_tokens (which requires
        # startswith at byte 0), but build()'s whitespace stripping then emits
        # the unresolved token directly. Reject the malformed shape here.
        if value != stripped:
            return BuilderValidationError(
                f"$ref token at {'.'.join(path)!r} has surrounding "
                f"whitespace ({value!r}); refs must be exact '$ref:KEY' strings.",
                error_code="MISSING_PROCESS_DEPENDENCY",
                field=".".join(path),
                hint=(
                    "Remove leading/trailing whitespace from the $ref:KEY value. "
                    "Apply-time substitution only matches refs that start at byte 0."
                ),
            )
        ref_key = value[5:]
        if ref_key not in declared:
            return BuilderValidationError(
                f"$ref:{ref_key} at {'.'.join(path)!r} is not declared in the "
                f"process component's depends_on.",
                error_code="MISSING_PROCESS_DEPENDENCY",
                field="depends_on",
                hint=(
                    f"Add {ref_key!r} to the process component's depends_on list "
                    "so $ref resolution can find it at apply time."
                ),
            )
    return None


def _validate_processcall_entry(
    call: Any, index: int
) -> Optional[BuilderValidationError]:
    """Per-entry well-formedness for a wrapper_subprocess process call (#90).

    Each entry targets a child via EXACTLY ONE of ``subprocess_ref`` (a
    ``$ref:KEY`` token resolved in-spec) or ``process_id`` (a literal/existing
    component id). Cross-component resolution checks (self-reference, in-spec
    presence, target type) run at the integration_builder plan layer.
    """
    field = f"process_calls[{index}]"
    if not isinstance(call, dict):
        return BuilderValidationError(
            f"{field} must be a JSON object with subprocess_ref or process_id.",
            error_code="PROCESS_REF_MISSING",
            field=field,
            hint="Each process call targets a child via subprocess_ref ('$ref:KEY') or process_id.",
        )
    sref = call.get("subprocess_ref")
    pid = call.get("process_id")
    has_sref = isinstance(sref, str) and sref.strip() != ""
    has_pid = isinstance(pid, str) and pid.strip() != ""
    if has_sref and has_pid:
        return BuilderValidationError(
            f"{field} sets both subprocess_ref and process_id; provide exactly one.",
            error_code="PROCESS_REF_AMBIGUOUS",
            field=field,
            hint="Use subprocess_ref ('$ref:KEY') for an in-spec child, or process_id for an existing component.",
        )
    if not has_sref and not has_pid:
        return BuilderValidationError(
            f"{field} requires subprocess_ref ('$ref:KEY') or process_id.",
            error_code="PROCESS_REF_MISSING",
            field=field,
            hint="Add subprocess_ref ('$ref:KEY') for an in-spec child, or process_id for an existing component.",
        )
    # Exact '$ref:KEY' token required (byte 0, no surrounding whitespace, non-empty
    # key) — a padded " $ref:KEY " is not resolved by _resolve_dependency_tokens
    # and would survive as a literal in the emitted XML; a literal id belongs in
    # process_id. An empty "$ref:" passes the startswith check but would emit
    # processId="$ref:" (build()'s non-empty guard sees a truthy "$ref:"), so the
    # empty-key case is rejected here. (Codex #90 review.)
    if has_sref:
        if not sref.startswith("$ref:"):
            return BuilderValidationError(
                f"{field}.subprocess_ref must be an exact '$ref:KEY' token (got {sref!r}); "
                "use process_id for a literal component id.",
                error_code="PROCESS_REF_MISSING",
                field=f"{field}.subprocess_ref",
                hint="subprocess_ref references an in-spec child by key (no surrounding whitespace); existing components use process_id.",
            )
        if not sref[len("$ref:"):]:
            return BuilderValidationError(
                f"{field}.subprocess_ref is an empty '$ref:' token; name an in-spec child component key.",
                error_code="PROCESS_REF_MISSING",
                field=f"{field}.subprocess_ref",
                hint="Use '$ref:<child_key>' naming a process component in the same spec.",
            )
    # process_id is for an EXISTING/literal component id — a $ref:KEY token here
    # would bypass the implicit child-first edge + cross-spec ref-type checks
    # (which only inspect subprocess_ref) yet still be substituted by
    # _resolve_dependency_tokens, risking an unresolved $ref or wrong ordering.
    # Direct the caller to subprocess_ref for in-spec children. (Codex #90 review.)
    if has_pid and pid.strip().startswith("$ref:"):
        return BuilderValidationError(
            f"{field}.process_id must be a literal component id, not a '$ref:' token; "
            "use subprocess_ref to reference an in-spec child.",
            error_code="PROCESS_CALL_CONFIG_INVALID",
            field=f"{field}.process_id",
            hint="subprocess_ref='$ref:KEY' references an in-spec child; process_id is for existing component ids.",
        )
    # wait / abort_on_error must be real JSON booleans when present — a string like
    # "false" would coerce to True via bool() at emit time and silently reverse the
    # requested behavior. (Codex #90 review.)
    for flag in ("wait", "abort_on_error"):
        if flag in call and not isinstance(call[flag], bool):
            return BuilderValidationError(
                f"{field}.{flag} must be a boolean (true/false); got {call[flag]!r}.",
                error_code="PROCESS_CALL_CONFIG_INVALID",
                field=f"{field}.{flag}",
                hint=f"Pass a JSON boolean for {flag}, not a string or number.",
            )
    return None


class WrapperSubprocessBuilder(ProcessFlowBuilder):
    """Thin wrapper-parent ("facade") process: start -> process call(s) -> stop.

    Issue #90 (M4.5.5). Emits a parent process whose only steps are standalone
    Process Call shapes invoking in-spec child processes (by ``subprocess_ref``
    = ``$ref:KEY``) or existing components (by ``process_id``). The standalone
    processcall shape is transcribed from the live ``work`` wrapper exemplar
    (component 6a432a0b-..., a processcall calling the main-logic subprocess
    57a5822c-...): ``abort="false"`` (the parent continues past a child failure),
    ``wait="true"``, empty parameters/returnpaths.

    Config shape::

        {"process_kind": "wrapper_subprocess",
         "process_calls": [
             {"subprocess_ref": "$ref:main_logic", "wait": true, "abort_on_error": false, "label": "..."},
             {"process_id": "<existing component id>"}
         ]}

    Inherits the plaintext-secret scan/redact from ProcessFlowBuilder; overrides
    validate_config (wrapper shape) and build (parent emission). Child-first apply
    ordering and ``$ref``->id substitution are handled by integration_builder's
    existing topo-sort + _resolve_dependency_tokens (an implicit parent->child
    dependency edge is synthesized at plan time); the cross-component ref checks
    (self-reference / in-spec presence / target type) run there too. Changing a
    child requires repackaging and redeploying the parent — the parent is the
    release boundary.
    """

    PROCESS_KIND = "wrapper_subprocess"

    @classmethod
    def validate_config(
        cls,
        config: Dict[str, Any],
        *,
        depends_on: Optional[Iterable[str]] = None,
    ) -> Optional[BuilderValidationError]:
        process_kind = str(
            config.get("process_kind") or config.get("process_type") or ""
        ).strip()
        if process_kind != cls.PROCESS_KIND:
            return BuilderValidationError(
                f"process_kind {process_kind!r} is not supported.",
                error_code="PROCESS_KIND_UNSUPPORTED",
                field="process_kind",
                hint=f"Use process_kind={cls.PROCESS_KIND!r} for the wrapper-parent (facade) structure.",
            )
        calls = config.get("process_calls")
        if not isinstance(calls, list) or not calls:
            return BuilderValidationError(
                "wrapper_subprocess requires a non-empty 'process_calls' list.",
                error_code="PROCESS_REF_MISSING",
                field="process_calls",
                hint="Each entry calls a child via subprocess_ref ('$ref:KEY') or process_id.",
            )
        for i, call in enumerate(calls):
            entry_err = _validate_processcall_entry(call, i)
            if entry_err is not None:
                return entry_err
        # Issue #99 G3: a wrapper may carry a process_extensions block — either
        # declared directly or HOISTED from a called child by integration_builder
        # (_synthesize_wrapper_subprocess_extensions) — so a wrapper-deployed
        # package surfaces the child connection override points through
        # get_extensions (the #90 facade + #92 env-ext composition; the override
        # points did NOT surface through a wrapper Process Call deployment before
        # this — #91 capstone gap G3). Validate the same shape ProcessFlowBuilder
        # enforces; build() lets it raise so both paths share one contract.
        try:
            _extract_process_extension_connections(config)
        except BuilderValidationError as exc:
            return exc
        # A process_extensions connection $ref must be reachable (in depends_on)
        # AND an exact '$ref:KEY' shape so apply-time substitution resolves it.
        # The synthesis pass adds in-spec hoisted/seeded connection refs to
        # depends_on; a hand-authored ref that is undeclared, whitespace-padded,
        # or an empty-key '$ref:' must fail cleanly here rather than leak an
        # unresolved token into the emitted processOverrides (the stripped
        # connection_id from _extract masks padding, and _resolve_dependency_tokens
        # only substitutes a value that starts with '$ref:' at byte 0 with a
        # non-empty key). Reuse the shared reachability helper, scoped to the
        # process_extensions subtree ONLY — the wrapper's subprocess_ref children
        # use edge synthesis (not depends_on), so the full-config walk cannot be
        # applied to a wrapper.
        pe_block = config.get("process_extensions")
        if pe_block is not None:
            reach_err = _validate_ref_reachability(
                {"process_extensions": pe_block}, set(depends_on or [])
            )
            if reach_err is not None:
                return reach_err
        # Reuse the plaintext-secret scan shared with the database_to_api_sync
        # builder. No depends_on-membership requirement here: a processcall
        # $ref:KEY child is wired by an implicit edge synthesized at plan time
        # (integration_builder._synthesize_wrapper_subprocess_edges), and the
        # cross-spec resolution checks (self-reference / not-found / type
        # mismatch) run there. The exact-$ref-token shape is enforced per entry
        # above. depends_on is accepted for interface parity but not required.
        #
        # Issue #107 M10.3: a wrapper that is itself a subprocess may end in a
        # Return Documents terminal instead of a Stop; validate the same shape
        # ProcessFlowBuilder enforces (build() shares _terminal_flow_entry).
        return_documents_err = _validate_return_documents(config.get("return_documents"))
        if return_documents_err is not None:
            return return_documents_err
        return cls.scan_forbidden_secret_fields(config)

    @classmethod
    def build(
        cls,
        config: Dict[str, Any],
        *,
        name: str,
        folder_name: Optional[str] = None,
    ) -> str:
        """Emit the thin parent: start -> processcall(s) -> stop.

        Assumes validate_config passed and that ``$ref`` tokens in process_calls
        have been substituted with real child component ids by the integration
        builder (so subprocess_ref now carries a resolved id).
        """
        description = str(config.get("description") or "")
        calls = config.get("process_calls") or []
        flow: List[Tuple[str, Dict[str, Any]]] = [("start_noaction", {})]
        for call in calls:
            # process_id (literal) or subprocess_ref (a $ref:KEY already resolved
            # to an id by integration_builder before build()).
            pid = str(call.get("process_id") or call.get("subprocess_ref") or "").strip()
            if not pid:
                # Stay total on the validate_config-bypass path: never emit
                # <processcall processId=""> — raise instead.
                raise BuilderValidationError(
                    "wrapper_subprocess process call is missing a resolved "
                    "target process id.",
                    error_code="PROCESS_REF_MISSING",
                    field="process_calls",
                    hint="Set subprocess_ref ('$ref:KEY') or process_id on each process call.",
                )
            flow.append((
                "processcall",
                {
                    "process_id": pid,
                    "wait": bool(call.get("wait", True)),
                    "abort": bool(call.get("abort_on_error", False)),
                    "userlabel": str(call.get("label") or ""),
                },
            ))
        # Issue #107 M10.3: a wrapper/facade that is itself a subprocess may end
        # in Return Documents to hand its documents back to the caller; default
        # stays a Stop (byte-for-byte the pre-#107 wrapper output).
        flow.append(_terminal_flow_entry(config))
        # Issue #99 G3: emit the hoisted/declared connection env-extension
        # override points so the wrapper-deployed package surfaces them through
        # get_extensions. Absent block -> empty <bns:processOverrides> (the
        # pre-#99 wrapper output is byte-for-byte unchanged). _extract stays
        # defensive so build() is total on a validate_config-bypass path.
        process_overrides_xml = ""
        connections = _extract_process_extension_connections(config)
        if connections:
            process_overrides_xml = _emit_process_overrides(connections)
        return _assemble_process_component_xml(
            _emit_linear_shapes(flow),
            name=name,
            description=description,
            folder_name=folder_name,
            process_overrides_xml=process_overrides_xml,
        )


# ----------------------------------------------------------------------
# Issue #70 M5.2 — sync_pipeline (verified-linear PipelineSpec lowering)
# ----------------------------------------------------------------------

# The PipelineStageKind values the verified-linear sync_pipeline builder lowers
# to XML. ``read`` (db_read DB source) and ``fetch`` (rest_fetch REST source) are
# the two supported source kinds; ``map`` the optional transform; ``send``
# (rest_send REST target) and ``write`` (db_write DB target, #74 M5.8) the two
# supported target kinds. Every other kind in the M5.1 (#69) vocabulary stays
# reserved — modeled in the contract, no PipelineSpec->XML lowering yet — and is
# rejected with a hint pointing at its owning issue (combine/lookup/control-flow
# owned by M10 (#103, e.g. Flow Control M10.7 #111)).
_SYNC_PIPELINE_SUPPORTED_KINDS = frozenset({"read", "fetch", "map", "send", "write"})

# The primitive discriminator each supported stage must declare in config. This
# is the PRIMARY (default) primitive per kind.
_SYNC_PIPELINE_STAGE_PRIMITIVE: Dict[str, str] = {
    "read": "db_read",
    "fetch": "rest_fetch",
    "map": "map",
    "send": "rest_send",
    "write": "db_write",
}

# #126: a fetch/send stage additionally accepts the SOAP Client primitive
# (soap_fetch / soap_send) as a thin adapter over the same source/target slot —
# the declared primitive selects the REST-vs-SOAP connector family.
_SYNC_PIPELINE_STAGE_ALT_PRIMITIVE: Dict[str, str] = {
    "fetch": "soap_fetch",
    "send": "soap_send",
}

# The SOAP primitive that lowers each fetch/send stage to a SOAP Client binding.
_SYNC_PIPELINE_SOAP_PRIMITIVES = frozenset({"soap_fetch", "soap_send"})

# Hints for reserved stage *kinds* (rejected by the kind gate).
_SYNC_PIPELINE_RESERVED_KIND_HINTS: Dict[str, str] = {
    "lookup": "The 'lookup' stage is reserved (modeled in M5.1 #69, no emitter yet).",
    "combine": "The 'combine' stage is reserved; combine/control-flow emitters are owned by M10 (issue #103).",
    "flow_control": "The 'flow_control' stage kind has no PipelineSpec lowering; the Flow Control shape is emittable via the process_config.flow_control block (M10.7, issue #111).",
    "branch": "The 'branch' stage kind has no PipelineSpec lowering; the Branch shape is owned by M10.8 (issue #112).",
    "decision": "The 'decision' stage kind has no PipelineSpec lowering; the Decision shape is emittable via the process_config.decision block (M10.9, issue #113).",
    "dataprocess": "The 'dataprocess' stage has no PipelineSpec lowering; the Data Process shape is owned by M10.2 (issue #106).",
    "exception": "The 'exception' stage has no PipelineSpec lowering; Exception/Throw is owned by M10.4 (issue #108).",
    "doccacheretrieve": "The 'doccacheretrieve' stage has no PipelineSpec lowering; Document Cache Retrieve is owned by M10.5 (issue #109).",
    "doccacheremove": "The 'doccacheremove' stage has no PipelineSpec lowering; Document Cache Remove is owned by M10.6 (issue #110).",
    "finalize": "The 'finalize' stage is reserved (no PipelineSpec lowering yet).",
}

# Hints for *primitives* mis-declared on the wrong supported kind. ``rest_fetch``
# is supported (on a ``fetch`` stage) but not on ``read``; ``db_write`` is the DB
# write target (#74, built on the #32 component builders) and belongs on a
# ``write`` stage, not a ``send`` (REST) stage. Each points the caller at the
# right stage/issue.
_SYNC_PIPELINE_RESERVED_PRIMITIVE_HINTS: Dict[str, str] = {
    "rest_fetch": "rest_fetch is the REST source primitive — declare it on a 'fetch' stage (M5.4 #72), not this one.",
    "db_write": "db_write is the DB 'write' target primitive — declare it on a 'write' stage (#74, #32 builders), not this one.",
}

# Config keys allowed on a read/send (connector-binding) stage. Anything else —
# e.g. a target.dynamic_path or a nested reliability block — is rejected so a
# gated sub-block can never be silently dropped into the lowered config.
_SYNC_PIPELINE_BINDING_KEYS = frozenset(
    {"primitive", "connector_type", "action_type", "connection_id", "operation_id", "label"}
)
# Config keys allowed on a map stage.
_SYNC_PIPELINE_MAP_KEYS = frozenset({"primitive", "map_ref", "map_id", "label"})

# Top-level config keys sync_pipeline accepts. The documented authoring surface
# (process_kind/pipeline/description/folder_name/process_extensions) PLUS the
# metadata the integration builder + update/safe-edit paths inject into the build
# payload (component_type/component_name/name). An unrecognized key — a misspelled
# block (``reliabilty``) or an unsupported setting (``execution``) — is rejected
# rather than silently dropped, so the verified-linear surface stays honest while
# framework-injected metadata is tolerated (the base process builders read only
# the keys they need and ignore these too). ``folder_id`` is deliberately NOT
# accepted: the process builder emits only ``folderName`` (never ``folderId``), so
# a create carrying folder_id would suppress FOLDER_REQUIRED_ON_CREATE yet still
# land in the account root. Placement goes through folder_name, which is emitted.
_SYNC_PIPELINE_ALLOWED_TOP_LEVEL = frozenset(
    {
        "process_kind",
        "process_type",
        "pipeline",
        "description",
        "folder_name",
        "process_extensions",
        "name",
        "component_type",
        "component_name",
    }
)
# Top-level blocks sync_pipeline does NOT lower. Rejected (not silently dropped)
# with a tailored hint because dropping them would change behavior. These take
# precedence over the generic unknown-key rejection so the caller gets the
# specific "use database_to_api_sync / wrapper_subprocess instead" guidance.
_SYNC_PIPELINE_GATED_TOP_LEVEL: Dict[str, str] = {
    "reliability": "Try/Catch retry + DLQ (reliability) is gated for sync_pipeline — it is verified-linear only (M5.2). Use process_kind='database_to_api_sync' for the reliability catch path.",
    "branch": "Branch fan-out is gated for sync_pipeline — it is verified-linear only (M5.2). The Branch shape is owned by M10.8 (issue #112).",
    "process_calls": "Process Call is gated for sync_pipeline. Use process_kind='wrapper_subprocess' for facade Process Calls.",
    "return_documents": "Return Documents is gated for sync_pipeline — it is verified-linear (db_read -> [map] -> rest_send -> stop) only (M5.2).",
    "source": "sync_pipeline takes a 'pipeline' stage graph, not a top-level 'source'. Express the DB source as a read stage (config.primitive='db_read').",
    "target": "sync_pipeline takes a 'pipeline' stage graph, not a top-level 'target'. Express the REST target as a send stage (config.primitive='rest_send').",
    "transform": "sync_pipeline takes a 'pipeline' stage graph, not a top-level 'transform'. Express the map as a map stage (config.primitive='map').",
}


class SyncPipelineBuilder(ProcessFlowBuilder):
    """Verified-linear ``process_kind="sync_pipeline"`` builder (issue #70, M5.2).

    Lowers an M5.1 :class:`PipelineSpec` (issue #69) stage graph into the proven
    ``database_to_api_sync`` ``source``/``transform``/``target`` config and
    delegates validation + XML emission to :class:`ProcessFlowBuilder`. It adds
    **no new shape emitter** — it only accepts the trivial all-``ordering``-edges
    linear case of the M10.0 (#104) typed-edge contract:

        ``read(db_read) -> [map] -> send(rest_send) -> stop``

    Everything else — non-``ordering`` edges, fan-out/fan-in, reserved stage
    kinds (fetch/write/lookup/combine/flow_control/branch/decision/dataprocess/
    exception/doccacheretrieve), and the gated reliability/branch/process_calls/
    return_documents blocks — is rejected at builder time with a structured error
    pointing at the owning issue. As of M5.3 (#71) the ``database_to_api_sync``
    archetype derives its linear process core through
    :meth:`lower_config` (this lowering path) while preserving its legacy process
    output — ``process_kind`` stays ``database_to_api_sync`` and the returned
    spec keeps ``pipeline=null``; it does NOT emit a ``sync_pipeline`` process
    directly, and no audit sink or reliability shell is injected into that legacy
    output.
    """

    PROCESS_KIND = "sync_pipeline"

    @classmethod
    def lower_config(cls, config: Dict[str, Any]) -> Dict[str, Any]:
        """Lower a sync_pipeline config to an equivalent database_to_api_sync config.

        Raises :class:`BuilderValidationError` on any structural defect so both
        ``validate_config`` (which returns it) and ``build`` (which lets it
        propagate) share one source of truth and stay total on a bypass path.
        """
        if not isinstance(config, dict):
            raise BuilderValidationError(
                "sync_pipeline config must be a JSON object.",
                error_code="SYNC_PIPELINE_CONFIG_INVALID",
                field="config",
                hint="Provide {process_kind: 'sync_pipeline', pipeline: {stages, dependencies}}.",
            )

        # Top-level key gate. A gated block gets its tailored hint; any other
        # unrecognized key (a typo or unsupported setting) is rejected too so it
        # is never silently dropped. Allow-listed keys — including the
        # component_type/component_name/name metadata the update + safe-edit build
        # payload injects — pass through (the base process builders ignore them).
        for key in config:
            gated_hint = _SYNC_PIPELINE_GATED_TOP_LEVEL.get(key)
            if gated_hint is not None:
                raise BuilderValidationError(
                    f"sync_pipeline does not support top-level config key {key!r}.",
                    error_code=(
                        "SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED"
                        if key in ("branch", "process_calls")
                        else "SYNC_PIPELINE_CONFIG_INVALID"
                    ),
                    field=key,
                    hint=gated_hint,
                )
            if key not in _SYNC_PIPELINE_ALLOWED_TOP_LEVEL:
                raise BuilderValidationError(
                    f"sync_pipeline does not support top-level config key {key!r}.",
                    error_code="SYNC_PIPELINE_CONFIG_INVALID",
                    field=key,
                    hint=(
                        "sync_pipeline is verified-linear (M5.2); supported top-level "
                        f"keys are {sorted(_SYNC_PIPELINE_ALLOWED_TOP_LEVEL)}. Express "
                        "all flow logic as pipeline.stages."
                    ),
                )

        raw_pipeline = config.get("pipeline")
        if not isinstance(raw_pipeline, dict):
            raise BuilderValidationError(
                "sync_pipeline requires a 'pipeline' object (a PipelineSpec stage graph).",
                error_code="SYNC_PIPELINE_CONFIG_INVALID",
                field="pipeline",
                hint="Shape: {stages: [{key, kind, config}], dependencies: [{from_stage, to_stage}]}.",
            )
        try:
            spec = PipelineSpec(**raw_pipeline)
        except (ValidationError, TypeError) as exc:
            raise BuilderValidationError(
                f"pipeline is not a valid PipelineSpec: {exc}",
                error_code="SYNC_PIPELINE_CONFIG_INVALID",
                field="pipeline",
                hint="See get_schema_template(resource_type='process', protocol='sync_pipeline').",
            )

        if not spec.stages:
            raise BuilderValidationError(
                "pipeline.stages must be a non-empty list.",
                error_code="SYNC_PIPELINE_CONFIG_INVALID",
                field="pipeline.stages",
                hint="A sync_pipeline is read(db_read) -> [map] -> send(rest_send).",
            )

        # 1. Every edge must be a plain 'ordering' edge (no branch/decision/loop).
        for edge in spec.dependencies:
            if edge.edge_kind != "ordering":
                raise BuilderValidationError(
                    f"sync_pipeline supports only 'ordering' edges; got "
                    f"edge_kind={edge.edge_kind!r} on "
                    f"{edge.from_stage!r}->{edge.to_stage!r}.",
                    error_code="SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED",
                    field="pipeline.dependencies",
                    hint="Branch/decision/loop edges are owned by M10 (issue #103).",
                )

        # 2. Every stage kind must be lowerable (read/map/send).
        stage_by_key: Dict[str, StageSpec] = {}
        for stage in spec.stages:
            if stage.kind not in _SYNC_PIPELINE_SUPPORTED_KINDS:
                raise BuilderValidationError(
                    f"sync_pipeline stage {stage.key!r} has unsupported kind "
                    f"{stage.kind!r}; M5.2 lowers only read/map/send.",
                    error_code="SYNC_PIPELINE_STAGE_UNSUPPORTED",
                    field=f"pipeline.stages[{stage.key}].kind",
                    hint=_SYNC_PIPELINE_RESERVED_KIND_HINTS.get(
                        stage.kind,
                        "Reserved stage kind (no PipelineSpec lowering in M5.2).",
                    ),
                )
            stage_by_key[stage.key] = stage

        # 3. Single linear chain over the ordering edges, covering every stage.
        # The source is exactly one of read(db_read) or fetch(rest_fetch); the
        # target is send(rest_send) OR — for the API-to-DB chain (#74 M5.8) —
        # write(db_write); an optional map sits between them. A db_write target is
        # only supported from a REST (fetch) source (the api_to_database_sync
        # preset); a read->write DB-to-DB chain has no archetype and stays out of
        # scope.
        order = cls._linear_stage_order(spec, stage_by_key)
        kinds = [stage_by_key[k].kind for k in order]
        if kinds not in (
            ["read", "send"],
            ["read", "map", "send"],
            ["fetch", "send"],
            ["fetch", "map", "send"],
            ["fetch", "write"],
            ["fetch", "map", "write"],
        ):
            raise BuilderValidationError(
                f"sync_pipeline must be read|fetch -> [map] -> send, or "
                f"fetch -> [map] -> write; got stage kinds {kinds}.",
                error_code="SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED",
                field="pipeline.stages",
                hint=(
                    "Exactly one db_read or rest_fetch source, an optional map, and "
                    "one target — a rest_send REST target, or (from a rest_fetch "
                    "source) a db_write database target."
                ),
            )

        source_stage = stage_by_key[order[0]]
        target_stage = stage_by_key[order[-1]]
        map_stage = stage_by_key[order[1]] if len(order) == 3 else None

        # A read source lowers to a DB connector binding; a fetch source to REST
        # (rest_fetch) or SOAP (soap_fetch, #126) per the declared primitive.
        if source_stage.kind == "read":
            source_default_connector = "database"
        elif source_stage.config.get("primitive") == "soap_fetch":
            source_default_connector = "soap_client"
        else:
            source_default_connector = "rest"
        # A send target lowers to a REST (rest_send) or SOAP (soap_send, #126)
        # binding per the declared primitive; a write target to a database
        # (db_write) Send binding (#74 M5.8).
        if target_stage.kind == "write":
            target_default_connector = "database"
        elif target_stage.config.get("primitive") == "soap_send":
            target_default_connector = "soap_client"
        else:
            target_default_connector = "rest"
        lowered: Dict[str, Any] = {
            "process_kind": ProcessFlowBuilder.PROCESS_KIND,
            "source": cls._lower_binding_stage(
                source_stage, default_connector_type=source_default_connector
            ),
            "transform": cls._lower_map_stage(map_stage),
            "target": cls._lower_binding_stage(
                target_stage, default_connector_type=target_default_connector
            ),
        }
        # Carry non-flow metadata the base builder already supports. name /
        # folder_name are consumed by the integration builder / build() kwargs,
        # not by the lowered config, so they are intentionally not copied here.
        if "description" in config:
            lowered["description"] = config["description"]
        if "process_extensions" in config:
            lowered["process_extensions"] = config["process_extensions"]
        return lowered

    @classmethod
    def _linear_stage_order(
        cls, spec: PipelineSpec, stage_by_key: Dict[str, "StageSpec"]
    ) -> List[str]:
        """Return the single linear stage order, or raise on any non-linear shape.

        Requires exactly one source (indegree 0), no fan-out (outdegree <= 1),
        and full coverage (every stage on the one path). PipelineSpec already
        rejected cycles, so the walk is acyclic; the in-loop guard is defensive.
        """
        out_edges: Dict[str, List[str]] = {k: [] for k in stage_by_key}
        indeg: Dict[str, int] = {k: 0 for k in stage_by_key}
        for edge in spec.dependencies:
            out_edges[edge.from_stage].append(edge.to_stage)
            indeg[edge.to_stage] += 1

        sources = [k for k in stage_by_key if indeg[k] == 0]
        if len(sources) != 1:
            raise BuilderValidationError(
                f"sync_pipeline must be a single linear chain; found "
                f"{len(sources)} start stages (expected exactly 1).",
                error_code="SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED",
                field="pipeline.dependencies",
                hint="Wire read -> [map] -> send with one start and no fan-in.",
            )

        order: List[str] = []
        seen: set = set()
        cur: Optional[str] = sources[0]
        while cur is not None:
            if cur in seen:  # defensive — PipelineSpec already rejects cycles.
                raise BuilderValidationError(
                    "sync_pipeline pipeline contains a cycle.",
                    error_code="SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED",
                    field="pipeline.dependencies",
                    hint="A sync_pipeline is an acyclic read -> [map] -> send chain.",
                )
            seen.add(cur)
            order.append(cur)
            nxts = out_edges[cur]
            if len(nxts) > 1:
                raise BuilderValidationError(
                    f"sync_pipeline stage {cur!r} fans out to {len(nxts)} stages; "
                    "it is linear (no fan-out).",
                    error_code="SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED",
                    field="pipeline.dependencies",
                    hint="Branch fan-out is owned by M10.8 (issue #112).",
                )
            cur = nxts[0] if nxts else None

        if len(order) != len(stage_by_key):
            raise BuilderValidationError(
                "sync_pipeline is not a single connected linear chain "
                "(unreachable or fanned-in stages).",
                error_code="SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED",
                field="pipeline.stages",
                hint="Every stage must lie on the one read -> [map] -> send path.",
            )
        return order

    @classmethod
    def _check_stage_primitive(cls, stage: "StageSpec") -> None:
        """Enforce config.primitive matches the stage kind (raises otherwise).

        A fetch/send stage accepts EITHER its REST primitive (rest_fetch/
        rest_send) OR its SOAP alternate (soap_fetch/soap_send, #126).
        """
        expected = _SYNC_PIPELINE_STAGE_PRIMITIVE[stage.kind]
        alt = _SYNC_PIPELINE_STAGE_ALT_PRIMITIVE.get(stage.kind)
        accepted = (expected, alt) if alt is not None else (expected,)
        primitive = stage.config.get("primitive")
        if primitive in accepted:
            return
        accepted_label = " or ".join(repr(p) for p in accepted)
        if primitive is None:
            raise BuilderValidationError(
                f"sync_pipeline {stage.kind} stage {stage.key!r} must declare "
                f"config.primitive={accepted_label}.",
                error_code="SYNC_PIPELINE_CONFIG_INVALID",
                field=f"pipeline.stages[{stage.key}].config.primitive",
                hint=f"A {stage.kind!r} stage is backed by the {accepted_label} primitive.",
            )
        raise BuilderValidationError(
            f"sync_pipeline {stage.kind} stage {stage.key!r} has primitive "
            f"{primitive!r}; expected {accepted_label}.",
            error_code="SYNC_PIPELINE_STAGE_UNSUPPORTED",
            field=f"pipeline.stages[{stage.key}].config.primitive",
            hint=_SYNC_PIPELINE_RESERVED_PRIMITIVE_HINTS.get(
                primitive, f"A {stage.kind!r} stage must use primitive {accepted_label}."
            ),
        )

    @classmethod
    def _check_source_connector_family(cls, stage: "StageSpec") -> None:
        """Reject an explicit source connector_type that contradicts the kind.

        Only source stages (read/fetch) are guarded — a read stage is a DB source
        and a fetch stage is a REST source; an explicit connector_type may restate
        that family but must not flip it. Absent connector_type is fine (the
        kind-derived default applies). The target family is guarded symmetrically
        by :meth:`_check_target_connector_family`, so this is a no-op for non-source
        kinds.
        """
        if stage.kind not in ("read", "fetch"):
            return
        explicit = stage.config.get("connector_type")
        if explicit is None:
            return
        is_rest = _resolve_rest_connector_type(explicit) is not None
        is_soap = _resolve_soap_client_connector_type(explicit) is not None
        is_database = isinstance(explicit, str) and explicit.strip().lower() == "database"
        if stage.kind == "read" and not is_database:
            raise BuilderValidationError(
                f"sync_pipeline read stage {stage.key!r} connector_type must be "
                f"'database' (a read stage is a db_read source); got {explicit!r}.",
                error_code="SYNC_PIPELINE_CONFIG_INVALID",
                field=f"pipeline.stages[{stage.key}].config.connector_type",
                hint="Use a 'fetch' stage (rest_fetch/soap_fetch) for an API source; a read stage is DB-only.",
            )
        if stage.kind == "fetch":
            # The declared primitive selects the family: soap_fetch -> SOAP,
            # otherwise rest_fetch -> REST. An explicit connector_type may restate
            # that family but must not flip it (#126).
            if stage.config.get("primitive") == "soap_fetch":
                if not is_soap:
                    raise BuilderValidationError(
                        f"sync_pipeline fetch stage {stage.key!r} declares primitive "
                        f"'soap_fetch' but connector_type is not a SOAP Client connector; "
                        f"got {explicit!r}.",
                        error_code="SYNC_PIPELINE_CONFIG_INVALID",
                        field=f"pipeline.stages[{stage.key}].config.connector_type",
                        hint="A soap_fetch source requires a SOAP Client connector_type (soap_client / wssoapclientsdk).",
                    )
            elif not is_rest:
                raise BuilderValidationError(
                    f"sync_pipeline fetch stage {stage.key!r} connector_type must be a "
                    f"REST Client connector (a rest_fetch source); got {explicit!r}.",
                    error_code="SYNC_PIPELINE_CONFIG_INVALID",
                    field=f"pipeline.stages[{stage.key}].config.connector_type",
                    hint="Use a 'read' stage (db_read) for a database source, or declare primitive='soap_fetch' for a SOAP source.",
                )

    @classmethod
    def _check_target_connector_family(cls, stage: "StageSpec") -> None:
        """Reject an explicit target connector_type that contradicts the kind (#74).

        The mirror of :meth:`_check_source_connector_family` for the target. A
        ``send`` stage is a REST (rest_send) target and a ``write`` stage is a
        database (db_write) target; an explicit connector_type may restate that
        family but must not flip it. Without this, the delegate's ``allow_db_target``
        would accept a ``send`` stage forced to ``connector_type='database'`` (a DB
        target from a REST-send stage) or a ``write`` stage forced back to REST —
        bypassing the send-vs-write split. Absent connector_type is fine (the
        kind-derived default applies). No-op for non-target kinds.
        """
        if stage.kind not in ("send", "write"):
            return
        explicit = stage.config.get("connector_type")
        if explicit is None:
            return
        is_rest = _resolve_rest_connector_type(explicit) is not None
        is_soap = _resolve_soap_client_connector_type(explicit) is not None
        is_database = isinstance(explicit, str) and explicit.strip().lower() == "database"
        if stage.kind == "send":
            # The declared primitive selects the family: soap_send -> SOAP,
            # otherwise rest_send -> REST. An explicit connector_type may restate
            # that family but must not flip it (#126).
            if stage.config.get("primitive") == "soap_send":
                if not is_soap:
                    raise BuilderValidationError(
                        f"sync_pipeline send stage {stage.key!r} declares primitive "
                        f"'soap_send' but connector_type is not a SOAP Client connector; "
                        f"got {explicit!r}.",
                        error_code="SYNC_PIPELINE_CONFIG_INVALID",
                        field=f"pipeline.stages[{stage.key}].config.connector_type",
                        hint="A soap_send target requires a SOAP Client connector_type (soap_client / wssoapclientsdk).",
                    )
            elif not is_rest:
                raise BuilderValidationError(
                    f"sync_pipeline send stage {stage.key!r} connector_type must be a "
                    f"REST Client connector (a rest_send target); got {explicit!r}.",
                    error_code="SYNC_PIPELINE_CONFIG_INVALID",
                    field=f"pipeline.stages[{stage.key}].config.connector_type",
                    hint="Use a 'write' stage (db_write) for a database target, or declare primitive='soap_send' for a SOAP target.",
                )
        if stage.kind == "write" and not is_database:
            raise BuilderValidationError(
                f"sync_pipeline write stage {stage.key!r} connector_type must be "
                f"'database' (a write stage is a db_write target); got {explicit!r}.",
                error_code="SYNC_PIPELINE_CONFIG_INVALID",
                field=f"pipeline.stages[{stage.key}].config.connector_type",
                hint="Use a 'send' stage (rest_send) for a REST target; a write stage is database-only.",
            )

    @classmethod
    def _lower_binding_stage(
        cls, stage: "StageSpec", *, default_connector_type: str
    ) -> Dict[str, Any]:
        """Lower a read/send stage to a source/target connector binding dict."""
        if stage.component_ref is not None:
            raise BuilderValidationError(
                f"sync_pipeline stage {stage.key!r} uses component_ref; M5.2 "
                "supports only config-backed primitive stages.",
                error_code="SYNC_PIPELINE_CONFIG_INVALID",
                field=f"pipeline.stages[{stage.key}].component_ref",
                hint="Provide the connector binding via config (component reuse stages are not lowered in M5.2).",
            )
        cls._check_stage_primitive(stage)
        extra = set(stage.config) - _SYNC_PIPELINE_BINDING_KEYS
        if extra:
            raise BuilderValidationError(
                f"sync_pipeline {stage.kind} stage {stage.key!r} has unsupported "
                f"config key(s) {sorted(extra)}.",
                error_code="SYNC_PIPELINE_CONFIG_INVALID",
                field=f"pipeline.stages[{stage.key}].config",
                hint=(
                    "Allowed keys: "
                    f"{sorted(_SYNC_PIPELINE_BINDING_KEYS)}. Gated sub-blocks "
                    "(e.g. dynamic_path, reliability, runtime_bindings) are not "
                    "lowered by the thin sync_pipeline stage — express a #96 "
                    "runtime binding on the rest_fetch/rest_send operation config."
                ),
            )
        # A read (db_read) source is always a Get; a fetch source is a GET
        # (rest_fetch, M5.4 #72) or EXECUTE (soap_fetch, #126); a write (db_write)
        # target is always a Send (#74 M5.8); a send target is an explicit HTTP
        # method (rest_send, no default) or EXECUTE (soap_send, #126).
        primitive = stage.config.get("primitive")
        is_soap_source = stage.kind == "fetch" and primitive == "soap_fetch"
        is_soap_target = stage.kind == "send" and primitive == "soap_send"
        if stage.kind == "read":
            default_action_type: Optional[str] = "Get"
        elif stage.kind == "fetch":
            default_action_type = "EXECUTE" if is_soap_source else "GET"
        elif stage.kind == "write":
            default_action_type = "Send"
        elif is_soap_target:
            default_action_type = "EXECUTE"
        else:
            default_action_type = None
        # An explicit connector_type on a SOURCE stage must agree with the stage
        # kind's connector family — otherwise the read↔fetch split is bypassable:
        # now that the delegate accepts both DB and REST sources, a read stage with
        # connector_type='rest' (or a fetch stage with connector_type='database')
        # would silently build the wrong source. Reject the contradiction here so a
        # read stays a DB source and a fetch stays a REST source (the kind, not a
        # config override, decides the source family).
        cls._check_source_connector_family(stage)
        # Symmetrically (#74), an explicit connector_type on a TARGET stage must
        # agree with the kind's family: now that the delegate accepts both REST and
        # DB targets (allow_db_target), a send stage forced to connector_type=
        # 'database' (or a write stage forced to REST) would otherwise bypass the
        # send-vs-write split. The kind, not a config override, decides the target
        # family.
        cls._check_target_connector_family(stage)
        # Resolve the binding action_type. For a fetch (rest_fetch) source and a
        # write (db_write) target, an explicit ``action_type: null`` (the key
        # present with value None) means "the default verb" — identical to
        # omitting the key — so it resolves to that kind's default (fetch -> GET,
        # write -> Send) rather than leaking a None that build() would emit as
        # actionType="". A send (rest_send) target has NO default: it requires an
        # explicit HTTP method (enforced by the send guard below).
        action_type = stage.config.get("action_type", default_action_type)
        # A fetch source, a write target, and a soap_send target all have a
        # kind/primitive-derived default verb, so an explicit action_type=None
        # (present key, null value) resolves to that default rather than leaking a
        # None that build() would emit as actionType="". Only a rest_send target
        # has NO default (its explicit-HTTP-method requirement is enforced below).
        if action_type is None and (
            stage.kind in ("fetch", "write") or is_soap_target
        ):
            action_type = default_action_type  # fetch->GET/EXECUTE, write->Send, soap_send->EXECUTE
        binding: Dict[str, Any] = {
            "connector_type": stage.config.get("connector_type", default_connector_type),
            "action_type": action_type,
            "connection_id": stage.config.get("connection_id"),
            "operation_id": stage.config.get("operation_id"),
        }
        # A fetch (rest_fetch) source is GET-only (M5.4 #72). Enforce it HERE in the
        # shared lowering so both validate_config AND a direct build() stay total —
        # build() delegates to ProcessFlowBuilder.build() WITHOUT re-running the
        # binding validators, so a non-GET fetch caught only by _validate_source_
        # binding would still emit on the validate_config-bypass path. Raise the same
        # PROCESS_CONNECTOR_BINDING_INVALID code the delegate uses so the validate
        # path's error is unchanged (just surfaced one step earlier). The None check
        # is defensive — null already resolved to GET above.
        if stage.kind == "fetch":
            action_value = binding["action_type"]
            if is_soap_source:
                # A soap_fetch source is EXECUTE-only (#126).
                if action_value is None or str(action_value).strip().upper() != "EXECUTE":
                    raise BuilderValidationError(
                        f"sync_pipeline fetch stage {stage.key!r} action_type must be "
                        f"'EXECUTE' (soap_fetch is EXECUTE-only, #126); got {action_value!r}.",
                        error_code="PROCESS_CONNECTOR_BINDING_INVALID",
                        field=f"pipeline.stages[{stage.key}].config.action_type",
                        hint="A SOAP fetch source is a single EXECUTE action; there is no verb split.",
                    )
            elif action_value is None or str(action_value).strip().upper() != "GET":
                raise BuilderValidationError(
                    f"sync_pipeline fetch stage {stage.key!r} action_type must be "
                    f"'GET' (rest_fetch is GET-only, M5.4 #72); got {action_value!r}.",
                    error_code="PROCESS_CONNECTOR_BINDING_INVALID",
                    field=f"pipeline.stages[{stage.key}].config.action_type",
                    hint="A fetch source is a REST GET; other verbs model a source-side write, which is out of scope.",
                )
        # A write (db_write) target is Send-only (#74 M5.8) — the mirror of the
        # fetch GET-only guard. Enforce it HERE in the shared lowering so both
        # validate_config AND a direct build() stay total: build() delegates to
        # ProcessFlowBuilder.build() WITHOUT re-running _validate_db_target_binding,
        # so an explicit non-Send write action (e.g. action_type='Get') caught only
        # by the validator would otherwise emit a malformed database target step
        # (connectorType='database' actionType='Get') on the validate_config-bypass
        # path. Match the validator's exact-'Send' contract (case-sensitive, like
        # the DB 'Get' source verb). The None check is defensive — null already
        # resolved to 'Send' above.
        if stage.kind == "write":
            action_value = binding["action_type"]
            if action_value is None or str(action_value).strip() != "Send":
                raise BuilderValidationError(
                    f"sync_pipeline write stage {stage.key!r} action_type must be "
                    f"'Send' (db_write is a database Send target, #74 M5.8); got {action_value!r}.",
                    error_code="PROCESS_CONNECTOR_BINDING_INVALID",
                    field=f"pipeline.stages[{stage.key}].config.action_type",
                    hint="A write target is a database Send; other verbs are not valid for db_write.",
                )
        # A send (rest_send) target carries an explicit HTTP method — there is NO
        # default (unlike read Get / fetch GET / write Send). Enforce a non-empty
        # action_type HERE in the shared lowering so both validate_config AND a
        # direct build() stay total: build() delegates to ProcessFlowBuilder.build()
        # WITHOUT re-running _validate_target_binding, so a send stage missing
        # action_type (action_type=None) would leak None and emit actionType="" on
        # the validate_config-bypass path. Raise the same PROCESS_CONNECTOR_BINDING_
        # INVALID code the delegate uses. Only the non-empty invariant is enforced
        # here (C1, #128) — the HTTP verb vocabulary stays with the delegate's
        # _validate_target_binding, which the archetype->plan->apply path exercises.
        if stage.kind == "send":
            action_value = binding["action_type"]
            if is_soap_target:
                # A soap_send target is EXECUTE-only (#126) — unlike rest_send it
                # has a default verb, so this both defaults and enforces EXECUTE.
                if action_value is None or str(action_value).strip().upper() != "EXECUTE":
                    raise BuilderValidationError(
                        f"sync_pipeline send stage {stage.key!r} action_type must be "
                        f"'EXECUTE' (soap_send is EXECUTE-only, #126); got {action_value!r}.",
                        error_code="PROCESS_CONNECTOR_BINDING_INVALID",
                        field=f"pipeline.stages[{stage.key}].config.action_type",
                        hint="A SOAP send target is a single EXECUTE action; there is no HTTP verb.",
                    )
            elif action_value is None or str(action_value).strip() == "":
                raise BuilderValidationError(
                    f"sync_pipeline send stage {stage.key!r} requires an explicit "
                    f"action_type (rest_send has no default HTTP method); got {action_value!r}.",
                    error_code="PROCESS_CONNECTOR_BINDING_INVALID",
                    field=f"pipeline.stages[{stage.key}].config.action_type",
                    hint="Set config.action_type to the REST verb (e.g. POST/PUT/PATCH/GET) for the send target.",
                )
        if "label" in stage.config:
            binding["label"] = stage.config["label"]
        return binding

    @classmethod
    def _lower_map_stage(cls, stage: Optional["StageSpec"]) -> Dict[str, Any]:
        """Lower an optional map stage to a transform block (passthrough if absent)."""
        if stage is None:
            return {"mode": "passthrough"}
        if stage.component_ref is not None:
            raise BuilderValidationError(
                f"sync_pipeline map stage {stage.key!r} uses component_ref; M5.2 "
                "supports only config-backed primitive stages.",
                error_code="SYNC_PIPELINE_CONFIG_INVALID",
                field=f"pipeline.stages[{stage.key}].component_ref",
                hint="Provide the map reference via config (component reuse stages are not lowered in M5.2).",
            )
        cls._check_stage_primitive(stage)
        extra = set(stage.config) - _SYNC_PIPELINE_MAP_KEYS
        if extra:
            raise BuilderValidationError(
                f"sync_pipeline map stage {stage.key!r} has unsupported config "
                f"key(s) {sorted(extra)}.",
                error_code="SYNC_PIPELINE_CONFIG_INVALID",
                field=f"pipeline.stages[{stage.key}].config",
                hint=f"Allowed keys: {sorted(_SYNC_PIPELINE_MAP_KEYS)}.",
            )
        map_ref = stage.config.get("map_ref") or stage.config.get("map_id")
        if not isinstance(map_ref, str) or not map_ref.strip():
            raise BuilderValidationError(
                f"sync_pipeline map stage {stage.key!r} requires a non-empty "
                "config.map_ref (or map_id).",
                error_code="SYNC_PIPELINE_CONFIG_INVALID",
                field=f"pipeline.stages[{stage.key}].config.map_ref",
                hint="Pass the Map component id or a $ref:KEY token (and add KEY to depends_on).",
            )
        transform: Dict[str, Any] = {"mode": "map_ref", "map_ref": map_ref}
        if "label" in stage.config:
            transform["label"] = stage.config["label"]
        return transform

    @classmethod
    def validate_config(
        cls,
        config: Dict[str, Any],
        *,
        depends_on: Optional[Iterable[str]] = None,
    ) -> Optional[BuilderValidationError]:
        """Validate a sync_pipeline config; return a structured error or None.

        Runs the process_kind guard, then lowers the pipeline (surfacing any
        lowering defect as a structured error) and delegates the lowered config
        to :meth:`ProcessFlowBuilder.validate_config` so the proven source/target/
        transform binding + $ref-reachability checks all apply unchanged.
        """
        process_kind = str(
            config.get("process_kind") or config.get("process_type") or ""
        ).strip()
        if process_kind != cls.PROCESS_KIND:
            return BuilderValidationError(
                f"process_kind {process_kind!r} is not supported.",
                error_code="PROCESS_KIND_UNSUPPORTED",
                field="process_kind",
                hint=f"Use process_kind={cls.PROCESS_KIND!r} for the verified-linear sync pipeline builder.",
            )
        try:
            lowered = cls.lower_config(config)
        except BuilderValidationError as exc:
            return exc
        # A lowered fetch stage carries a REST GET source and a lowered write stage
        # carries a database Send target; allow both here (only the sync_pipeline
        # path does). The base database_to_api_sync validation stays DB-source-only
        # and REST-target-only.
        return ProcessFlowBuilder.validate_config(
            lowered,
            depends_on=depends_on,
            allow_rest_source=True,
            allow_db_target=True,
            allow_soap_source=True,
            allow_soap_target=True,
        )

    @classmethod
    def build(
        cls,
        config: Dict[str, Any],
        *,
        name: str,
        folder_name: Optional[str] = None,
    ) -> str:
        """Lower the pipeline and delegate XML emission to ProcessFlowBuilder.

        lower_config raises on any structural defect, so a direct build() that
        bypasses validate_config still fails cleanly instead of emitting a
        malformed process.
        """
        lowered = cls.lower_config(config)
        return ProcessFlowBuilder.build(lowered, name=name, folder_name=folder_name)


# ----------------------------------------------------------------------
# Registry (parallel to PROFILE_BUILDERS / CONNECTOR_BUILDERS)
# ----------------------------------------------------------------------

PROCESS_FLOW_BUILDERS: Dict[str, type] = {
    ProcessFlowBuilder.PROCESS_KIND: ProcessFlowBuilder,
    WrapperSubprocessBuilder.PROCESS_KIND: WrapperSubprocessBuilder,
    SyncPipelineBuilder.PROCESS_KIND: SyncPipelineBuilder,
}


# Issue #45 — update-preservation policy. The builder owns the entire
# `<process>` subtree (shapes/transitions/etc.). The sibling
# `<bns:processOverrides>` (which Boomi populates with per-environment
# override values via UI) is NOT in owned_paths, so it survives a
# structured update. bns:encryptedValues and any unknown
# bns:Component-level children are also preserved.
ProcessFlowBuilder.PRESERVATION_POLICY = PreservationPolicy(
    component_type="process",
    owned_paths=(OwnedPath(path="bns:object/process"),),
)

# The wrapper-parent builder owns the same `<process>` subtree (issue #90).
WrapperSubprocessBuilder.PRESERVATION_POLICY = PreservationPolicy(
    component_type="process",
    owned_paths=(OwnedPath(path="bns:object/process"),),
)

# The sync_pipeline builder emits the same `<process>` subtree via the delegated
# ProcessFlowBuilder.build, so it owns the same path (issue #70 M5.2).
SyncPipelineBuilder.PRESERVATION_POLICY = PreservationPolicy(
    component_type="process",
    owned_paths=(OwnedPath(path="bns:object/process"),),
)


def get_process_flow_builder(process_kind: Optional[str]):
    """Return the process-flow builder for process_kind, or None."""
    if not process_kind:
        return None
    return PROCESS_FLOW_BUILDERS.get(str(process_kind).strip().lower())


__all__ = [
    "ProcessFlowBuilder",
    "WrapperSubprocessBuilder",
    "SyncPipelineBuilder",
    "PROCESS_FLOW_BUILDERS",
    "get_process_flow_builder",
    "DB_CONNECTION_EXTENSION_FIELDS_CREDENTIAL",
    "DB_CONNECTION_EXTENSION_FIELDS_ENDPOINT",
]
