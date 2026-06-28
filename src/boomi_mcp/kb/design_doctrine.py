"""Served ``design_doctrine`` knowledge surface (issue #86, epic #85 / M4.5.1).

A read-only catalog of integration *architecture* decisions — the half of Boomi
design knowledge the LLM still owns once this MCP's typed builders take over the
low-level XML construction. It is served through the existing
``get_schema_template`` + ``list_capabilities`` machinery (see
``categories/meta_tools.py``), parallel to the inline ``operating_doctrine``.

Scope boundary (the #86 abstraction filter): every entry carries an
architecture-level DECISION, never the mechanics the typed builders own. The
``boomi_shape_mapping`` field names which shapes realize a pattern, in what role
— it is conceptual, never an XML/attribute/step-config spec. A token-lint test
(``tests/test_design_doctrine.py``) enforces that no mechanic literals leak into
served prose.

This module is intentionally stdlib-only (``copy`` is the sole import) so it is
safe to import on every server start — it must never pull in the heavy docs-KB
ML stack that lives in ``boomi_mcp.kb.service``.

Catalog size: **38 entries** = 14 seed + 16 net-new design patterns + 8
testing/observability. The 11 seed enrichments fold into the seed entries as
extra clauses; they are NOT separate rows. ``account_governance`` is the
separate child #93 and is deliberately out of scope here.
"""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Schema / vocabularies
# ---------------------------------------------------------------------------

#: Every catalog entry MUST carry these 11 fields (enforced by tests).
DESIGN_DOCTRINE_REQUIRED_FIELDS = (
    "name",
    "problem",
    "boomi_shape_mapping",
    "when_to_use",
    "when_not_to_use",
    "verification_status",
    "capability_status",
    "category",
    "mutual_exclusion",
    "cross_refs",
    "provenance",
)

#: Whether the MCP's typed builders can emit the pattern today.
CAPABILITY_STATUSES = frozenset(
    {"emittable_today", "gated", "guidance_only", "na"}
)

#: #76 verification vocabulary, extended with ``course_unverified`` for
#: third-party-training-only claims (per #86 augmented schema).
VERIFICATION_STATUSES = frozenset(
    {
        "live_verified",
        "docs_corroborated",
        "companion_unverified",
        "course_unverified",
        "disputed",
    }
)

#: Provenance shares the verification vocabulary — it labels where a claim came
#: from. ``course_unverified`` marks the Boomi architect course (a labeled
#: third-party training hypothesis source, not official docs nor the Companion).
PROVENANCE_LABELS = VERIFICATION_STATUSES

#: Faceting category for the single served surface.
CATEGORIES = frozenset(
    {
        "governance",
        "sync",
        "messaging",
        "reliability",
        "decomposition",
        "routing",
        "security",
        "testing",
        "observability",
        "migration",
        "process_tuning",
    }
)

#: Structured source of truth (M10.1, issue #105) for which process *shapetypes*
#: the typed builders can emit today. This is deliberately a discrete
#: ``{shapetype -> {emittable, emitter_kind}}`` table — the rest of this module
#: is pattern-keyed free-text prose with per-pattern ``capability_status``, which
#: cannot be iterated to prove emitter coverage. ``emitter_kind`` is the INTERNAL
#: process-flow dispatch key consumed by ``_emit_flow_shape`` in
#: ``process_flow_builder.py`` (e.g. ``setproperties`` realizes the
#: ``documentproperties`` shape); for catch-path-only shapes
#: (``catcherrors``/``notify``/``doccacheload``) it is the catch-leg emission
#: token verified through the Try/Catch wrapper. It is NOT a method name and NOT
#: a doctrine pattern name. ``tests/test_doctrine_emitter_consistency.py`` asserts
#: every emittable entry is backed by a real dispatch/emission branch (keyed off
#: the actual dispatch mechanism, not method-name coincidence). This constant is
#: intentionally NOT served via ``get_design_doctrine_catalog()`` — it carries
#: mechanic tokens (``catcherrors`` etc.) that the served-prose token-lint bans.
#: Each later M10 shape issue flips/adds an entry here as part of making its shape
#: emittable, updating the consistency test in lockstep.
EMITTABLE_SHAPE_REGISTRY: Dict[str, Dict[str, Any]] = {
    "start": {"emittable": True, "emitter_kind": "start_noaction"},
    "connectoraction": {"emittable": True, "emitter_kind": "connectoraction_source"},
    "message": {"emittable": True, "emitter_kind": "message"},
    "map": {"emittable": True, "emitter_kind": "map"},
    "documentproperties": {"emittable": True, "emitter_kind": "setproperties"},
    "stop": {"emittable": True, "emitter_kind": "stop"},
    "catcherrors": {"emittable": True, "emitter_kind": "catcherrors"},
    "notify": {"emittable": True, "emitter_kind": "notify"},
    "doccacheload": {"emittable": True, "emitter_kind": "doccacheload"},
    "processcall": {"emittable": True, "emitter_kind": "processcall"},
    # M10.2 (issue #106): process-level Data Process shape. Emittable today for
    # the live-observed Custom Scripting operation via the dataprocess transform
    # mode; the dispatch key is ``dataprocess`` in _emit_flow_shape.
    "dataprocess": {"emittable": True, "emitter_kind": "dataprocess"},
    # M10.3 (issue #107): process-level Return Documents terminal shape. Emittable
    # today via the return_documents config block on ProcessFlowBuilder /
    # WrapperSubprocessBuilder (it replaces the trailing Stop — the subprocess
    # return value); the dispatch key is ``returndocuments`` in _emit_flow_shape.
    # Live-captured from work component 64e5397b-3583-42c9-8fe3-08ccefb0da6c.
    "returndocuments": {"emittable": True, "emitter_kind": "returndocuments"},
    # M10.4 (issue #108): deliberate Exception (Throw) terminal on the Try/Catch
    # catch leg. Emittable today via the reliability.catch_exception block on
    # ProcessFlowBuilder (the catcherrors Catch leg ends in a thrown error message
    # instead of a bare Stop). It is a CATCH-PATH shape (like catcherrors/notify/
    # doccacheload) — NOT in the _emit_flow_shape dispatch ladder; the catch-leg
    # emitter _emit_exception produces it. Live-captured from work component
    # 1139079f-fff5-434c-aedc-d2758cc20525.
    "exception": {"emittable": True, "emitter_kind": "exception"},
    # M10.8 (issue #112): Branch (N-way forward fan-out) shape. Emittable today
    # via the branch config block on ProcessFlowBuilder (the post-source document
    # fans to N independent target legs — leg 1 = top-level target, legs 2..N =
    # branch.targets — each ending in its own Stop; forward-only, no join/merge).
    # Like exception, it is NOT in the _emit_flow_shape single-edge dispatch ladder
    # (a Branch shape carries N labelled outgoing edges); the dedicated emitter
    # _emit_branch (driven by _emit_branch_shapes) produces it. Live-captured from
    # work component b34d3812-900d-41b6-b44c-c812fb9b04aa (shape53).
    "branch": {"emittable": True, "emitter_kind": "branch"},
    # M10.9 (issue #113): Decision (conditional two-path routing) shape. Emittable
    # today via the decision config block on ProcessFlowBuilder (the post-source
    # document routes down a labelled true/false dragpoint by a value comparison —
    # true = top-level target -> Stop, false = optional notify Message -> Stop or a
    # backward loop to an earlier shape). Like branch, it is NOT in the
    # _emit_flow_shape single-edge dispatch ladder (two labelled outgoing edges);
    # the dedicated emitter _emit_decision (driven by _emit_decision_shapes)
    # produces it. Live-verified via companion decision_step.md + a work-profile
    # decision export.
    "decision": {"emittable": True, "emitter_kind": "decision"},
    # M10.5 (issue #109): process-level Document Cache Retrieve shape. Emittable
    # today via the transform.mode='doccacheretrieve' block on ProcessFlowBuilder
    # (a linear non-terminal step that pulls documents from a Document Cache into
    # the current flow — the read half of Document Cache CRUD, pairing the Add to
    # Cache / doccacheload shape); the dispatch key is ``doccacheretrieve`` in
    # _emit_flow_shape. Live-captured from work component
    # 64e5397b-3583-42c9-8fe3-08ccefb0da6c (shape2).
    "doccacheretrieve": {"emittable": True, "emitter_kind": "doccacheretrieve"},
    # M10.6 (issue #110): process-level Document Cache Remove shape. Emittable
    # today via the transform.mode='doccacheremove' block on ProcessFlowBuilder
    # (a linear non-terminal step that clears documents from a Document Cache —
    # the delete half of Document Cache CRUD, completing the set alongside Add to
    # Cache / doccacheload and Document Cache Retrieve / doccacheretrieve); the
    # dispatch key is ``doccacheremove`` in _emit_flow_shape. Live-captured from
    # work component 6e56df6a-1fc0-43f6-8db2-1b9e4eefa7a0 (shapes 3-7).
    "doccacheremove": {"emittable": True, "emitter_kind": "doccacheremove"},
    # M10.7 (issue #111): process-level Flow Control shape. Emittable today via the
    # flow_control config block on ProcessFlowBuilder (a linear non-terminal step
    # inserted right after the source that batches the document stream — only the
    # live-verified per-document batching mode; true parallel chunks / combine stay
    # design guidance); the dispatch key is ``flowcontrol`` in _emit_flow_shape.
    # Live-captured from work component 7ce0d74d-e71a-408b-9d59-a6f4498c64e2.
    "flowcontrol": {"emittable": True, "emitter_kind": "flowcontrol"},
}

#: JSON-schema-shaped description of one entry, returned alongside the catalog so
#: callers (and tests) share one schema source.
DESIGN_DOCTRINE_ENTRY_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "problem": {"type": "string"},
        "boomi_shape_mapping": {
            "type": "string",
            "description": "Conceptual — which shapes realize the pattern, in "
            "what role. Never XML attributes / step-config literals.",
        },
        "when_to_use": {"type": "string"},
        "when_not_to_use": {"type": "string"},
        "verification_status": {"enum": sorted(VERIFICATION_STATUSES)},
        "capability_status": {"enum": sorted(CAPABILITY_STATUSES)},
        "category": {"enum": sorted(CATEGORIES)},
        "mutual_exclusion": {
            "type": "array",
            "items": {"type": "string"},
            "description": "First-class cross-entry tradeoffs the architect "
            "must weigh.",
        },
        "cross_refs": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Related entry names — a concept is defined once and "
            "referenced, not duplicated.",
        },
        "provenance": {"enum": sorted(PROVENANCE_LABELS)},
    },
    "required": list(DESIGN_DOCTRINE_REQUIRED_FIELDS),
}


# ---------------------------------------------------------------------------
# The catalog — 38 entries. Defined as an ordered list; indexed by name below.
# ---------------------------------------------------------------------------

_ENTRIES: List[Dict[str, Any]] = [
    # =====================================================================
    # 14 SEED entries (issue #86 body). 11 are enriched (§6.2) with extra
    # clauses folded into their prose; 3 are not (try_catch_placement,
    # error_routing_and_dlq, component_profile_reuse).
    # =====================================================================
    {
        "name": "wrapper_subprocess_separation",
        "problem": (
            "A single monolithic process mixes trigger/orchestration with "
            "business logic, so it is hard to test, reuse, and redeploy. "
            "Production designs separate a thin orchestrator from thick "
            "logic units."
        ),
        "boomi_shape_mapping": (
            "A thin parent (facade) process owns the trigger and "
            "orchestration only, calling thick subprocesses (via Process "
            "Call) that own the logic; shared framework and error-handling "
            "subprocesses are front-loaded and reused across integrations. "
            "The parent and its children can be authored together in one "
            "integration spec — the parent references each child by key and "
            "the children are built first. Process Route gives each "
            "subprocess an independent deploy unit with a back-compatible "
            "interface contract. A subprocess hands its documents back to the "
            "calling parent through a Return Documents terminal at the end of "
            "its document path — the subprocess return value, emittable today by "
            "the typed builder and live-verified — which Process Route maps to "
            "named return paths; a Return Documents path never routes onward to "
            "a Stop."
        ),
        "when_to_use": (
            "Any integration with more than one logical operation, or where "
            "a unit of logic must be reused, tested in isolation, or "
            "redeployed independently. Decompose when the flow no longer "
            "fits one canvas screen, or by logical operation. Subprocess "
            "encapsulation enables direct-invoke unit testing."
        ),
        "when_not_to_use": (
            "A genuinely trivial single-step flow where a parent/child "
            "split adds indirection without reuse or testability gain. Note "
            "the coupling cost: a subprocess change requires repackaging and "
            "redeploying the parent."
        ),
        "verification_status": "live_verified",
        "capability_status": "emittable_today",
        "category": "decomposition",
        "mutual_exclusion": [
            "Synchronous parent-to-subprocess handoff couples their "
            "lifecycles; decouple by default and choose the coupling level "
            "deliberately (see inline_vs_branch_cache_invocation)."
        ],
        "cross_refs": [
            "inline_vs_branch_cache_invocation",
            "process_route_fanout",
            "cross_cutting_framework_services",
            "unit_testing_via_swappable_data_source",
            "microservice_vs_monolith_decomposition",
        ],
        "provenance": "live_verified",
    },
    {
        "name": "connector_retry_design",
        "problem": (
            "Connector operations have no built-in retry (database and "
            "HTTP/REST included), so transient failures must be handled by "
            "explicit design or they surface as hard failures."
        ),
        "boomi_shape_mapping": (
            "A Try/Catch shape wraps the retriable connector call, and the "
            "retriable call lives in a dedicated subprocess so the retry "
            "unit is isolated and reusable. Reliable messaging promotes this "
            "further: persist-and-acknowledge plus a separate drainer "
            "subprocess that retries failed sends, rather than synchronous "
            "send-and-hope."
        ),
        "when_to_use": (
            "Around connector calls subject to transient faults. The Try/Catch "
            "Retry Count is bounded 0 to 5: a count of one retries immediately, "
            "and two to five apply the platform's built-in escalating wait "
            "schedule. Tune by connector class (design guidance): network/HTTP "
            "transients tolerate two to three retries; database writes risk "
            "duplicate transactions so allow at most one retry and prefer "
            "idempotent statements; transform/map failures take zero retries."
        ),
        "when_not_to_use": (
            "Transform/map errors (zero retries — fix the map instead) and "
            "non-idempotent writes that cannot be made safe. The platform owns "
            "the retry timing, so a caller-selected fixed or exponential backoff "
            "interval is not available; if a custom backoff window is required, "
            "design a scheduled re-run or queue-based retry instead. The typed "
            "builder now scopes the Try/Catch to each connector (the source read "
            "in its own Try/Catch with zero retries, the target send in its own "
            "Try/Catch with the bounded retry), so a target retry re-runs only "
            "the target connector and the upstream read executes once — keep that "
            "upstream read idempotent anyway as defense in depth."
        ),
        "verification_status": "live_verified",
        "capability_status": "emittable_today",
        "category": "reliability",
        "mutual_exclusion": [],
        "cross_refs": [
            "try_catch_placement",
            "error_routing_and_dlq",
            "idempotency_and_duplicates",
            "reliable_and_sequential_messaging",
        ],
        "provenance": "live_verified",
    },
    {
        "name": "try_catch_placement",
        "problem": (
            "Where error handling is placed determines whether failures are "
            "diagnosable. A single catch at the start of a long scheduled "
            "process obscures which step actually failed."
        ),
        "boomi_shape_mapping": (
            "Specific Try/Catch shapes placed immediately after the "
            "connectors that return data, scoping each catch to one "
            "failure source — not one catch wrapping the whole process. The "
            "typed builder emits this connector-scoped placement: one Try/Catch "
            "per connector, separated by the connectors so each scopes its own "
            "failures independently, each with its own dead-letter catch leg."
        ),
        "when_to_use": (
            "After each data-returning connector in a process, so the catch "
            "leg names the exact failing operation. Prefer this over a single "
            "process-wide catch whenever more than one connector can fail."
        ),
        "when_not_to_use": (
            "A single Try/Catch at the start of a scheduled main process — "
            "it hides the real error source. Avoid blanket process-wide "
            "catches. (A whole-process catch remains available as an explicit "
            "legacy scope for the rare single-failure-source flow.)"
        ),
        "verification_status": "live_verified",
        "capability_status": "emittable_today",
        "category": "reliability",
        "mutual_exclusion": [],
        "cross_refs": ["connector_retry_design", "error_routing_and_dlq"],
        "provenance": "live_verified",
    },
    {
        "name": "error_routing_and_dlq",
        "problem": (
            "Failed documents that are silently dropped become invisible "
            "data loss. Catch paths must route failures to a durable "
            "dead-letter destination and raise notification."
        ),
        "boomi_shape_mapping": (
            "A catch leg routes failed documents to a dead-letter "
            "destination (a Document Cache as the verified destination, or "
            "a reusable error-handling subprocess for handler reuse) and "
            "fires notification — never a silent swallow. A catch leg may "
            "also END in a deliberate Exception terminal that throws a "
            "user-defined error and fails/halts the path (either the single "
            "reaching document or the whole process), which the builder emits "
            "today; unlike a plain successful end-of-path, the Exception "
            "terminal surfaces the failure on the reporting page and keeps the "
            "catch leg traceable rather than dropping the rejected documents."
        ),
        "when_to_use": (
            "Every process that mutates or forwards data: give failures a "
            "traceable sink plus an alert so they can be reprocessed. Use the "
            "Exception terminal when an unrecoverable failure should "
            "deliberately fail/halt with a custom error message rather than be "
            "queued for replay."
        ),
        "when_not_to_use": (
            "Do not route to a bare Stop with no record of the failed "
            "document. Retry-with-replay paths belong to "
            "connector_retry_design, not the dead-letter leg. A Document Cache "
            "dead-letter sink captures the failed payload only on a best-effort "
            "basis — a malformed failed document may not be cleanly stored — and "
            "it is execution-scoped, so cross-run replay is manual; when "
            "guaranteed capture of the failed payload or durable cross-run replay "
            "is required, route to a reusable error-handling subprocess instead. "
            "The notification still records the failure durably either way, so "
            "the failure is never silent."
        ),
        "verification_status": "live_verified",
        "capability_status": "emittable_today",
        "category": "reliability",
        "mutual_exclusion": [],
        "cross_refs": [
            "try_catch_placement",
            "notification_logging",
            "caching_lookup_join",
            "idempotency_and_duplicates",
        ],
        "provenance": "live_verified",
    },
    {
        "name": "notification_logging",
        "problem": (
            "Operators need to know when a run fails and what it did. "
            "Notification and run-level audit must be designed in, not "
            "bolted on per process."
        ),
        "boomi_shape_mapping": (
            "A Notify step at the head of the catch path logs the "
            "platform-provided caught-error message to the process log at a "
            "chosen level, before the caught documents route to the failure "
            "handler. The builder emits this today on a wired catch leg "
            "(notify, then dead-letter route). Heavier email/alert delivery "
            "stays a reusable asynchronous notification subprocess invoked off "
            "the catch leg; run-level summary and audit outputs are separate. "
            "Split run-level logging from per-subprocess logging."
        ),
        "when_to_use": (
            "Every production integration: log the caught error on the catch "
            "path so failures are visible, and standardize one notification "
            "service and a run summary so alerting and audit are uniform."
        ),
        "when_not_to_use": (
            "On a hot low-latency path, heavy synchronous notification adds "
            "latency — keep the inline catch-path log lightweight and make "
            "email/alert delivery asynchronous or off the critical path. The "
            "observability plane (tracked business identifiers) is a distinct "
            "concern — see document_tracking_as_monitoring."
        ),
        "verification_status": "live_verified",
        "capability_status": "emittable_today",
        "category": "reliability",
        "mutual_exclusion": [],
        "cross_refs": [
            "error_routing_and_dlq",
            "document_tracking_as_monitoring",
            "cross_cutting_framework_services",
        ],
        "provenance": "live_verified",
    },
    {
        "name": "idempotency_and_duplicates",
        "problem": (
            "Retries and re-runs can create duplicates. A process must be "
            "safely re-runnable after a partial failure."
        ),
        "boomi_shape_mapping": (
            "Upsert-style targets (keyed updates that tolerate replay) where "
            "available; otherwise a Decision plus a connector existence-check "
            "before insert. A data-gathering start step lets per-record "
            "re-run drive reprocessing; a source-side ready flag or a "
            "destination-id write-back drives re-selection. Rejects route to "
            "a traceable sink."
        ),
        "when_to_use": (
            "Any target where the same input could be delivered twice "
            "(retries, scheduled overlap, manual replay). Prefer key-based "
            "upserts or dedup keys."
        ),
        "when_not_to_use": (
            "Plain inserts without a dedup key do not tolerate retries — do "
            "not retry them blindly (ties to connector_retry_design)."
        ),
        "verification_status": "companion_unverified",
        "capability_status": "guidance_only",
        "category": "reliability",
        "mutual_exclusion": [],
        "cross_refs": [
            "connector_retry_design",
            "error_routing_and_dlq",
            "incremental_watermark",
            "document_tracking_as_monitoring",
        ],
        "provenance": "companion_unverified",
    },
    {
        "name": "incremental_watermark",
        "problem": (
            "Re-extracting a full dataset every run is wasteful and can "
            "reprocess unchanged records. A high-water-mark drives "
            "incremental extraction — but a poorly chosen watermark drops "
            "or double-processes records."
        ),
        "boomi_shape_mapping": (
            "A persisted high-water-mark field gating the source query, "
            "chosen up a strict ladder: (1) a status/flag the source owns "
            "(stateless, user self-service retry); (2) a last-modified "
            "timestamp (stateful — persist the maximum record value); "
            "(3) change data capture only as a last resort."
        ),
        "when_to_use": (
            "Large or frequently-polled sources where only changed records "
            "should flow. Migrate stateful watermark values at cutover."
        ),
        "when_not_to_use": (
            "Reject relative-date or last-successful-run-time watermarks — "
            "they silently skip records around run boundaries. Full extract "
            "is simpler for small, slow-changing sources."
        ),
        "verification_status": "companion_unverified",
        "capability_status": "gated",
        "category": "sync",
        "mutual_exclusion": [],
        "cross_refs": [
            "change_data_capture_strategy",
            "idempotency_and_duplicates",
            "state_persistence_parking_lot",
        ],
        "provenance": "companion_unverified",
    },
    {
        "name": "caching_lookup_join",
        "problem": (
            "Repeated per-record lookups against an endpoint are slow and "
            "fragile. A cache turns lookups, joins, and existence checks "
            "into in-memory operations."
        ),
        "boomi_shape_mapping": (
            "A Document Cache as a design tool — populated up front, indexed "
            "at the retrieval granularity, used for lookups, multi-source "
            "joins, and existence-check upserts against a pre-cached "
            "destination set; shared across parent and subprocesses; a "
            "multi-row join uses a richer cached profile, a single-row "
            "lookup a scalar one. Acts as a cross-branch aggregator keyed by "
            "id rather than process properties. Retrieving the cached set back "
            "into a process (all-document) and removing the cached set "
            "(all-document) are builder-emittable today; populating the cache, "
            "indexed lookups/removes, and map-based joins remain design "
            "guidance, not yet builder-emitted."
        ),
        "when_to_use": (
            "When the same reference data is read many times in a run, or to "
            "join multiple sources, or to check existence before write. "
            "Use multiple or composite indices for multiple retrieval "
            "patterns."
        ),
        "when_not_to_use": (
            "Caches are execution-scoped only — do not treat them as durable "
            "state across runs. Very large reference sets may not fit; "
            "re-query selectively instead."
        ),
        # Issue #109 M10.5: the Document Cache Retrieve (read) step is now
        # builder-emittable and live-verified against work component
        # 64e5397b-3583-42c9-8fe3-08ccefb0da6c, so the capability/verification flip
        # from guidance_only/companion_unverified. Issue #110 M10.6 adds the
        # Document Cache Remove (delete) step, live-verified against work component
        # 6e56df6a-1fc0-43f6-8db2-1b9e4eefa7a0 — the rest of the pattern (populate,
        # indexed lookups/removes, map joins) stays guidance, per the prose.
        "verification_status": "live_verified",
        "capability_status": "emittable_today",
        "category": "decomposition",
        "mutual_exclusion": [],
        "cross_refs": ["error_routing_and_dlq", "idempotency_and_duplicates"],
        "provenance": "live_verified",
    },
    {
        "name": "content_based_routing",
        "problem": (
            "Documents of different types or values must follow different "
            "paths. Where the routing decision lives — in the process or in "
            "the map — is a design choice."
        ),
        "boomi_shape_mapping": (
            "Unconditional fan-out (the same document down every path) uses "
            "a Branch; value-comparing two-way selection uses a Decision; "
            "multi-condition or per-record selection uses Route or Business "
            "Rules; per-type dispatch uses Process Route. The unconditional "
            "Branch fan-out AND the value-comparing Decision are both "
            "builder-emittable today: Branch fans the same document to N "
            "independent target paths (each run in sequence to its own end, no "
            "rejoin); Decision routes the document down a true or false path by "
            "a value comparison (true = the top-level target, false = an "
            "optional notify Message before Stop, or a backward loop to an "
            "earlier shape). Multi-condition Route, Business Rules, and "
            "per-type Process Route remain design guidance, not yet "
            "builder-emitted. Transformation can live in a Map, Data Process, "
            "Business Rules, Route, or Process Route — not the Map alone. "
            "Publish/subscribe fan-out routes to per-subscriber subprocesses "
            "backed by a queue."
        ),
        "when_to_use": (
            "When document type or field values determine downstream "
            "handling. Put routing in the process when whole documents take "
            "different paths; in the map when only field shaping differs. "
            "Reach for the emittable Branch fan-out when every path should "
            "receive the same document (for example one path sends to a "
            "target while another logs an audit copy); reach for the emittable "
            "Decision when a value comparison chooses between two paths (for "
            "example send active records, notify on the rest), including a "
            "loop-back retry edge."
        ),
        "when_not_to_use": (
            "Do not scatter routing logic across both process and map for "
            "the same decision — pick one home. Branch is for same-document "
            "fan-out, not value selection — use a Decision where one path is "
            "chosen by a value comparison. A Decision inspects only the first "
            "record of a batch — use Business Rules to evaluate every record."
        ),
        "verification_status": "live_verified",
        "capability_status": "emittable_today",
        "category": "routing",
        "mutual_exclusion": [],
        "cross_refs": [
            "business_rules_vs_decision",
            "process_route_fanout",
            "combine_split_flow_control",
        ],
        "provenance": "live_verified",
    },
    {
        "name": "combine_split_flow_control",
        "problem": (
            "Document grouping and parallelism shape throughput and memory. "
            "Combining, splitting, batching, and parallel execution are "
            "deliberate design levers, not defaults."
        ),
        "boomi_shape_mapping": (
            "Combine documents with a Data Process combine step or a combined "
            "Message; split per-document execution with a Data Process split "
            "step; batch per-document execution with a Flow Control shape. The "
            "combine, split, and per-document batching levers are builder-emittable "
            "today; true parallel chunk fan-out remains design guidance. The split "
            "and per-document batching levers are live-captured; the combine lever's "
            "emitted form is reconciled from the companion reference rather than a "
            "live capture. A "
            "scheduled extract delegates large documents to a dedicated fetch "
            "subprocess."
        ),
        "when_to_use": (
            "High-volume flows where batching or bounded parallelism "
            "improves throughput. The builder emits per-document batching today; "
            "tune the batch size to a modest level, not the maximum, and keep "
            "true parallel execution as a deliberate design choice."
        ),
        "when_not_to_use": (
            "Disable simultaneous execution for stateful or large batches "
            "where ordering or shared state matters. Parallel fan-out is "
            "incompatible with strict ordering."
        ),
        "verification_status": "live_verified",
        "capability_status": "emittable_today",
        "category": "process_tuning",
        "mutual_exclusion": [
            "Parallel Flow Control fan-out is incompatible with the strict "
            "ordering of reliable_and_sequential_messaging — choosing "
            "parallelism gives up first-in-first-out order."
        ],
        "cross_refs": [
            "reliable_and_sequential_messaging",
            "process_mode_and_options_selection",
            "content_based_routing",
        ],
        "provenance": "live_verified",
    },
    {
        "name": "config_externalization",
        "problem": (
            "Per-environment values baked into components force rebuilds to "
            "promote between environments. Externalizing them is a design "
            "output, not an afterthought."
        ),
        "boomi_shape_mapping": (
            "Environment extensions as the override surface for connection "
            "fields, operation fields, dynamic and process properties, "
            "object definitions, data maps, cross-reference tables, and "
            "encryption certificates; externalized business rules via a "
            "Business Rules component plus cross-reference tables. Model "
            "overridable values as process or extended process properties."
        ),
        "when_to_use": (
            "Any value that differs per environment or per tenant, or a "
            "business rule that changes without a redeploy. One connection "
            "per endpoint per environment — no duplicate test/prod copies "
            "(a licensing-cost driver); a fleet-wide change is one "
            "environment-level extension plus a single redeploy. The typed "
            "database-to-API builder now declares the source connection's "
            "credential fields as override points by default, so promotion "
            "supplies the credential per environment rather than baking it in."
        ),
        "when_not_to_use": (
            "Operation static values and literals are not extendable — do "
            "not design as if they were. Truly constant values do not need "
            "externalization."
        ),
        "verification_status": "live_verified",
        "capability_status": "emittable_today",
        "category": "governance",
        "mutual_exclusion": [],
        "cross_refs": ["component_profile_reuse", "platform_selection"],
        "provenance": "live_verified",
    },
    {
        "name": "component_profile_reuse",
        "problem": (
            "Near-duplicate profiles and connections drift apart over time, "
            "breaking maps and multiplying maintenance."
        ),
        "boomi_shape_mapping": (
            "One profile shared across wrapper and subprocess boundaries and "
            "across operations of the same structure; reuse existing "
            "profiles and connections over authoring near-duplicates. The "
            "decision is made at design time (reuse over clone)."
        ),
        "when_to_use": (
            "Whenever two components would have the same structure — reuse "
            "the existing one to keep maps consistent and prevent drift."
        ),
        "when_not_to_use": (
            "When structures genuinely differ, a shared profile forces "
            "awkward optional fields — model them separately."
        ),
        "verification_status": "companion_unverified",
        "capability_status": "emittable_today",
        "category": "decomposition",
        "mutual_exclusion": [],
        "cross_refs": ["config_externalization", "wrapper_subprocess_separation"],
        "provenance": "companion_unverified",
    },
    {
        "name": "connector_selection",
        "problem": (
            "The right connector depends on protocol, direction, and whether "
            "a branded application connector or a technology connector fits."
        ),
        "boomi_shape_mapping": (
            "Technology connectors (HTTP Client, Database) versus branded "
            "application connectors; a REST client versus a raw HTTP Client. "
            "Operation direction is a deliberate axis (fetch/get, send, or "
            "passive listen) with a matching terminator. Protocol bridging "
            "is a source connector plus a target connector plus a Map — no "
            "single connector crosses protocols."
        ),
        "when_to_use": (
            "At design time when choosing how to reach an endpoint. Some "
            "branded connectors require GUI-side OAuth authorization — name "
            "that capability boundary honestly."
        ),
        "when_not_to_use": (
            "Listener connectors cannot run Test Mode — do not pick a "
            "listener where a front test source is needed (see "
            "test_mode_workaround_for_listener_connectors). The branded REST "
            "connector is not always the right default over a raw HTTP "
            "client."
        ),
        "verification_status": "companion_unverified",
        "capability_status": "guidance_only",
        "category": "decomposition",
        "mutual_exclusion": [],
        "cross_refs": [
            "platform_selection",
            "test_mode_workaround_for_listener_connectors",
        ],
        "provenance": "companion_unverified",
    },
    {
        "name": "platform_selection",
        "problem": (
            "Forcing every requirement into a linear Integration process "
            "ignores better-fit Boomi platforms and the MCP's build-scope "
            "boundaries."
        ),
        "boomi_shape_mapping": (
            "Recommend the right platform conceptually: Event Streams for "
            "publish/subscribe and async queuing; Flow for human workflow; "
            "DataHub for master data; MFT, B2B/EDI, and API Management for "
            "their domains; a web-services listener for real-time APIs "
            "(low-latency versus general); the MCP Server connector where it "
            "fits. Service versioning rides a base API path with per-env "
            "deploy."
        ),
        "when_to_use": (
            "Early, when the requirement may be served better outside a "
            "single process — human-in-the-loop, pub/sub, MDM, "
            "fire-and-forget, or design-first API management."
        ),
        "when_not_to_use": (
            "Do not reach for a heavier platform when a simple scheduled "
            "process suffices. State MCP build-scope boundaries honestly "
            "rather than implying the MCP can build every platform."
        ),
        "verification_status": "companion_unverified",
        "capability_status": "guidance_only",
        "category": "decomposition",
        "mutual_exclusion": [],
        "cross_refs": [
            "connector_selection",
            "async_queue_decoupling",
            "microservice_vs_monolith_decomposition",
        ],
        "provenance": "companion_unverified",
    },
    # =====================================================================
    # 16 NET-NEW design patterns (spec §6.1).
    # =====================================================================
    {
        "name": "process_route_fanout",
        "problem": (
            "A feed carrying many record types, or a volatile set of types, "
            "needs per-type handling without one giant branching process."
        ),
        "boomi_shape_mapping": (
            "A thin main process derives a route key and dispatches to a "
            "per-type passthrough subprocess via Process Route, so each "
            "route deploys independently; a few static routes can use plain "
            "Process Call instead."
        ),
        "when_to_use": (
            "Multi-record-type or volatile feeds where types are added over "
            "time and each deserves independent deployment."
        ),
        "when_not_to_use": (
            "A small fixed number of routes — plain Process Call is simpler "
            "and avoids the route-registry overhead."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "routing",
        "mutual_exclusion": [],
        "cross_refs": [
            "content_based_routing",
            "wrapper_subprocess_separation",
            "inline_vs_branch_cache_invocation",
        ],
        "provenance": "course_unverified",
    },
    {
        "name": "process_mode_and_options_selection",
        "problem": (
            "Process execution mode and run options change throughput, "
            "latency, and observability — picking them by default leaves "
            "performance on the table or breaks latency budgets."
        ),
        "boomi_shape_mapping": (
            "Pick General, Bridge, or Low-Latency mode according to the "
            "workload (volume, a sub-thirty-second budget, logging needs); "
            "tune simultaneous execution, run-date capture, and error-only "
            "logging to match."
        ),
        "when_to_use": (
            "When the workload has a clear latency budget or volume profile "
            "that a non-default mode serves better."
        ),
        "when_not_to_use": (
            "Low-Latency mode forbids persisted properties, heavy I/O, and "
            "heavy notification on the hot path — do not select it for "
            "stateful or I/O-bound flows."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "process_tuning",
        "mutual_exclusion": [
            "Low-Latency mode is mutually exclusive with persisted state, "
            "heavy I/O, and heavy notification on the hot path — choosing "
            "low latency gives those up."
        ],
        "cross_refs": ["combine_split_flow_control", "notification_logging"],
        "provenance": "course_unverified",
    },
    {
        "name": "business_rules_vs_decision",
        "problem": (
            "Choosing the wrong evaluation shape silently changes "
            "correctness: one evaluates every record, the other only the "
            "first."
        ),
        "boomi_shape_mapping": (
            "Business Rules evaluates every record; a Decision evaluates only "
            "the first. Consolidate multi-condition logic in one Business "
            "Rules step. The Decision shape is builder-emittable today "
            "(value-comparing true/false routing with an optional false-path "
            "notify and a backward loop edge); Business Rules remains design "
            "guidance."
        ),
        "when_to_use": (
            "Use Business Rules when each record must be evaluated; use a "
            "Decision only for a single per-document branch (one value "
            "comparison choosing the true or false path)."
        ),
        "when_not_to_use": (
            "Do not use a Decision to filter or classify a multi-record "
            "batch — it inspects only the first record and silently passes "
            "the rest."
        ),
        "verification_status": "live_verified",
        "capability_status": "emittable_today",
        "category": "routing",
        "mutual_exclusion": [],
        "cross_refs": ["content_based_routing"],
        "provenance": "live_verified",
    },
    {
        "name": "async_queue_decoupling",
        "problem": (
            "Coupling a producer directly to a consumer makes one fail when "
            "the other is slow or down, and is hard to scale independently."
        ),
        "boomi_shape_mapping": (
            "An internal Atom Queue or Event Streams (or an external message "
            "broker) as an intermediary between producer and consumer, "
            "preferred over disk or database staging for this decoupling "
            "role."
        ),
        "when_to_use": (
            "To decouple producer and consumer cadence, absorb bursts, or "
            "isolate microservice failures."
        ),
        "when_not_to_use": (
            "A simple synchronous request/response with no burst or "
            "isolation need does not warrant a queue's operational "
            "overhead."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "messaging",
        "mutual_exclusion": [],
        "cross_refs": [
            "reliable_and_sequential_messaging",
            "microservice_vs_monolith_decomposition",
            "platform_selection",
        ],
        "provenance": "course_unverified",
    },
    {
        "name": "reliable_and_sequential_messaging",
        "problem": (
            "Guaranteed delivery and strict ordering are topologies, not "
            "flags — naive single-process sends lose messages or reorder "
            "them."
        ),
        "boomi_shape_mapping": (
            "Guaranteed delivery is a two-process topology: one persists and "
            "acknowledges, a separate drainer retries failed sends. Ordering "
            "replays failed messages before current ones so sequence is "
            "preserved."
        ),
        "when_to_use": (
            "When loss is unacceptable (guaranteed delivery) or downstream "
            "requires strict order (first-in-first-out)."
        ),
        "when_not_to_use": (
            "When throughput matters more than order — strict ordering "
            "serializes processing and forfeits parallelism."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "reliability",
        "mutual_exclusion": [
            "Strict first-in-first-out ordering requires serialized "
            "processing — incompatible with the parallel fan-out promoted by "
            "combine_split_flow_control; choosing ordering gives up that "
            "parallelism."
        ],
        "cross_refs": [
            "combine_split_flow_control",
            "async_queue_decoupling",
            "connector_retry_design",
        ],
        "provenance": "course_unverified",
    },
    {
        "name": "transaction_saga_compensation",
        "problem": (
            "There is no distributed commit across endpoints, so a "
            "multi-endpoint change can leave the system half-updated on "
            "failure."
        ),
        "boomi_shape_mapping": (
            "Persist prior state before each mutation and run a compensating "
            "rollback on failure — an application-level saga that undoes "
            "completed steps when a later step fails."
        ),
        "when_to_use": (
            "When a logical unit of work spans multiple endpoints that must "
            "be all-or-nothing and none offers a shared transaction."
        ),
        "when_not_to_use": (
            "When a single endpoint or a native transaction already "
            "guarantees atomicity — compensation adds needless complexity."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "reliability",
        "mutual_exclusion": [],
        "cross_refs": ["idempotency_and_duplicates", "state_persistence_parking_lot"],
        "provenance": "course_unverified",
    },
    {
        "name": "data_confidentiality_layering",
        "problem": (
            "Assuming transport security protects the payload leaves "
            "sensitive data exposed at rest or to intermediaries."
        ),
        "boomi_shape_mapping": (
            "Channel security (transport encryption) and in-payload "
            "encryption are distinct design decisions applied at different "
            "layers; choose each on its own merits."
        ),
        "when_to_use": (
            "When data is sensitive end-to-end or passes intermediaries that "
            "should not read it — encrypt the payload in addition to the "
            "channel."
        ),
        "when_not_to_use": (
            "Do not assume transport encryption alone protects content at "
            "rest or past a terminating proxy."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "security",
        "mutual_exclusion": [],
        "cross_refs": ["config_externalization"],
        "provenance": "course_unverified",
    },
    {
        "name": "state_persistence_parking_lot",
        "problem": (
            "One-way asynchronous flows with large or hard-to-recreate "
            "payloads need somewhere durable to hold and re-drive work and "
            "report status."
        ),
        "boomi_shape_mapping": (
            "A keyed database parking-lot table that holds payloads for "
            "reprocessing and status reporting, separate from in-flow "
            "watermarking; the cost is a keyed table plus archiving."
        ),
        "when_to_use": (
            "One-way fire-and-forget flows where the source cannot easily "
            "resupply a failed payload and operators need reprocess/status "
            "visibility."
        ),
        "when_not_to_use": (
            "When the source can be re-queried cheaply — an in-flow "
            "watermark is lighter than a parking-lot table."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "sync",
        "mutual_exclusion": [],
        "cross_refs": ["incremental_watermark", "idempotency_and_duplicates"],
        "provenance": "course_unverified",
    },
    {
        "name": "change_data_capture_strategy",
        "problem": (
            "When a source exposes only its full current dataset, detecting "
            "what changed requires a strategy — and the wrong one deletes "
            "live data."
        ),
        "boomi_shape_mapping": (
            "Cache-diff the full dataset against the prior snapshot to derive "
            "changes — but only for master data with a whole-dataset "
            "guarantee; a partial dataset would be read as mass deletions. "
            "Distributed runtimes must externalize the change state."
        ),
        "when_to_use": (
            "Master-data sources that return the whole dataset and offer no "
            "native change feed or watermark."
        ),
        "when_not_to_use": (
            "Sources that can ever return a partial dataset (the diff reads "
            "the missing rows as deletes), or where a status flag or "
            "last-modified watermark is available — prefer those first."
        ),
        "verification_status": "course_unverified",
        "capability_status": "gated",
        "category": "sync",
        "mutual_exclusion": [],
        "cross_refs": ["incremental_watermark", "bidirectional_sync_conflict_and_circularity"],
        "provenance": "course_unverified",
    },
    {
        "name": "bidirectional_sync_conflict_and_circularity",
        "problem": (
            "Two-way sync can loop forever and must resolve conflicting "
            "edits — both are design decisions before any field is mapped."
        ),
        "boomi_shape_mapping": (
            "A pair of one-way processes with a designated master and an "
            "ordered run sequence; a change-origin discriminator stops "
            "circular updates; conflict granularity is field-level (costs "
            "per-field origin tracking) or record-level (simpler)."
        ),
        "when_to_use": (
            "When two systems both own edits to the same records and changes "
            "must propagate both ways without looping."
        ),
        "when_not_to_use": (
            "When one system is authoritative — a single one-way sync avoids "
            "all conflict and circularity machinery."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "sync",
        "mutual_exclusion": [
            "Field-level conflict resolution costs per-field origin tracking "
            "and throughput; record-level is simpler but coarser — the two "
            "granularities are mutually exclusive per record."
        ],
        "cross_refs": ["change_data_capture_strategy", "incremental_watermark"],
        "provenance": "course_unverified",
    },
    {
        "name": "api_pagination_contract",
        "problem": (
            "A published API returning a large dataset in one response is "
            "fragile; pagination must be a stateless contract the client "
            "drives."
        ),
        "boomi_shape_mapping": (
            "Each response returns a page plus a pagination flag; the client "
            "echoes the flag and a counter on the next call — stateless, "
            "client-driven paging with no server-held cursor."
        ),
        "when_to_use": (
            "Designing a published API over a large or unbounded result "
            "set."
        ),
        "when_not_to_use": (
            "Small bounded result sets that fit one response do not need a "
            "pagination contract."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "routing",
        "mutual_exclusion": [],
        "cross_refs": ["platform_selection"],
        "provenance": "course_unverified",
    },
    {
        "name": "cross_cutting_framework_services",
        "problem": (
            "Notification, metadata collection, and platform-API access are "
            "re-implemented per integration when they should be shared "
            "services."
        ),
        "boomi_shape_mapping": (
            "Standard reusable subprocess services in every integration: an "
            "asynchronous notification service invoked off the catch leg, a "
            "synchronous-first metadata-collection service, and an "
            "in-process platform-API service."
        ),
        "when_to_use": (
            "Across an account's integrations, to standardize the cross-"
            "cutting concerns every process needs."
        ),
        "when_not_to_use": (
            "A single throwaway integration may not justify standing up the "
            "shared framework first."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "decomposition",
        "mutual_exclusion": [],
        "cross_refs": [
            "wrapper_subprocess_separation",
            "notification_logging",
            "migration_pattern_templating",
        ],
        "provenance": "course_unverified",
    },
    {
        "name": "inline_vs_branch_cache_invocation",
        "problem": (
            "How a parent hands off to a subprocess trades performance "
            "against input/output guardrails — the wrong default costs speed "
            "or safety."
        ),
        "boomi_shape_mapping": (
            "In-line invocation (Process Call) for performance and "
            "orchestration; a Branch with a cache when input/output "
            "guardrails are needed. The guarded path is materially slower "
            "and uses more memory. Default to in-line."
        ),
        "when_to_use": (
            "Default in-line for orchestration; choose the guarded "
            "branch/cache path only when you need strict input/output "
            "isolation."
        ),
        "when_not_to_use": (
            "Do not pay the guarded path's speed and memory cost where "
            "in-line orchestration is sufficient."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "decomposition",
        "mutual_exclusion": [
            "In-line invocation maximizes performance but forgoes the "
            "input/output guardrails of the branch/cache path — the two are "
            "mutually exclusive for a given handoff."
        ],
        "cross_refs": ["wrapper_subprocess_separation", "caching_lookup_join"],
        "provenance": "course_unverified",
    },
    {
        "name": "microservice_vs_monolith_decomposition",
        "problem": (
            "Splitting an integration estate into many small services adds "
            "operational complexity that not every team can absorb."
        ),
        "boomi_shape_mapping": (
            "Small loosely-coupled services (REST plus Atom Queues between "
            "them), each with a runtime topology matched to its needs — "
            "adopted only when the team can run the added operational "
            "surface."
        ),
        "when_to_use": (
            "When independent scaling, deployment, and failure isolation are "
            "worth the operational cost and the team can sustain it."
        ),
        "when_not_to_use": (
            "When the team cannot absorb the operational complexity — a "
            "well-decomposed monolith of subprocesses is the safer default."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "decomposition",
        "mutual_exclusion": [],
        "cross_refs": [
            "wrapper_subprocess_separation",
            "async_queue_decoupling",
            "platform_selection",
        ],
        "provenance": "course_unverified",
    },
    {
        "name": "native_over_custom_scripting",
        "problem": (
            "Custom scripting is reached for when native functionality would "
            "do, raising maintenance cost and obscuring intent."
        ),
        "boomi_shape_mapping": (
            "Prefer native functionality — map functions, execution-parameter "
            "set-property steps, and the native profile-driven Data Process "
            "Split Documents / Combine Documents operations for document "
            "cardinality changes (one-to-many and many-to-one; the split "
            "operation is live-captured, the combine operation is reconciled "
            "from the companion reference rather than a live capture) — over custom "
            "scripts; the design-level companion of the operating doctrine's "
            "no-throwaway-scripts rule. A cardinality change in particular is a "
            "native operation, never a reason to reach for a script. When a "
            "script is genuinely required, the process-level Data Process Custom "
            "Scripting shape is the supported escape hatch, and it is emittable "
            "today by the typed builder (live-verified against a real account "
            "export)."
        ),
        "when_to_use": (
            "Whenever a native map function, step, or Data Process operation "
            "(including profile-driven Split / Combine for cardinality changes) "
            "expresses the logic — choose it over a script."
        ),
        "when_not_to_use": (
            "Genuinely novel logic with no native equivalent may need a "
            "script; keep it small and justified. A split or combine is a "
            "native operation and never qualifies. The Data Process Custom "
            "Scripting shape that carries a justified script is emittable today, "
            "so reserve it for that case rather than reaching for it by default."
        ),
        "verification_status": "live_verified",
        "capability_status": "emittable_today",
        "category": "process_tuning",
        "mutual_exclusion": [],
        "cross_refs": ["wrapper_subprocess_separation"],
        "provenance": "live_verified",
    },
    {
        "name": "migration_pattern_templating",
        "problem": (
            "Migrating many legacy interfaces ad hoc duplicates effort and "
            "loses consistency; a template-driven rebuild scales it."
        ),
        "boomi_shape_mapping": (
            "Rebuild native off the legacy system: survey interfaces into "
            "common technical patterns, build one reusable template per "
            "pattern, front-load shared framework and error subprocesses, "
            "and migrate stateful watermark and counter values at cutover. "
            "The client contract is a fixed constraint."
        ),
        "when_to_use": (
            "A migration program with many interfaces that fall into a small "
            "number of technical patterns."
        ),
        "when_not_to_use": (
            "A one-off migration of a single interface does not need a "
            "template library."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "migration",
        "mutual_exclusion": [],
        "cross_refs": [
            "wrapper_subprocess_separation",
            "cross_cutting_framework_services",
            "incremental_watermark",
        ],
        "provenance": "course_unverified",
    },
    # =====================================================================
    # 8 TESTING / OBSERVABILITY entries (spec §6.3). One observability entry
    # (document_tracking_as_monitoring) is consolidated, not triplicated.
    # =====================================================================
    {
        "name": "document_tracking_as_monitoring",
        "problem": (
            "Production observability of business records is a design plane, "
            "not an afterthought — without it, operators cannot find or "
            "trace a specific record's run."
        ),
        "boomi_shape_mapping": (
            "Account-level tracked fields capturing business identifiers ARE "
            "the production observability plane, read through Process "
            "Reporting; distinct from notification alerting. Defined once and "
            "referenced by the reliability and testing entries."
        ),
        "when_to_use": (
            "Every production integration: track the business identifiers "
            "operators search by so a record's path is traceable."
        ),
        "when_not_to_use": (
            "Do not overload tracked fields as an alerting channel — that is "
            "notification_logging's role — nor as durable state."
        ),
        "verification_status": "course_unverified",
        "capability_status": "emittable_today",
        "category": "observability",
        "mutual_exclusion": [],
        "cross_refs": [
            "notification_logging",
            "idempotency_and_duplicates",
            "regression_test_path_coverage",
            "test_harness_process_pattern",
        ],
        "provenance": "course_unverified",
    },
    {
        "name": "unit_testing_via_swappable_data_source",
        "problem": (
            "A workflow welded to its inbound source cannot be exercised in "
            "isolation, so the core logic is hard to unit test."
        ),
        "boomi_shape_mapping": (
            "Split the inbound source away from the main workflow so the "
            "core subprocess can run in Test Mode against a swappable data "
            "source — testability as a driver of decomposition."
        ),
        "when_to_use": (
            "Whenever the core logic should be testable independent of the "
            "live source — reinforces wrapper_subprocess_separation."
        ),
        "when_not_to_use": (
            "A trivial pass-through with no logic to isolate gains little "
            "from the split."
        ),
        "verification_status": "course_unverified",
        "capability_status": "gated",
        "category": "testing",
        "mutual_exclusion": [],
        "cross_refs": [
            "wrapper_subprocess_separation",
            "test_mode_workaround_for_listener_connectors",
            "document_tracking_as_monitoring",
        ],
        "provenance": "course_unverified",
    },
    {
        "name": "test_mode_workaround_for_listener_connectors",
        "problem": (
            "Listener connectors cannot run Test Mode, so a listener-driven "
            "process cannot be exercised directly."
        ),
        "boomi_shape_mapping": (
            "Front the listener with a parallel test process (disk or "
            "Message based) that feeds the SAME main subprocess, so the "
            "logic is testable without the listener — a testability driver "
            "of decomposition."
        ),
        "when_to_use": (
            "Any listener-triggered integration that must be tested before "
            "deployment."
        ),
        "when_not_to_use": (
            "Non-listener triggers that already run in Test Mode do not need "
            "the front process."
        ),
        "verification_status": "course_unverified",
        "capability_status": "gated",
        "category": "testing",
        "mutual_exclusion": [],
        "cross_refs": [
            "connector_selection",
            "unit_testing_via_swappable_data_source",
        ],
        "provenance": "course_unverified",
    },
    {
        "name": "test_harness_process_pattern",
        "problem": (
            "Ad hoc testing misses cases; a structured harness makes "
            "coverage repeatable, and is the only way to test listeners."
        ),
        "boomi_shape_mapping": (
            "A harness process: known input data, initialization, invoke the "
            "process under test, assert outcomes, and include negative "
            "cases."
        ),
        "when_to_use": (
            "For any non-trivial process, and mandatorily for "
            "listener-driven ones that cannot self-test."
        ),
        "when_not_to_use": (
            "A throwaway proof-of-concept may defer a full harness until it "
            "is promoted."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "testing",
        "mutual_exclusion": [],
        "cross_refs": [
            "mock_endpoint_process_design",
            "test_suite_master_process_automation",
            "regression_test_path_coverage",
            "document_tracking_as_monitoring",
        ],
        "provenance": "course_unverified",
    },
    {
        "name": "mock_endpoint_process_design",
        "problem": (
            "Tests that hit live endpoints are slow, flaky, and have side "
            "effects; a mock endpoint isolates the process under test."
        ),
        "boomi_shape_mapping": (
            "Boomi web-service processes that simulate endpoint responses "
            "and status codes, standing in for real endpoints during "
            "testing."
        ),
        "when_to_use": (
            "When the process under test calls endpoints that are "
            "unavailable, expensive, or have side effects in test."
        ),
        "when_not_to_use": (
            "When a safe sandbox of the real endpoint exists and gives "
            "higher-fidelity coverage."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "testing",
        "mutual_exclusion": [],
        "cross_refs": ["test_harness_process_pattern"],
        "provenance": "course_unverified",
    },
    {
        "name": "test_suite_master_process_automation",
        "problem": (
            "Individual harnesses run by hand do not scale; a master suite "
            "automates them."
        ),
        "boomi_shape_mapping": (
            "A master process that aggregates individual harnesses and is "
            "driven via the Execute Process API or an external test runner."
        ),
        "when_to_use": (
            "When a body of harnesses should run together as regression "
            "automation."
        ),
        "when_not_to_use": (
            "A single harness under active development does not yet need "
            "suite automation."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "testing",
        "mutual_exclusion": [],
        "cross_refs": [
            "test_harness_process_pattern",
            "regression_test_path_coverage",
        ],
        "provenance": "course_unverified",
    },
    {
        "name": "regression_test_path_coverage",
        "problem": (
            "Tests that exercise only the happy path miss regressions in "
            "branches and error legs."
        ),
        "boomi_shape_mapping": (
            "A test corpus covering every logical path through the process, "
            "verifying end-system state rather than only the process "
            "return."
        ),
        "when_to_use": (
            "Before each change to a production integration, to catch "
            "regressions across all branches."
        ),
        "when_not_to_use": (
            "Early prototyping where the path set is still churning may defer "
            "full coverage."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "testing",
        "mutual_exclusion": [],
        "cross_refs": [
            "test_harness_process_pattern",
            "document_tracking_as_monitoring",
        ],
        "provenance": "course_unverified",
    },
    {
        "name": "parallel_day_in_the_life_testing",
        "problem": (
            "A new integration may behave differently under real production "
            "load and data than in isolated tests."
        ),
        "boomi_shape_mapping": (
            "Run the new process beside the legacy one in a production-like "
            "environment where only the extensions differ, comparing "
            "outcomes over a representative period."
        ),
        "when_to_use": (
            "Cutover of a business-critical integration where parity with "
            "the incumbent must be proven before switchover."
        ),
        "when_not_to_use": (
            "A greenfield integration with no incumbent to run beside, or a "
            "low-risk change."
        ),
        "verification_status": "course_unverified",
        "capability_status": "guidance_only",
        "category": "testing",
        "mutual_exclusion": [],
        "cross_refs": [
            "migration_pattern_templating",
            "config_externalization",
            "regression_test_path_coverage",
        ],
        "provenance": "course_unverified",
    },
]


# ---------------------------------------------------------------------------
# Corroboration backlog (issue #86 acceptance criterion). These behavioral
# claims stay labeled ``course_unverified`` until corroborated via
# ``search_boomi_docs`` / live QA. Most of the underlying mechanics are routed
# OUT to the gotcha KB (#77); doctrine keeps only the labeled decision.
# ---------------------------------------------------------------------------

CORROBORATION_BACKLOG: List[Dict[str, str]] = [
    {
        "claim": "Try/Catch Retry Count ranges 0..5; a count of one retries "
        "immediately and two to five apply the platform's built-in escalating "
        "wait schedule (no caller-selected backoff interval).",
        "entry": "connector_retry_design",
        "status": "docs_corroborated",
        "docs_page_key": "https://help.boomi.com/docs/Atomsphere/Integration/"
        "Process%20building/r-atm-Try_Catch_shape_7b3dd8df-426e-4ed7-824a-40cc0b5dc68d",
        "verification": "search_boomi_docs (2026-06-15): the Try/Catch shape page "
        "documents the 0..5 retry range and the built-in wait schedule; un-gated "
        "in process_flow_builder (#88 M4.5.3).",
    },
    {
        "claim": "A change-data-capture diff over a partial/test dataset reads "
        "the missing rows as mass deletions.",
        "entry": "change_data_capture_strategy",
        "status": "course_unverified",
        "verification": "search_boomi_docs (2026-06-15) returned low_confidence — "
        "the official KB does not cover this design-level CDC diff behavior; "
        "course_unverified retained.",
    },
    {
        "claim": "Return Documents returns only after the full subprocess "
        "completes (a deferred batch).",
        "entry": "combine_split_flow_control",
        "status": "docs_corroborated",
        "docs_page_key": "https://help.boomi.com/docs/Atomsphere/Integration/"
        "Process%20building/r-atm-Return_Documents_shape_61192114-0f9d-49d8-bcd0-1c8d6a843db2",
        "verification": "search_boomi_docs (2026-06-15): the Return Documents step "
        "page states documents that reach the step are 'batched and returned to "
        "the parent process' — deferred-batch return corroborated.",
    },
    {
        "claim": "A tracked field on a repeating element captures the first "
        "occurrence only.",
        "entry": "document_tracking_as_monitoring",
        "status": "course_unverified",
        "verification": "search_boomi_docs (2026-06-15) returned low_confidence — "
        "the official KB covers tracked-field setup but not the repeating-element "
        "first-occurrence behavior; course_unverified retained.",
    },
    {
        "claim": "Test-Mode extension values are runtime-scoped and shared "
        "across all users.",
        "entry": "test_mode_workaround_for_listener_connectors",
        "status": "course_unverified",
        "docs_page_key": "https://help.boomi.com/docs/Atomsphere/Integration/"
        "Process%20building/c-atm-Setting_Extension_Values_for_Use_in_Test_Mode_"
        "a3aa3a4a-03b7-488c-b70c-f145af642897",
        "verification": "search_boomi_docs (2026-06-15): the cited page corroborates "
        "the runtime-scoped half ('Test mode extension values are remembered per "
        "Runtime'), but the 'shared across all users' half is NOT stated in the KB. "
        "Because only part of the claim is documented, the full claim is retained as "
        "course_unverified pending verification of the user-sharing behavior.",
    },
]


# Index by name (insertion order preserved). Built at import so lookups are O(1)
# and a duplicate name fails loudly at import rather than silently shadowing.
def _build_index(entries: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for entry in entries:
        name = entry["name"]
        if name in index:
            raise ValueError(f"Duplicate design_doctrine entry name: {name!r}")
        index[name] = entry
    return index


DESIGN_DOCTRINE_ENTRIES: Dict[str, Dict[str, Any]] = _build_index(_ENTRIES)

#: Stable published catalog size — see module docstring for the derivation.
DESIGN_DOCTRINE_ENTRY_COUNT = len(DESIGN_DOCTRINE_ENTRIES)


# ---------------------------------------------------------------------------
# Public accessors — every accessor returns a deepcopy so per-call mutation by
# a caller never corrupts the shared module state (same discipline as
# meta_tools._authoring_workflow_sequences()).
# ---------------------------------------------------------------------------


def get_design_doctrine_catalog() -> Dict[str, Any]:
    """Full catalog payload: all entries, the count, and the entry schema."""
    return {
        "entry_count": DESIGN_DOCTRINE_ENTRY_COUNT,
        "entries": copy.deepcopy(list(DESIGN_DOCTRINE_ENTRIES.values())),
        "entry_schema": copy.deepcopy(DESIGN_DOCTRINE_ENTRY_SCHEMA),
        "corroboration_backlog": copy.deepcopy(CORROBORATION_BACKLOG),
    }


def get_design_pattern(name: str) -> Optional[Dict[str, Any]]:
    """One entry by name, or ``None`` if unknown."""
    entry = DESIGN_DOCTRINE_ENTRIES.get(name)
    return copy.deepcopy(entry) if entry is not None else None


def list_design_doctrine_index() -> List[Dict[str, str]]:
    """Compact index rows for ``list_capabilities`` — no prose, just the
    name / category / capability_status triple per entry."""
    return [
        {
            "name": entry["name"],
            "category": entry["category"],
            "capability_status": entry["capability_status"],
        }
        for entry in DESIGN_DOCTRINE_ENTRIES.values()
    ]


def valid_design_pattern_names() -> List[str]:
    """Sorted entry names — used by ``_valid_schema_names`` and the
    unknown-pattern error envelope."""
    return sorted(DESIGN_DOCTRINE_ENTRIES)
