"""Served ``operational_gotchas`` knowledge surface (issue #77, epic M9.1).

A small, curated catalog of Boomi **operational gotchas** — known silent-failure
modes and field traps that help.boomi.com does not cover and that the docs KB
deliberately excludes (``kb/manifest.py`` "Known exclusions: community posts,
support tickets, ..."). This is a *separate surface* from ``search_boomi_docs``:
operational field knowledge versus official documentation.

The corpus is intentionally **stdlib-only** (``copy`` and ``re`` are the sole
imports) so it is safe to import on every server start and never pulls in the
heavy docs-KB ML stack that lives in ``boomi_mcp.kb.service``. Exact ``issue_ids``
lookup and lexical ranking both run without any embedding dependency.

Provenance discipline (per #76 / #77 verification gate): each entry carries a
``verification_status`` — ``live_verified``, ``docs_corroborated``,
``companion_unverified``, ``disputed``, or ``course_unverified`` — and a separate
``provenance`` block naming the source label and retrieval date. Companion and
architect-course material is a *hypothesis*, not authority: claims contradicted by
independent evidence are never seeded as fact, and the two 2026-06-10 verification
passes recorded on the issue supersede the original Companion wording where they
differ (e.g. a WSS listener path appends the object name verbatim, not
sentence-cased; the parent-redeploy snapshot is taken at package-creation time;
the connector-step override is corroborated for the HTTP Client connector only).

Entries are **curated summaries** derived from OfficialBoomi Companion material
(BSD-2-Clause) plus local issue/course triage and first-party QA findings — never
wholesale verbatim copies, and never canned SQL / payload / mapping templates
(the anti-template rule, enforced by ``tests/kb/test_operational_gotchas.py``).
"""

from __future__ import annotations

import copy
import re
from typing import Any, Dict, Iterable, List, Optional

# ---------------------------------------------------------------------------
# Schema / vocabularies
# ---------------------------------------------------------------------------

#: Every catalog entry MUST carry these fields (enforced at import + by tests).
OPERATIONAL_GOTCHA_REQUIRED_FIELDS = (
    "id",
    "title",
    "symptom",
    "detection",
    "frequency",
    "root_cause",
    "wrong_pattern",
    "correct_pattern",
    "remediation",
    "applies_to",
    "provenance",
    "verification_status",
    "category",
)

#: How the failure first becomes visible to an agent or operator.
DETECTIONS = frozenset({"silent", "runtime_error", "gui_error", "design_time"})

#: Rough field frequency of the trap.
FREQUENCIES = frozenset({"very_high", "high", "medium", "low", "unknown"})

#: #76 verification vocabulary, extended with ``course_unverified`` for
#: architect-course-only claims (shared with design_doctrine #86 /
#: account_governance #93).
VERIFICATION_STATUSES = frozenset(
    {
        "live_verified",
        "docs_corroborated",
        "companion_unverified",
        "course_unverified",
        "disputed",
    }
)

#: The six issue-#77 source domains, used as the catalog facet.
CATEGORIES = frozenset(
    {
        "listener_wss",
        "platform_entities",
        "connector_behavior",
        "deployment_testing",
        "process_serialization",
        "marketplace",
    }
)

#: JSON-schema-shaped description of one entry, returned alongside the catalog so
#: callers (and tests) share one schema source. ``provenance`` is a structured
#: block (source label + retrieval date) — distinct from ``verification_status``,
#: which is the trust label.
GOTCHA_ENTRY_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "title": {"type": "string"},
        "symptom": {
            "type": "string",
            "description": "What the agent/operator observes — the surface "
            "behavior, before the cause is known.",
        },
        "detection": {"enum": sorted(DETECTIONS)},
        "frequency": {"enum": sorted(FREQUENCIES)},
        "root_cause": {"type": "string"},
        "wrong_pattern": {
            "type": "string",
            "description": "The antipattern that triggers the gotcha, in prose "
            "— never a reusable code/SQL/XML/payload template.",
        },
        "correct_pattern": {
            "type": "string",
            "description": "The corrected approach, in prose.",
        },
        "remediation": {"type": "string"},
        "applies_to": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Tools / builders / component shapes the gotcha bears "
            "on — used for ranking and routing.",
        },
        "provenance": {
            "type": "object",
            "properties": {
                "source_label": {"type": "string"},
                "retrieval_date": {"type": "string"},
            },
            "required": ["source_label", "retrieval_date"],
        },
        "verification_status": {"enum": sorted(VERIFICATION_STATUSES)},
        "category": {"enum": sorted(CATEGORIES)},
    },
    "required": list(OPERATIONAL_GOTCHA_REQUIRED_FIELDS),
}


# ---------------------------------------------------------------------------
# The catalog — tranche 1. Ordered list; indexed by id below.
# Covers all six issue-#77 body domains + the 2026-06-14 architect-course
# comment (its four ★ entries: tracked-field repeating, CDC mass-delete,
# Return Documents deferred batch, Test-Mode extensions shared across users).
# 2026-06-10 verification-pass wording is authoritative where it differs from
# the original Companion phrasing.
# ---------------------------------------------------------------------------

_COMPANION_DOCS = "OfficialBoomi Companion catalog, corroborated via search_boomi_docs (#77 verification pass)"
_COMPANION_ONLY = "OfficialBoomi Companion catalog (#77), not independently corroborated"
_COURSE = "Boomi architect course triage (#77 comment)"

_ENTRIES: List[Dict[str, Any]] = [
    # ===================================================================
    # Listener / WSS (3)
    # ===================================================================
    {
        "id": "listener_no_test_mode",
        "title": "Listener and WSS connectors cannot be exercised by Test Mode",
        "symptom": (
            "A process whose start shape is a listener (Web Services Server, "
            "AS2, or another listen connector) produces nothing when run through "
            "the design-time Test Mode, and the developer concludes the process "
            "is broken when it is not."
        ),
        "detection": "design_time",
        "frequency": "high",
        "root_cause": (
            "Test Mode injects a document into the start shape, but a listener "
            "start is driven by an inbound request the test harness cannot "
            "originate, so the listen path is never entered."
        ),
        "wrong_pattern": (
            "Validating an inbound listener process by clicking Test and "
            "treating the empty result as a defect, or worse, wiring a "
            "throwaway data source into the live process to make Test produce "
            "output and then forgetting to remove it."
        ),
        "correct_pattern": (
            "Exercise listener processes through a dedicated test-harness "
            "process that calls the logic as a subprocess with seeded data, or "
            "with a permanent wrapper flag that swaps the listener start for a "
            "static source only when an isolation flag is set — never by "
            "mutating the production process."
        ),
        "remediation": (
            "Build the isolation harness once and keep it; route real "
            "verification through it rather than design-time Test Mode."
        ),
        "applies_to": [
            "listener_connector",
            "web_services_server",
            "execute_process",
            "test_mode",
        ],
        "provenance": {"source_label": _COMPANION_ONLY, "retrieval_date": "2026-06-10"},
        "verification_status": "companion_unverified",
        "category": "listener_wss",
    },
    {
        "id": "wss_path_objectname_verbatim",
        "title": "WSS listener endpoint path appends the object name verbatim",
        "symptom": (
            "A deployed Web Services Server listener returns HTTP 404 for every "
            "request even though the process deployed cleanly, because the "
            "caller is hitting a guessed URL that does not match the generated "
            "endpoint path."
        ),
        "detection": "gui_error",
        "frequency": "medium",
        "root_cause": (
            "The simple-path endpoint is the fixed prefix ws/simple followed by "
            "the lowercased operation type and the object name appended exactly "
            "as written; the object name is NOT sentence-cased by the platform "
            "(sentence-casing is only a naming convention some teams adopt)."
        ),
        "wrong_pattern": (
            "Assuming the listener URL re-cases the object name, or that the "
            "operation type is preserved in mixed case, and publishing a caller "
            "URL that the runtime never serves."
        ),
        "correct_pattern": (
            "Derive the route from the actual rule: lowercase the operation "
            "type, append the object name byte-for-byte as configured, and "
            "confirm the live path before publishing it. The valid operation "
            "types are Get, Query, Create, Update, Upsert, Delete, and Execute."
        ),
        "remediation": (
            "Read the deployed listener's reported path rather than "
            "reconstructing it from the object's display name."
        ),
        "applies_to": [
            "web_services_server",
            "listener_connector",
            "shared_web_server",
        ],
        "provenance": {"source_label": _COMPANION_DOCS, "retrieval_date": "2026-06-10"},
        "verification_status": "docs_corroborated",
        "category": "listener_wss",
    },
    {
        "id": "wss_listener_concurrency_http_500",
        "title": "A Web Services Server listener at default options returns HTTP 500 under concurrent calls",
        "symptom": (
            "A deployed Web Services Server listener process that passed "
            "single-request testing returns HTTP 500 errors in production as "
            "soon as two requests arrive at the same time."
        ),
        "detection": "runtime_error",
        "frequency": "medium",
        "root_cause": (
            "A Web Services Server listener process that does not permit "
            "simultaneous executions rejects an overlapping concurrent request "
            "with HTTP 500 while one execution is already in progress; the "
            "general-mode defaults are the bad combination for a WSS listener "
            "that must serve parallel callers."
        ),
        "wrong_pattern": (
            "Deploying a Web Services Server listener at its default execution "
            "options and assuming it tolerates parallel callers because "
            "single-request tests passed."
        ),
        "correct_pattern": (
            "Allow simultaneous executions on a Web Services Server listener "
            "that must serve parallel callers, and design the process to be safe "
            "under concurrency. Other listener types differ — the HTTP 500 "
            "behavior is specific to the Web Services Server, so confirm each "
            "connector's own concurrency semantics rather than assuming it."
        ),
        "remediation": (
            "Enable simultaneous executions for a Web Services Server listener "
            "expecting parallel traffic, and load-test concurrently before "
            "release."
        ),
        "applies_to": ["web_services_server", "shared_web_server", "manage_deployment"],
        "provenance": {"source_label": _COMPANION_DOCS, "retrieval_date": "2026-06-10"},
        "verification_status": "docs_corroborated",
        "category": "listener_wss",
    },
    # ===================================================================
    # Platform entities (3)
    # ===================================================================
    {
        "id": "tracked_field_repeating_first_occurrence",
        "title": "A tracked field on a repeating element captures only the first occurrence",
        "symptom": (
            "Process Reporting shows a single value for a tracked field that "
            "should reflect many rows of a document, so downstream auditing "
            "silently under-reports because only the first occurrence was kept."
        ),
        "detection": "silent",
        "frequency": "medium",
        "root_cause": (
            "A tracked field bound to an element that repeats within a document "
            "records only that element's first occurrence; there is no error and "
            "no indication the remaining occurrences were dropped."
        ),
        "wrong_pattern": (
            "Pointing a tracked field at a line-item or other repeating element "
            "expecting it to capture every occurrence for monitoring."
        ),
        "correct_pattern": (
            "Track a non-repeating header-level value, or aggregate the "
            "repeating data into a single field before tracking it, and treat "
            "tracked fields as scalar monitoring keys rather than collectors."
        ),
        "remediation": (
            "Move the tracked binding to a unique header field; if per-row "
            "capture is required, persist it through reporting designed for "
            "collections instead."
        ),
        "applies_to": ["tracked_field", "process_reporting", "document_tracking"],
        "provenance": {"source_label": _COURSE, "retrieval_date": "2026-06-14"},
        "verification_status": "course_unverified",
        "category": "platform_entities",
    },
    {
        "id": "component_locking_last_writer_wins",
        "title": "With component locking off, concurrent edits silently overwrite each other",
        "symptom": (
            "Two developers edit the same component around the same time and one "
            "person's changes vanish without any conflict warning — the last "
            "save silently wins."
        ),
        "detection": "silent",
        "frequency": "medium",
        "root_cause": (
            "Component locking is off by default; when it is off the platform "
            "does not serialize concurrent edits, so the second save overwrites "
            "the first with no merge and no notification."
        ),
        "wrong_pattern": (
            "Letting multiple authors edit shared components in an account that "
            "has never enabled component locking and assuming edits are merged."
        ),
        "correct_pattern": (
            "Enable component locking so a component opens read-only until a "
            "user explicitly takes the lock, serializing concurrent edits; pair "
            "it with revision discipline rather than cloning copies."
        ),
        "remediation": (
            "Have an administrator enable component locking at the account "
            "level; coordinate edits on hot components until it is on."
        ),
        "applies_to": ["component_locking", "manage_component"],
        "provenance": {"source_label": _COURSE, "retrieval_date": "2026-06-14"},
        "verification_status": "course_unverified",
        "category": "platform_entities",
    },
    {
        "id": "mcp_server_schema_change_redeploy",
        "title": "Changing an MCP Server connection's tool schema needs a profile reimport and redeploy",
        "symptom": (
            "After a tool's request or response schema changes on an MCP Server "
            "connection, processes that use it keep running against the stale "
            "shape and silently mis-map fields until the change is propagated."
        ),
        "detection": "silent",
        "frequency": "high",
        "root_cause": (
            "The connection caches the tool schema at import time; a schema "
            "change is not picked up until the profile is reimported, the "
            "operation is updated, and every process on that connection is "
            "redeployed."
        ),
        "wrong_pattern": (
            "Editing the upstream tool schema and assuming the running processes "
            "pick up the new shape automatically."
        ),
        "correct_pattern": (
            "On any connection-level schema change, reimport the profile, update "
            "the affected operation, and redeploy each process bound to that "
            "connection before relying on the new shape."
        ),
        "remediation": (
            "Treat an MCP Server schema change as a redeploy event for all "
            "processes on the connection, not an in-place edit."
        ),
        "applies_to": ["mcp_server_connector", "manage_deployment", "profile_reimport"],
        "provenance": {"source_label": _COMPANION_DOCS, "retrieval_date": "2026-06-10"},
        "verification_status": "docs_corroborated",
        "category": "platform_entities",
    },
    # ===================================================================
    # Connector behavior (3)
    # ===================================================================
    {
        "id": "http_client_step_overrides_document",
        "title": "HTTP Client connector-step parameters override the incoming document",
        "symptom": (
            "An HTTP Client call sends the parameters configured on the "
            "connector step instead of the document content the prior steps "
            "built, so the request body or query looks nothing like what the "
            "process produced."
        ),
        "detection": "silent",
        "frequency": "high",
        "root_cause": (
            "Parameters set on the HTTP Client connector step take precedence "
            "over the in-flight document; the platform documents a workaround "
            "precisely because this override is the default behavior."
        ),
        "wrong_pattern": (
            "Assuming the upstream document flows through to the request while "
            "also populating parameters on the connector step, then debugging a "
            "request that ignored the document."
        ),
        "correct_pattern": (
            "Decide deliberately whether the request is driven by the document "
            "or by step parameters; to send the document, leave the overriding "
            "parameters unset and shape the body upstream."
        ),
        "remediation": (
            "Audit the connector step's parameters when a request ignores "
            "upstream data; clear the ones that should come from the document."
        ),
        "applies_to": ["http_client_connector", "rest_connector", "connector_action"],
        "provenance": {"source_label": _COMPANION_DOCS, "retrieval_date": "2026-06-10"},
        "verification_status": "docs_corroborated",
        "category": "connector_behavior",
    },
    {
        "id": "env_var_literal_in_component_xml",
        "title": "An environment-variable reference is stored literally in component XML",
        "symptom": (
            "A connection or field configured to read from an environment "
            "variable sends the unresolved reference token to the endpoint "
            "instead of the intended value, and authentication or routing fails "
            "with no substitution having occurred."
        ),
        "detection": "silent",
        "frequency": "medium",
        "root_cause": (
            "The environment-variable reference is persisted verbatim in the "
            "component definition; the platform exposes no documented mechanism "
            "that resolves such a token at runtime, so it is carried through as "
            "a literal string."
        ),
        "wrong_pattern": (
            "Typing an environment-variable reference into a connection field "
            "expecting the runtime to expand it the way a shell or CI system "
            "would."
        ),
        "correct_pattern": (
            "Externalize the value through environment extensions on the "
            "deployed process, which is the supported indirection, rather than "
            "embedding an unresolved reference token in the component."
        ),
        "remediation": (
            "Replace embedded reference tokens with environment-extension-backed "
            "values and confirm the effective value after deploy."
        ),
        "applies_to": ["connection", "environment_extensions", "manage_component"],
        "provenance": {"source_label": _COMPANION_ONLY, "retrieval_date": "2026-06-10"},
        "verification_status": "companion_unverified",
        "category": "connector_behavior",
    },
    {
        "id": "cdc_partial_dataset_mass_delete",
        "title": "A CDC cache seeded from a partial or test dataset reads as mass deletes",
        "symptom": (
            "A change-data-capture run propagates a flood of deletions to the "
            "destination because records present in the source but missing from "
            "the corrupted cache snapshot are interpreted as removed."
        ),
        "detection": "silent",
        "frequency": "medium",
        "root_cause": (
            "CDC infers deletes from rows that disappear between snapshots; if "
            "the cache was first populated from a partial or test dataset, the "
            "next full run sees the absent rows as deletions and pushes them "
            "downstream."
        ),
        "wrong_pattern": (
            "Priming the CDC cache from a trimmed test extract and then pointing "
            "the same cache at production data."
        ),
        "correct_pattern": (
            "Seed the CDC baseline from the complete production dataset, and "
            "reset or rebuild the cache cleanly before switching environments so "
            "the first comparison is against a faithful snapshot."
        ),
        "remediation": (
            "If a mass-delete is observed, halt propagation, rebuild the cache "
            "from a full snapshot, and reconcile the destination."
        ),
        "applies_to": ["change_data_capture", "document_cache", "database_connector"],
        "provenance": {"source_label": _COURSE, "retrieval_date": "2026-06-14"},
        "verification_status": "course_unverified",
        "category": "connector_behavior",
    },
    # ===================================================================
    # Deployment / testing (4 — includes the Test-Mode-extensions ★ entry)
    # ===================================================================
    {
        "id": "process_call_parent_redeploy",
        "title": "A parent must be redeployed after a Process Call subprocess changes",
        "symptom": (
            "An edit to a subprocess does not take effect in production: the "
            "parent keeps executing the old subprocess logic even though the "
            "subprocess itself was saved and deployed."
        ),
        "detection": "silent",
        "frequency": "high",
        "root_cause": (
            "A Process Call subprocess is a dependent component frozen into the "
            "parent's package at package-creation time; the deployed parent "
            "ships that frozen snapshot, so a later subprocess change is invisible "
            "until the parent is repackaged and redeployed."
        ),
        "wrong_pattern": (
            "Deploying only the changed subprocess and expecting parents that "
            "call it to pick up the new behavior."
        ),
        "correct_pattern": (
            "Redeploy the parent process after changing any Process Call "
            "subprocess it invokes. The exception is a subprocess invoked through "
            "Process Route, which is not a dependent component — there you "
            "redeploy only the subprocess."
        ),
        "remediation": (
            "Track parent-child Process Call chains and repackage parents on "
            "subprocess change; do not assume a subprocess-only deploy propagates."
        ),
        "applies_to": ["process_call", "subprocess", "manage_deployment", "process_route"],
        "provenance": {"source_label": _COMPANION_DOCS, "retrieval_date": "2026-06-10"},
        "verification_status": "docs_corroborated",
        "category": "deployment_testing",
    },
    {
        "id": "subprocess_no_execution_record_passthrough",
        "title": "A Data Passthrough subprocess produces no execution record of its own",
        "symptom": (
            "Looking for a standalone execution record or process log for a "
            "subprocess invocation turns up nothing, making it look like the "
            "subprocess never ran."
        ),
        "detection": "silent",
        "frequency": "medium",
        "root_cause": (
            "Only Data Passthrough subprocesses fold their output inline into "
            "the parent's process log and get no separate execution record; "
            "non-passthrough subprocess types DO get their own record per "
            "invocation, so the no-record rule is conditional on passthrough."
        ),
        "wrong_pattern": (
            "Assuming every subprocess emits its own execution record, or "
            "concluding a passthrough subprocess failed because no separate "
            "record exists."
        ),
        "correct_pattern": (
            "For a Data Passthrough subprocess, inspect the parent's process log "
            "for the inlined output; reserve standalone-record expectations for "
            "non-passthrough subprocess types."
        ),
        "remediation": (
            "Trace passthrough subprocess behavior through the parent execution; "
            "do not stand up a separate standalone test that relies on a record "
            "that will never exist."
        ),
        "applies_to": ["subprocess", "process_call", "process_reporting"],
        "provenance": {"source_label": _COMPANION_DOCS, "retrieval_date": "2026-06-10"},
        "verification_status": "docs_corroborated",
        "category": "deployment_testing",
    },
    {
        "id": "groovy_compiles_first_execution",
        "title": "Groovy scripts compile only at the first Atom execution, not at deploy",
        "symptom": (
            "A process deploys cleanly and then fails at runtime with a script "
            "compilation error the very first time it executes on the Atom, "
            "surprising a team that treated a clean deploy as verification."
        ),
        "detection": "runtime_error",
        "frequency": "medium",
        "root_cause": (
            "A script is compiled and cached the first time it is invoked after "
            "a new deployment; a successful push or deploy never exercises the "
            "compiler, so a syntax or binding error stays hidden until first run."
        ),
        "wrong_pattern": (
            "Treating a clean deploy of a process that contains scripting as "
            "proof the scripting is valid."
        ),
        "correct_pattern": (
            "Execute the deployed process at least once against representative "
            "data so the script compiles, and read the execution log — deploy "
            "success is not behavioral verification."
        ),
        "remediation": (
            "Add a first-run smoke execution after any deploy that introduces or "
            "changes scripting, and check the log for compilation errors."
        ),
        "applies_to": ["groovy_script", "data_process_shape", "execute_process"],
        "provenance": {"source_label": _COMPANION_DOCS, "retrieval_date": "2026-06-10"},
        "verification_status": "docs_corroborated",
        "category": "deployment_testing",
    },
    {
        "id": "test_mode_extensions_shared_across_users",
        "title": "Test-Mode extension values are runtime-scoped and shared across all users",
        "symptom": (
            "A test run uses extension values someone else set earlier — for "
            "example silently hitting a production endpoint — because the "
            "override is not private to the developer who entered it."
        ),
        "detection": "silent",
        "frequency": "medium",
        "root_cause": (
            "Extension values entered for Test Mode are scoped to the runtime "
            "and shared across everyone testing on it; one person's override "
            "persists and affects later test runs by other users."
        ),
        "wrong_pattern": (
            "Assuming Test-Mode extension overrides are private per user and "
            "leaving a sensitive endpoint configured for the next tester."
        ),
        "correct_pattern": (
            "Treat Test-Mode extension values as shared runtime state: confirm "
            "the effective values before each run and reset them afterward, and "
            "never leave a production endpoint set."
        ),
        "remediation": (
            "Verify and re-set extension overrides at the start of every test "
            "session; coordinate on a shared runtime so testers do not collide."
        ),
        "applies_to": ["environment_extensions", "test_mode", "runtime"],
        "provenance": {"source_label": _COURSE, "retrieval_date": "2026-06-14"},
        "verification_status": "course_unverified",
        "category": "deployment_testing",
    },
    # ===================================================================
    # Process serialization (3)
    # ===================================================================
    {
        "id": "return_documents_deferred_batch",
        "title": "Return Documents returns to the caller only after the whole subprocess completes",
        "symptom": (
            "A caller expecting streamed or incremental results from a Return "
            "Documents step receives nothing until the entire invoked process "
            "finishes, so a long subprocess looks hung."
        ),
        "detection": "silent",
        "frequency": "medium",
        "root_cause": (
            "Return Documents is a deferred batch return, not a streaming one: "
            "documents are handed back to the caller only after the subprocess "
            "run completes in full."
        ),
        "wrong_pattern": (
            "Designing a caller around incremental delivery from Return "
            "Documents and timing out while the subprocess is still working."
        ),
        "correct_pattern": (
            "Design the caller for a single batched hand-off after the "
            "subprocess completes; if incremental delivery is required, choose a "
            "streaming or queue-based decoupling instead."
        ),
        "remediation": (
            "Size caller timeouts to the full subprocess duration, or restructure "
            "to an asynchronous queue when partial results are needed."
        ),
        "applies_to": ["return_documents", "subprocess", "process_call"],
        "provenance": {"source_label": _COMPANION_DOCS, "retrieval_date": "2026-06-10"},
        "verification_status": "docs_corroborated",
        "category": "process_serialization",
    },
    {
        "id": "empty_process_overrides_hides_extensions",
        "title": "Pushing empty process overrides hides extension declarations",
        "symptom": (
            "After a deploy, environment-visible extension declarations "
            "disappear and their values look orphaned, even though the process "
            "still references them."
        ),
        "detection": "silent",
        "frequency": "medium",
        "root_cause": (
            "Pushing an empty overrides set at the package-and-deploy boundary "
            "replaces the declared extensions, so the environment-visible "
            "declarations change; the underlying values are orphaned rather than "
            "destroyed and remain recoverable."
        ),
        "wrong_pattern": (
            "Deploying with an empty overrides payload and assuming existing "
            "extension declarations are preserved."
        ),
        "correct_pattern": (
            "Carry the existing extension declarations forward on every package "
            "and deploy rather than pushing an empty set; preserve the override "
            "layer explicitly."
        ),
        "remediation": (
            "If declarations vanish, re-supply the full overrides set on the "
            "next deploy to restore the orphaned values."
        ),
        "applies_to": ["environment_extensions", "manage_deployment", "process_overrides"],
        "provenance": {"source_label": _COMPANION_DOCS, "retrieval_date": "2026-06-10"},
        "verification_status": "docs_corroborated",
        "category": "process_serialization",
    },
    {
        "id": "edi_taglist_loop_vs_segment",
        "title": "An EDI tagList elementKey must target the loop, not a segment within it",
        "symptom": (
            "An EDI map emits more output documents than expected, or child "
            "segment data (such as the address lines under a name/address loop) "
            "is silently missing from the output, with no error raised."
        ),
        "detection": "silent",
        "frequency": "high",
        "root_cause": (
            "When the tagList elementKey points at a segment inside a qualified "
            "repeating loop instead of at the loop element itself, each loop "
            "iteration splits into a separate document AND the segment's sibling "
            "segments fall out of scope entirely — silent data loss. Omitting the "
            "tagList altogether also splits the loop into duplicated documents."
        ),
        "wrong_pattern": (
            "Pointing the elementKey at a segment within the qualified loop (or "
            "leaving the tagList off), so qualified iterations split into "
            "separate documents and the loop's sibling segments are dropped."
        ),
        "correct_pattern": (
            "Point the elementKey at the loop element's key, so the map "
            "consolidates the qualified iterations into different target fields "
            "of a single output document per transaction set."
        ),
        "remediation": (
            "Set the elementKey to the loop's key, never a segment's key, and "
            "verify against a transaction whose loop carries multiple sibling "
            "segments that all reach the output."
        ),
        "applies_to": ["edi_profile", "trading_partner", "document_routing"],
        "provenance": {"source_label": _COMPANION_ONLY, "retrieval_date": "2026-06-10"},
        "verification_status": "companion_unverified",
        "category": "process_serialization",
    },
    # ===================================================================
    # Marketplace (3)
    # ===================================================================
    {
        "id": "marketplace_recipes_not_production",
        "title": "Marketplace recipes are reference samples, not production-ready integrations",
        "symptom": (
            "A recipe installed from the Marketplace is treated as a finished "
            "integration and shipped, then fails or under-handles real data "
            "because it was only ever a reference sample."
        ),
        "detection": "design_time",
        "frequency": "medium",
        "root_cause": (
            "Marketplace recipes are illustrative starting points that omit "
            "error handling, idempotency, and hardening; they demonstrate a "
            "shape rather than provide a production-grade build."
        ),
        "wrong_pattern": (
            "Installing a Marketplace recipe and deploying it as-is for "
            "production traffic."
        ),
        "correct_pattern": (
            "Use a recipe as a design reference, then rebuild it into a governed "
            "component with the reliability, error handling, and naming the "
            "estate requires before deploying."
        ),
        "remediation": (
            "Fork the recipe into a properly authored integration and apply the "
            "design doctrine before promoting it."
        ),
        "applies_to": ["marketplace", "build_from_archetype", "recipe"],
        "provenance": {"source_label": _COMPANION_ONLY, "retrieval_date": "2026-06-10"},
        "verification_status": "companion_unverified",
        "category": "marketplace",
    },
    {
        "id": "marketplace_recipe_search_filter",
        "title": "Marketplace search returns non-recipe assets unless filtered to recipes",
        "symptom": (
            "A search for a starting integration in the Marketplace surfaces "
            "connectors and other asset types mixed in, and a non-recipe asset "
            "is mistaken for a recipe and installed."
        ),
        "detection": "design_time",
        "frequency": "low",
        "root_cause": (
            "Marketplace search spans multiple asset types; without filtering to "
            "the recipe asset type, non-recipe results are interleaved and "
            "easily mistaken for recipes."
        ),
        "wrong_pattern": (
            "Searching the Marketplace and treating any top result as a recipe "
            "without filtering by asset type."
        ),
        "correct_pattern": (
            "Filter Marketplace search to the recipe asset type before selecting "
            "a starting point, so only genuine recipes are considered."
        ),
        "remediation": (
            "Apply the recipe asset-type filter in Marketplace search and "
            "confirm the asset type before installing."
        ),
        "applies_to": ["marketplace", "recipe", "build_from_archetype"],
        "provenance": {"source_label": _COMPANION_ONLY, "retrieval_date": "2026-06-10"},
        "verification_status": "companion_unverified",
        "category": "marketplace",
    },
    {
        "id": "marketplace_bundle_install_folder_id",
        "title": "A Marketplace bundle install rejects a Base64 folder id and needs a plain numeric one",
        "symptom": (
            "A Marketplace bundle install is rejected when the folder id is "
            "supplied in the Platform API's Base64 form, or it quietly creates "
            "one oddly-named folder when a folder name containing slashes was "
            "expected to build a nested path."
        ),
        "detection": "runtime_error",
        "frequency": "low",
        "root_cause": (
            "The Bundle API requires a plain numeric folder id and rejects a "
            "Base64-encoded Platform API folder id. A supplied folder name is "
            "taken literally — slashes are part of the name, not path "
            "separators — and is ignored when a folder id is given. An HTTP 200 "
            "response alone does not confirm the install succeeded."
        ),
        "wrong_pattern": (
            "Passing the Platform API's Base64 folder id straight to the Bundle "
            "API, treating folder-name slashes as a folder path, or trusting an "
            "HTTP 200 as proof the install succeeded."
        ),
        "correct_pattern": (
            "Pass a plain numeric folder id, decoding a Base64 Platform id to "
            "its numeric portion first; do not rely on a folder name to build a "
            "nested path; and confirm the result from the returned installation "
            "status and artifact folder id rather than the HTTP status alone."
        ),
        "remediation": (
            "Decode Base64 folder ids to numeric before installing, and verify "
            "the returned installation status and folder id after the call."
        ),
        "applies_to": ["marketplace", "manage_folders", "bundle_install"],
        "provenance": {"source_label": _COMPANION_ONLY, "retrieval_date": "2026-06-10"},
        "verification_status": "companion_unverified",
        "category": "marketplace",
    },
]


# ---------------------------------------------------------------------------
# Index by id (insertion order preserved). Built at import so a duplicate id or
# an invalid entry fails loudly at import rather than at first call.
# ---------------------------------------------------------------------------


def _validate_entry(entry: Dict[str, Any]) -> None:
    for field in OPERATIONAL_GOTCHA_REQUIRED_FIELDS:
        if field not in entry:
            raise ValueError(f"operational gotcha entry missing field {field!r}: {entry.get('id')!r}")
    gid = entry["id"]
    for prose in ("title", "symptom", "root_cause", "wrong_pattern", "correct_pattern", "remediation"):
        value = entry[prose]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{gid}.{prose} must be non-empty prose")
    if entry["detection"] not in DETECTIONS:
        raise ValueError(f"{gid}.detection invalid: {entry['detection']!r}")
    if entry["frequency"] not in FREQUENCIES:
        raise ValueError(f"{gid}.frequency invalid: {entry['frequency']!r}")
    if entry["verification_status"] not in VERIFICATION_STATUSES:
        raise ValueError(f"{gid}.verification_status invalid: {entry['verification_status']!r}")
    if entry.get("category") not in CATEGORIES:
        raise ValueError(f"{gid}.category invalid: {entry.get('category')!r}")
    applies_to = entry["applies_to"]
    if not isinstance(applies_to, list) or not applies_to or not all(
        isinstance(x, str) and x.strip() for x in applies_to
    ):
        raise ValueError(f"{gid}.applies_to must be a non-empty list of strings")
    prov = entry["provenance"]
    if not isinstance(prov, dict) or not prov.get("source_label") or not prov.get("retrieval_date"):
        raise ValueError(f"{gid}.provenance must carry source_label and retrieval_date")


def _build_index(entries: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for entry in entries:
        _validate_entry(entry)
        gid = entry["id"]
        if gid in index:
            raise ValueError(f"Duplicate operational gotcha id: {gid!r}")
        index[gid] = entry
    return index


OPERATIONAL_GOTCHA_ENTRIES: Dict[str, Dict[str, Any]] = _build_index(_ENTRIES)

#: Published catalog size.
OPERATIONAL_GOTCHA_ENTRY_COUNT = len(OPERATIONAL_GOTCHA_ENTRIES)


# ---------------------------------------------------------------------------
# Lexical search — stdlib only, deterministic. No embeddings.
# ---------------------------------------------------------------------------

#: Per-field weights. id/title/symptom dominate; remediation is a faint signal.
_FIELD_WEIGHTS = {
    "title": 5,
    "id": 4,
    "symptom": 4,
    "applies_to": 3,
    "category": 2,
    "root_cause": 2,
    "wrong_pattern": 2,
    "correct_pattern": 2,
    "remediation": 1,
}

#: Tokens too generic to carry signal — dropped from the query and the index.
_STOPWORDS = frozenset(
    {
        "a", "an", "and", "the", "to", "of", "in", "on", "for", "is", "are",
        "be", "by", "or", "it", "as", "at", "with", "that", "this", "from",
        "not", "no", "does", "do", "did", "why", "how", "when", "what",
    }
)

#: Score at/above which the top hit is reported as a confident match.
_OK_THRESHOLD = 4


def _tokenize(text: str) -> List[str]:
    return [t for t in re.findall(r"[a-z0-9]+", text.lower()) if t not in _STOPWORDS]


def _entry_field_tokens(entry: Dict[str, Any]) -> Dict[str, frozenset]:
    tokens: Dict[str, frozenset] = {}
    for field in _FIELD_WEIGHTS:
        value = entry.get(field, "")
        if isinstance(value, list):
            value = " ".join(value)
        # id tokens split on underscores via the alphanumeric tokenizer.
        tokens[field] = frozenset(_tokenize(str(value)))
    return tokens


# Precompute per-entry field tokens once at import (corpus is tiny + immutable).
_ENTRY_TOKENS: Dict[str, Dict[str, frozenset]] = {
    gid: _entry_field_tokens(entry) for gid, entry in OPERATIONAL_GOTCHA_ENTRIES.items()
}


def _score(entry_tokens: Dict[str, frozenset], query_tokens: List[str]) -> int:
    score = 0
    for tok in query_tokens:
        for field, weight in _FIELD_WEIGHTS.items():
            if tok in entry_tokens[field]:
                score += weight
    return score


def _normalize_issue_ids(issue_ids: Optional[Iterable[str] | str]) -> List[str]:
    if issue_ids is None:
        return []
    if isinstance(issue_ids, str):
        issue_ids = [issue_ids]
    normalized: List[str] = []
    for raw in issue_ids:
        if raw is None:
            continue
        text = str(raw).strip()
        if text:
            normalized.append(text)
    return normalized


def search_operational_gotchas(
    query: str = "",
    top_k: int = 5,
    issue_ids: Optional[Iterable[str] | str] = None,
) -> Dict[str, Any]:
    """Search the curated operational-gotcha catalog.

    ``issue_ids`` takes precedence over ``query``: when supplied it is an exact,
    deterministic, order-preserving lookup that never requires a query and never
    fabricates unknown ids (they are reported in ``missing_issue_ids``).
    Otherwise a lexical ranking is run over the catalog. Returns a structured
    envelope; never raises for a miss. Read-only.
    """
    ids = _normalize_issue_ids(issue_ids)
    warnings: List[str] = []

    # --- Exact issue_ids lookup (precedence; no query required) -------------
    if ids:
        results: List[Dict[str, Any]] = []
        missing: List[str] = []
        seen = set()
        for gid in ids:
            entry = OPERATIONAL_GOTCHA_ENTRIES.get(gid)
            if entry is None:
                if gid not in missing:
                    missing.append(gid)
            elif gid not in seen:
                seen.add(gid)
                results.append(copy.deepcopy(entry))
        if missing:
            warnings.append(
                "Unknown gotcha id(s); no entry was fabricated: " + ", ".join(missing)
            )
        status = "ok" if results else "no_match"
        return {
            "_success": bool(results),
            "status": status,
            "mode": "issue_ids",
            "query": query,
            "issue_ids": ids,
            "results": results,
            "missing_issue_ids": missing,
            "warnings": warnings,
            "read_only": True,
        }

    # --- Empty query with no issue_ids --------------------------------------
    query_tokens = _tokenize(query or "")
    if not query_tokens:
        return {
            "_success": False,
            "status": "no_match",
            "mode": "query",
            "error": "empty_query",
            "query": query,
            "issue_ids": [],
            "results": [],
            "missing_issue_ids": [],
            "warnings": [
                "Provide a non-empty query, or pass issue_ids for an exact lookup."
            ],
            "read_only": True,
        }

    # --- Lexical ranking ----------------------------------------------------
    # Only a missing/uncoercible top_k falls back to the default; an explicit
    # value (including 0 or a negative) is clamped to the documented 1..10.
    if top_k is None:
        k = 5
    else:
        try:
            k = max(1, min(int(top_k), 10))
        except (TypeError, ValueError):
            k = 5
    scored = [
        (gid, _score(_ENTRY_TOKENS[gid], query_tokens))
        for gid in OPERATIONAL_GOTCHA_ENTRIES
    ]
    hits = [(gid, sc) for gid, sc in scored if sc > 0]
    hits.sort(key=lambda pair: (-pair[1], pair[0]))

    if not hits:
        return {
            "_success": False,
            "status": "no_match",
            "mode": "query",
            "query": query,
            "issue_ids": [],
            "results": [],
            "missing_issue_ids": [],
            "warnings": [
                "No operational gotcha matched; the catalog does not fabricate "
                "entries. Reformulate the query or browse the catalog resource."
            ],
            "read_only": True,
        }

    best = hits[0][1]
    status = "ok" if best >= _OK_THRESHOLD else "low_confidence"
    if status == "low_confidence":
        warnings.append(
            "Only weak matches were found; verify relevance before relying on "
            "these entries."
        )
    results = [copy.deepcopy(OPERATIONAL_GOTCHA_ENTRIES[gid]) for gid, _ in hits[:k]]
    return {
        "_success": True,
        "status": status,
        "mode": "query",
        "query": query,
        "issue_ids": [],
        "results": results,
        "missing_issue_ids": [],
        "warnings": warnings,
        "read_only": True,
    }


# ---------------------------------------------------------------------------
# Symptom triage — route observed failure symptoms to catalog ids (issue #78).
#
# A deterministic, stdlib-only mapping from common troubleshooting symptoms to
# existing catalog entries. troubleshoot_execution(action="error_details") uses
# this to surface known failure modes alongside logs/artifacts/dependency
# findings. Each route is (gotcha_id, [signature, ...]) where a signature is a
# tuple of lowercased substrings that must ALL appear in the symptom text for
# that signature to fire; a route matches if ANY of its signatures fire. The
# ids referenced here are asserted to exist in the catalog at import.
# ---------------------------------------------------------------------------

_SYMPTOM_ROUTES: List[tuple] = [
    # Variables/credentials/uniform-401 → an env-var reference carried verbatim.
    # The catalog has no dedicated API-auth-route entry, so auth-despite-creds
    # and uniform-401 both route here (the literal-reference gotcha is the
    # documented #78 choice for these auth symptoms).
    (
        "env_var_literal_in_component_xml",
        [
            ("variable", "literal"),       # "variables appearing literally"
            ("literally", "output"),
            ("unresolved", "variable"),
            ("unresolved", "reference"),   # "unresolved reference(s)"
            ("unresolved", "env"),         # "unresolved env refs" / "env var"
            ("unresolved", "token"),
            ("despite", "credential"),     # "auth failures despite configured credentials"
            ("auth", "despite"),
            ("uniform", "401"),            # "uniform 401 on every route"
            ("401", "every"),
        ],
    ),
    # A subprocess edit that does not take effect → parent needs a redeploy.
    (
        "process_call_parent_redeploy",
        [
            ("subprocess", "ignored"),     # "subprocess changes apparently ignored"
            ("subprocess", "not taking effect"),
            ("subprocess", "no effect"),
            ("subprocess", "didn't take effect"),
            ("subprocess", "did not take effect"),
            ("subprocess", "stale"),
        ],
    ),
    # A deployed Boomi listener returning 404 → endpoint path objectname trap.
    # Require unambiguous Boomi listener / deployed-endpoint context alongside the
    # 404. Generic tokens like "api"/"path"/"endpoint" are deliberately NOT used:
    # an outbound HTTP Client step that *receives* 404 from a third-party "api" or
    # "path" is a different failure and must not be routed here. The documented
    # symptom "404 on a deployed API" still matches via "deployed".
    (
        "wss_path_objectname_verbatim",
        [
            ("404", "deployed"),
            ("404", "listener"),
            ("404", "wss"),
            ("404", "web services server"),
            ("404", "shared web server"),
        ],
    ),
    # Extension declarations vanishing after deploy → empty-overrides push.
    (
        "empty_process_overrides_hides_extensions",
        [
            ("extension", "disappear"),    # "extension values disappearing"
            ("extension", "vanish"),
            ("extension", "orphan"),
            ("extension", "gone"),
        ],
    ),
    # Missing records / no map output → EDI tagList elementKey on a segment.
    (
        "edi_taglist_loop_vs_segment",
        [
            ("no data produced",),         # "no data produced from map"
            ("no data", "map"),
            ("silently missing",),         # "record silently missing from multi-record output"
            ("missing", "multi-record"),
            ("missing", "multi record"),
            ("split", "document"),
        ],
    ),
]

# Fail loudly at import if a route points at an id the catalog no longer has.
for _gid, _signatures in _SYMPTOM_ROUTES:
    if _gid not in OPERATIONAL_GOTCHA_ENTRIES:
        raise ValueError(
            f"_SYMPTOM_ROUTES references unknown gotcha id: {_gid!r}"
        )


def triage_symptoms(symptom_text: str) -> List[str]:
    """Map free-text symptom prose to matching catalog ids (ordered, de-duped).

    Pure and deterministic — no SDK, no I/O. Returns an empty list when nothing
    matches or the input is not usable text.
    """
    if not isinstance(symptom_text, str) or not symptom_text.strip():
        return []
    text = symptom_text.lower()
    matched: List[str] = []
    for gid, signatures in _SYMPTOM_ROUTES:
        for signature in signatures:
            if all(token in text for token in signature):
                if gid not in matched:
                    matched.append(gid)
                break
    return matched


def gotcha_matches_for_symptoms(symptom_text: str) -> List[Dict[str, str]]:
    """Triage ``symptom_text`` and project the matched catalog entries to a
    compact match shape (id / title / remediation / verification_status / lookup).

    Returns an empty list when no symptom routes. The projection is intentionally
    compact — it never dumps the full entry prose into the troubleshooting
    response; callers wanting the whole entry use the ``lookup`` pointer.
    """
    ids = triage_symptoms(symptom_text)
    if not ids:
        return []
    found = search_operational_gotchas(issue_ids=ids)
    matches: List[Dict[str, str]] = []
    for entry in found.get("results", []):
        gid = entry["id"]
        # verification_status travels with every match so callers can honor the
        # KB's provenance discipline (treat companion_unverified /
        # course_unverified as hypotheses) without a second lookup.
        matches.append(
            {
                "id": gid,
                "title": entry["title"],
                "remediation": entry["remediation"],
                "verification_status": entry["verification_status"],
                "lookup": f"search_boomi_gotchas(issue_ids=[{gid!r}])",
            }
        )
    return matches


# ---------------------------------------------------------------------------
# Catalog / resource / index accessors — each returns a deepcopy so a caller can
# never mutate shared module state.
# ---------------------------------------------------------------------------


def get_operational_gotchas_catalog() -> Dict[str, Any]:
    """Full catalog payload: all entries, the count, the schema, taxonomies."""
    return {
        "entry_count": OPERATIONAL_GOTCHA_ENTRY_COUNT,
        "entries": copy.deepcopy(list(OPERATIONAL_GOTCHA_ENTRIES.values())),
        "entry_schema": copy.deepcopy(GOTCHA_ENTRY_SCHEMA),
        "detections": sorted(DETECTIONS),
        "frequencies": sorted(FREQUENCIES),
        "verification_statuses": sorted(VERIFICATION_STATUSES),
        "categories": sorted(CATEGORIES),
        "read_only": True,
    }


def list_operational_gotchas_index() -> List[Dict[str, str]]:
    """Compact index rows — id / title / detection / frequency / category."""
    return [
        {
            "id": entry["id"],
            "title": entry["title"],
            "detection": entry["detection"],
            "frequency": entry["frequency"],
            "category": entry["category"],
            "verification_status": entry["verification_status"],
        }
        for entry in OPERATIONAL_GOTCHA_ENTRIES.values()
    ]


def valid_operational_gotcha_ids() -> List[str]:
    """Sorted entry ids."""
    return sorted(OPERATIONAL_GOTCHA_ENTRIES)


_TAXONOMY_BLURB = (
    "These are curated operational-gotcha summaries — known silent-failure modes "
    "and field traps — derived from OfficialBoomi Companion material "
    "(BSD-2-Clause) plus local issue/architect-course triage and first-party QA. "
    "They are not official Boomi documentation and not verbatim copies. Each "
    "entry carries a verification_status; treat companion_unverified and "
    "course_unverified entries as hypotheses, not authority."
)


def render_operational_gotchas_resource() -> str:
    """Render the full catalog as a markdown coverage map for the MCP resource."""
    lines: List[str] = []
    lines.append("# Boomi Operational Gotchas")
    lines.append("")
    lines.append(_TAXONOMY_BLURB)
    lines.append("")
    lines.append(f"Entries: {OPERATIONAL_GOTCHA_ENTRY_COUNT}")
    lines.append("")
    lines.append("Detection taxonomy: " + ", ".join(sorted(DETECTIONS)))
    lines.append("Frequency taxonomy: " + ", ".join(sorted(FREQUENCIES)))
    lines.append(
        "Verification statuses: " + ", ".join(sorted(VERIFICATION_STATUSES))
    )
    lines.append("Categories: " + ", ".join(sorted(CATEGORIES)))
    lines.append("")
    for entry in OPERATIONAL_GOTCHA_ENTRIES.values():
        prov = entry["provenance"]
        lines.append(f"## {entry['id']} — {entry['title']}")
        lines.append("")
        lines.append(f"- Category: {entry['category']}")
        lines.append(
            f"- Detection: {entry['detection']} | Frequency: {entry['frequency']}"
        )
        lines.append(f"- Verification: {entry['verification_status']}")
        lines.append(
            f"- Provenance: {prov['source_label']} (retrieved {prov['retrieval_date']})"
        )
        lines.append(f"- Applies to: {', '.join(entry['applies_to'])}")
        lines.append(f"- Symptom: {entry['symptom']}")
        lines.append(f"- Root cause: {entry['root_cause']}")
        lines.append(f"- Wrong: {entry['wrong_pattern']}")
        lines.append(f"- Correct: {entry['correct_pattern']}")
        lines.append(f"- Remediation: {entry['remediation']}")
        lines.append("")
    return "\n".join(lines)
