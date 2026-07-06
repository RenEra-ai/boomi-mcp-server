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

#: The issue-#77 source domains, used as the catalog facet. ``scripting`` was
#: added for the Groovy custom-scripting authoring traps (storeStream omission,
#: null property assignment, DDP prefix) surfaced at the emit point.
CATEGORIES = frozenset(
    {
        "listener_wss",
        "platform_entities",
        "connector_behavior",
        "deployment_testing",
        "process_serialization",
        "marketplace",
        "scripting",
        # Issue #124 M11.5: process-building step/component behaviors (cache
        # and dynamic-property authoring traps surfaced by the #119 census).
        "process_building",
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
_M11_CENSUS = "M11 #119 live census (work-account captures) + official cache/property docs"
_M6_ASC_RECON = (
    "M6.1 #133 pre-implementation recon on the renera advanced cloud "
    "attachment (live ASC fixture serving POST /ws/rest/generalListener)"
)
_M6_ASC_COMPANION = (
    "OfficialBoomi Companion api_service_component.md (#133), not "
    "independently live-verified"
)
_M6_ASC_RECON_QA = (
    "M6.1 #133 live QA A/B/A base-shadowing proof on the renera advanced "
    "cloud attachment (2026-07-05), superseding the companion's per-path "
    "granularity claim"
)

_ENTRIES: List[Dict[str, Any]] = [
    # ===================================================================
    # Listener / WSS (9)
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
        # Stable id kept from the original (mis-titled) entry — external
        # cross-links reference it. The content was CORRECTED after M6 live QA
        # (2026-07-04, renera local atom, intermediate apiType) disproved the
        # verbatim claim: the platform sentence-cases the object name's first
        # letter on the served bare-WSS path.
        "id": "wss_path_objectname_verbatim",
        "title": "WSS listener endpoint path sentence-cases the object name's first letter",
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
            "the lowercased operation type and the object name with its FIRST "
            "letter upper-cased (the rest of the casing preserved). The object "
            "name is stored verbatim on the operation component, but the served "
            "path re-cases its first letter — live-verified 2026-07-04: "
            "objectName 'qaM6IntakeA' with EXECUTE serves "
            "/ws/simple/executeQaM6IntakeA (200) while the verbatim "
            "/ws/simple/executeqaM6IntakeA returns 404."
        ),
        "wrong_pattern": (
            "Publishing the verbatim-cased URL (or assuming the operation type "
            "keeps mixed case), so callers hit a path the runtime never serves."
        ),
        "correct_pattern": (
            "Derive the route from the actual rule: lowercase the operation "
            "type, then append the object name with its first letter "
            "upper-cased, and confirm the live path before publishing it. The "
            "valid operation types are Get, Query, Create, Update, Upsert, "
            "Delete, and Execute."
        ),
        "remediation": (
            "Use the endpoint recorded by the listener build/verify tooling "
            "(validation_rules.listener.endpoint_path / the listener_verify "
            "stage's endpoint_url) rather than reconstructing it by hand."
        ),
        "applies_to": [
            "web_services_server",
            "listener_connector",
            "shared_web_server",
        ],
        "provenance": {
            "source_label": (
                "M6 live QA on the renera local atom (issue #12, intermediate "
                "apiType) — supersedes the earlier docs-corroborated verbatim claim"
            ),
            "retrieval_date": "2026-07-04",
        },
        "verification_status": "live_verified",
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
    {
        "id": "api_service_required_for_advanced_apitype",
        "title": "On an apiType=advanced runtime, bare WSS listeners deploy clean but every route 404s",
        "symptom": (
            "A Web Services Server listener process deploys successfully to a "
            "runtime whose Shared Web Server apiType is 'advanced', yet every "
            "request to its /ws/simple path returns 404 — nothing in the deploy "
            "response hints at a problem."
        ),
        "detection": "silent",
        "frequency": "high",
        "root_cause": (
            "An 'advanced' Shared Web Server serves routes ONLY through a "
            "deployed API Service Component (/ws/rest gateway); bare /ws/simple "
            "WSS routes are not registered at all. The discriminator is "
            "SharedServerInformation.apiType, NOT the Atom/runtime type — "
            "live-confirmed 2026-07-04 on the renera cloud attachment (bare "
            "path 404, ASC route 200 on the same runtime)."
        ),
        "wrong_pattern": (
            "Treating a clean deploy as proof the listener serves, then "
            "debugging the 404 as an auth or path-casing problem on a runtime "
            "whose tier simply never serves bare WSS routes."
        ),
        "correct_pattern": (
            "Read SharedServerInformation.apiType BEFORE choosing the listener "
            "pattern: basic/intermediate -> bare WSS; advanced -> an API "
            "Service Component routing to the WSS Listen process (listener "
            "archetypes: asc_wrapper.enabled=true). orchestrate_deploy's "
            "listener_verify preflight fails fast (LISTENER_ASC_REQUIRED) on "
            "the wrong pairing."
        ),
        "remediation": (
            "Rebuild with the ASC wrapper (or flip the runtime's apiType to "
            "basic/intermediate where that is acceptable), redeploy, and "
            "re-verify with an authenticated probe plus execution-record "
            "readback."
        ),
        "applies_to": [
            "web_services_server",
            "shared_web_server",
            "api_service_component",
            "orchestrate_deploy",
            "build_from_archetype",
        ],
        "provenance": {"source_label": _M6_ASC_RECON, "retrieval_date": "2026-07-04"},
        "verification_status": "live_verified",
        "category": "listener_wss",
    },
    {
        "id": "api_service_not_for_basic_intermediate",
        "title": "An API Service Component deployed to a basic/intermediate runtime does not serve its routes",
        "symptom": (
            "An API Service Component and its route process both deploy "
            "successfully, but requests to the expected /ws/rest path on a "
            "basic or intermediate apiType runtime never reach the process."
        ),
        "detection": "silent",
        "frequency": "medium",
        "root_cause": (
            "The /ws/rest API-gateway routing that resolves API Service "
            "Component routes is an 'advanced' Shared Web Server feature; on "
            "basic/intermediate tiers the runtime serves bare /ws/simple WSS "
            "paths instead and the deployed ASC's routes are never registered."
        ),
        "wrong_pattern": (
            "Wrapping every listener in an API Service Component 'to be safe' "
            "and assuming the wrapper is tier-neutral because it deploys "
            "without error everywhere."
        ),
        "correct_pattern": (
            "Match the publish pattern to the tier: bare WSS on "
            "basic/intermediate, ASC only on advanced. listener_verify fails "
            "the inverse pairing with LISTENER_ASC_UNSUPPORTED_FOR_APITYPE "
            "before probing."
        ),
        "remediation": (
            "Disable the asc_wrapper (bare WSS) for basic/intermediate "
            "runtimes, or move the deployment to an advanced-tier attachment, "
            "then re-verify the served route."
        ),
        "applies_to": [
            "api_service_component",
            "web_services_server",
            "shared_web_server",
            "orchestrate_deploy",
        ],
        "provenance": {"source_label": _M6_ASC_COMPANION, "retrieval_date": "2026-07-04"},
        "verification_status": "companion_unverified",
        "category": "listener_wss",
    },
    {
        "id": "api_service_deploy_does_not_cascade",
        "title": "Deploying an API Service Component does not deploy its route processes (and vice versa)",
        "symptom": (
            "An API Service Component is deployed and active, yet its route "
            "returns 404 — or the listener process is deployed but the "
            "/ws/rest path never registers — because only ONE of the two "
            "components was actually deployed."
        ),
        "detection": "silent",
        "frequency": "high",
        "root_cause": (
            "Packaging/deployment is per component: an ASC's PackagedComponent "
            "does not include the processes its routes reference, and "
            "deploying a route process does not pull in the ASC. Both must be "
            "independently packaged and deployed to the SAME environment (the "
            "live fixture keeps both active in one environment; companion "
            "api_service_component.md states the no-cascade rule)."
        ),
        "wrong_pattern": (
            "Deploying only the API Service Component (assuming it bundles its "
            "routes like a packaged process bundles its dependencies) and "
            "treating the resulting 404 as a path or auth problem."
        ),
        "correct_pattern": (
            "Always deploy BOTH the ASC and every route process to the same "
            "environment. orchestrate_deploy's api_service publish mode "
            "packages and deploys the ASC alongside the process and verifies "
            "both deployments are active before probing."
        ),
        "remediation": (
            "Package and deploy the missing component to the same environment "
            "(manage_deployment), confirm both active, and re-probe the route."
        ),
        "applies_to": [
            "api_service_component",
            "manage_deployment",
            "orchestrate_deploy",
            "packaged_component",
        ],
        "provenance": {"source_label": _M6_ASC_COMPANION, "retrieval_date": "2026-07-04"},
        "verification_status": "docs_corroborated",
        "category": "listener_wss",
    },
    {
        "id": "api_service_first_deployed_wins_collision",
        "title": "One deployed API Service Component serves per BASE urlPath — a later same-base ASC is shadowed in its entirety",
        "symptom": (
            "Two API Service Components sharing a base urlPath (both empty is "
            "the common case) are deployed and both report active=true, but "
            "every route of the later-deployed one answers 404 — even routes "
            "whose effective paths are unique account-wide; undeploying the "
            "winner does NOT make the loser start serving."
        ),
        "detection": "silent",
        "frequency": "high",
        "root_cause": (
            "The /ws/rest perimeter binds exactly ONE deployed webservice "
            "component per base urlPath, first-deployed wins for the ENTIRE "
            "component — routes are NOT merged across ASCs sharing a base. "
            "Live-proven A/B/A 2026-07-05 (renera advanced cloud): the same "
            "component 404'd all routes on base '' while an earlier base-'' "
            "ASC was deployed, served 200 within 75s on a distinct base, and "
            "404'd again after reverting. There is no collision error at "
            "deploy time and no API signal identifying the shadowed "
            "component; registration does not re-run when the winner is "
            "undeployed."
        ),
        "wrong_pattern": (
            "Assuming shadowing is per effective route path (so unique "
            "objectNames are safe under a shared base), or relying on deploy "
            "success / a live pre-probe (the cloud perimeter answers a "
            "uniform 401 before any route exists) to detect collisions."
        ),
        "correct_pattern": (
            "Give every ASC a distinct base_url_path (the default '' collides "
            "with any other default-base ASC in the environment), and detect "
            "collisions from data, not probes: query active deployed "
            "webservice packages and compare BASE urlPaths — "
            "listener_verify's ASC collision scan does exactly this "
            "(LISTENER_ASC_COLLISION), with per-route effective-path "
            "comparison as a secondary signal."
        ),
        "remediation": (
            "Choose a distinct base_url_path (asc_wrapper.base_url_path) or "
            "undeploy the colliding ASC, then REDEPLOY the intended winner so "
            "its routes register."
        ),
        "applies_to": [
            "api_service_component",
            "manage_deployment",
            "orchestrate_deploy",
            "build_from_archetype",
        ],
        "provenance": {"source_label": _M6_ASC_RECON_QA, "retrieval_date": "2026-07-05"},
        "verification_status": "live_verified",
        "category": "listener_wss",
    },
    {
        "id": "api_service_cloud_401_404_triage",
        "title": "Cloud listener triage: uniform 401 before any route exists; 404 with valid credentials means no matching route",
        "symptom": (
            "Probing a Boomi-managed cloud listener endpoint returns 401 even "
            "with valid credentials, or 404 after authentication succeeds, and "
            "the two signals get misread as each other."
        ),
        "detection": "runtime_error",
        "frequency": "high",
        "root_cause": (
            "Before the FIRST route is registered for a tenant, the cloud "
            "perimeter answers a uniform 401 for every path regardless of "
            "credentials (live-confirmed 2026-07-04). Once at least one route "
            "is registered, valid credentials get a 404 on unknown paths — so "
            "401-vs-404 encodes registration state, not just auth."
        ),
        "wrong_pattern": (
            "Treating a pre-first-route 401 as a credential problem and "
            "rotating tokens, or using a live pre-probe as a collision/route "
            "check (the uniform 401 makes it uninformative)."
        ),
        "correct_pattern": (
            "Triage in order: 401 -> either no route registered for the "
            "tenant yet (fresh deploy; registration can lag minutes) or bad "
            "credentials; 404 after auth -> the request reached the runtime "
            "but no route matches (check apiType tier, path segments, and "
            "that BOTH the ASC and its route process are deployed)."
        ),
        "remediation": (
            "Wait out route-registration lag on fresh deploys, verify the "
            "apiType/publish-mode pairing, and read back execution records "
            "instead of relying on the HTTP status alone."
        ),
        "applies_to": [
            "web_services_server",
            "api_service_component",
            "shared_web_server",
            "orchestrate_deploy",
        ],
        "provenance": {"source_label": _M6_ASC_RECON, "retrieval_date": "2026-07-04"},
        "verification_status": "live_verified",
        "category": "listener_wss",
    },
    {
        "id": "listener_status_not_wss_asc",
        "title": "ListenerStatus does not report WSS or API Service routes — an empty result does not mean the listener is down",
        "symptom": (
            "The async ListenerStatus query returns no entries for a Web "
            "Services Server or API Service listener that is demonstrably "
            "serving traffic, and the operator concludes the listener failed "
            "to start (or a verifier concludes it is not deployed)."
        ),
        "detection": "silent",
        "frequency": "medium",
        "root_cause": (
            "ListenerStatus covers connector listeners (JMS, AS2, Salesforce "
            "and similar listen connectors); WSS/ASC HTTP routes are served by "
            "the Shared Web Server and never appear in it — live-confirmed "
            "2026-07-04 on both a local atom and the cloud attachment while "
            "the routes were serving 200s."
        ),
        "wrong_pattern": (
            "Using ListenerStatus (or ChangeListenerStatusRequest) as the "
            "deploy/health verification for a WSS or API Service listener and "
            "failing the rollout when the result is empty."
        ),
        "correct_pattern": (
            "Verify WSS/ASC listeners behaviorally: authenticated probe of the "
            "computed endpoint plus an execution-record readback (HTTP 200 "
            "with outputType=none is an ack, not process success — an ERROR "
            "execution behind a 200 is live-proven). Keep ListenerStatus for "
            "connector listeners only."
        ),
        "remediation": (
            "Switch the verification to orchestrate_deploy's listener_verify "
            "stage (probe + readback) and disregard empty ListenerStatus "
            "results for WSS/ASC routes."
        ),
        "applies_to": [
            "manage_listeners",
            "web_services_server",
            "api_service_component",
            "orchestrate_deploy",
            "monitor_platform",
        ],
        "provenance": {"source_label": _M6_ASC_RECON, "retrieval_date": "2026-07-04"},
        "verification_status": "live_verified",
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
    {
        "id": "groovy_dataprocess_storestream_required",
        "title": "A Data Process Groovy step that omits dataContext.storeStream silently drops the document",
        "symptom": (
            "Documents vanish from the process flow after a Custom Scripting "
            "(Groovy) Data Process step: downstream steps receive fewer "
            "documents, or none at all, and no error appears in the log."
        ),
        "detection": "silent",
        "frequency": "high",
        "root_cause": (
            "The custom scripting framework forwards a document to the next "
            "step only when the script calls dataContext.storeStream(is, props) "
            "for it. A per-document loop that reads getStream and getProperties "
            "but never calls storeStream emits zero documents, and the omission "
            "raises no error. The number of streams stored is the number of "
            "documents passed on."
        ),
        "wrong_pattern": (
            "Modifying properties or content inside the "
            "dataContext.getDataCount() loop and falling through the iteration "
            "without calling dataContext.storeStream(is, props) for each "
            "document the step should keep."
        ),
        "correct_pattern": (
            "Call dataContext.storeStream(is, props) for every document the "
            "step emits: once per iteration for a pass-through, or once per "
            "produced stream when splitting one document into many."
        ),
        "remediation": (
            "Add dataContext.storeStream(is, props) at the end of the "
            "per-document loop, then run the process once against "
            "representative data and confirm the downstream document count."
        ),
        "applies_to": ["groovy_script", "data_process_shape"],
        "provenance": {"source_label": _COMPANION_DOCS, "retrieval_date": "2026-06-30"},
        "verification_status": "docs_corroborated",
        "category": "scripting",
    },
    {
        "id": "groovy_props_setproperty_null_npe",
        "title": "Passing null to props.setProperty in a Groovy script throws a NullPointerException at first run",
        "symptom": (
            "A Custom Scripting (Groovy) step deploys cleanly and then fails "
            "the first time it executes with a NullPointerException originating "
            "from a props.setProperty call."
        ),
        "detection": "runtime_error",
        "frequency": "medium",
        "root_cause": (
            "The script props object is a java.util.Properties (a Hashtable), "
            "whose setProperty rejects a null value with a NullPointerException "
            "at assignment time. A value computed from an upstream lookup or "
            "parse that can return null is passed straight into setProperty."
        ),
        "wrong_pattern": (
            "Calling props.setProperty(key, value) where value may be null, for "
            "example assigning the result of a lookup or parse that can return "
            "null without a guard."
        ),
        "correct_pattern": (
            "Guard before assigning: call props.setProperty(key, value) only "
            "when value is not null, substituting an explicit empty string or "
            "skipping the property otherwise, and convert numbers or booleans "
            "to String first."
        ),
        "remediation": (
            "Wrap each property assignment in a null check, then run the "
            "deployed process once against representative data so the script "
            "compiles and the assignment path executes."
        ),
        "applies_to": ["groovy_script", "data_process_shape"],
        "provenance": {"source_label": _COMPANION_ONLY, "retrieval_date": "2026-06-30"},
        "verification_status": "companion_unverified",
        "category": "scripting",
    },
    {
        "id": "groovy_ddp_prefix_required",
        "title": "Dynamic document property access in a Groovy script silently misses without the document.dynamic.userdefined. prefix",
        "symptom": (
            "A Groovy Data Process script sets or reads a dynamic document "
            "property by a bare name and the value never appears downstream: a "
            "later Get or Set sees nothing, with no error raised."
        ),
        "detection": "silent",
        "frequency": "medium",
        "root_cause": (
            "User-defined dynamic document properties are keyed by the full "
            "document.dynamic.userdefined. prefix plus the property name. A "
            "props.getProperty or setProperty call that uses the bare name "
            "addresses a different key, so the read returns null and the write "
            "is invisible to later steps; property names are also "
            "case-sensitive. Inside a map Scripting function the props object is "
            "not available at all, so document properties there go through the "
            "Get Document Property and Set Document Property function steps."
        ),
        "wrong_pattern": (
            "Calling props.getProperty or props.setProperty with a bare "
            "property name instead of the document.dynamic.userdefined. "
            "prefixed key, or attempting props access from inside a map "
            "Scripting function body."
        ),
        "correct_pattern": (
            "In a Data Process script, prefix every user-defined dynamic "
            "document property key with document.dynamic.userdefined. and match "
            "the exact case on read and write. In a map Scripting function, use "
            "the Get and Set Document Property function steps instead of props."
        ),
        "remediation": (
            "Update each property key to include the "
            "document.dynamic.userdefined. prefix, then run the process once "
            "and confirm the value is present on the document at the next step."
        ),
        "applies_to": ["groovy_script", "data_process_shape", "map"],
        "provenance": {"source_label": _COMPANION_DOCS, "retrieval_date": "2026-06-30"},
        "verification_status": "docs_corroborated",
        "category": "scripting",
    },
    # ===================================================================
    # Process building — cache / dynamic-property authoring (4) (#124 M11.5)
    # ===================================================================
    {
        "id": "document_cache_zero_id_silent_noop",
        "title": "Document Cache index or key id of 0 silently indexes nothing",
        "symptom": (
            "Add to Cache reports success and the process runs green, but every "
            "retrieve and map join against the cache finds no documents."
        ),
        "detection": "silent",
        "frequency": "medium",
        "root_cause": (
            "The platform API accepts a cache index id or cache key id of 0 in "
            "the component, but the runtime indexer treats 0 as unassigned, so "
            "entries are written without a retrievable index."
        ),
        "wrong_pattern": (
            "Generating cache components programmatically with zero-based index "
            "or key ids because the API accepted them on create."
        ),
        "correct_pattern": (
            "Use 1-based sequential index ids and any non-zero key id — the "
            "typed cache builder rejects zeros at validation time."
        ),
        "remediation": (
            "Recreate or update the cache component with non-zero ids and rerun; "
            "no cached data is recoverable from the zero-id runs."
        ),
        "applies_to": ["documentcache", "build_integration", "manage_component"],
        "provenance": {"source_label": _M11_CENSUS, "retrieval_date": "2026-07-02"},
        "verification_status": "docs_corroborated",
        "category": "process_building",
    },
    {
        "id": "document_cache_missing_profile_type_crash",
        "title": "A Document Cache without a profile type crashes at runtime",
        "symptom": (
            "A process using a programmatically created cache fails at run time "
            "with a data-parser error about a component that does not exist, "
            "even though the cache component looks fine in the GUI."
        ),
        "detection": "runtime_error",
        "frequency": "medium",
        "root_cause": (
            "The cache indexer always parses documents through the declared "
            "profile binding; omitting the profile type on the component leaves "
            "the parser unresolvable at execution."
        ),
        "wrong_pattern": (
            "Emitting a cache component without the profile-type attribute "
            "because the create API accepted it."
        ),
        "correct_pattern": (
            "Always declare the profile type; the typed cache builder makes it "
            "a required field."
        ),
        "remediation": "Update the cache component with the profile binding and redeploy.",
        "applies_to": ["documentcache", "build_integration"],
        "provenance": {"source_label": _M11_CENSUS, "retrieval_date": "2026-07-02"},
        "verification_status": "docs_corroborated",
        "category": "process_building",
    },
    {
        "id": "add_to_cache_consumes_documents",
        "title": "Add to Cache consumes the documents it stores",
        "symptom": (
            "Steps placed after an Add to Cache step never receive documents — "
            "the flow appears to stop at the cache write."
        ),
        "detection": "silent",
        "frequency": "high",
        "root_cause": (
            "The Add to Cache step is a terminal sink on its path: documents "
            "flow INTO the cache, not through it."
        ),
        "wrong_pattern": (
            "Chaining business steps after an Add to Cache on the same path and "
            "expecting the documents to continue."
        ),
        "correct_pattern": (
            "Put the cache write on its own branch path (the live pattern: an "
            "earlier branch path writes, a later path retrieves), or retrieve "
            "the cached set back when the documents are needed again."
        ),
        "remediation": (
            "Restructure with a branch so the write path terminates in the "
            "cache and the continuation path retrieves or re-reads its input."
        ),
        "applies_to": ["documentcache", "cache_put", "flow_sequence"],
        "provenance": {"source_label": _M11_CENSUS, "retrieval_date": "2026-07-02"},
        "verification_status": "live_verified",
        "category": "process_building",
    },
    {
        "id": "ddp_not_visible_across_branch_legs",
        "title": "Dynamic document properties never cross sibling branch paths",
        "symptom": (
            "A dynamic document property set inside one branch path reads back "
            "empty in a sibling path, while the same handoff works when the set "
            "happens before the branch."
        ),
        "detection": "silent",
        "frequency": "high",
        "root_cause": (
            "Each branch path processes its own copy of the pre-branch "
            "documents; a document property set on one path's copy never "
            "reaches the copies flowing through sibling paths."
        ),
        "wrong_pattern": (
            "Handing a value from branch path 1 to branch path 2 through a "
            "dynamic document property."
        ),
        "correct_pattern": (
            "Set the document property on the trunk before the branch, or use "
            "an execution-scoped dynamic process property or a typed document "
            "cache for cross-path handoff — the plan-time lineage validation "
            "rejects the sibling-path read before any mutation."
        ),
        "remediation": (
            "Move the property write to the trunk or switch the handoff to "
            "execution scope, then re-plan."
        ),
        "applies_to": ["set_ddp", "set_dpp", "flow_sequence", "branch"],
        "provenance": {"source_label": _M11_CENSUS, "retrieval_date": "2026-07-02"},
        "verification_status": "docs_corroborated",
        "category": "process_building",
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

# The storeStream gotcha's effect-based routes are generated as the FULL cross
# product of a document-loss verb and an UNAMBIGUOUS scripting-context phrase,
# so the verb×context grid is symmetric by construction (no manually-missed
# pairing). Context phrases must be substring-safe, because triage matches with
# raw ``token in text``: bare "script" is a substring of "description", and
# "script step" is a substring of "transcript step" (a transcript is itself a
# document) — both are excluded. "custom script" (needs the "custom " prefix),
# "groovy", "scripting", and the word-anchored "custom script" / "data process
# script" carry no realistic collision in a Boomi troubleshooting symptom and
# cover the canonical phrasings for a Data Process script. The two
# storeStream-method signatures fire on the method name itself.
_STORESTREAM_LOSS_VERBS = ("dropped", "disappear", "no output")
_STORESTREAM_SCRIPT_CONTEXT = (
    "scripting",
    "groovy",
    "custom script",
    "data process script",
)
_STORESTREAM_SIGNATURES = [
    ("storestream",),
    ("store", "stream"),
] + [
    (verb, "document", context)
    for verb in _STORESTREAM_LOSS_VERBS
    for context in _STORESTREAM_SCRIPT_CONTEXT
]

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
            ("unresolved", "variable"),    # "unresolved (environment) variable"
            ("unresolved", "reference"),   # "unresolved (environment) reference(s)"
            ("unresolved", "token"),
            ("unresolved", "env ref"),     # abbreviated "unresolved env refs"
            ("unresolved", "env var"),     # abbreviated "unresolved env var(s)"
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
    # Documents vanishing after a Data Process custom script that forgot
    # dataContext.storeStream → the storeStream-method signatures plus the
    # generated verb×scripting-context grid (see _STORESTREAM_SIGNATURES above).
    ("groovy_dataprocess_storestream_required", _STORESTREAM_SIGNATURES),
    # NullPointerException from a script property assignment → null setProperty.
    (
        "groovy_props_setproperty_null_npe",
        [
            ("setproperty", "null"),
            ("null", "setproperty"),
            ("nullpointer", "script"),
            ("npe", "script"),
            ("nullpointerexception", "property"),
        ],
    ),
    # A dynamic document property set/read in a script that never lands → prefix.
    (
        "groovy_ddp_prefix_required",
        [
            ("userdefined", "prefix"),
            ("document.dynamic.userdefined",),
            ("ddp", "prefix"),
            ("ddp", "missing", "script"),
            ("dynamic document property", "missing"),
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
