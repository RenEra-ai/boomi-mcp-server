"""The read-only discovery boundary (#144).

This module defines the ONLY shape through which live data may reach the
planner, and it is a seven-method read port with no write verb in it. The
planner itself never sees the port — it takes the resulting snapshot — so the
network boundary is one function wide.

No concrete adapter ships in #144. ``ReadOnlyTopologyDiscoveryPort`` is a
``typing.Protocol``: structural, unregistered, with no ABC registry, no
metaclass, and no SDK import. Wiring it to the MCP layer is #146's job.

Deliberately absent, and why
----------------------------
* **Account capability limits** — nothing was captured, so nothing is modeled.
  A limit invented here would look like evidence.
* **Listener status** — its observed behavior conflicts with the documented
  example, and a conflicting source is reported, never resolved by precedence.
* **Environment extensions** — they carry override VALUES, which is exactly the
  secret class topology refuses to hold.
* **Shared web-server / channel resources** — the ``shared_resources`` module
  does not represent Document Caches or Process Properties at all; it exposes
  unrelated secret-bearing resources with mixed write verbs.
* **Runtime queue listings** — a troubleshooting surface. Queue authoring is
  gated precisely because runtime introspection is not authoring evidence.
"""

from __future__ import annotations

from typing import Any, List, Mapping, Optional, Protocol, Sequence, Tuple

from ...models.system_topology import SystemTopologySpecV1
from .context import (
    ComponentFactV1,
    DeploymentFactV1,
    DiscoveryPageProvenanceV1,
    EnvironmentFactV1,
    RuntimeFactV1,
    ScheduleBindingFactV1,
    TopologyDiscoverySnapshotV1,
)

#: The component types topology reads. Closed: a type absent here is a type the
#: planner cannot learn about, which is the point.
DISCOVERY_COMPONENT_TYPES: Tuple[str, ...] = (
    "process",
    "documentcache",
    "processproperty",
    "webservice",
    "queue",
)


class ReadOnlyTopologyDiscoveryPort(Protocol):
    """Seven reads. No create, update, delete, deploy, schedule or execute.

    Every method takes ``profile`` explicitly rather than binding one at
    construction: a port that remembers its profile can be handed to a capture
    for a different profile and silently answer from the wrong account.
    """

    def list_profiles(self) -> Sequence[str]:
        ...

    def query_components(
        self, profile: str, component_type: str
    ) -> Mapping[str, Any]:
        ...

    def read_component_xml(self, profile: str, component_ref: str) -> Optional[str]:
        ...

    def read_component_dependencies(
        self, profile: str, component_ref: str
    ) -> Sequence[Mapping[str, Any]]:
        ...

    def list_environments(self, profile: str) -> Sequence[Mapping[str, Any]]:
        ...

    def list_schedules(self, profile: str) -> Sequence[Mapping[str, Any]]:
        ...

    def list_deployments(self, profile: str) -> Sequence[Mapping[str, Any]]:
        ...


class TopologyDiscoveryError(RuntimeError):
    """Raised when discovery cannot proceed safely (e.g. unknown profile)."""


def capture_topology_discovery_snapshot(
    spec: SystemTopologySpecV1,
    port: ReadOnlyTopologyDiscoveryPort,
    *,
    captured_at: str,
    source_revision: str,
    service_release: str,
) -> TopologyDiscoverySnapshotV1:
    """Read live topology facts for exactly one profile and sanitize them.

    The profile is validated FIRST and then forwarded verbatim to every call. An
    unknown profile is a hard error rather than an empty snapshot: an empty
    snapshot reads as "this account has nothing", which would turn a typo into a
    confident claim that no queues exist.

    Pagination is recorded per component query. The caller gets the returned
    count, the reported total, and whether more remained — because the counts in
    this issue's own description turned out to be page-capped artifacts, and a
    census that cannot say it was truncated is not evidence.
    """
    profile = spec.profile_ref
    available = tuple(port.list_profiles())
    if profile not in available:
        raise TopologyDiscoveryError(
            "topology discovery: the spec's profile is not among the available profiles"
        )

    components: List[ComponentFactV1] = []
    pagination: List[DiscoveryPageProvenanceV1] = []
    for component_type in DISCOVERY_COMPONENT_TYPES:
        payload = port.query_components(profile, component_type)
        observed = _is_successful_listing(payload)
        rows = (payload or {}).get("components") or () if observed else ()
        for row in rows:
            component_id = row.get("component_id") or row.get("id")
            if not component_id:
                continue
            components.append(
                ComponentFactV1(
                    profile=profile,
                    component_id=str(component_id),
                    component_type=str(row.get("type") or component_type),
                )
            )
        pagination.append(
            DiscoveryPageProvenanceV1(
                component_type=component_type,
                returned_count=len(rows),
                total_available=_opt_int((payload or {}).get("total_available"))
                if observed
                else None,
                has_more=bool((payload or {}).get("has_more")) if observed else False,
                observed=observed,
            )
        )

    environments = tuple(
        EnvironmentFactV1(
            profile=profile,
            environment_id=str(row["id"]),
            # Only a value the source ACTUALLY carried. A missing classification
            # stays None; inventing one would let a correct authored PROD be
            # contradicted by a default nobody read.
            classification=_opt_classification(row.get("classification")),
        )
        for row in port.list_environments(profile)
        if row.get("id")
    )

    schedule_rows = tuple(port.list_schedules(profile))
    schedule_bindings = tuple(
        ScheduleBindingFactV1(
            profile=profile,
            process_id=str(row["process_id"]),
            runtime_id=str(row["atom_id"]),
            active=bool(row.get("active")),
            # An empty ``schedules`` array is the live norm; recording it as a
            # boolean keeps the absence visible without modelling cron content.
            has_schedule_body=bool(row.get("schedules")),
        )
        for row in schedule_rows
        if row.get("process_id") and row.get("atom_id")
    )

    runtimes = tuple(
        RuntimeFactV1(profile=profile, runtime_id=runtime_id)
        for runtime_id in sorted(
            {binding.runtime_id for binding in schedule_bindings}
        )
    )

    deployments = tuple(
        DeploymentFactV1(
            profile=profile,
            component_id=str(row["component_id"]),
            environment_id=str(row["environment_id"]),
            active=bool(row.get("active")),
        )
        for row in port.list_deployments(profile)
        if row.get("component_id") and row.get("environment_id")
    )

    return TopologyDiscoverySnapshotV1(
        profile=profile,
        captured_at=captured_at,
        source_revision=source_revision,
        service_release=service_release,
        components=tuple(components),
        environments=environments,
        runtimes=runtimes,
        schedule_bindings=schedule_bindings,
        deployments=deployments,
        pagination=tuple(pagination),
    )


def _is_successful_listing(payload: Any) -> bool:
    """Did this query actually answer?

    A tool envelope that reports failure, omits ``components``, or is not a
    mapping at all has told us nothing. Treating that as "zero components" is
    how a transient outage becomes a published capability claim — and the queue
    gates are the rows that cite an empty listing as their live evidence.
    """
    if not isinstance(payload, Mapping):
        return False
    if payload.get("_success") is False:
        return False
    if payload.get("error"):
        return False
    return "components" in payload


def _opt_classification(value: Any) -> Optional[str]:
    """The observed classification, or None. Never a default."""
    if isinstance(value, str) and value in ("TEST", "PROD"):
        return value
    return None


def _opt_int(value: Any) -> Optional[int]:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    return None


__all__: List[str] = [
    "DISCOVERY_COMPONENT_TYPES",
    "ReadOnlyTopologyDiscoveryPort",
    "TopologyDiscoveryError",
    "capture_topology_discovery_snapshot",
]
