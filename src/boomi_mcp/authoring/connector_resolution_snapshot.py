"""Slice C — what the request itself settles about its connector components.

The compiler receives connector family and action as ``connector_metadata``, a
mapping the CALLER supplies. Today that mapping is authoritative: whatever it
says is what the compiler classifies against. So a component whose config
resolves to a POST could be declared a GET, and the declaration would win all
the way to a capability decision.

This module resolves the same facts from the components themselves and hands
them down as a snapshot. Downstream, the caller's mapping becomes an ASSERTION
that is compared against the resolution rather than an override — the same
discipline ``connector_resolution`` already applies to a node's authored
``action_intent``.

Two things this deliberately does NOT do:

* It never runs a builder. ``normalized_identity_projection`` is a pure read of
  the config, because this runs on a plan path over configs that have not been
  validated yet, and a projection that raised could not report "this config
  settles nothing" — which is the answer it most often needs to give.
* It never mints an identity it had to guess at. A route bound to an
  environment extension, a dynamic per-document path, an unreadable config: all
  of them resolve to NOTHING rather than to a default, and a comparison against
  nothing is skipped rather than assumed to pass.

The codes this raises live in ``errors`` and are never imported under
``compiler/process_ir/``: connector identity is an account fact, and a code
named inside the compiler joins the compiler's published surface.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from pydantic import BaseModel, ConfigDict

from ..errors import (
    CONNECTOR_REPLAY_IDENTITY_MISMATCH,
    CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE,
)


class ConnectorIdentityError(Exception):
    """A connector component's identity cannot be stood behind.

    Carries the stable code so the surfaces that translate authoring failures
    into responses do not have to re-derive it from the message.
    """

    def __init__(self, code: str, message: str, *, component_key: str) -> None:
        super().__init__(message)
        self.code = code
        self.component_key = component_key


class _SnapshotModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ResolvedConnectorComponentIdentityV1(_SnapshotModel):
    """What ONE connector component's own configuration settles, pre-apply.

    ``endpoint`` is a credential-free skeleton — a base URL's PATH can carry a
    webhook token, so it never reaches this model. That is inherited from the
    projection and is why this is an identity for COMPARISON, not a route
    digest: the digest reads the applied component's XML and keeps the path.
    """

    component_key: str
    family: Optional[str] = None
    action: Optional[str] = None
    endpoint: Optional[str] = None
    path: Optional[str] = None
    #: ``"static"`` | ``"dynamic"`` | ``"unavailable"`` — see the projection.
    route_state: str = "unavailable"

    @property
    def resolved(self) -> bool:
        """Whether this identity settles enough to be compared against."""
        return self.route_state == "static" and self.family is not None


class TrustedConnectorResolutionSnapshotV1(_SnapshotModel):
    """Every connector identity the request settles, keyed by component.

    Sorted at construction so a caller's component order cannot reach anything
    derived from this, matching how the symbol table treats its own rows.
    """

    identities: Tuple[ResolvedConnectorComponentIdentityV1, ...] = ()

    def lookup(self, component_key: str) -> Optional[ResolvedConnectorComponentIdentityV1]:
        for identity in self.identities:
            if identity.component_key == component_key:
                return identity
        return None


def build_connector_resolution_snapshot(
    components: Sequence[Any],
    *,
    live_projections: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> TrustedConnectorResolutionSnapshotV1:
    """Resolve every connector component in ``components`` from its own config.

    Non-connector components are skipped rather than recorded as unresolved:
    the snapshot answers "what did this connector resolve to", and a process or
    a profile has no answer to give. A connector whose config settles nothing
    IS recorded, with ``route_state`` saying why — the difference between "not
    a connector" and "a connector I could not read" is the whole point.
    """
    from ..categories.components.builders.connector_builder import (
        normalized_identity_projection,
    )

    live = live_projections or {}
    resolved = []
    for component in components:
        component_type = getattr(component, "type", None)
        if not isinstance(component_type, str) or "connector" not in component_type:
            continue
        key = getattr(component, "key", None)
        if not isinstance(key, str) or not key:
            continue
        projection = normalized_identity_projection(
            getattr(component, "config", None) or {},
            live_projection=live.get(key),
        )
        resolved.append(
            ResolvedConnectorComponentIdentityV1(
                component_key=key,
                family=projection.family,
                action=projection.action,
                endpoint=projection.endpoint,
                path=projection.path,
                route_state=projection.route_state,
            )
        )
    return TrustedConnectorResolutionSnapshotV1(
        identities=tuple(sorted(resolved, key=lambda item: item.component_key))
    )


def assert_declared_matches_resolved(
    snapshot: TrustedConnectorResolutionSnapshotV1,
    declared: Mapping[str, Tuple[Optional[str], Optional[str]]],
) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
    """Compare the caller's declaration against what the components resolve to.

    Returns the declaration unchanged when they agree, so a caller can keep
    passing it down. Raises when they DISAGREE — never when the resolution is
    simply unavailable, because "I could not tell" is not evidence that the
    declaration is wrong, and refusing on it would break every config whose
    endpoint is bound to an environment extension.

    Only the FAMILY and ACTION are compared. The endpoint is deliberately out of
    scope here: the declaration does not carry one, so there is nothing to
    disagree with.
    """
    for key, pair in sorted(declared.items()):
        identity = snapshot.lookup(key)
        if identity is None or not identity.resolved:
            continue
        from ..categories.components.builders.connector_builder import (
            connector_family_of,
        )

        # The declared half carries the RAW connector type; the resolved half a
        # canonical family. Comparing them directly made every REST component
        # unequal to itself. Both sides go through the same derivation.
        declared_family = connector_family_of(pair[0])
        declared_action = pair[1]
        for label, mine, theirs in (
            ("family", identity.family, declared_family),
            ("action", identity.action, declared_action),
        ):
            if theirs is None or mine is None:
                # A declaration that says nothing asserts nothing.
                continue
            if str(theirs).strip().lower() != str(mine).strip().lower():
                raise ConnectorIdentityError(
                    CONNECTOR_REPLAY_IDENTITY_MISMATCH,
                    (
                        "component {0!r} is declared with {1} {2!r}, but its own "
                        "configuration resolves to {3!r}. The declaration is an "
                        "assertion, not an override."
                    ).format(key, label, theirs, mine),
                    component_key=key,
                )
    return dict(declared)


def require_resolved_identity(
    snapshot: TrustedConnectorResolutionSnapshotV1, component_key: str
) -> ResolvedConnectorComponentIdentityV1:
    """The identity for one component, or a refusal naming why it is missing.

    For callers that cannot proceed without one. Everything on the plan path
    uses :func:`assert_declared_matches_resolved` instead, which tolerates an
    unresolved identity — most configs legitimately settle nothing until the
    account they will run against is known.
    """
    identity = snapshot.lookup(component_key)
    if identity is not None and identity.resolved:
        return identity
    detail = "no connector component under that key"
    if identity is not None:
        detail = "its route is {0}".format(identity.route_state)
    raise ConnectorIdentityError(
        CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE,
        "component {0!r} has no resolvable connector identity: {1}".format(
            component_key, detail
        ),
        component_key=component_key,
    )
