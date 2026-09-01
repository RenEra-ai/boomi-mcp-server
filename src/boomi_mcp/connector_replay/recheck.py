"""Apply-boundary identity recheck for evidence-bound calls (#155 slice E).

The compiler mints a grant from what it can know: which contract covers a call,
which components the call's references resolve to, and the family and action the
binding carries. It deliberately does NOT compare component versions, the account
scope, or route coverage — it has no live reading of them, and taking them from
the record would compare the record with itself. `_registry_corroborates` says so
in as many words, and names this module as where that comparison belongs.

So this is the half of the check that needs an account. Between the moment a plan
is compiled and the moment it is written, the operation or connection it was
evidenced against can be edited, replaced, or become unreadable. A grant minted
before that edit authorises a retry the evidence no longer covers.

SCOPE IS DELIBERATELY NARROW. Only identities a GRANT consumed are rechecked: a
root that minted no grant performs no live read at all, so the ordinary apply path
pays nothing and its platform-call count is unchanged. That is a decision, not an
oversight — a blanket re-read would make every apply depend on extra reads whose
failure could only ever refuse work the evidence channel was not involved in.

Two boundaries, two meanings:

- PRE-SUBMISSION, before the write. Nothing has been written for this step, so a
  drift refuses cleanly. Whether the WHOLE apply has written nothing depends on
  where the boundary sits: the global recheck runs before the first write and can
  honestly report that nothing was created, while a just-in-time recheck before a
  later component's submission cannot — earlier components are already written.
  The caller is told which it is rather than being left to assume.
- POST-SUBMISSION, after the write. The component exists. A mismatch here is a
  reconciliation failure over a RETAINED result, never "no mutation", and the
  vocabulary keeps that distinction because collapsing it is how an operator is
  told nothing happened when something did.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

#: What a recheck compares, in the order it reports them. Written as data so the
#: reasons are a closed set: a caller cannot be handed a reason this module does
#: not define, and a test can assert the set rather than a sample of it.
DRIFT_REASONS: Tuple[str, ...] = (
    "operation_version",
    "operation_config_digest",
    "connection_version",
    "connection_config_digest",
    "account_scope",
)


class RecheckOutcome:
    """One recheck's verdict: the drifts found, or the read that failed."""

    __slots__ = ("drifts", "unavailable", "boundary")

    def __init__(self, boundary: str, drifts=(), unavailable=None):
        self.boundary = boundary
        self.drifts = tuple(drifts)
        self.unavailable = unavailable

    @property
    def ok(self) -> bool:
        return not self.drifts and self.unavailable is None


def _identity_drifts(label: str, recorded, observed) -> List[Dict[str, Any]]:
    """Compare ONE component's pinned identity against a live reading.

    Both halves are compared, and the reason says which. The version alone moves
    on a credential-only edit that changes no behaviour; the digest alone is
    computed over a projection that omits most of the component. A check that
    reported only "drifted" would send an operator to look at the wrong thing.
    """
    drifts: List[Dict[str, Any]] = []
    recorded_version = getattr(recorded, "version", None)
    observed_version = observed.get("version") if observed else None
    if observed_version is not None and recorded_version != observed_version:
        drifts.append(
            {
                "reason": f"{label}_version",
                "component_id": getattr(recorded, "component_id", None),
                "recorded": recorded_version,
                "observed": observed_version,
            }
        )
    recorded_digest = getattr(recorded, "config_digest", None)
    observed_digest = observed.get("config_digest") if observed else None
    if observed_digest is not None and recorded_digest != observed_digest:
        drifts.append(
            {
                "reason": f"{label}_config_digest",
                "component_id": getattr(recorded, "component_id", None),
                # The digests are NOT served. A config digest is computed over a
                # projection of the component, and this refusal travels to a
                # caller; naming the field that drifted is what an operator needs,
                # and the value is what the account already holds.
                "recorded": "<redacted>",
                "observed": "<redacted>",
            }
        )
    return drifts


def recheck_grant_identities(
    *,
    grants: Sequence[Any],
    registry: Any,
    live_identity: Callable[[str, str, Optional[str]], Optional[Mapping[str, Any]]],
    account_scope_hash: Optional[str] = None,
    boundary: str = "pre_submission",
) -> RecheckOutcome:
    """Recheck every identity the supplied grants consumed.

    ``live_identity`` takes a component id, the KIND being read, and the record's
    FAMILY. It returns ``{"version", "config_digest"}``, or a mapping carrying
    ``reason`` when no identity could be established — ``account_unreadable`` when
    the account did not answer, ``projection_unsupported`` when it answered and
    this server cannot project the answer. ``None`` is still accepted and read as
    the former, so a caller written against the older contract fails closed rather
    than silently.

    THE KIND IS PASSED, NOT SNIFFED. A first version derived it from the fetched
    component's type string, and the platform spells a connection
    ``connector-settings`` — which contains no substring a sniff would key on, so
    every connection projected as an operation and refused, and the refusal looked
    exactly like a healthy fail-closed read. The caller knows which side of the
    record it is reading; there is no reason to re-derive it from prose. It is injected rather than
    imported so this module stays free of the transport layer — the same reason
    the rest of this package does.

    An unreadable component is NOT a pass. Silence there is the fail-open this
    whole channel exists to remove: the checks derived from the reading would have
    nothing to fire on, and a process would be applied against a component nobody
    could examine.
    """
    if not grants:
        # THE ORDINARY PATH. No grant, no read, no cost — stated here because the
        # alternative was measured as a per-apply platform-call increase for
        # documents that carry no evidence at all.
        return RecheckOutcome(boundary)

    records = {
        getattr(record, "record_digest", None): record
        for record in getattr(registry, "operation_records", ()) or ()
    }

    drifts: List[Dict[str, Any]] = []
    read_once: Dict[Tuple[Any, ...], Optional[Mapping[str, Any]]] = {}

    def _read(component_id, kind, family=None):
        # Read ONCE per (component, kind, family). The key was the component id
        # alone, which was right while the reading depended on nothing else — and
        # stopped being right the moment the family reached the projection, because
        # the same component digests differently under two families. Two grants of
        # different families over one component then shared the first one's
        # reading, and a truthful record took a false digest drift. Each grant
        # alone passed, which is why only a cross-family pair exposes it.
        key = (component_id, kind, family)
        if key not in read_once:
            read_once[key] = live_identity(component_id, kind, family)
        return read_once[key]

    for grant in grants:
        digest = getattr(grant, "record_digest", None)
        record = records.get(digest)
        if record is None:
            # A grant whose record is gone is not a drift to describe — there is
            # nothing left to compare it against. It is an unavailability, and it
            # is reported as one so the caller does not read "no drift" as "still
            # evidenced".
            return RecheckOutcome(
                boundary,
                unavailable={
                    "subject": "operation_record",
                    "contract_ref": getattr(grant, "contract_ref", None),
                    "detail": (
                        "the registry no longer holds the operation record this "
                        "grant was minted against"
                    ),
                },
            )
        if account_scope_hash is not None:
            recorded_scope = getattr(record, "account_scope_hash", None)
            if recorded_scope != account_scope_hash:
                drifts.append(
                    {
                        "reason": "account_scope",
                        "contract_ref": getattr(grant, "contract_ref", None),
                        # Scope hashes identify an ACCOUNT. Neither side is served.
                        "recorded": "<redacted>",
                        "observed": "<redacted>",
                    }
                )
        for label, recorded in (
            ("operation", getattr(record, "operation_identity", None)),
            ("connection", getattr(record, "connection_identity", None)),
        ):
            component_id = getattr(recorded, "component_id", None)
            if not component_id:
                continue
            observed = _read(component_id, label, getattr(record, "family", None))
            reason = (observed or {}).get("reason") if observed is not None else None
            if observed is None or reason:
                # TWO CAUSES, TWO SENTENCES. Both fail closed — an identity nobody
                # could establish never authorises a retry — but they name different
                # systems, and a remediation aimed at the wrong one is worse than
                # none. Kept as one code because the CONSEQUENCE is identical and a
                # caller branches on consequence; the detail says which.
                detail = (
                    f"the account's identity for the {label} component "
                    f"{component_id!r} could not be read"
                    if reason != "projection_unsupported"
                    else (
                        f"the {label} component {component_id!r} was read from the "
                        "account, but this server cannot project its configuration "
                        "for comparison — the account is not at fault and "
                        "re-authoring will not change it"
                    )
                )
                return RecheckOutcome(
                    boundary,
                    unavailable={
                        "subject": label,
                        "component_id": component_id,
                        "contract_ref": getattr(grant, "contract_ref", None),
                        "reason": reason or "account_unreadable",
                        "detail": detail,
                    },
                )
            drifts.extend(_identity_drifts(label, recorded, observed))

    # Deduplicated by the whole reason tuple: the same component drifting under two
    # grants is one drift, reported once.
    seen, unique = set(), []
    for drift in drifts:
        key = (drift.get("reason"), drift.get("component_id"), drift.get("contract_ref"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(drift)
    return RecheckOutcome(boundary, drifts=unique)


def live_identity_reader(boomi_client, *, read_component_xml=None):
    """A ``live_identity`` callable backed by the account, built once per apply.

    Kept HERE rather than in the apply module so the recheck's two halves — what
    to compare, and how to obtain the live side — live together and can be tested
    together. The transport is still injected: ``read_component_xml`` defaults to
    the repository's component getter but a caller supplies its own, which is what
    lets the boundary tests drive a real apply against a fake client.

    Returns the identity, or a mapping carrying ``reason`` when none could be
    established. The two reasons are NOT interchangeable and this docstring said
    the opposite for one round: it argued that collapsing every cause was
    deliberate, because a transport error and an unprojectable body have the same
    consequence. They do — neither authorises a retry — but they do not have the
    same CAUSE, and the served sentence named the account for both. Live QA
    measured a projection gap on the large majority of one account's connector
    components, every one of them reported as an account that could not be read.
    So ``account_unreadable`` and ``projection_unsupported`` are distinguished
    here and stay distinguished all the way to the caller. Both still fail closed.
    """
    from .digests import component_config_digest_v1

    if read_component_xml is None:
        from ..categories.components._shared import component_get_xml

        def read_component_xml(component_id, _kind):
            return component_get_xml(boomi_client, component_id)

    def live_identity(component_id, kind, family=None):
        """Read one component, or say WHY it could not be turned into an identity.

        Returns the identity, or a ``{"reason": …}`` mapping the caller renders.
        A first version collapsed every failure into ``None`` and the comparison
        rendered all of them as "the account's identity … could not be read" —
        which is false for two of the causes and sends an operator to look at the
        wrong system. Live QA measured it on THIRTY of forty-five of this account's
        connector components: the read succeeded perfectly and the SERVER could not
        project the result, because the projection is an allowlist and refuses any
        component carrying a field the published spec does not list. The remediation
        that sentence carries — recompile, ingest evidence — cannot resolve a
        projection gap in this repository's own code.
        """
        try:
            fetched = read_component_xml(component_id, kind)
        except Exception:  # noqa: BLE001 — a transport failure IS an unread account
            return {"reason": "account_unreadable"}
        if not isinstance(fetched, Mapping):
            return {"reason": "account_unreadable"}
        xml = fetched.get("xml") or fetched.get("component_xml")
        version = fetched.get("version")
        if not xml or version in (None, ""):
            return {"reason": "account_unreadable"}
        try:
            version = int(version)
        except (TypeError, ValueError):
            return {"reason": "account_unreadable"}
        try:
            # THE RECORD'S FAMILY, when the caller supplies it. The projection is
            # per-family and the default is `rest`; digesting a database component
            # under the REST projection compares two different things, and the
            # evidence record carries the family precisely so it need not be
            # guessed.
            digest = (
                component_config_digest_v1(xml, kind, family)
                if family
                else component_config_digest_v1(xml, kind)
            )
        except Exception:  # noqa: BLE001
            # NOT an account failure. The account answered; this server cannot
            # project what it answered with.
            return {"reason": "projection_unsupported"}
        return {"version": version, "config_digest": digest}

    return live_identity


__all__ = [
    "DRIFT_REASONS",
    "RecheckOutcome",
    "live_identity_reader",
    "recheck_grant_identities",
]
