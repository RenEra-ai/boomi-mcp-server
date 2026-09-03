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

import re
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

#: What a recheck compares, in the order it reports them. Written as data so the
#: reasons are a closed set: a caller cannot be handed a reason this module does
#: not define, and a test can assert the set rather than a sample of it.
DRIFT_REASONS: Tuple[str, ...] = (
    "route_coverage",
    "operation_version",
    "operation_config_digest",
    "connection_version",
    "connection_config_digest",
    "account_scope",
)


class RecheckOutcome:
    """One recheck's verdict, and the concrete evidence it checked against.

    ``bindings`` carries what each grant actually resolved to — the identities,
    route coverage kind and capture digest of the record that authorised it. The
    attestation is built from THIS rather than from a second registry lookup at
    write time: a row can rotate or be removed between the two, and a durable
    accounting record that depends on the registry's present contents cannot say
    what authorised a write once the registry has moved on.
    """

    __slots__ = ("drifts", "unavailable", "boundary", "bindings")

    def __init__(self, boundary: str, drifts=(), unavailable=None, bindings=()):
        self.boundary = boundary
        self.drifts = tuple(drifts)
        self.unavailable = unavailable
        self.bindings = tuple(bindings)

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

    # KEYED BY WHAT IDENTIFIES A RECORD, not by the digest alone. A dict
    # comprehension over `record_digest` silently keeps the LAST record on a
    # collision, and the loader dedupes on `contract_ref` — it does not reject
    # duplicate digests. A grant minted for one contract would then be rechecked
    # against a different record entirely, so matching identities on the wrong
    # record could return `ok` while the granted one had drifted. The pair is what
    # a grant actually names, and a colliding pair is refused rather than resolved.
    records = {}
    for record in getattr(registry, "operation_records", ()) or ():
        key = (
            getattr(record, "record_digest", None),
            getattr(record, "contract_ref", None),
        )
        if key in records:
            return RecheckOutcome(
                boundary,
                unavailable={
                    "subject": "operation_record",
                    "contract_ref": key[1],
                    "reason": "ambiguous_record",
                    "detail": (
                        "the registry holds more than one operation record for this "
                        "contract and digest, so which one authorised the grant "
                        "cannot be decided"
                    ),
                },
            )
        records[key] = record

    drifts: List[Dict[str, Any]] = []
    bindings: List[Dict[str, Any]] = []
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
        record = records.get(
            (
                getattr(grant, "record_digest", None),
                getattr(grant, "contract_ref", None),
            )
        )
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
        # ROUTE COVERAGE, WHICH NOTHING READ. Every comparison in the closed set
        # above can match while the record covers a different route entirely — or
        # covers enumerated static routes while the call composes its path per
        # document. The models say it plainly: a dynamically bound path requires
        # SERVICE-WIDE coverage, because its route is composed per document, so no
        # static digest identifies it and none may be minted. The compiler cannot
        # check this (it has no live reading) and the ledger assigns it here; it
        # simply was not implemented, and the closed field set made that invisible.
        coverage = getattr(record, "route_coverage", None)
        coverage_kind = getattr(coverage, "kind", None)
        if getattr(grant, "dynamic_path", False) and coverage_kind != "service_wide":
            drifts.append(
                {
                    "reason": "route_coverage",
                    "contract_ref": getattr(grant, "contract_ref", None),
                    "recorded": coverage_kind or "none",
                    "observed": "dynamic_path",
                }
            )
        # The static-coverage arm is checked AFTER the identities are read, below,
        # because the route digest is computed from the live bytes those reads
        # return. A first version expected the digest on the grant; the grant model
        # forbids extra fields and has none, so every real static grant was refused
        # and only a fake grant that invented the field passed.

        # AN ACCOUNT-BOUND RECORD REQUIRES AN OBSERVED SCOPE. This ran only when
        # one had been derived, so a boundary that could not read the account
        # skipped the comparison entirely and the recheck passed — the same
        # fail-open ordering the minter had, at the layer that exists to catch
        # what the minter missed. A record naming an account scope is claiming
        # WHICH account it was observed in, and an unknown account cannot satisfy
        # that claim.
        recorded_scope = getattr(record, "account_scope_hash", None)
        if recorded_scope is not None or account_scope_hash is not None:
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
        read_xml: Dict[str, Any] = {}
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
                    (
                        f"the {label} component {component_id!r} is DELETED in the "
                        "account, so the replay evidence describes a component that "
                        "is no longer live"
                    )
                    if reason == "component_deleted"
                    else f"the account's identity for the {label} component "
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
            read_xml[label] = (observed or {}).get("xml")

        if coverage_kind == "static_path":
            covered = tuple(getattr(coverage, "route_digests", ()) or ())
            route_digest = None
            if read_xml.get("connection") and read_xml.get("operation"):
                try:
                    from .digests import route_digest_v1

                    route_digest = route_digest_v1(
                        read_xml["connection"], read_xml["operation"]
                    )
                except Exception:  # noqa: BLE001 — an uncomputable route is not a covered one
                    route_digest = None
            # A static record authorises the routes it enumerates and no others, and
            # a route this boundary cannot compute is NOT thereby covered — that is
            # the fail-open direction.
            if route_digest is None or route_digest not in covered:
                drifts.append(
                    {
                        "reason": "route_coverage",
                        "contract_ref": getattr(grant, "contract_ref", None),
                        "recorded": "static_path",
                        # A route is a PATH. Neither side is served.
                        "observed": "<redacted>" if route_digest else "uncomputable",
                    }
                )

        # THE CONCRETE EVIDENCE, captured from the record this grant resolved to.
        capture = getattr(record, "capture", None)
        bindings.append(
            {
                "contract_ref": getattr(grant, "contract_ref", None),
                "operation_ref": getattr(grant, "operation_ref", None),
                "call_source_path": getattr(grant, "call_source_path", None),
                # CARRIED FROM THE GRANT, not re-derived. The minter recorded
                # which root it minted for and which connection the operation
                # resolves to; asking a second authority here is how the durable
                # record comes to name a different connection than the check did.
                "process_root_ref": getattr(grant, "process_root_ref", None),
                "connection_ref": getattr(grant, "connection_ref", None),
                "record_digest": getattr(record, "record_digest", None),
                "account_scope_hash": getattr(record, "account_scope_hash", None),
                "operation_component_id": getattr(
                    getattr(record, "operation_identity", None), "component_id", None
                ),
                "operation_version": getattr(
                    getattr(record, "operation_identity", None), "version", None
                ),
                "connection_component_id": getattr(
                    getattr(record, "connection_identity", None), "component_id", None
                ),
                "connection_version": getattr(
                    getattr(record, "connection_identity", None), "version", None
                ),
                # BOTH CONFIGURATION DIGESTS. This module compares them — they
                # are two of its six drift reasons — and the durable record
                # carried neither, so an auditor reading an attestation could see
                # that ids and versions had been checked and could not see that
                # configuration had been. A credential-only version advance is
                # exactly the case these distinguish, and it is the case the
                # whole comparison exists for.
                #
                # They are digests over a projection that deliberately EXCLUDES
                # credential material (the username, the OAuth2 element, the
                # token URL and every credential reference), so recording them
                # adds an identifier, never a secret.
                "operation_config_digest": getattr(
                    getattr(record, "operation_identity", None), "config_digest", None
                ),
                "connection_config_digest": getattr(
                    getattr(record, "connection_identity", None), "config_digest", None
                ),
                # THE KIND AND THE DIGESTS. The kind alone was recorded on the
                # stated ground that "a static coverage carries the routes it
                # covers, and a route is a path" — which is false about this
                # model: it carries `route_digests`, which are versioned hashes,
                # not paths. The reasoning was right about paths and wrong about
                # what the field holds, and the cost was that an auditor could
                # read that SOME static coverage authorised a write and never
                # which route it covered — the registry rotates, and the record is
                # then unreconstructable. This module computes and COMPARES the
                # live route digest a few lines above; recording what it compared
                # is the whole point of a durable binding.
                "route_coverage_kind": getattr(coverage, "kind", None),
                "route_digests": tuple(getattr(coverage, "route_digests", ()) or ()),
                "capture_digest": getattr(capture, "capture_digest", None)
                or getattr(capture, "digest", None),
                # SERVICE-WIDE COVERAGE HAS ITS OWN CAPTURE, and its digest can
                # differ from the operation record's. Persisting only the latter
                # leaves the attestation unable to name the capture that
                # established the coverage once the registry has rotated — which
                # is the whole reason the concrete tuple is carried at all.
                "route_capture_digest": (
                    getattr(
                        getattr(coverage, "service_wide_capture", None),
                        "capture_digest",
                        None,
                    )
                    or getattr(
                        getattr(coverage, "service_wide_capture", None), "digest", None
                    )
                ),
            }
        )

    # Deduplicated by the whole reason tuple: the same component drifting under two
    # grants is one drift, reported once.
    seen, unique = set(), []
    for drift in drifts:
        key = (drift.get("reason"), drift.get("component_id"), drift.get("contract_ref"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(drift)
    return RecheckOutcome(boundary, drifts=unique, bindings=bindings)


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
        # A SOFT DELETE IS NOT A LIVE COMPONENT. Boomi returns a deleted component
        # with its original version and XML, and the configuration digest projects
        # neither the root's deletion flag nor anything that moves with it — so
        # every comparison passes and the write proceeds against a component that
        # is gone. This repository already recorded the platform behaviour at
        # discovery; the reader simply was not looking.
        #
        # THE COMPONENT'S OWN PARSER IS THE AUTHORITY, and a first version claimed
        # the getter supplied `deleted` "where the transports differ". Live QA
        # measured that branch DEAD — the getter builds its dict from an explicit
        # key list with no `deleted` entry, on 34 of 34 connector components — so a
        # hand-rolled regex over the first two kilobytes was the entire guard. The
        # repository's own `parse_component_xml` reads the flag; it is asked first,
        # the fetched key is honoured if a caller does supply one, and the regex
        # remains only as a last resort for a payload neither can read.
        deleted = fetched.get("deleted")
        if deleted is None:
            try:
                from ..categories.components._shared import parse_component_xml

                deleted = (parse_component_xml(xml) or {}).get("deleted")
            except Exception:  # noqa: BLE001 — fall through to the textual probe
                deleted = None
        if deleted is None:
            match = re.search(r'\bdeleted="([^"]*)"', xml[:2048])
            deleted = match.group(1) if match else None
        if str(deleted).strip().lower() == "true":
            return {"reason": "component_deleted"}
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
        # THE BYTES ARE KEPT. A route digest is computed from the connection and
        # the operation TOGETHER, and this reader is the only thing that holds
        # both — the grant cannot carry a route digest, because the route depends
        # on live configuration the compiler never saw.
        return {"version": version, "config_digest": digest, "xml": xml}

    return live_identity


__all__ = [
    "DRIFT_REASONS",
    "RecheckOutcome",
    "live_identity_reader",
    "recheck_grant_identities",
]
