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
    CONNECTOR_REPLAY_SUBMITTED_XML_UNREADABLE,
    SUBMITTED_XML_UNSETTLED_REASONS,
    submitted_xml_unsettled_summary,
)


class ConnectorIdentityError(Exception):
    """A connector component's identity cannot be stood behind.

    Carries the stable code so the surfaces that translate authoring failures
    into responses do not have to re-derive it from the message.
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        component_key: str,
        failures: "Tuple[ConnectorIdentityError, ...]" = (),
        partial: "Optional[TrustedConnectorResolutionSnapshotV1]" = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.component_key = component_key
        #: EVERY component that failed, not just the first. A surface whose
        #: contract is to report everything wrong at once cannot honour it if the
        #: builder stops at the first bad component — and the surface that
        #: REFUSES does not care how many there were, so carrying them all costs
        #: it nothing.
        self.failures = failures or (self,)
        #: The identities that DID resolve. Discarding them suppressed
        #: snapshot-dependent diagnostics for components that were perfectly fine.
        self.partial = partial


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
    #: Whether the document carried a BLANK operation type beside anything else.
    #: Separate from ``action_contradicted`` because the two rungs that read this
    #: model want opposite things: the rung that REFUSES caller bytes treats a
    #: blank as unsettled, and the rung that RESOLVES account bytes must still
    #: read the real verb sitting next to it or the checks built on it go silent.
    action_blank_present: Optional[bool] = None
    #: The component type the DOCUMENT declares, which is what the platform
    #: installs. Distinct from the type declared beside it in the request: a
    #: request declaring a connection while submitting an action payload installs
    #: an action, so any rule selected by the request's declaration is skipped by
    #: mis-declaring one field.
    document_component_type: Optional[str] = None
    #: Whether the document named MORE THAN ONE operation type. ``None`` when no
    #: document was read. Carried rather than inferred from ``action is None``,
    #: which is true for two different reasons — a contradiction, and a document
    #: that names no operation type at all. A connector CONNECTION legitimately
    #: names none, so the proxy refused every raw-XML connection with a message
    #: telling its author to supply one operation type instead of the several it
    #: had not written. The reader knows which case it saw; discarding that and
    #: making the caller re-derive it is the same weaker-key defect one level in.
    action_contradicted: Optional[bool] = None
    #: Whether the DOCUMENT this came from parsed. ``None`` when no document was
    #: read at all (a config projection). Tracked rather than inferred: the first
    #: version derived it from "did we find a family or an action", which labels a
    #: perfectly well-formed component for a family this module does not classify
    #: as unreadable — and that is exactly the unsupported connector the raw-XML
    #: escape hatch exists to create. A proxy for a fact is not the fact.
    document_parsed: Optional[bool] = None
    #: ``"config"`` when the request's own configuration settled this, ``"live"``
    #: when it was read back from the component as it exists in the account. The
    #: distinction is the whole point: a declaration compared against the config
    #: it was derived FROM cannot disagree, so only a ``"live"`` identity makes
    #: the comparison downstream independent evidence.
    source: str = "config"

    @property
    def resolved(self) -> bool:
        """Whether this identity settles enough to be compared against."""
        return self.route_state == "static" and self.family is not None

    @property
    def readable(self) -> bool:
        """Whether the document this came from parsed.

        Says nothing about whether it CLASSIFIED. A well-formed component for a
        family this module does not recognise is readable and unclassified, and
        those are the components the raw-XML escape hatch exists to create.
        """
        return self.document_parsed is not False


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


def live_identity_from_component_xml(
    component_key: str, component_xml: str
) -> ResolvedConnectorComponentIdentityV1:
    """The identity a component ALREADY APPLIED in the account carries.

    Read from the platform's own XML, so it is the account's answer rather than
    the request's. This is what makes the declared-vs-resolved comparison mean
    something: a request that declares a GET while REUSING a component the
    account stores as a POST is exactly the case the comparison exists to catch,
    and it is invisible while both halves come from the same config.

    Credential-free by construction — only the component's type, its connector
    subtype, its operation verb and its stored path are read, and none of those
    can hold a secret. Deliberately NOT the config digest: that reads the full
    projection and refuses on unknown content, which is right for an evidence
    digest and wrong for a pre-write comparison that must tolerate a component
    it only partly understands.

    STREAMED, not tree-built, and ENTITY DECLARATIONS ARE REFUSED. The bytes
    reaching this function are not always the account's: a raw create hands its
    own ``config["xml"]`` here, so a caller controls them, and a few hundred
    bytes of internal entity declarations measured a 2,300-fold expansion on a
    plan path. External entities do not resolve — the standard library does not
    fetch them — so this is amplification rather than disclosure, and the
    refusal lands on the DECLARATION, before any expansion is performed.

    The repository already carries three copies of a regex that screens for
    ``<!DOCTYPE``/``<!ENTITY`` before parsing. A fourth copy would be the
    hand-model this slice keeps finding, so this asks the PARSER instead: expat
    reports every entity declaration it reads, and a reader that never builds a
    tree cannot hold an expanded one. A document carrying a DOCTYPE is simply
    unreadable here, which is the answer this function already gives for
    anything else it cannot read.
    """
    import xml.parsers.expat

    from ..categories.components.builders.connector_builder import connector_family_of

    class _EntityDeclared(Exception):
        pass

    subtype = None
    component_type = None
    actions = []
    path_fields = []

    def _start(name, attributes):
        nonlocal subtype, component_type
        local = name.rsplit("}", 1)[-1].rsplit(":", 1)[-1]
        if subtype is None and "subType" in attributes:
            subtype = attributes["subType"]
        # WHAT THE PAYLOAD SAYS IT IS. The caller's declaration beside the XML is
        # not this: a request declaring a connection while submitting an action
        # payload installs an ACTION, because the platform reads the bytes and
        # not the request. Reading the declaration let that request skip the very
        # rule the declaration was used to select.
        if component_type is None and "type" in attributes:
            component_type = attributes["type"]
        if "customOperationType" in attributes:
            actions.append(attributes["customOperationType"])
        if local == "field" and attributes.get("id") == "path":
            path_fields.append(attributes.get("value"))

    def _entity_declared(*_args, **_kwargs):
        raise _EntityDeclared

    unreadable = ResolvedConnectorComponentIdentityV1(
        component_key=component_key, source="live", document_parsed=False
    )
    if not isinstance(component_xml, str):
        return unreadable
    parser = xml.parsers.expat.ParserCreate()
    parser.EntityDeclHandler = _entity_declared
    parser.StartElementHandler = _start
    try:
        parser.Parse(component_xml, True)
    except (_EntityDeclared, xml.parsers.expat.ExpatError):
        # UNREADABLE, and the caller above decides what that means. For the
        # account's bytes it means silence; for the caller's own it means
        # refusal. The distinction cannot be made here, which is why this
        # returns a value the caller can recognise rather than deciding.
        return unreadable

    family = connector_family_of(subtype)

    # EVERY verb the document carries, not the first one. A document with two
    # different ``customOperationType`` values is one this reader cannot resolve:
    # which one the platform runtime honours is not established here, and picking
    # the first made a decoy element spliced ahead of the real operation decide
    # the identity. Refusing to choose is the only answer that cannot be wrong —
    # the same rule this module applies to every other fact it cannot settle.
    #
    # Identical repeats are not a conflict: a document may state one verb twice.
    # ONE SET, TWO ANSWERS. A blank verb is normalised to a distinct marker
    # rather than discarded, so both questions this document must answer come
    # from the same place: how many different things does it say, and is the one
    # thing it says a verb at all.
    #
    # Discarding blanks answered the first question wrongly. `customOperationType
    # =""` alone had to stop being a "settled" empty-string verb — that was a
    # one-character defeat of the rule — but filtering blanks out before counting
    # made a blank on the real operation plus a nonblank decoy elsewhere collapse
    # to a singleton, so the payload was accepted while the verb it installs is
    # blank. Keeping the blank IN the set makes that a contradiction, which it is.
    _BLANK = ""
    verb_tokens = {
        (value.strip().upper() if value.strip() else _BLANK)
        for value in actions
        if isinstance(value, str)
    }
    distinct_actions = verb_tokens - {_BLANK}
    # TWO RUNGS, OPPOSITE DISPOSITIONS, SO TWO SIGNALS. The refusal rung is
    # additive — every extra thing it treats as unsettled REFUSES more — while
    # the resolution rung is subtractive: everything it treats as unsettled makes
    # it resolve LESS, and resolving less is how a net goes silent. One predicate
    # serving both meant widening the refusal quietly widened the silence.
    #
    # Measured: a component the account stores with one real verb and a stray
    # blank stopped resolving, so the blank-path refusal and the identity
    # comparison both went quiet and a path-less connector action applied.
    #
    # RESOLUTION reads the verbs the document actually names: exactly one
    # distinct non-blank verb IS the verb, whatever else sits beside it.
    action = next(iter(distinct_actions)) if len(distinct_actions) == 1 else None
    # REFUSAL is the caller's problem and keeps the stricter reading: a blank
    # beside a real verb means the bytes do not settle which one is installed.
    contradicted = len(distinct_actions) > 1
    blank_present = _BLANK in verb_tokens
    resolved_enough = family is not None and action is not None

    # THE STORED PATH, and it is the whole point for a reused component. Reading
    # only the subtype and the verb made every live identity "static", which
    # DISARMED the blank-path net exactly where it matters: a reused operation
    # the account stores with a blank path needs a binding, and reporting it as a
    # settled static route let a process be applied whose connector action
    # carried no Path property at all.
    route_state = "static" if resolved_enough else "unavailable"
    if family == "rest" and resolved_enough:
        if not path_fields:
            # A REST operation whose path we cannot find is a route we cannot
            # read. Silence, not "static" — "static" is the answer that disarms.
            route_state = "unavailable"
        elif any((value or "").strip() == "" for value in path_fields):
            route_state = "dynamic"

    return ResolvedConnectorComponentIdentityV1(
        component_key=component_key,
        family=family,
        action=action,
        route_state=route_state,
        source="live",
        document_parsed=True,
        action_contradicted=contradicted,
        action_blank_present=blank_present,
        document_component_type=component_type,
    )


def build_connector_resolution_snapshot(
    components: Sequence[Any],
    *,
    live_projections: Optional[Mapping[str, Mapping[str, Any]]] = None,
    live_component_xml: Optional[Mapping[str, str]] = None,
    reused_keys: Optional[Sequence[str]] = None,
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
    live_xml = live_component_xml or {}
    # Keys apply will REUSE, answered by the apply-time predicate rather than
    # inferred here. It decides ONE thing: whether the request's own bytes will
    # be written at all. When they will not be, they are not this component's
    # identity, however emphatically they are declared.
    reused = set(reused_keys or ())
    resolved = []
    failures: list = []
    for component in components:
        component_type = getattr(component, "type", None)
        if not isinstance(component_type, str) or "connector" not in component_type:
            continue
        key = getattr(component, "key", None)
        if not isinstance(key, str) or not key:
            continue
        config = getattr(component, "config", None) or {}

        # ONE PRECEDENCE CHAIN, and the question it answers at every step is the
        # same one: which bytes will this component ACTUALLY have after apply?
        #
        # 1. SUBMITTED XML outranks the declarations beside it — WHEN IT WILL BE
        #    WRITTEN. That XML is what gets installed for a real create, so a
        #    request may declare GET while the payload installs a POST, and
        #    reading only the declarations let the assertion pass on the thing
        #    that was NOT applied. But a raw create that COLLIDES is planned as a
        #    reuse: apply returns the existing component and the submitted XML is
        #    never written, so treating it as the identity compared the
        #    declaration against a payload nothing applies and missed both a
        #    stored verb mismatch and a stored blank path.
        # 2. Otherwise the account's reading, for a component that names one and
        #    is not an UPDATE. An update is not a reuse: its live component is
        #    the state being CHANGED, so preferring it would reject a legitimate
        #    POST-to-GET update as a mismatch with its own former self, and read
        #    the path requirement off the route the request is replacing.
        # 3. Otherwise the config's own projection.
        #
        # A reuse therefore reaches rung 2 and takes the account's bytes. An
        # explicit reuse-first rung was written here and then REMOVED after a
        # mutation showed it decided nothing: a reused component is always a
        # declared create, so rung 2's update test can never exclude it. A rung
        # no mutant can reach is a rung the next reader has to disprove.
        action = getattr(component, "action", None)
        submitted = config.get("xml") if isinstance(config, Mapping) else None
        if key not in reused and isinstance(submitted, str) and submitted.strip():
            identity = live_identity_from_component_xml(key, submitted)
            # FAIL CLOSED on the caller's OWN bytes. The tolerance this module
            # applies elsewhere — an unreadable component contributes nothing and
            # the comparison skips it — is right for the ACCOUNT's bytes, where an
            # unreadable answer is a platform problem and refusing would turn a
            # transient error into an authoring refusal. It is INVERTED here,
            # because the caller chooses whether its own payload is readable.
            #
            # Measured: a thirty-eight byte document-type prefix carrying an
            # entity subset made this identity empty, the comparison skip, and an
            # apply proceed that the same document without the prefix refuses —
            # and the platform DISCARDS that prefix on write, so what landed was
            # exactly the refused bytes. Two guards this slice ships, defeated by
            # a prologue neither of them was about.
            # THE CALLER'S OWN BYTES MUST SETTLE WHAT THEY INSTALL. Two ways
            # they can fail to: the document does not parse, or it parses and
            # says two contradictory things. Both leave nothing to check against
            # the declaration, and both are the caller's choice — which is the
            # whole reason this branch refuses where the account branch is
            # silent. The first version refused only unparseability, so a
            # document naming two different verbs was accepted with an empty
            # action and the comparison skipped it: a component declared with a
            # read verb could install XML that executes a write one.
            if not identity.readable:
                failures.append(
                    ConnectorIdentityError(
                        CONNECTOR_REPLAY_SUBMITTED_XML_UNREADABLE,
                        (
                            "component {0!r} supplies raw component XML that "
                            "cannot be read, so nothing can be checked about what "
                            "it would install. Remove any document-type or entity "
                            "declaration: the platform discards them on write, so "
                            "they change nothing about the component and hide "
                            "everything about it."
                        ).format(key),
                        component_key=key,
                    )
                )
                continue
            # CONTRADICTED, not merely unsettled. Keying on "no action was
            # resolved" refused every raw-XML connector CONNECTION, which names
            # no operation type because it has none to name — and told its author
            # to supply one operation type instead of the several they had not
            # written. A document that says nothing about a fact leaves that fact
            # to the configuration beside it; a document that says two
            # contradictory things settles nothing and is the caller's to fix.
            # A REST CONNECTOR ACTION MUST NAME ITS VERB. Everything about this
            # rule's scope is now MEASURED rather than asserted, because the
            # previous version asserted its scope in a comment and was wrong
            # about two thirds of it.
            #
            # WHERE THE VERB LIVES IS PER FAMILY, and only REST puts it in
            # `customOperationType` — this repository's own builders emit
            # `<DatabaseGetAction>`/`<DatabaseSendAction>` for the database
            # family, where the verb IS the element name, and SOAP carries
            # `operationType`. Scoping this to "families we model" therefore
            # refused raw-XML replay of the platform's own bytes for two of the
            # three, which is the block-a-documented-feature mistake this slice
            # has now made twice. The scope is one family because one family is
            # what has been measured; extending it needs the same measurement
            # for the family being added, not a wider adjective.
            #
            # WHAT KIND OF COMPONENT THIS IS comes from the PAYLOAD, not from the
            # request's declaration beside it. The platform installs the bytes,
            # so a request declaring a connection while submitting an action
            # payload installs an action — and reading the declaration let
            # exactly that request skip this rule.
            document_type = identity.document_component_type or ""
            if (
                "connector-action" in document_type
                and identity.family == "rest"
                and identity.action is None
                and not identity.action_contradicted
            ):
                failures.append(
                    ConnectorIdentityError(
                        CONNECTOR_REPLAY_SUBMITTED_XML_UNREADABLE,
                        (
                            "component {0!r} supplies raw component XML for a "
                            "connector action that names no operation type, so "
                            "the verb it would install is unsettled. Name the "
                            "operation type in the submitted XML."
                        ).format(key),
                        component_key=key,
                    )
                )
                continue
            if identity.family == "rest" and (
                identity.action_contradicted or identity.action_blank_present
            ):
                failures.append(
                    ConnectorIdentityError(
                        CONNECTOR_REPLAY_SUBMITTED_XML_UNREADABLE,
                        (
                            "component {0!r} supplies raw component XML that "
                            "names {1}, so what it would install is not settled "
                            "by its own bytes. Supply exactly one non-blank "
                            "operation type."
                        ).format(
                            key,
                            "more than one operation type"
                            if identity.action_contradicted
                            else "an operation type alongside a blank one",
                        ),
                        component_key=key,
                    )
                )
                continue
            resolved.append(identity.model_copy(update={"source": "config"}))
            continue
        if key in live_xml and action != "update":
            resolved.append(live_identity_from_component_xml(key, live_xml[key]))
            continue
        if key in reused:
            # A REUSE whose account bytes could not be read resolves to NOTHING.
            # Falling through to the config would make the request's own values
            # authoritative for a component apply is about to DISCARD — measured:
            # a reused operation whose read failed reported a dynamic route from
            # config and demanded a path binding the account's component may not
            # need. "I could not tell" has to stay uncertain here; it is the one
            # answer that cannot be wrong.
            resolved.append(
                ResolvedConnectorComponentIdentityV1(
                    component_key=key, source="live"
                )
            )
            continue
        projection = normalized_identity_projection(
            config, live_projection=live.get(key)
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
    snapshot = TrustedConnectorResolutionSnapshotV1(
        identities=tuple(sorted(resolved, key=lambda item: item.component_key))
    )
    if failures:
        first = failures[0]
        raise ConnectorIdentityError(
            first.code,
            str(first),
            component_key=first.component_key,
            failures=tuple(failures),
            partial=snapshot,
        )
    return snapshot


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
        if identity is None:
            continue
        # NO `resolved` gate here. `resolved` answers a question about the ROUTE,
        # and this loop asks about the FAMILY and the VERB — independent facts.
        # Gating on it meant an operation whose path could not be read stopped
        # having its verb checked at all, so a component the account stores as a
        # PATCH could be declared a GET and applied. The per-field checks below
        # already skip whatever is genuinely unknown, which is the precise
        # version of what the gate was doing bluntly.
        #
        # This hole was OPENED by the fix for the blank-path critical: adding an
        # `unavailable` route state gave `resolved` a new way to be false.
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
                        "component {0!r} is declared with {1} {2!r}, but {3} "
                        "resolves to {4!r}. The declaration is an assertion, not "
                        "an override."
                    ).format(
                        key,
                        label,
                        theirs,
                        # Naming the SOURCE is the difference between a real
                        # finding and a tautology: "the account stores" is
                        # independent evidence, "its own configuration" is not.
                        "the component stored in the account"
                        if identity.source == "live"
                        else "its own configuration",
                        mine,
                    ),
                    component_key=key,
                )
    return dict(declared)
