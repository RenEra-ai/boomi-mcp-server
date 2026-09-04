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

from typing import Any, Dict, Literal, Mapping, Optional, Sequence, Tuple

from pydantic import BaseModel, ConfigDict

from ..errors import (
    CONNECTOR_REPLAY_IDENTITY_MISMATCH,
    CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE,
    CONNECTOR_REPLAY_SUBMITTED_XML_UNREADABLE,
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
        field: "Optional[str]" = None,
        remediation: "Optional[str]" = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.component_key = component_key
        #: WHICH DECLARED FIELD disagreed, as a token rather than as English. Live
        #: QA measured the gap this closes: a path mismatch served `field: ""`,
        #: no evidence, no cause codes and no contract entry id, so the only
        #: statement of what was wrong sat inside a sentence — and the generic
        #: remediation it inherited told the caller to correct the family and the
        #: action, which are the two fields that were right. A caller following
        #: it literally edits what is already correct.
        self.field = field
        #: WHAT TO DO, from the code that knows why. Inferring it downstream from
        #: the field alone produced a sentence that was wrong for two of the three
        #: cases: it told a caller their declaration disagreed with the account,
        #: which is false for raw submitted XML that disagrees with ITSELF, and
        #: false again for a stored route conflict where no declaration exists.
        self.remediation = remediation
        #: EVERY component that failed, not just the first. A surface whose
        #: contract is to report everything wrong at once cannot honour it if the
        #: builder stops at the first bad component — and the surface that
        #: REFUSES does not care how many there were, so carrying them all costs
        #: it nothing.
        self.failures = failures or (self,)
        #: The identities that DID resolve. Discarding them suppressed
        #: snapshot-dependent diagnostics for components that were perfectly fine.
        self.partial = partial


#: The families whose verb location this module has MEASURED — REST and SOAP in
#: `GenericOperationConfig`, the database family in its action element. A family
#: outside this set is one whose bytes we cannot judge, and the raw-XML escape
#: hatch exists to create exactly those.
_FAMILIES_WITH_A_KNOWN_VERB_LOCATION = frozenset({"rest", "soap_client", "database"})


#: CLOSED, as the plan specifies. These were open optional strings, which is a
#: contract that cannot be violated because it promises nothing: any typo, any
#: value from a future caller, any silently-renamed mode passed validation.
ResolvedConnectorModeV1 = Literal["reuse", "create", "update"]

#: WHICH ARTIFACT ANSWERED. The distinction is load-bearing rather than
#: descriptive: a declaration compared against the configuration it was derived
#: from cannot disagree, so only a live readback makes the comparison downstream
#: independent evidence — and the refusal message says which it was.
ConnectorIdentityAuthorityV1 = Literal[
    "normalized_structured_fields",
    "submitted_xml",
    "live_readback_xml",
]


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
    #: Whether the account stores MORE THAN ONE distinct non-blank route for this
    #: component. Its own field because it is not a degree of unreadability: a
    #: route we could not find is silence, and a route that contradicts itself is
    #: a component that cannot serve one.
    route_conflicting: Optional[bool] = None
    #: Whether the document carried a BLANK operation type beside anything else.
    #: Separate from ``action_contradicted`` because the two rungs that read this
    #: model want opposite things: the rung that REFUSES caller bytes treats a
    #: blank as unsettled, and the rung that RESOLVES account bytes must still
    #: read the real verb sitting next to it or the checks built on it go silent.
    action_blank_present: Optional[bool] = None
    #: What the request will DO with this component — ``"reuse"`` when apply will
    #: use it as it already exists, otherwise the declared action. AC8j asks the
    #: snapshot for the mode, and every consumer was reconstructing it from the
    #: reuse predicate instead of reading it here.
    mode: Optional[ResolvedConnectorModeV1] = None
    #: Which artifact this identity was read from. Closed, and derived rather
    #: than asserted: the reader that produced it sets it.
    authority: Optional[ConnectorIdentityAuthorityV1] = None
    #: The route is bound to an environment extension, so the ACCOUNT decides it
    #: and nothing here can pin it. A closed flag rather than an inference from
    #: an unavailable route state, which has three other causes.
    extension_bound_endpoint: Optional[bool] = None
    #: The account this resolution was taken against. AC8j asks for the ACTUAL
    #: account, and a later slice binds its grants to one; a resolution that
    #: cannot say which account it describes cannot be bound to it.
    account_id: Optional[str] = None
    #: The component and revision the account reading came from. Both arrived
    #: with the fetched bytes and were discarded, so nothing downstream could say
    #: WHICH component in WHICH revision it had resolved.
    component_id: Optional[str] = None
    #: The revision as a STRING, coerced where it enters. The platform client
    #: returns it as an integer, and this model is frozen with a declared type —
    #: so inserting it unvalidated emitted a serializer warning and broke strict
    #: round-trip. My own test hid that by injecting a string the production path
    #: never produces, which is the fixture-cannot-exhibit-the-defect shape this
    #: slice keeps finding elsewhere.
    component_version: Optional[str] = None
    #: The configuration digest of the document this identity was read from,
    #: when that document is admissible to the digest projection. ``None`` when
    #: no document was read or the projection refuses it — a refusal is not a
    #: digest, and recording one anyway would publish a value nothing can
    #: reproduce.
    config_digest: Optional[str] = None
    #: True when the account WAS consulted for this component and could not be
    #: read; ``None`` when it was never consulted. The distinction is the whole
    #: difference between a check that does not apply and a check that failed —
    #: collapsing them is what let an unreadable reuse pass as an absence.
    live_read_failed: Optional[bool] = None
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
    #: The account these identities were resolved against, or ``None`` when the
    #: resolution had no account to consult. Strict: a snapshot that cannot say
    #: which account it describes must say so rather than imply one.
    account_scope: Optional[str] = None

    def for_root(
        self, root_key: str, component_keys: Sequence[str]
    ) -> "TrustedConnectorResolutionSnapshotV1":
        """The identities one process root references, sorted, same scope.

        Takes the keys explicitly rather than deriving them: this model holds no
        root-to-component edges, and inventing one here would be a second graph
        walk beside the compiler's own — the defect class this slice has spent
        the most rounds on. The caller that knows the root's references passes
        them; this projects and preserves the account scope so a projection can
        never claim a wider one than the resolution it came from.
        """
        wanted = set(component_keys)
        return TrustedConnectorResolutionSnapshotV1(
            identities=tuple(
                identity for identity in self.identities
                if identity.component_key in wanted
            ),
            account_scope=self.account_scope,
        )

    def lookup(self, component_key: str) -> Optional[ResolvedConnectorComponentIdentityV1]:
        for identity in self.identities:
            if identity.component_key == component_key:
                return identity
        return None


def rest_route_decision(path_fields, *, modelled: bool, resolved_enough: bool):
    """``(route_state, conflicting, live_path)`` from a component's stored paths.

    PURE, and its own function, for two reasons that both bit. The state, the
    conflict flag and the retained path were derived from two DIFFERENT sets —
    the conflict from normalized routes, the other two from raw spellings — so a
    component repeating one route in two spellings read as non-conflicting AND
    unreadable AND path-less, which made the comparison skip it and accept any
    declaration at all. One function, one set, and they cannot disagree again.

    And the served revision has to fingerprint this DECISION, not just the
    normalizer under it: changing the reader from rejecting two spellings of one
    route to accepting them moved acceptance while every normalizer output stood
    still. A pure function can be projected by calling it, which is why this is
    not a block inside the XML reader — the oracle would otherwise have to build
    a component document to ask a question about a list of strings.
    """
    if not (modelled and resolved_enough):
        return ("static" if resolved_enough else "unavailable"), False, None
    if not path_fields:
        # A REST operation whose path we cannot find is a route we cannot read.
        # Silence, not "static" — "static" is the answer that disarms.
        return "unavailable", False, None

    from ..connector_replay.digests import comparable_path

    # THE CONFLICT IS COUNTED BEFORE ANY EARLY RETURN, because it is a fact about
    # the non-blank routes and nothing about a blank one changes it. The first
    # version tested for a blank path first and returned `dynamic`, so a
    # component holding a blank path AND two distinct routes was reported as
    # merely dynamic — and the conflict refusal it should have triggered was
    # bypassed by supplying the very binding the blank path asks for.
    routes = {comparable_path(v) for v in (x.strip() for x in path_fields) if v}
    conflicting = len(routes) > 1

    if any((value or "").strip() == "" for value in path_fields):
        # A blank path means the route is composed per document. That is still
        # true when the stored routes contradict each other, and the contradiction
        # is still disqualifying — a binding composes ONE of them, and which one
        # is exactly what nobody can say.
        return "dynamic", conflicting, None

    if conflicting:
        # DISTINCT AFTER NORMALIZATION, so a genuine contradiction rather than two
        # spellings of one thing. Unreadable for the same reason a missing path
        # is, and additionally a CONFLICT: one is silence, this is a component
        # that cannot serve a single route whatever anyone declares about it.
        return "unavailable", True, None
    return "static", False, (routes.pop() if routes else None)


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

    #: The element each family's operation configuration is stored in, read off
    #: this repository's own builders rather than assumed. FAMILY-SPECIFIC: a
    #: `GenericOperationConfig` inside a database component is not that family's
    #: operation config, and treating it as one made the reader answer for a
    #: document it had not actually understood.
    _CONFIG_ELEMENTS_BY_FAMILY = {
        "rest": {"GenericOperationConfig"},
        "soap_client": {"GenericOperationConfig"},
        "database": {"DatabaseGetAction", "DatabaseSendAction"},
    }


    class _EntityDeclared(Exception):
        pass

    subtype = None
    component_type = None
    actions = []
    path_fields = []

    # THE OPERATION'S OWN ELEMENT, not the document. Measured from a real
    # platform capture: a connector action stores its facts at
    # ``Operation/Configuration/<FamilyOperationConfig>`` — for REST that
    # element carries `customOperationType` and the `path` field beneath it.
    #
    # Reading the whole document instead made anything ANYWHERE speak for the
    # operation: a verb on an unrelated element became the operation's verb, and
    # an unrelated `field id="path"` made the route look settled. The refusal
    # this module carries for "a document naming two operation types" exists
    # because of that, and with the scope corrected a stray attribute elsewhere
    # is not a contradiction at all — it simply is not the operation's verb.
    stack = []
    config_depth = None

    def _in_operation_config():
        return config_depth is not None and len(stack) >= config_depth

    def _start(name, attributes):
        nonlocal subtype, component_type, config_depth
        local = name.rsplit("}", 1)[-1].rsplit(":", 1)[-1]
        parent = stack[-1] if stack else None
        stack.append(local)
        if config_depth is None and parent == "Configuration" and len(stack) >= 2:
            grandparent = stack[-3] if len(stack) >= 3 else None
            # THE FAMILY'S OWN ELEMENT, by name. Accepting any direct child of
            # `Configuration` left an arbitrary `<Other customOperationType=...>`
            # speaking for the operation — the same defect as reading the whole
            # document, one level narrower. Measured from the builders: REST and
            # SOAP both emit `GenericOperationConfig` (REST carries
            # `customOperationType`, SOAP `operationType`), and the database
            # family emits its verb AS the element.
            # ANCHORED to the path the platform actually stores, measured from a
            # capture: the component's object element, then Operation, then
            # Configuration. Accepting that trio at any depth let a nested
            # look-alike stand in for the real one.
            #
            # EVERY candidate is collected and the FAMILY decides afterwards.
            # Deciding here needed the family before the walk, which meant
            # scanning the raw text for a subtype — and unscoped text is not the
            # document: a comment naming another family hijacked the hint, the
            # real operation config was then ignored, and the identity resolved
            # with a family and no action, which the comparison skips. The root
            # element is the first thing this walk sees; using what it parsed is
            # both stricter and simpler than looking for it twice.
            great = stack[-4] if len(stack) >= 4 else None
            if grandparent == "Operation" and great == "object":
                config_depth = len(stack)
                # The family decides WHERE the verb is; this element is where to
                # look. Database names its verb, the others carry it as an
                # attribute — both measured, from the builders and a capture.
                actions.append(
                    {
                        "element": local,
                        "customOperationType": attributes.get("customOperationType"),
                        "operationType": attributes.get("operationType"),
                        # THE PATHS THIS CANDIDATE OWNS. Collected per candidate
                        # so they are filtered by family alongside it: a shared
                        # list let a decoy sibling's path field make a genuinely
                        # path-less operation resolve as a settled route, and the
                        # blank-path refusal then had nothing to fire on.
                        "paths": [],
                    }
                )
        if subtype is None and "subType" in attributes:
            subtype = attributes["subType"]
        # WHAT THE PAYLOAD SAYS IT IS. The caller's declaration beside the XML is
        # not this: a request declaring a connection while submitting an action
        # payload installs an ACTION, because the platform reads the bytes and
        # not the request. Reading the declaration let that request skip the very
        # rule the declaration was used to select.
        if component_type is None and "type" in attributes:
            component_type = attributes["type"]
        # A DIRECT CHILD of the family config, not any descendant. A nested
        # decoy path otherwise masked an operation that names none.
        if (
            local == "field"
            and attributes.get("id") == "path"
            and config_depth is not None
            and len(stack) == config_depth + 1
            and actions
        ):
            actions[-1]["paths"].append(attributes.get("value"))

    def _end(_name):
        nonlocal config_depth
        if config_depth is not None and len(stack) == config_depth:
            config_depth = None
        if stack:
            stack.pop()

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
    parser.EndElementHandler = _end
    try:
        parser.Parse(component_xml, True)
    except (_EntityDeclared, xml.parsers.expat.ExpatError):
        # UNREADABLE, and the caller above decides what that means. For the
        # account's bytes it means silence; for the caller's own it means
        # refusal. The distinction cannot be made here, which is why this
        # returns a value the caller can recognise rather than deciding.
        return unreadable

    family = connector_family_of(subtype)

    def _config_digest():
        """Slice B's digest over this document, or None when it refuses.

        The digest refuses unknown content BY DESIGN, which is right for an
        evidence digest and is exactly why this records ``None`` rather than a
        placeholder: "not admissible" and "digested to nothing" are different
        facts and a later slice reads this to tell them apart.
        """
        # THE DIGEST'S OWN VOCABULARY, not the component-type string. It takes
        # "operation" or "connection"; passing the platform's component type
        # made it refuse every real capture, which would have shipped a field
        # that is always empty — the dark-surface trap this slice argued against
        # and would otherwise have walked into itself.
        kind = (
            "connection"
            if "connector-settings" in (component_type or "")
            else "operation"
        )
        try:
            from ..connector_replay.digests import component_config_digest_v1

            return component_config_digest_v1(
                component_xml, kind, family=family or "rest"
            )
        except Exception:
            return None

    # WHERE EACH FAMILY KEEPS ITS VERB, measured rather than assumed — the
    # earlier version asserted one location for every family and was wrong for
    # two of three, which refused raw-XML replay of the platform's own bytes.
    #
    # REST keeps it in ``customOperationType`` (real capture), SOAP in
    # ``operationType`` (live capture), and the database family keeps it in the
    # ELEMENT NAME, because its builders emit `<DatabaseGetAction>` and
    # `<DatabaseSendAction>` and no operation-type attribute at all.
    _DATABASE_ACTION_ELEMENTS = {
        "DatabaseGetAction": "GET",
        "DatabaseSendAction": "SEND",
    }

    # NOW the family is known, from the parsed root. Keep only the candidates
    # that are this family's own operation element; a family we do not model
    # keeps all of them, because we do not know which element is its config and
    # refusing to look would block the raw-XML escape hatch.
    _allowed = _CONFIG_ELEMENTS_BY_FAMILY.get(family)
    if _allowed is not None:
        actions = [c for c in actions if c["element"] in _allowed]
    # The surviving candidates' paths, and only theirs.
    path_fields = [value for candidate in actions for value in candidate["paths"]]

    def _verb_of(config):
        if family == "database":
            return _DATABASE_ACTION_ELEMENTS.get(config["element"])
        if family == "soap_client":
            return config["operationType"]
        return config["customOperationType"]

    actions = [_verb_of(config) for config in actions]

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
    route_state, route_conflicting, live_path = rest_route_decision(
        path_fields, modelled=(family == "rest"), resolved_enough=resolved_enough
    )

    return ResolvedConnectorComponentIdentityV1(
        component_key=component_key,
        family=family,
        action=action,
        route_state=route_state,
        route_conflicting=route_conflicting or None,
        path=live_path or None,
        source="live",
        authority="live_readback_xml",
        config_digest=_config_digest(),
        document_parsed=True,
        action_contradicted=contradicted,
        action_blank_present=blank_present,
        document_component_type=component_type,
    )


def build_connector_resolution_snapshot(
    components: Sequence[Any],
    *,
    live_projections: Optional[Mapping[str, Mapping[str, Any]]] = None,
    live_component_xml: Optional[Mapping[str, Any]] = None,
    reused_keys: Optional[Sequence[str]] = None,
    account_id: Optional[str] = None,
    declared: Optional[Mapping[str, Tuple[Optional[str], Optional[str]]]] = None,
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
        action = getattr(component, "action", None)

        # DERIVED ONCE, before any rung, because every rung records it. The
        # account reading now carries what the fetch learned rather than only
        # its bytes; a bare string is still accepted so a caller without the
        # collector can hand one in.
        reading = live_xml.get(key)
        if isinstance(reading, str):
            reading = {"xml": reading, "read_failed": False}
        reading = reading or {}
        live_document = reading.get("xml")
        mode = "reuse" if key in reused else (action or None)
        carried = {
            "mode": mode,
            "account_id": account_id,
            "component_id": reading.get("component_id"),
            "component_version": reading.get("component_version"),
            "live_read_failed": (
                bool(reading.get("read_failed")) if key in live_xml else None
            ),
        }

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
        submitted = config.get("xml") if isinstance(config, Mapping) else None
        if key not in reused and isinstance(submitted, str) and submitted.strip():
            identity = live_identity_from_component_xml(key, submitted).model_copy(
                update={
                    "mode": mode,
                    "account_id": account_id,
                    "authority": "submitted_xml",
                    "source": "config",
                }
            )
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
            # FAMILY-GENERAL. Scoping settledness to REST left a database
            # Get-versus-Send or a conflicting SOAP configuration resolving to
            # nothing, and the comparison then SKIPS an unknown action — so the
            # caller's declaration stood for both. The scope that belongs to one
            # family is WHERE THE VERB LIVES, which the reader now knows for all
            # three; whether the caller's own bytes settle it is the same
            # question everywhere.
            document_type = identity.document_component_type or ""
            if (
                "connector-action" in document_type
                and identity.family in _FAMILIES_WITH_A_KNOWN_VERB_LOCATION
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
            if identity.family in _FAMILIES_WITH_A_KNOWN_VERB_LOCATION and (
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
        if live_document and action != "update":
            live_read = live_identity_from_component_xml(key, live_document)
            if not live_read.readable:
                # FETCHED AND UNREADABLE IS NOT SILENCE. The refusal below covers
                # a fetch that failed; this covers a fetch that SUCCEEDED and
                # whose bytes the strict reader rejected — the account was
                # consulted and could not answer, which is the same fail-open by
                # a different route. Appending it as a live identity left every
                # field unknown, and the declared-versus-live comparison skips
                # unknown fields, so the caller's declaration stood unchallenged:
                # an operation the account stores as a PATCH could be declared a
                # GET and compiled inside a retried region with no grant.
                failures.append(
                    ConnectorIdentityError(
                        CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE,
                        (
                            "component {0!r} was read back from the account, but "
                            "its document could not be parsed, so nothing about "
                            "what it will actually do could be checked. A "
                            "declaration is an assertion, and there is nothing "
                            "here to check it against."
                        ).format(key),
                        component_key=key,
                    )
                )
                continue
            resolved.append(live_read.model_copy(update=carried))
            continue
        if key in reused and reading.get("read_failed"):
            # THE ACCOUNT WAS CONSULTED AND COULD NOT ANSWER. That is not the
            # same as never having asked, and it is the one case where silence
            # is a fail-open: every check this slice adds derives from the
            # reading, so a component nobody could examine passes them all.
            # Measured before this refusal existed — an unbound reused operation
            # compiled, because the path requirement fell to unknown and the
            # compiler refuses only an explicit requirement.
            failures.append(
                ConnectorIdentityError(
                    CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE,
                    (
                        "component {0!r} is reused from the account, but reading "
                        "it back failed, so nothing about what it will actually "
                        "do could be checked."
                    ).format(key),
                    component_key=key,
                )
            )
            continue
        if key in reused:
            # A REUSE whose account bytes are simply absent resolves to NOTHING.
            # Falling through to the config would make the request's own values
            # authoritative for a component apply is about to DISCARD — measured:
            # a reused operation whose read failed reported a dynamic route from
            # config and demanded a path binding the account's component may not
            # need. "I could not tell" has to stay uncertain here; it is the one
            # answer that cannot be wrong.
            resolved.append(
                ResolvedConnectorComponentIdentityV1(
                    component_key=key, source="live", **carried
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
                authority="normalized_structured_fields",
                extension_bound_endpoint=projection.extension_bound,
                **carried,
            )
        )
    snapshot = TrustedConnectorResolutionSnapshotV1(
        identities=tuple(sorted(resolved, key=lambda item: item.component_key)),
        account_scope=account_id,
    )

    # THE COMPARISON IS PART OF CONSTRUCTION, not a call a route can omit. It was
    # a separate function for most of this slice, and the same finding recurred
    # three times on three different routes — each fix reaching the routes that
    # existed in front of me and none reaching the next one. A guard did not
    # close it either, twice, because a guard has to model the population and I
    # modelled it wrong both times.
    #
    # There is nothing left to forget: a resolution cannot be built without
    # saying what was expected of it, and a disagreement joins the same failure
    # set as every other way this snapshot refuses.
    # A CONTRADICTORY STORED ROUTE IS REFUSED WHETHER OR NOT ANYTHING WAS
    # DECLARED. The check below runs inside the comparator, and the comparator
    # runs only when a declaration exists — so a raw-XML create that names no
    # connector type produced an EMPTY declaration map, skipped the comparator
    # entirely, and carried a component with two routes straight to the write.
    # The refusal was written to be declaration-independent and was then gated on
    # a declaration, which is the whole of the defect.
    for _identity in getattr(snapshot, "identities", ()) or ():
        # ONLY WHAT THE COMPARATOR WILL NOT VISIT. It raises its own conflict for
        # every component it examines, and both failure lists are serialized by
        # the authoring workflow and the recipe engine — so covering the same
        # component here handed an ordinary caller the identical path problem
        # twice. This loop exists for the components the comparator never sees,
        # which is exactly the ones absent from the declaration map.
        #
        # AND ITS REACH IS BOUNDED BY WHAT THE SNAPSHOT WAS GIVEN, which the
        # sentence above overstated until live QA measured it. An identity only
        # carries an account-side route when the caller handed this builder the
        # live XML for that component. On a components-only apply — one that
        # reuses components and authors no process root — the apply path collects
        # no live XML at all, so a reused component holds no account identity,
        # `route_conflicting` is never set, and this loop has nothing to refuse.
        # The SUBMITTED-XML rung is unaffected and does refuse there; it is the
        # ACCOUNT rung that is unreachable on that one surface.
        #
        # Recorded rather than closed by widening the read, because the widening
        # would buy nothing: a reuse writes nothing to the component it reuses,
        # and a request that adds a process root re-enables the live read, so
        # COMPILE and APPLY refuse. `plan` does not — measured, and stated here
        # rather than rounded off, because the comment this replaced was wrong in
        # exactly this direction and a replacement that overstates by a little is
        # the same defect wearing a correction's clothes. Planning mutates
        # nothing, so what it admits is a preview, not a write.
        #
        # So the gap admits no bad write — what it admitted was a comment
        # claiming a coverage the code does not have, which is the defect fixed
        # here.
        if _identity.component_key in (declared or {}):
            continue
        if getattr(_identity, "route_conflicting", None):
            # NOT a mismatch code. The registered summary for the mismatch says a
            # DECLARED family or action disagrees with the account — and this
            # block exists for the case where nothing was declared at all, so a
            # code-driven client would have been told a declaration disagreed
            # when there was none. The unavailable code's own summary is the
            # accurate one here: nothing about what the component will actually
            # do can be checked, so the request is refused rather than applied
            # against a component nobody could examine.
            failures.append(_route_conflict_error(_identity))

    if declared:
        try:
            # THE PATHS TOO, derived here from the components this function is
            # already resolving. Without them the COMPILE route compared family
            # and action and skipped the path, so a declaration naming a path the
            # account does not store compiled CLEAN and was refused only at wet
            # apply — a preview that disagrees with the write it previews, which
            # is an asymmetry this repository has been caught by before.
            #
            # THIS COMMENT HAS BEEN WRONG IN BOTH DIRECTIONS, so it now states
            # only what a probe at the public boundary measures. It first claimed
            # "the typed plan and compile route"; live QA measured that false and
            # it was narrowed to compile alone. Narrowing it was right THEN and is
            # wrong NOW, because the correction that followed moved the comparison
            # down into snapshot construction — which the planning surface also
            # calls, with the client, reading the named live components. So `plan`
            # compares too, and the narrowed comment outlived the behaviour it
            # described. Measured at the public boundary, not inferred from the
            # call graph: a declaration whose path disagrees with the account
            # returns an invalid plan carrying
            # `CONNECTOR_REPLAY_IDENTITY_MISMATCH`, and the agreeing control plans
            # valid. A test pins both arms so the next reader is not left grading
            # a comment against the code again.
            assert_declared_matches_resolved(
                snapshot, declared, declared_paths_from_components(components)
            )
        except ConnectorIdentityError as mismatch:
            failures.extend(mismatch.failures)

    if failures:
        first = failures[0]
        # THE FIELD SURVIVES THIS WRAPPER TOO. It was threaded onto the
        # comparator's own exception and then dropped here, one layer out — and
        # this is the exception the production apply path actually catches, so
        # the machine-readable field existed everywhere except where it is read.
        # The test that covered it stopped at the comparator, which is why it
        # passed while the served envelope carried nothing.
        raise ConnectorIdentityError(
            first.code,
            str(first),
            component_key=first.component_key,
            failures=tuple(failures),
            field=first.field,
            remediation=first.remediation,
            partial=snapshot,
        )
    return snapshot


def declared_paths_from_components(
    components: Sequence[Any],
) -> Dict[str, Optional[str]]:
    """``component key -> the path the caller DECLARED``, read off the plan.

    Its own function rather than a third element on the connector-metadata tuple:
    that tuple's shape is consumed across the recipes layer by several producers,
    and widening it for one field would be a cross-subsystem refactor. Read from
    the same component configs in the same order, so the two derivations cannot
    describe different components.

    A component that declares no path is absent, not present-and-empty — the
    comparison treats a missing declaration as asserting nothing, and an empty
    string is the dynamic-path authoring form rather than a claim about a route.
    """
    paths: Dict[str, Optional[str]] = {}
    for component in components:
        config = component.config or {}
        declared = config.get("path")
        if isinstance(declared, str) and declared.strip():
            paths[component.key] = declared
    return paths


def _route_conflict_error(identity) -> "ConnectorIdentityError":
    """The refusal for a component that carries more than one route.

    CLASSIFIED BY WHERE THE BYTES CAME FROM. The first version always used the
    unavailable code, which is registered for a reused ACCOUNT component that
    could not be read — and the case this refusal was added for is a raw create
    whose own submitted payload contradicts itself, where no stored component
    exists at all. A code-driven client was told to go and look at a component
    the request had not yet created, and the remediation sent it to the wrong
    document. The submitted-XML code's own summary covers this exactly: raw
    component XML that does not settle what it would install.
    """
    submitted = getattr(identity, "authority", None) == "submitted_xml"
    where = "the XML this request submits" if submitted else "the component in the account"
    return ConnectorIdentityError(
        CONNECTOR_REPLAY_SUBMITTED_XML_UNREADABLE if submitted
        else CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE,
        (
            "component {0!r} carries more than one route in {1}, so what it "
            "would actually call cannot be established."
        ).format(identity.component_key, where),
        component_key=identity.component_key,
        field="path",
        remediation=(
            "Remove the surplus path from {0}. No declaration can resolve this, "
            "because the ambiguity is in the document itself.".format(where)
        ),
    )


def assert_declared_matches_resolved(
    snapshot: TrustedConnectorResolutionSnapshotV1,
    declared: Mapping[str, Tuple[Optional[str], Optional[str]]],
    declared_paths: Optional[Mapping[str, Optional[str]]] = None,
) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
    """Compare the caller's declaration against what the components resolve to.

    Returns the declaration unchanged when they agree, so a caller can keep
    passing it down. Raises when they DISAGREE — never when the resolution is
    simply unavailable, because "I could not tell" is not evidence that the
    declaration is wrong, and refusing on it would break every config whose
    endpoint is bound to an environment extension.

    FAMILY, ACTION and — when the caller supplies them — the declared PATHS are
    compared. The paths arrive in their own argument rather than inside
    ``declared`` deliberately: that mapping's two-tuple shape is consumed across
    the recipes layer by several producers, and widening it to carry a third fact
    would be a cross-subsystem refactor for one field. The endpoint proper stays
    out of scope, as it always was — no declaration carries one.
    """
    # EVERY mismatch, collected. Raising on the first meant the planning
    # surface — whose contract is to hand back everything wrong at once — caught
    # one exception and reported one component, however many disagreed.
    mismatches = []
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
        # THE PATH IS COMPARED ONLY WHERE BOTH SIDES SETTLE IT. A declaration
        # carrying no path asserts nothing about the route; a live identity whose
        # route is `dynamic` has no single stored path by definition, and one
        # whose route is `unavailable` could not be read — both are "I could not
        # tell", which this function has never treated as evidence a declaration
        # is wrong. Live QA measured the gap this closes: a caller declaring a
        # path that differs from the account's compiled clean, the live path
        # executed, and the served envelope echoed the caller's own value back,
        # so the one value shown was the one that would not be used.
        # A CONTRADICTORY STORED ROUTE REFUSES, whatever was declared. The
        # comparison's usual posture — "I could not tell" is not evidence a
        # declaration is wrong — does not apply here, because this is not a
        # failure to read: the account was read, and it holds two different routes
        # for one component. No declaration can be checked against that, and a
        # process reusing it would call whichever the platform picks. Refusing at
        # the write boundary is the only answer that is not a guess.
        # A CONTRADICTORY ROUTE SUPPRESSES ONLY THE PATH COMPARISON. The first
        # version continued past the whole component, which silently dropped the
        # family and action checks for it — contradicting the rule this function
        # states forty lines above, that route readability must never gate verb
        # verification, and forcing a caller through one correction cycle per
        # field on a component with two things wrong. The report-all contract
        # applies within a component, not only across them.
        _route_unusable = bool(identity.route_conflicting)
        if _route_unusable:
            mismatches.append(_route_conflict_error(identity))
        declared_path = None if _route_unusable else (declared_paths or {}).get(key)
        live_path = identity.path if identity.route_state == "static" else None
        # CASE FOLDING IS PER FIELD, because the fields are not the same kind of
        # thing. A family and an action are vocabulary tokens whose spelling the
        # platform varies — `rest` and `REST`, `PATCH` and `patch` — and folding
        # them is what makes the comparison work at all. A PATH is not a token:
        # RFC 3986 makes the scheme and host case-insensitive and everything after
        # them case-sensitive, so `/Orders/42` and `/orders/42` are different
        # resources on any case-sensitive upstream. Folding it would accept a
        # declaration that names a resource the account does not serve, and this
        # module's own route digest already treats paths that way — it lower-cases
        # the scheme and nothing else.
        # WHETHER THE VALUE MAY BE SHOWN is decided per field too, and for a
        # different reason than the folding. A family and an action are closed
        # vocabulary tokens — `rest`, `PATCH` — and naming them is what makes the
        # diagnostic actionable. A PATH is caller- and account-supplied and can
        # carry a webhook or session token in a segment, so echoing it would let a
        # read-only plan or compile recover the account's stored route. The
        # authority contract is explicit: a diagnostic may name the offending
        # field, never the value at it. The apply-boundary recheck already reports
        # drift this way, naming which field diverged and redacting what it held.
        for label, mine, theirs, fold, show in (
            ("family", identity.family, declared_family, True, True),
            ("action", identity.action, declared_action, True, True),
            ("path", live_path, declared_path, False, False),
        ):
            if theirs is None or mine is None:
                # A declaration that says nothing asserts nothing.
                continue
            if not str(theirs).strip():
                # A declared BLANK path is the dynamic-path authoring form, not a
                # claim about a route — the binding composes it per document.
                continue
            _mine = str(mine)
            _theirs = str(theirs)
            if fold:
                # A vocabulary token: trimmed and case-folded here, because there
                # is no published normalizer for one.
                _mine, _theirs = _mine.strip().lower(), _theirs.strip().lower()
            else:
                # THE EQUIVALENCE THE EVIDENCE LAYER ALREADY USES. Percent-escape
                # hex is case-insensitive under RFC 3986 and dot segments resolve,
                # so a strict string compare refused `%2f` against a stored `%2F`
                # — a request the route digest considers the same route. Both
                # sides go through the published normalizer rather than a second
                # reading of the standard.
                from ..connector_replay.digests import comparable_path

                _mine, _theirs = comparable_path(_mine), comparable_path(_theirs)
            if _theirs != _mine:
                mismatches.append(ConnectorIdentityError(
                    CONNECTOR_REPLAY_IDENTITY_MISMATCH,
                    (
                        "component {0!r} is declared with {1} {2!r}, but {3} "
                        "resolves to {4!r}. The declaration is an assertion, not "
                        "an override."
                    ).format(
                        key,
                        label,
                        theirs if show else "<redacted>",
                        # Naming the SOURCE is the difference between a real
                        # finding and a tautology: "the account stores" is
                        # independent evidence, "its own configuration" is not.
                        "the component stored in the account"
                        if identity.source == "live"
                        else "its own configuration",
                        mine if show else "<redacted>",
                    ),
                    component_key=key,
                    field=label,
                ))
    # THE REMEDIATION IS BUILT AFTER EVERY MISMATCH IS KNOWN. Attaching it inside
    # the loop meant the first failure asserted that "the other declared fields
    # matched" before the later fields had been compared — and when both the verb
    # and the path disagreed, the served text said the one that was also wrong
    # was fine. It also said "the account" for an identity read from the caller's
    # OWN submitted XML, which names the wrong document. `_pre_write_refusal`
    # serves this verbatim, so it is contract text and not a note.
    _declared_mismatches = [m for m in mismatches if m.remediation is None]
    if _declared_mismatches:
        _fields = sorted({m.field for m in _declared_mismatches if m.field})
        _identity = snapshot.lookup(_declared_mismatches[0].component_key)
        _where = (
            "the XML this request submits"
            if getattr(_identity, "authority", None) == "submitted_xml"
            else "the account"
        )
        _text = (
            "The declaration disagrees with {0} on {1}. Correct {2} to what the "
            "component resolves to.".format(
                _where,
                " and ".join(repr(f) for f in _fields),
                "those fields" if len(_fields) > 1 else "that field",
            )
        )
        for m in _declared_mismatches:
            m.remediation = _text

    if mismatches:
        first = mismatches[0]
        # THE FIELD TRAVELS WITH THE CODE. Every surface that renders one of these
        # reads the outer exception, so a field carried only on the inner one is a
        # field nothing serves — which is the defect this attribute was added to
        # close, reproduced one layer out.
        raise ConnectorIdentityError(
            first.code, str(first), component_key=first.component_key,
            failures=tuple(mismatches), field=first.field,
            remediation=first.remediation,
        )
    return dict(declared)


def live_readings_for_declared_components(boomi_client, components) -> Dict[str, Any]:
    """Live identity readings for components the author NAMES as already existing.

    #155 slice F. A contract symbol is minted by placing a registry record
    against this snapshot's component identity and VERSION, and both fields come
    only from a live reading. The plan and compile route supplied none, so no
    record could ever be placed there and an evidenced retried write refused for
    want of evidence it had no way to present — measured by live QA across five
    plan shapes, every one create-mode with nothing minted, while the identical
    code at apply placed the record correctly.

    THE SCOPE IS THE NARROW ONE ON PURPOSE. Slice C deferred the live reading to
    apply and the plan-time cost of that decision was measured when it was taken,
    so this does not reopen it wholesale: only components carrying an EXPLICIT
    component id are read — the ones the author has already told us exist. A plan
    that creates everything reads nothing and costs exactly what it cost before.
    Resolving ids by NAME would be the wholesale version and is deliberately not
    done here; a caller who wants a replay contract can name the component the
    contract was minted against, which is the same component the record binds.

    A component that cannot be read contributes NOTHING rather than refusing:
    matching the existing rule at apply, where a transient platform error must
    not become an authoring refusal.
    """
    from ..categories.components._shared import component_get_xml

    readings: Dict[str, Any] = {}
    if boomi_client is None:
        return readings
    for component in components or ():
        key = getattr(component, "key", None)
        component_id = getattr(component, "component_id", None)
        if not key or not component_id:
            continue
        try:
            fetched = component_get_xml(boomi_client, str(component_id))
        except Exception:  # noqa: BLE001 - an unreadable component observed nothing
            readings[key] = {"read_failed": True}
            continue
        if not isinstance(fetched, dict):
            readings[key] = {"read_failed": True}
            continue
        # STRINGIFIED, because the identity model types the version as a string
        # and the platform reports it as an integer. Passing it through raw made
        # the public compile entry raise a validation error — found by driving
        # that entry rather than a hand-built snapshot, which is the same lesson
        # this slice has already paid for once.
        version = fetched.get("version")
        readings[key] = {
            "component_id": fetched.get("component_id") or str(component_id),
            "component_version": None if version is None else str(version),
            "xml": fetched.get("xml"),
            "read_failed": False,
        }
    return readings
