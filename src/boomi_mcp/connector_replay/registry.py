"""Loading and querying the packaged replay-evidence registry.

The registry answers one question — *is re-executing this action known to be safe?*
— and its default answer is no. An action with no row is ``unverified``, which
refuses a retry. That direction matters: a registry that failed open would turn a
missing file, a parse error, or a typo into permission to replay a write.

Unknown content is REFUSED rather than skipped. The allowlist is data, so extending
the vocabulary is a data change backed by a capture, never a code change — and a
row this module cannot parse means the packaged data disagrees with the code that
reads it, which is not a condition to continue past.
"""

from __future__ import annotations

import json
from functools import lru_cache
from importlib import resources
from typing import Any

from boomi_mcp.errors import CONNECTOR_REPLAY_REGISTRY_INVALID

from .ids import authored_contract_ref
from .models import (
    CapabilityEvidenceRecordV1,
    ComponentProjectionAllowlistV1,
    ConnectorReplayRegistryV1,
    ConnectorVocabularyMappingV1,
    ContractKeySemanticsDefinitionV1,
    OperationContractRecordV1,
    RetrySafetyV1,
)

__all__ = [
    "RegistryInvalid",
    "ReplayRegistry",
    "load_registry",
]

_RESOURCE = "registry_v1.json"
_SUPPORTED_SCHEMA = 1


class RegistryInvalid(Exception):
    """The packaged registry could not be read as this module understands it."""

    code = CONNECTOR_REPLAY_REGISTRY_INVALID


class ReplayRegistry:
    """An immutable view over the packaged registry."""

    def __init__(
        self,
        vocabulary: tuple[ConnectorVocabularyMappingV1, ...],
        evidence: tuple[CapabilityEvidenceRecordV1, ...],
        operation_records: tuple = (),
        projection_allowlists: tuple = (),
        semantics_definitions: tuple = (),
    ) -> None:
        self._vocabulary = vocabulary
        self._evidence = evidence
        self._operation_records = operation_records
        self._projection_allowlists = projection_allowlists
        self._semantics_definitions = semantics_definitions
        by_type: dict[str, ConnectorVocabularyMappingV1] = {}
        for entry in vocabulary:
            if entry.platform_connector_type in by_type:
                raise RegistryInvalid(
                    f"connector type {entry.platform_connector_type!r} is mapped "
                    "more than once; which family applies would depend on order"
                )
            by_type[entry.platform_connector_type] = entry
        self._by_type = by_type
        self._by_pair: dict[tuple[str, str], CapabilityEvidenceRecordV1] = {}
        for row in evidence:
            key = (row.family, row.action)
            if key in self._by_pair:
                raise RegistryInvalid(
                    f"two evidence rows for {key!r}; a pair has one verdict or none"
                )
            self._by_pair[key] = row

    @property
    def vocabulary(self) -> tuple[ConnectorVocabularyMappingV1, ...]:
        return self._vocabulary

    @property
    def evidence_records(self) -> tuple[CapabilityEvidenceRecordV1, ...]:
        return self._evidence

    @property
    def projection_allowlists(self) -> tuple:
        """Typed per-component-kind projection specs. Data, so extending needs a capture."""
        return self._projection_allowlists

    @property
    def semantics_definitions(self) -> tuple:
        """Versioned key-semantics definitions. Empty here; a later slice mints them."""
        return self._semantics_definitions

    def projection_for(self, component_kind: str, family: str = "rest"):
        """The projection for a (family, kind) pair, or None.

        Scoped by FAMILY as well as kind: two families project different facts, so
        a lookup that ignored family would digest a database component under a REST
        projection and let the two collide.
        """
        matches = [s for s in self._projection_allowlists
                   if s.component_kind == component_kind and s.family == family]
        if not matches:
            return None
        if len(matches) > 1:
            # Digests from different projection revisions are not comparable, so
            # returning "the first match" would make component identity depend on
            # registry ORDER and carry no indication of which revision produced it.
            raise RegistryInvalid(
                f"the registry publishes {len(matches)} projection revisions for "
                f"({family!r}, {component_kind!r}); a component identity must name "
                "one revision, and digests across revisions are not comparable"
            )
        return matches[0]

    @property
    def operation_records(self) -> tuple:
        """Account-bound records. Empty here; a later slice mints them.

        Exposed even while always empty, because the alternative is a key the file
        declares and the reader cannot see — which is how the drop was missed.
        """
        return self._operation_records

    def family_for(self, platform_connector_type: str) -> str | None:
        """The family for a raw platform connector type, or None if unmapped.

        None rather than a guess: an unmapped type is one this registry has never
        seen executed, and inventing a family for it would let evidence captured
        for one connector authorise a retry on another.
        """
        entry = self._by_type.get(platform_connector_type)
        return entry.family if entry else None

    def retry_safety(self, family: str, action: str) -> RetrySafetyV1:
        """The recorded verdict, defaulting to ``unverified``.

        This is the fail-closed seam. Callers may retry only on an explicit
        affirmative verdict, so every absence — unmapped connector, unobserved
        action, empty registry — arrives here as ``unverified``.
        """
        # RESOLVE THROUGH THE VOCABULARY FIRST. A row is only meaningful for a
        # family the registry can actually map a live connector onto; answering
        # from the row alone let an evidence row for an unmapped family return an
        # affirmative verdict, which is the one answer this registry exists to
        # withhold.
        recognised = {
            action_name
            for entry in self._vocabulary if entry.family == family
            for action_name in entry.recognised_actions
        }  # already a union across mappings
        if not recognised or action not in recognised:
            # The family must be mapped AND the action recognised. Resolving only
            # the family let an invented action inherit a mapped family's
            # authority, which is the same fail-open one level down.
            return RetrySafetyV1.UNVERIFIED
        row = self._by_pair.get((family, action))
        return row.retry_safety if row else RetrySafetyV1.UNVERIFIED


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RegistryInvalid(message)


def _parse(payload: Any) -> ReplayRegistry:
    _require(isinstance(payload, dict), "registry root must be an object")
    version = payload.get("schema_version")
    _require(
        version == _SUPPORTED_SCHEMA,
        f"registry schema_version {version!r} is not the supported {_SUPPORTED_SCHEMA}",
    )
    # REQUIRED, not defaulted. `payload.get(key, [])` turned a truncated or
    # corrupted packaged file into an apparently valid deny-all registry: the
    # safe-looking outcome silently replaced the real one, and nothing said so.
    for section in ("vocabulary", "evidence_records", "operation_records",
                    "projection_allowlists", "semantics_definitions"):
        if section not in payload:
            raise RegistryInvalid(
                f"registry is missing the required {section!r} section. An absent "
                "section is corruption, not an intentionally empty registry — the "
                "packaged file declares all three"
            )
        if not isinstance(payload[section], list):
            raise RegistryInvalid(f"registry section {section!r} must be a list")
    try:
        vocabulary = tuple(
            ConnectorVocabularyMappingV1(**entry)
            for entry in payload.get("vocabulary", [])
        )
        evidence = tuple(
            CapabilityEvidenceRecordV1(**row)
            for row in payload.get("evidence_records", [])
        )
    except Exception as exc:  # pydantic ValidationError, TypeError on a non-mapping
        raise RegistryInvalid(f"registry content is not valid: {exc}") from exc
    # Every top-level key the packaged file carries must be one this build knows.
    # The loader previously read `vocabulary` and `evidence_records` and IGNORED the
    # rest — including `operation_records`, which the shipped file advertises. A
    # loader that silently drops a key its own data declares will one day drop the
    # rows that decide whether a write may be retried, and it would do so quietly.
    known = set(ConnectorReplayRegistryV1.model_fields)
    # No underscore carve-out. Narrative keys were being STRIPPED here, which is the
    # silent version of the same problem the design forbids: a served contract that
    # also carries prose invites the prose to drift from the contract, and stripping
    # it means the file can say something the loader never reads. Prose belongs
    # beside the registry, not inside it.
    unknown = sorted(k for k in payload if k not in known)
    if unknown:
        raise RegistryInvalid(
            "registry carries keys this build does not understand: {0}. Refusing "
            "rather than ignoring them — a key present in the data and absent from "
            "the reader is a disagreement, not a default.".format(unknown)
        )
    # TYPED, not passed through. Untyped dictionaries used to load here, so an
    # arbitrary object could sit in `operation_records` and be indistinguishable
    # from a real one — in a registry whose records decide whether a write may be
    # retried.
    try:
        operation_records = tuple(
            OperationContractRecordV1(**row) for row in payload["operation_records"]
        )
        allowlists = tuple(
            ComponentProjectionAllowlistV1(**row)
            for row in payload["projection_allowlists"]
        )
        semantics = tuple(
            ContractKeySemanticsDefinitionV1(**row)
            for row in payload["semantics_definitions"]
        )
    except Exception as exc:
        raise RegistryInvalid(f"registry content is not valid: {exc}") from exc
    _refuse_unresolvable_records(vocabulary, evidence, operation_records, semantics)
    return ReplayRegistry(vocabulary, evidence, operation_records, allowlists, semantics)


def _refuse_unresolvable_records(vocabulary, evidence, operation_records, semantics) -> None:
    """CROSS-RECORD validation: a typed record is not yet an authoritative one.

    Field-level types make a record well-formed. They say nothing about whether it
    refers to anything — a record could name a family no vocabulary maps, a
    semantics revision that was never published, or duplicate another record
    outright, and still be perfectly typed. In a registry whose records decide
    whether a write may be retried, "well-formed" is not the bar.
    """
    # UNION across mappings. Several account-specific connector types can map to one
    # portable family, and a dict comprehension kept only the last mapping's
    # actions — so whether a row was accepted depended on vocabulary ORDER.
    families: dict[str, set] = {}
    #: raw (family, action) -> the grammar-safe identifier pair the contract
    #: reference is built from. Assembled beside `families` from the same
    #: mappings, so the two cannot disagree about which actions a family has.
    identifiers: dict[tuple[str, str], tuple[str, str]] = {}
    for entry in vocabulary:
        families.setdefault(entry.family, set()).update(entry.recognised_actions)
        for raw_action, action_id in entry.action_ids:
            key = (entry.family, raw_action)
            pair = (entry.family_id, action_id)
            # INJECTIVE ACROSS THE WHOLE VOCABULARY, not only within one mapping.
            # Two connector types can map to one family, and the per-mapping
            # validators cannot see each other — so a collision between mappings
            # would only surface here, as two raw actions minting one reference.
            if identifiers.get(key, pair) != pair:
                raise RegistryInvalid(
                    f"vocabulary maps {key!r} to two different identifier pairs; "
                    "a raw action with two identifiers means one contract has two "
                    "references, and a set of references stops being a set")
            identifiers[key] = pair
    # A SET would silently accept two CONTRADICTORY definitions for one id and
    # revision — they differ, so both survive, and which one interprets a record
    # then depends on iteration order.
    published: dict[tuple[str, int], object] = {}
    for definition in semantics:
        key = (definition.semantics_id, definition.revision)
        if key in published and published[key] != definition:
            raise RegistryInvalid(
                f"two different semantics definitions for {key!r}; a record citing "
                "it would be interpreted differently depending on which is read")
        published[key] = definition

    for row in evidence:
        if row.family not in families:
            raise RegistryInvalid(
                f"evidence row {row.family}/{row.action} names a family no "
                "vocabulary maps, so no live connector can ever resolve to it")
        if row.action not in families[row.family]:
            raise RegistryInvalid(
                f"evidence row {row.family}/{row.action} names an action the "
                f"{row.family!r} vocabulary does not recognise")

    seen: set[str] = set()
    for record in operation_records:
        if record.family not in families:
            raise RegistryInvalid(
                f"operation record {record.contract_ref} names an unmapped family "
                f"{record.family!r}")
        if record.action not in families[record.family]:
            raise RegistryInvalid(
                f"operation record {record.contract_ref} names an unrecognised "
                f"action {record.action!r}")
        # THE REFERENCE IS DERIVED, NOT TRUSTED. The record carries its own name
        # because whoever reads the record needs it, but the registry rebuilds it
        # from the vocabulary and the cited semantics and refuses a disagreement.
        # Without this the reference is a free string that merely LOOKS
        # structured: it would pass the grammar while naming a family, an action
        # or a revision the record does not actually describe, and every consumer
        # downstream resolves by that name.
        expected = authored_contract_ref(
            *identifiers[(record.family, record.action)],
            record.semantics_id,
            record.semantics_revision,
        )
        if record.contract_ref != expected:
            raise RegistryInvalid(
                f"operation record names itself {record.contract_ref!r}, but its "
                f"own family, action, semantics and revision derive {expected!r}; "
                "a reference that does not follow from the record it names is a "
                "name for something else")
        if (record.semantics_id, record.semantics_revision) not in published:
            raise RegistryInvalid(
                f"operation record {record.contract_ref} cites semantics "
                f"{record.semantics_id!r} revision {record.semantics_revision}, which "
                "this registry does not publish — a record whose meaning is not "
                "defined here cannot be interpreted here")
        # THE CAPTURE MUST HAVE OBSERVED WHAT THE SEMANTICS CLAIM. A class-level
        # evidence row already binds its verdict to its capture's replay
        # observation — that binding is what makes a row evidence rather than an
        # assertion — but an operation record was fields-only: it named a
        # semantics definition carrying a duplicate guarantee, and nothing
        # checked that guarantee against what the capture actually saw. The
        # issue-level architect gate demonstrated the consequence by probe: a
        # record whose replay observation said NOT EXERCISED, with its
        # same-effect semantics and a stale digest left in place, loaded, minted
        # a contract and a grant, and compiled. Provenance-shaped authorization
        # is not replay evidence.
        #
        # The rule is equality, not a lattice. A guarantee is a claim about what
        # the counterparty does with a repeated key; the observation is what it
        # was seen to do. `not_exercised` and `duplicate_effect` support NO
        # guarantee at all — the first saw nothing, and the second saw the
        # opposite of every guarantee this enum can express.
        # THE DIGEST IS THE EVIDENCE IDENTITY, so it must describe THIS record.
        # Every check around it compares fields against each other, which a
        # tampered record can satisfy by changing them together — the capture's
        # replay observation and the semantics it cites, for instance. None of
        # them looks at the identifier the compiler, the apply-boundary recheck
        # and the durable attestation all carry, so materially different evidence
        # could keep an existing authority identifier and every downstream record
        # would name a digest that no longer describes what authorised the write.
        #
        # Recomputed from the published minter rather than a second copy of the
        # rule: until now the only thing that computed this value was the
        # out-of-repo capture harness, which is why nothing here could check it.
        from .digests import operation_record_digest_v1

        recomputed = operation_record_digest_v1(record)
        if recomputed != record.record_digest:
            raise RegistryInvalid(
                f"operation record {record.contract_ref} carries record digest "
                f"{record.record_digest}, but its own content hashes to "
                f"{recomputed}; the digest is the identity every grant and "
                "attestation records, so it must describe the record it sits on")
        definition = published[(record.semantics_id, record.semantics_revision)]
        observed = record.capture.summary.replay.value
        guaranteed = definition.duplicate_guarantee.value
        if observed != guaranteed:
            raise RegistryInvalid(
                f"operation record {record.contract_ref} cites semantics "
                f"guaranteeing {guaranteed!r}, but its capture observed "
                f"{observed!r}; a record authorises the replay its capture SAW, "
                "not the one its semantics describe")
        # The record and the capture it rests on must belong to the SAME account.
        # A record bound to one account, resting on evidence gathered in another, is
        # a claim about an environment nobody observed.
        if record.capture.account_scope_hash != record.account_scope_hash:
            raise RegistryInvalid(
                f"operation record {record.contract_ref} is bound to one account "
                "while its capture was taken in another")
        # Keyed on the CONTRACT REFERENCE alone. Including family and action let
        # one reference occur under several actions, so a single contract could
        # carry conflicting verdicts and the reference would no longer identify
        # anything.
        if record.contract_ref in seen:
            raise RegistryInvalid(
                f"contract reference {record.contract_ref!r} appears on more than "
                "one operation record; a reference that identifies several records "
                "identifies none of them")
        seen.add(record.contract_ref)


@lru_cache(maxsize=1)
def load_registry() -> ReplayRegistry:
    """Load the packaged registry.

    Read through ``importlib.resources`` so the data is found the same way whether
    the package is on disk, zipped, or inside a built image — the parity the image
    check exists to prove.
    """
    try:
        text = resources.files(__package__).joinpath(_RESOURCE).read_text("utf-8")
    except (FileNotFoundError, OSError) as exc:
        raise RegistryInvalid(f"packaged registry {_RESOURCE} is unreadable: {exc}") from exc
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RegistryInvalid(f"packaged registry is not valid JSON: {exc}") from exc
    return _parse(payload)
