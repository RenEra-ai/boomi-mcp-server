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

    def projection_for(self, component_kind: str):
        """The projection spec for a component kind, or None."""
        for spec in self._projection_allowlists:
            if spec.component_kind == component_kind:
                return spec
        return None

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
        }
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
    return ReplayRegistry(vocabulary, evidence, operation_records, allowlists, semantics)


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
