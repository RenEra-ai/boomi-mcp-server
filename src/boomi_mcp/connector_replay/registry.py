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
    ConnectorVocabularyMappingV1,
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
    ) -> None:
        self._vocabulary = vocabulary
        self._evidence = evidence
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
    try:
        vocabulary = tuple(
            ConnectorVocabularyMappingV1(
                **{k: v for k, v in entry.items() if not k.startswith("_")}
            )
            for entry in payload.get("vocabulary", [])
        )
        evidence = tuple(
            CapabilityEvidenceRecordV1(
                **{k: v for k, v in row.items() if not k.startswith("_")}
            )
            for row in payload.get("evidence_records", [])
        )
    except Exception as exc:  # pydantic ValidationError, TypeError on a non-mapping
        raise RegistryInvalid(f"registry content is not valid: {exc}") from exc
    return ReplayRegistry(vocabulary, evidence)


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
