"""Immutable registry of migrated legacy dialects (issue #139 M12.4).

Keyed by QUALIFIED dialect, not only process kind, because one process kind
(``database_to_api_sync``) hosts more than one executable dialect (its ordinary
single/linear form vs its composed ``flow_sequence`` sub-dialect) that migrate
on different schedules. A dialect present here is CUT OVER to the canonical
``ProcessIRV1 -> compile -> emit`` chain; a reserved name is known but NOT yet
migrated, so a lookup returns ``None`` and the legacy renderer stays
authoritative for it.

The mapping is frozen at import (``MappingProxyType``) — mirroring the fail-closed
emitter registry — so no caller can register a dialect at runtime.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Callable, Mapping, Optional

from .contracts import LegacyAdapterResultV1
from .flow_sequence import adapt_flow_sequence
from .sync_pipeline import adapt_sync_pipeline
from .wrapper_subprocess import adapt_wrapper_subprocess

LegacyAdapter = Callable[[dict], LegacyAdapterResultV1]

# Qualified dialect key for the composed flow_sequence sub-dialect of
# database_to_api_sync. The ordinary database_to_api_sync form keeps a distinct
# (reserved, unmigrated) identity.
FLOW_SEQUENCE_DIALECT = "database_to_api_sync/flow_sequence"
WRAPPER_SUBPROCESS_DIALECT = "wrapper_subprocess"
SYNC_PIPELINE_DIALECT = "sync_pipeline"

# Reserved-but-unmigrated: named so a caller can tell "known, pending" from
# "unknown", but deliberately absent from the migrated registry.
RESERVED_DIALECTS = frozenset(
    {
        # Ordinary single/linear form. Still pending-capability for FOUR reasons a
        # sync_pipeline config can never trigger (they are all rejected at that
        # dialect's config gate): the fused start_listen entry and connector
        # dynamic_path (#140), and catcherrors / notify (#142). #139C's adapter is
        # deliberately shaped to be promoted here once those close.
        "database_to_api_sync",
    }
)

_MIGRATED: Mapping[str, LegacyAdapter] = MappingProxyType(
    {
        WRAPPER_SUBPROCESS_DIALECT: adapt_wrapper_subprocess,
        FLOW_SEQUENCE_DIALECT: adapt_flow_sequence,
        # #139C: migrated for its 6 non-listener stage chains. The 4 WSS listener
        # chains stay on the legacy renderer behind an explicit routing gate
        # (#140), so this entry means "the dialect is cut over", not "every one of
        # its configs is".
        SYNC_PIPELINE_DIALECT: adapt_sync_pipeline,
    }
)


def adapter_for(dialect: str) -> Optional[LegacyAdapter]:
    """Return the migrated adapter for a qualified dialect, or ``None``.

    ``None`` means "not migrated" — either a reserved-but-pending dialect or an
    unknown one. Callers keep the legacy renderer authoritative on ``None``.
    """
    return _MIGRATED.get(dialect)


def is_migrated(dialect: str) -> bool:
    return dialect in _MIGRATED


def migrated_dialects() -> frozenset:
    return frozenset(_MIGRATED)


__all__ = [
    "FLOW_SEQUENCE_DIALECT",
    "SYNC_PIPELINE_DIALECT",
    "WRAPPER_SUBPROCESS_DIALECT",
    "RESERVED_DIALECTS",
    "adapter_for",
    "is_migrated",
    "migrated_dialects",
]
