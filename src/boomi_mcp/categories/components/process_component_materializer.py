"""The neutral process component materializer (issue #153 / M12.15).

Turns emitted canonical shape XML into a deployable ``<bns:Component>`` envelope.
This is the last step of the canonical chain — ``ProcessIRV1`` -> compile ->
``emit_process`` -> HERE -> the existing apply machinery — and it is the piece
that did not exist before #153: ``emit_process`` deliberately returns shapes plus
a bare ``<process xmlns="">`` wrapper carrying no options attributes, documented
as "NOT a deployable Boomi Component envelope".

**Neutral means two specific prohibitions, both mechanically checked.** This
module must never

1. call ``ProcessFlowBuilder.build()`` or reach the legacy builder registry, and
2. read ``process_kind``.

That is the whole point of the milestone: a canonical process is materialized
from its own compiled artifact, not by resolving a legacy dialect. The rules are
enforced by tests rather than trusted — a static guard over this module's source,
and a dynamic test that bombs every legacy builder entry point and still expects
a successful materialization.

**This is a re-homing, not a rewrite.** The bytes produced here are the bytes the
legacy assembler produced: the same envelope layout, the same escaping, the same
round-trip validation, and the same two process-option strings. The legacy path
keeps calling the same implementations through thin aliases and stays the
byte-exact parity oracle until #160. Nothing about the emitted XML changes in
this slice — which is why the golden corpus is the differential.

**Why the execution profile arrives pre-derived.** The materializer maps a
RECORDED ``scheduled`` / ``listener`` profile onto the exact option-byte string.
It never inspects the IR to work the profile out, because the compiler already
decided it (``compiler.process_ir.execution_profile``) and recorded it on the
materialization plan. Two independent derivations of one fact is how a process
ends up emitted with attributes that contradict its own graph.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence
from xml.etree import ElementTree as ET

#: The two builder helpers this module needs, imported LAZILY.
#:
#: ``builders/__init__`` imports ``process_flow_builder``, which imports this
#: module for the re-homed aliases — so a module-scope import here closes a cycle
#: and breaks whichever module is imported first. Deferring the import to call
#: time breaks the cycle without duplicating either helper, which matters: a
#: local copy of ``_escape_xml`` would be a second escaping authority, and two
#: escaping authorities is how emitted XML silently diverges.


def _builder_helpers():
    from .builders.connector_builder import BuilderValidationError, _escape_xml

    return BuilderValidationError, _escape_xml



# ---------------------------------------------------------------------------
# Process options — the profile -> option-bytes mapping
# ---------------------------------------------------------------------------

#: Default (scheduled) process options — byte-for-byte the pre-M6 hard-coded
#: attribute string, so every existing process emission is unchanged.
DEFAULT_PROCESS_OPTIONS = (
    'allowSimultaneous="false" '
    'enableUserLog="false" '
    'processLogOnErrorOnly="false" '
    'purgeDataImmediately="false" '
    'stopProcessingIfZeroDocuments="true" '
    'updateRunDates="true" '
    'workload="general"'
)

#: Listener process options — the exact live-captured attribute set from the
#: Process Library ``Weblistener to Slack`` process (a5d9f624, 2026-07-04):
#: ``allowSimultaneous="true"`` (concurrent inbound requests; false causes HTTP
#: 500 / queued requests under concurrency) and ``updateRunDates="false"``
#: (listener performance recommendation). NOTE: the live listener capture omits
#: ``stopProcessingIfZeroDocuments`` entirely — do not add it here.
LISTENER_PROCESS_OPTIONS = (
    'allowSimultaneous="true" '
    'enableUserLog="false" '
    'processLogOnErrorOnly="false" '
    'purgeDataImmediately="false" '
    'updateRunDates="false" '
    'workload="general"'
)

#: Execution profile -> the exact ``<process>`` attribute bytes. A MAPPING keyed
#: by the recorded profile, never a re-derivation: the materializer's whole
#: contract is that it consumes the compiler's decision.
_PROFILE_OPTIONS = {
    "scheduled": DEFAULT_PROCESS_OPTIONS,
    "listener": LISTENER_PROCESS_OPTIONS,
}


def process_options_for_profile(execution_profile: str) -> str:
    """The option-byte string for a RECORDED execution profile.

    Unknown profiles fail closed rather than silently emitting scheduled bytes:
    a profile this mapping does not know is a compiler/materializer version skew,
    and guessing would emit a process whose attributes contradict its graph.
    """
    BuilderValidationError, _ = _builder_helpers()
    try:
        return _PROFILE_OPTIONS[execution_profile]
    except KeyError:
        raise BuilderValidationError(
            "unknown process execution profile: {0!r}".format(execution_profile),
            error_code="PROCESS_MATERIALIZATION_EXECUTION_PROFILE_INVALID",
            field="execution_profile",
            hint="Supported profiles: {0}.".format(sorted(_PROFILE_OPTIONS)),
        ) from None


# ---------------------------------------------------------------------------
# Extension bindings -> <bns:processOverrides>
# ---------------------------------------------------------------------------


def extension_bindings_from_legacy_config(
    config: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Read + normalize ``config["process_extensions"]["connections"]``.

    Returns ``[]`` when the block is absent/empty. Otherwise returns a list of
    ``{"connection_id": <resolved id or $ref token>, "connector_type"?: str,
    "fields": [{"id","label","xpath"}, ...]}`` with field order preserved.

    Raises ``BuilderValidationError(error_code="PROCESS_EXTENSIONS_INVALID")``
    on any malformed shape so both validate_config (which catches it) and
    build() (which lets it raise) share one source of truth for the contract.
    """
    BuilderValidationError, _ = _builder_helpers()
    process_extensions = config.get("process_extensions")
    if process_extensions in (None, {}, []):
        return []
    if not isinstance(process_extensions, dict):
        raise BuilderValidationError(
            "process_extensions must be a JSON object with a 'connections' list.",
            error_code="PROCESS_EXTENSIONS_INVALID",
            field="process_extensions",
            hint='Shape: {"connections": [{"connection_id": "...", "fields": [...]}]}.',
        )
    # A present, non-empty process_extensions block MUST carry a 'connections'
    # key. A missing/misspelled key (e.g. "connection") or a null value would
    # otherwise silently drop the caller's override declaration — reject it so
    # the documented PROCESS_EXTENSIONS_INVALID contract holds. An absent/empty
    # block (handled above) or an explicitly empty connections list is a no-op.
    if "connections" not in process_extensions:
        raise BuilderValidationError(
            "process_extensions must contain a 'connections' list.",
            error_code="PROCESS_EXTENSIONS_INVALID",
            field="process_extensions.connections",
            hint=(
                'Shape: {"connections": [{"connection_id": "...", "fields": '
                '[...]}]}. (Did you mean "connections"?)'
            ),
        )
    raw_connections = process_extensions.get("connections")
    if raw_connections is None:
        raise BuilderValidationError(
            "process_extensions.connections must be a list, not null.",
            error_code="PROCESS_EXTENSIONS_INVALID",
            field="process_extensions.connections",
            hint='Provide a list: {"connections": [{"connection_id": "...", "fields": [...]}]}.',
        )
    if not isinstance(raw_connections, list):
        raise BuilderValidationError(
            "process_extensions.connections must be a list of connection-override "
            "declarations.",
            error_code="PROCESS_EXTENSIONS_INVALID",
            field="process_extensions.connections",
            hint='Each entry: {"connection_id": "...", "fields": [{"id","label","xpath"}]}.',
        )
    if not raw_connections:
        # Explicitly empty connections list — nothing to declare, valid no-op.
        return []

    normalized: List[Dict[str, Any]] = []
    for i, entry in enumerate(raw_connections):
        loc = f"process_extensions.connections[{i}]"
        if not isinstance(entry, dict):
            raise BuilderValidationError(
                f"{loc} must be a JSON object.",
                error_code="PROCESS_EXTENSIONS_INVALID",
                field=loc,
                hint='Each entry: {"connection_id": "...", "fields": [{"id","label","xpath"}]}.',
            )
        conn_id = entry.get("connection_id")
        if not isinstance(conn_id, str) or not conn_id.strip():
            raise BuilderValidationError(
                f"{loc}.connection_id is required and must be a non-empty string.",
                error_code="PROCESS_EXTENSIONS_INVALID",
                field=f"{loc}.connection_id",
                hint=(
                    "Use the same connection id / $ref:KEY token the connector "
                    "shapes bind to, so the override declaration resolves to the "
                    "same component."
                ),
            )
        raw_fields = entry.get("fields")
        if not isinstance(raw_fields, list) or not raw_fields:
            raise BuilderValidationError(
                f"{loc}.fields must be a non-empty list of field declarations.",
                error_code="PROCESS_EXTENSIONS_INVALID",
                field=f"{loc}.fields",
                hint='Each field: {"id": "password", "label": "Password", "xpath": "..."}.',
            )
        fields: List[Dict[str, str]] = []
        for j, raw_field in enumerate(raw_fields):
            floc = f"{loc}.fields[{j}]"
            if not isinstance(raw_field, dict):
                raise BuilderValidationError(
                    f"{floc} must be a JSON object with id, label, and xpath.",
                    error_code="PROCESS_EXTENSIONS_INVALID",
                    field=floc,
                    hint='Each field: {"id": "...", "label": "...", "xpath": "..."}.',
                )
            normalized_field: Dict[str, str] = {}
            for key in ("id", "label"):
                value = raw_field.get(key)
                if not isinstance(value, str) or not value.strip():
                    raise BuilderValidationError(
                        f"{floc}.{key} is required and must be a non-empty string.",
                        error_code="PROCESS_EXTENSIONS_INVALID",
                        field=f"{floc}.{key}",
                        hint="Field declarations carry an id and a label (xpath optional).",
                    )
                # label intentionally not stripped — a leading/trailing space is
                # cosmetic and the value is escaped on emission anyway. id is
                # structural, so canonicalize it.
                normalized_field[key] = value.strip() if key == "id" else value
            # xpath is REQUIRED only for an EXPLICIT DB override entry
            # (connector_type='database'), which is xpath-keyed (e.g.
            # DatabaseConnectionSettings/@username) — a missing xpath there emits a
            # declaration that never maps to the DB field. A no-xpath field is the
            # id-keyed (REST) form, valid by ITSELF without requiring connector_type
            # to be set (Codex review: a hand-authored REST override that omits
            # connector_type must still build). So only an explicitly-DB entry
            # mandates xpath; REST aliases / omitted / unknown connector_type leave
            # it optional.
            connector_type = entry.get("connector_type")
            entry_is_db = (
                isinstance(connector_type, str)
                and connector_type.strip().lower() == "database"
            )
            xpath = raw_field.get("xpath")
            if xpath is None:
                if entry_is_db:
                    raise BuilderValidationError(
                        f"{floc}.xpath is required for a database connection override.",
                        error_code="PROCESS_EXTENSIONS_INVALID",
                        field=f"{floc}.xpath",
                        hint=(
                            "DB overrides are xpath-keyed; set xpath (e.g. "
                            "'DatabaseConnectionSettings/@username'). REST overrides "
                            "are id-keyed and omit xpath."
                        ),
                    )
            elif not isinstance(xpath, str) or not xpath.strip():
                raise BuilderValidationError(
                    f"{floc}.xpath must be a non-empty string when present.",
                    error_code="PROCESS_EXTENSIONS_INVALID",
                    field=f"{floc}.xpath",
                    hint="Omit xpath entirely for id-keyed (REST) overrides.",
                )
            else:
                normalized_field["xpath"] = xpath.strip()
            fields.append(normalized_field)
        normalized_entry: Dict[str, Any] = {
            "connection_id": conn_id.strip(),
            "fields": fields,
        }
        connector_type = entry.get("connector_type")
        if isinstance(connector_type, str) and connector_type.strip():
            normalized_entry["connector_type"] = connector_type.strip()
        normalized.append(normalized_entry)
    return normalized


def render_process_overrides(connections: Sequence[Dict[str, Any]]) -> str:
    """Emit a non-empty ``<bns:processOverrides>`` declaring connection-field
    environment extensions (issue #92 M4.5.7).

    ``connections`` is the normalized list from
    :func:`extension_bindings_from_legacy_config`. The container shape and
    sibling order are ``live_verified`` from the ``work``-profile main-sync
    exemplar; field order is preserved from the input. All attribute values are
    XML-escaped. ``connector_type`` is carried in config for downstream tooling
    but is not part of the emitted declaration (Boomi keys overrides by the
    connection id + field id, not by connector type).
    """
    _, _escape_xml = _builder_helpers()
    connection_parts: List[str] = []
    for conn in connections:
        field_parts: List[str] = []
        for field in conn["fields"]:
            # xpath is emitted only when present (#102 B1): DB overrides carry an
            # xpath; live REST Client overrides key purely by field id and emit
            # none — matching the live `Rest Example` process export.
            xpath = field.get("xpath")
            xpath_attr = f' xpath="{_escape_xml(str(xpath))}"' if xpath else ""
            field_parts.append(
                f'<field id="{_escape_xml(str(field["id"]))}" '
                f'label="{_escape_xml(str(field["label"]))}" '
                f'overrideable="true"{xpath_attr}/>'
            )
        connection_parts.append(
            f'<ConnectionOverride id="{_escape_xml(str(conn["connection_id"]))}">'
            f"{''.join(field_parts)}"
            '</ConnectionOverride>'
        )
    return (
        '<bns:processOverrides>'
        '<Overrides xmlns="">'
        f"<Connections>{''.join(connection_parts)}</Connections>"
        '<Operations/>'
        '<PartnerOverrides/>'
        '<Properties/>'
        '<Extensions>'
        '<ObjectDefinitions><unusedProfiles/></ObjectDefinitions>'
        '<DataMaps><unusedMaps/></DataMaps>'
        '</Extensions>'
        '<CrossReferenceOverrides/>'
        '<PGPOverrides/>'
        '<DefinedProcessPropertyOverrides/>'
        '</Overrides>'
        '</bns:processOverrides>'
    )


# ---------------------------------------------------------------------------
# The component envelope
# ---------------------------------------------------------------------------


def assemble_component_xml(
    shape_xml_parts: Sequence[str],
    *,
    name: str,
    description: str = "",
    folder_name: Optional[str] = None,
    process_overrides_xml: str = "",
    process_options: Optional[str] = None,
) -> str:
    """Wrap emitted shapes in the ``<process>`` / ``<bns:Component>`` envelope.

    Shared by ProcessFlowBuilder and WrapperSubprocessBuilder (issue #90). Coerces
    and requires a non-empty name, emits ``folderName`` when set, and round-trips
    the result through ElementTree (PROCESS_XML_VALIDATION_FAILED on malformation).

    folderName is the writable folder attribute on Component create/update;
    folderFullPath is response-only metadata Boomi ignores on writes. Other
    builders emit folderName for placement — match them (Codex review r8 F2).

    ``process_options`` (M6, #12) is the ``<process>`` attribute string; None
    keeps the scheduled default byte-for-byte. A WSS listener build passes
    :data:`LISTENER_PROCESS_OPTIONS` — derived by the builder from the Listen
    source, never caller-supplied. The CANONICAL path passes the string
    :func:`process_options_for_profile` returns for the compiler-recorded
    profile, which is the same mapping expressed once.
    """
    BuilderValidationError, _escape_xml = _builder_helpers()
    name = str(name) if name is not None else ""
    if not name or not name.strip():
        raise BuilderValidationError(
            "Process component name is required.",
            error_code="PROCESS_XML_VALIDATION_FAILED",
            field="name",
            hint="Pass a non-empty name via the IntegrationComponentSpec.name field.",
        )
    process_inner = (
        '<process xmlns="" '
        f'{process_options or DEFAULT_PROCESS_OPTIONS}>'
        '<shapes>'
        f"{''.join(shape_xml_parts)}"
        '</shapes>'
        '</process>'
    )
    folder_attr = (
        f' folderName="{_escape_xml(str(folder_name))}"' if folder_name else ""
    )
    component_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<bns:Component '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xmlns:bns="http://api.platform.boomi.com/" '
        f'type="process" name="{_escape_xml(name)}"'
        f"{folder_attr}>"
        '<bns:encryptedValues/>'
        f'<bns:description>{_escape_xml(description)}</bns:description>'
        '<bns:object>'
        f"{process_inner}"
        '</bns:object>'
        # Issue #92 M4.5.7: a connection-field extension declaration when one was
        # emitted, else the empty element (byte-for-byte unchanged for all
        # existing process XML, including wrapper_subprocess).
        f"{process_overrides_xml or '<bns:processOverrides/>'}"
        '</bns:Component>'
    )

    # Internal invariant: the XML we just produced must round-trip through
    # ElementTree without raising. Catches stray unescaped chars or malformed
    # manual concatenation early — surfaces as PROCESS_XML_VALIDATION_FAILED
    # rather than a confusing Boomi API error at apply time.
    try:
        ET.fromstring(component_xml)
    except ET.ParseError as exc:  # pragma: no cover — defensive
        raise BuilderValidationError(
            f"Generated process Component XML did not round-trip: {exc}",
            error_code="PROCESS_XML_VALIDATION_FAILED",
            field="config",
            hint="Internal builder bug — please report.",
        ) from exc

    return component_xml


class ProcessComponentMaterializer:
    """The canonical chain's envelope writer.

    A class rather than a bare function because #153's provider wiring holds one
    and the "never calls the legacy builder" test spies on the call. It carries
    no state: everything it needs — the emitted shapes, the envelope data, the
    recorded execution profile, the typed extension bindings — arrives per call.
    """

    def materialize(
        self,
        shape_xml_parts: Sequence[str],
        *,
        name: str,
        execution_profile: str,
        description: str = "",
        folder_name: Optional[str] = None,
        extension_connections: Sequence[Dict[str, Any]] = (),
    ) -> str:
        """One process root's deployable ``<bns:Component>`` XML.

        ``execution_profile`` is the value the COMPILER recorded on the
        materialization plan. It is mapped straight to option bytes; it is never
        re-derived here, and there is deliberately no parameter by which a caller
        could supply option bytes directly.
        """
        overrides_xml = (
            render_process_overrides(extension_connections)
            if extension_connections
            else ""
        )
        return assemble_component_xml(
            shape_xml_parts,
            name=name,
            description=description,
            folder_name=folder_name,
            process_overrides_xml=overrides_xml,
            process_options=process_options_for_profile(execution_profile),
        )


__all__ = [
    "DEFAULT_PROCESS_OPTIONS",
    "LISTENER_PROCESS_OPTIONS",
    "ProcessComponentMaterializer",
    "assemble_component_xml",
    "extension_bindings_from_legacy_config",
    "process_options_for_profile",
    "render_process_overrides",
]
