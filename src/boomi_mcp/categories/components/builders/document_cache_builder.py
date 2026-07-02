"""Issue #122 (M11.3, epic #118) — DocumentCacheBuilder.

Emits ``<bns:Component type="documentcache">`` XML from a structured config
declaring the cache's profile binding plus its index/key structure. The
component is referenced by the cache steps (``doccacheload`` / ``cache_put``,
``doccacheretrieve`` / ``cache_get``, ``doccacheremove``) and by map-level
``DocumentCacheJoins`` entries — none of which define cache structure inline.

Reference XML shape evidence (#119 census):

* work ``4f436363-d5d5-4e93-8ac0-cce6f3245449`` — profiled JSON cache with one
  ``CacheIndex`` and one ``ProfileElementKeyConfig`` key
  (``tests/fixtures/live_xml/m11/documentcache_profile_key_component.xml``).
* Companion ``document_cache_component.md`` corroborates the attribute set
  and documents the id-zero silent-failure gotcha (indexId / cacheKey id
  must be non-zero) plus the missing-``profileType`` runtime crash.

Envelope shape:

.. code-block:: xml

    <bns:Component type="documentcache" name="..." folderFullPath="...">
      <bns:encryptedValues/>
      <bns:description>...</bns:description>
      <bns:object>
        <DocumentCache xmlns="" enforceSingleLucene="true"
                       profile="<uuid>" profileType="profile.json">
          <CacheIndex indexId="1" indexName="by id">
            <cacheKey alias="..." elementKey="7" id="2" name="..."
                      taglistKey="0" xsi:type="ProfileElementKeyConfig"/>
          </CacheIndex>
        </DocumentCache>
      </bns:object>
    </bns:Component>

v1 evidence gates (#119 census):

* ``profile_type`` accepts only ``profile.json`` / ``profile.xml`` (the
  live-verified profiled-cache family). ``profile.none`` and the other
  profile families are companion-documented but have no live capture — they
  are rejected with ``DOCUMENT_CACHE_PROFILE_TYPE_UNSUPPORTED`` until a
  renera round-trip verifies them.
* Cache keys support only the live-verified ``ProfileElementKeyConfig``
  shape. ``kind='document_property'`` (``DocumentPropertyKeyConfig``) is
  rejected with ``DOCUMENT_CACHE_KEY_KIND_GATED``.
* ``indexId`` and ``cacheKey id`` must be positive non-zero — ``0`` is
  accepted by the platform API but silently indexes nothing at runtime.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Tuple

from ._preservation_policy import OwnedPath, PreservationPolicy
from .connector_builder import BuilderValidationError, _escape_xml
from .profile_generation import (
    DOCUMENT_CACHE_INDEX_INVALID,
    DOCUMENT_CACHE_INDEX_REQUIRED,
    DOCUMENT_CACHE_KEY_INVALID,
    DOCUMENT_CACHE_KEY_KIND_GATED,
    DOCUMENT_CACHE_NAME_REQUIRED,
    DOCUMENT_CACHE_PROFILE_REQUIRED,
    DOCUMENT_CACHE_PROFILE_TYPE_UNSUPPORTED,
    DOCUMENT_CACHE_RAW_XML_UNSUPPORTED,
    DOCUMENT_CACHE_VALIDATION_FAILED,
)


# Live-verified profiled-cache family only (see module docstring gates).
_SUPPORTED_PROFILE_TYPES: Tuple[str, ...] = ("profile.json", "profile.xml")

# Companion-documented profile types without a live capture — named in the
# rejection hint so callers learn the unlock path instead of guessing.
_GATED_PROFILE_TYPES: Tuple[str, ...] = (
    "profile.none",
    "profile.flatfile",
    "profile.db",
    "profile.edi",
)

_FORBIDDEN_SECRET_FIELDS: Tuple[str, ...] = (
    "password",
    "password_ref",
    "secret",
    "token",
    "access_token",
    "client_secret",
    "api_key",
    "credentials",
    "authorization",
    "bearer",
)

_ALLOWED_TOP_LEVEL_KEYS: Tuple[str, ...] = (
    "component_type",
    "component_name",
    "folder_path",
    "description",
    "profile_type",
    "profile_id",
    "enforce_single_lucene",
    "indexes",
    "xml",
)

_ALLOWED_INDEX_KEYS: Tuple[str, ...] = ("index_id", "index_name", "keys")

_ALLOWED_KEY_KEYS: Tuple[str, ...] = ("id", "element_key", "name", "alias", "kind")


def _scan_forbidden_secret_fields(
    config: Any, _path_prefix: str = ""
) -> Optional[BuilderValidationError]:
    """Recursive secret-shaped key scan (dict keys only)."""
    if isinstance(config, dict):
        for key in _FORBIDDEN_SECRET_FIELDS:
            if key in config:
                field_path = f"{_path_prefix}{key}"
                return BuilderValidationError(
                    f"{field_path!r} cannot be supplied in a documentcache "
                    "config — cache definitions must not transport credentials.",
                    error_code="PLAINTEXT_SECRET_REJECTED",
                    field=field_path,
                )
        for key, value in config.items():
            nested = _scan_forbidden_secret_fields(
                value, _path_prefix=f"{_path_prefix}{key}."
            )
            if nested is not None:
                return nested
    elif isinstance(config, list):
        base = _path_prefix[:-1] if _path_prefix.endswith(".") else _path_prefix
        for index, item in enumerate(config):
            nested = _scan_forbidden_secret_fields(
                item, _path_prefix=f"{base}[{index}]."
            )
            if nested is not None:
                return nested
    return None


def _positive_int(value: Any) -> Optional[int]:
    """Return the int when ``value`` is a positive non-zero integer, else None."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if value > 0 else None


class DocumentCacheBuilder:
    """Emit ``documentcache`` component XML from structured config."""

    SUPPORTED_COMPONENT_TYPES: Tuple[str, ...] = ("documentcache",)
    FORBIDDEN_SECRET_FIELDS: Tuple[str, ...] = _FORBIDDEN_SECRET_FIELDS
    SUPPORTED_PROFILE_TYPES: Tuple[str, ...] = _SUPPORTED_PROFILE_TYPES

    # ------------------------------------------------------------------
    # Public secret-scan helpers (mirrors ScriptMappingBuilder shape).
    # ------------------------------------------------------------------

    @classmethod
    def scan_forbidden_secret_fields(
        cls, config: Any, _path_prefix: str = ""
    ) -> Optional[BuilderValidationError]:
        return _scan_forbidden_secret_fields(config, _path_prefix=_path_prefix)

    @classmethod
    def redact_forbidden_secret_fields_in_place(cls, config: Any) -> None:
        if isinstance(config, dict):
            for key in cls.FORBIDDEN_SECRET_FIELDS:
                if key in config:
                    config[key] = "[REDACTED]"
            for value in config.values():
                cls.redact_forbidden_secret_fields_in_place(value)
        elif isinstance(config, list):
            for item in config:
                cls.redact_forbidden_secret_fields_in_place(item)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @classmethod
    def validate_config(
        cls, config: Dict[str, Any]
    ) -> Optional[BuilderValidationError]:
        """Validate a documentcache config; return None on success."""
        secret_err = cls.scan_forbidden_secret_fields(config)
        if secret_err is not None:
            return secret_err

        for key in config.keys():
            if key in _ALLOWED_TOP_LEVEL_KEYS:
                continue
            if key in ("document_cache", "cache_indexes", "object", "bns_object"):
                return BuilderValidationError(
                    f"{key!r} is not accepted — the builder owns the "
                    "DocumentCache subtree.",
                    error_code=DOCUMENT_CACHE_RAW_XML_UNSUPPORTED,
                    field=key,
                    hint=(
                        "Declare 'indexes' entries instead of raw XML. The "
                        "raw-XML escape hatch is config['xml'], handled "
                        "upstream of this builder."
                    ),
                )
            return BuilderValidationError(
                f"unknown top-level field {key!r} for documentcache",
                error_code=DOCUMENT_CACHE_VALIDATION_FAILED,
                field=key,
                hint=f"Supported top-level keys: {sorted(_ALLOWED_TOP_LEVEL_KEYS)}.",
            )

        component_name = config.get("component_name")
        if not isinstance(component_name, str) or not component_name.strip():
            return BuilderValidationError(
                "component_name is required",
                error_code=DOCUMENT_CACHE_NAME_REQUIRED,
                field="component_name",
                hint="Provide a non-empty component_name string.",
            )

        profile_type = config.get("profile_type")
        normalized_profile_type = (
            profile_type.strip() if isinstance(profile_type, str) else ""
        )
        if normalized_profile_type not in _SUPPORTED_PROFILE_TYPES:
            gated = normalized_profile_type in _GATED_PROFILE_TYPES
            return BuilderValidationError(
                f"profile_type must be one of {_SUPPORTED_PROFILE_TYPES} "
                f"(got {profile_type!r})",
                error_code=DOCUMENT_CACHE_PROFILE_TYPE_UNSUPPORTED,
                field="profile_type",
                hint=(
                    (
                        f"{normalized_profile_type!r} is companion-documented but "
                        "has no live-captured wire shape (#119 census) — it is "
                        "gated until a disposable-account round-trip verifies it."
                    )
                    if gated
                    else (
                        "Profiled caches (profile.json / profile.xml) are the "
                        "live-verified v1 family. profileType is mandatory — "
                        "omitting it crashes the cache at runtime."
                    )
                ),
            )

        profile_id = config.get("profile_id")
        if not isinstance(profile_id, str) or not profile_id.strip():
            return BuilderValidationError(
                "profile_id is required for a profiled cache (literal id or "
                "$ref:KEY token)",
                error_code=DOCUMENT_CACHE_PROFILE_REQUIRED,
                field="profile_id",
                hint=(
                    "Pass the profile component id the cache parses documents "
                    "through — the cacheKey elementKey values reference that "
                    "profile's element keys."
                ),
            )

        enforce = config.get("enforce_single_lucene")
        if enforce is not None and not isinstance(enforce, bool):
            return BuilderValidationError(
                "enforce_single_lucene must be a boolean when provided",
                error_code=DOCUMENT_CACHE_VALIDATION_FAILED,
                field="enforce_single_lucene",
                hint=(
                    "true (default, matches the GUI) enables retrieve-all and "
                    "remove-by-index; false allows multiple index entries per "
                    "document."
                ),
            )

        indexes = config.get("indexes")
        if not isinstance(indexes, list) or not indexes:
            return BuilderValidationError(
                "indexes must be a non-empty list (at least one index with at "
                "least one key)",
                error_code=DOCUMENT_CACHE_INDEX_REQUIRED,
                field="indexes",
                hint=(
                    "Each entry is {index_id: <non-zero int>, index_name: <str>, "
                    "keys: [{id, element_key, name, alias?}]}."
                ),
            )

        seen_index_ids: Dict[int, str] = {}
        for i, index in enumerate(indexes):
            index_field = f"indexes[{i}]"
            if not isinstance(index, Mapping):
                return BuilderValidationError(
                    f"{index_field} must be a mapping object",
                    error_code=DOCUMENT_CACHE_INDEX_INVALID,
                    field=index_field,
                )
            extra = set(index) - set(_ALLOWED_INDEX_KEYS)
            if extra:
                return BuilderValidationError(
                    f"{index_field} has unsupported key(s): {sorted(extra)}.",
                    error_code=DOCUMENT_CACHE_INDEX_INVALID,
                    field=index_field,
                    hint=f"Allowed index keys: {sorted(_ALLOWED_INDEX_KEYS)}.",
                )
            index_id = _positive_int(index.get("index_id"))
            if index_id is None:
                return BuilderValidationError(
                    f"{index_field}.index_id must be a positive non-zero integer",
                    error_code=DOCUMENT_CACHE_INDEX_INVALID,
                    field=f"{index_field}.index_id",
                    hint=(
                        "indexId=0 is accepted by the API but silently indexes "
                        "nothing at runtime — use 1-based sequential ids."
                    ),
                )
            if index_id in seen_index_ids:
                return BuilderValidationError(
                    f"{index_field}.index_id duplicates {seen_index_ids[index_id]}",
                    error_code=DOCUMENT_CACHE_INDEX_INVALID,
                    field=f"{index_field}.index_id",
                )
            seen_index_ids[index_id] = f"{index_field}.index_id"
            index_name = index.get("index_name")
            if not isinstance(index_name, str) or not index_name.strip():
                return BuilderValidationError(
                    f"{index_field}.index_name is required",
                    error_code=DOCUMENT_CACHE_INDEX_INVALID,
                    field=f"{index_field}.index_name",
                )

            keys = index.get("keys")
            if not isinstance(keys, list) or not keys:
                return BuilderValidationError(
                    f"{index_field}.keys must be a non-empty list",
                    error_code=DOCUMENT_CACHE_INDEX_INVALID,
                    field=f"{index_field}.keys",
                    hint="Each index needs at least one cacheKey.",
                )
            seen_key_ids: Dict[int, str] = {}
            for j, key_entry in enumerate(keys):
                key_field = f"{index_field}.keys[{j}]"
                if not isinstance(key_entry, Mapping):
                    return BuilderValidationError(
                        f"{key_field} must be a mapping object",
                        error_code=DOCUMENT_CACHE_KEY_INVALID,
                        field=key_field,
                    )
                extra = set(key_entry) - set(_ALLOWED_KEY_KEYS)
                if extra:
                    return BuilderValidationError(
                        f"{key_field} has unsupported key(s): {sorted(extra)}.",
                        error_code=DOCUMENT_CACHE_KEY_INVALID,
                        field=key_field,
                        hint=f"Allowed key fields: {sorted(_ALLOWED_KEY_KEYS)}.",
                    )
                kind = key_entry.get("kind")
                if kind is not None and kind != "profile_element":
                    if kind == "document_property":
                        return BuilderValidationError(
                            f"{key_field}.kind 'document_property' is gated — "
                            "no live-captured DocumentPropertyKeyConfig exists "
                            "(#119 census).",
                            error_code=DOCUMENT_CACHE_KEY_KIND_GATED,
                            field=f"{key_field}.kind",
                            hint=(
                                "v1 supports profile_element keys only. The "
                                "document-property key kind unlocks after a "
                                "disposable-account round-trip capture."
                            ),
                        )
                    return BuilderValidationError(
                        f"{key_field}.kind {kind!r} is not supported",
                        error_code=DOCUMENT_CACHE_KEY_INVALID,
                        field=f"{key_field}.kind",
                        hint="Supported: 'profile_element' (default).",
                    )
                key_id = _positive_int(key_entry.get("id"))
                if key_id is None:
                    return BuilderValidationError(
                        f"{key_field}.id must be a positive non-zero integer",
                        error_code=DOCUMENT_CACHE_KEY_INVALID,
                        field=f"{key_field}.id",
                        hint=(
                            "cacheKey id=0 is accepted by the API but silently "
                            "indexes nothing at runtime — use any non-zero id."
                        ),
                    )
                if key_id in seen_key_ids:
                    return BuilderValidationError(
                        f"{key_field}.id duplicates {seen_key_ids[key_id]}",
                        error_code=DOCUMENT_CACHE_KEY_INVALID,
                        field=f"{key_field}.id",
                    )
                seen_key_ids[key_id] = f"{key_field}.id"
                element_key = key_entry.get("element_key")
                if isinstance(element_key, bool) or not isinstance(
                    element_key, (str, int)
                ) or not str(element_key).strip():
                    return BuilderValidationError(
                        f"{key_field}.element_key is required (the referenced "
                        "profile element's key)",
                        error_code=DOCUMENT_CACHE_KEY_INVALID,
                        field=f"{key_field}.element_key",
                    )
                name = key_entry.get("name")
                if not isinstance(name, str) or not name.strip():
                    return BuilderValidationError(
                        f"{key_field}.name is required",
                        error_code=DOCUMENT_CACHE_KEY_INVALID,
                        field=f"{key_field}.name",
                        hint=(
                            "Use the profile element display form, e.g. "
                            "'ID (Root/Object/wall/Object/ID)'."
                        ),
                    )
                alias = key_entry.get("alias")
                if alias is not None and (
                    not isinstance(alias, str) or not alias.strip()
                ):
                    return BuilderValidationError(
                        f"{key_field}.alias must be a non-blank string when provided",
                        error_code=DOCUMENT_CACHE_KEY_INVALID,
                        field=f"{key_field}.alias",
                    )

        return None

    # ------------------------------------------------------------------
    # XML emission
    # ------------------------------------------------------------------

    def build(self, **params: Any) -> str:
        """Emit the ``<bns:Component type='documentcache'>`` XML."""
        config = dict(params)
        validation_err = self.validate_config(config)
        if validation_err is not None:
            raise validation_err

        component_name = str(config["component_name"]).strip()
        folder_path = config.get("folder_path")
        description = config.get("description") or ""
        profile_type = str(config["profile_type"]).strip()
        profile_id = str(config["profile_id"]).strip()
        enforce = bool(config.get("enforce_single_lucene", True))

        index_parts: List[str] = []
        for index in config["indexes"]:
            key_parts: List[str] = []
            for key_entry in index["keys"]:
                name = str(key_entry["name"]).strip()
                alias = str(key_entry.get("alias") or name).strip()
                key_parts.append(
                    f'<cacheKey alias="{_escape_xml(alias)}" '
                    f'elementKey="{_escape_xml(str(key_entry["element_key"]).strip())}" '
                    f'id="{int(key_entry["id"])}" '
                    f'name="{_escape_xml(name)}" '
                    'taglistKey="0" '
                    'xsi:type="ProfileElementKeyConfig"/>'
                )
            index_parts.append(
                f'<CacheIndex indexId="{int(index["index_id"])}" '
                f'indexName="{_escape_xml(str(index["index_name"]).strip())}">'
                f"{''.join(key_parts)}"
                "</CacheIndex>"
            )

        folder_attr = (
            f' folderFullPath="{_escape_xml(str(folder_path))}"'
            if folder_path
            else ""
        )
        return (
            '<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            'xmlns:bns="http://api.platform.boomi.com/" '
            f'type="documentcache"{folder_attr} '
            f'name="{_escape_xml(component_name)}">'
            "<bns:encryptedValues/>"
            f"<bns:description>{_escape_xml(description)}</bns:description>"
            "<bns:object>"
            '<DocumentCache xmlns="" '
            f'enforceSingleLucene="{"true" if enforce else "false"}" '
            f'profile="{_escape_xml(profile_id)}" '
            f'profileType="{_escape_xml(profile_type)}">'
            f"{''.join(index_parts)}"
            "</DocumentCache>"
            "</bns:object>"
            "</bns:Component>"
        )


PROCESS_DOCUMENT_CACHE_BUILDERS: Dict[str, type] = {
    "documentcache": DocumentCacheBuilder,
}

# Backwards-compatible canonical name for the registry.
DOCUMENT_CACHE_BUILDERS = PROCESS_DOCUMENT_CACHE_BUILDERS


# Update-preservation policy: the builder owns the entire `<DocumentCache>`
# subtree. bns:encryptedValues, bns:processOverrides, and unknown siblings
# are preserved automatically.
DocumentCacheBuilder.PRESERVATION_POLICY = PreservationPolicy(
    component_type="documentcache",
    owned_paths=(OwnedPath(path="bns:object/DocumentCache"),),
)


def get_document_cache_builder(component_type: str) -> Optional[type]:
    """Return the builder class for ``component_type`` or ``None``."""
    return DOCUMENT_CACHE_BUILDERS.get(component_type)
