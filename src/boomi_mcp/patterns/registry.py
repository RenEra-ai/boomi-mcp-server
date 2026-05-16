"""Pattern registry: discover, list, and look up V3 archetypes and primitives."""

from __future__ import annotations

import importlib
import inspect
import logging
import pkgutil
from types import ModuleType
from typing import Any, Iterable, Optional

from pydantic import BaseModel

from .base import (
    ArchetypePattern,
    PatternBase,
    PatternKind,
    PatternMetadata,
    PrimitivePattern,
)
from .errors import PatternError

logger = logging.getLogger(__name__)

PatternClass = type[PatternBase]

# Class objects we should never register — the abstract base hierarchy itself.
_BASE_CLASSES: frozenset[type] = frozenset(
    {PatternBase, ArchetypePattern, PrimitivePattern}
)


class PatternRegistryError(Exception):
    """Raised by PatternRegistry for lookup, duplicate, kind, contract, and discovery failures."""

    def __init__(
        self,
        *,
        error_code: str,
        error: str,
        suggestion: Optional[str] = None,
        retryable: bool = False,
        context: Optional[dict[str, Any]] = None,
    ) -> None:
        super().__init__(error)
        self.error_code = error_code
        self.error = error
        self.suggestion = suggestion
        self.retryable = retryable
        self.context: dict[str, Any] = context or {}

    def to_pattern_error(self) -> PatternError:
        return PatternError(
            error_code=self.error_code,
            error=self.error,
            suggestion=self.suggestion,
            retryable=self.retryable,
            context=self.context,
        )


class PatternRegistry:
    """In-memory registry of archetype and primitive pattern classes."""

    def __init__(self, patterns: Iterable[PatternClass] = ()) -> None:
        self._by_name: dict[str, PatternClass] = {}
        for cls in patterns:
            self.register(cls)

    @classmethod
    def from_package(
        cls, package: str | ModuleType = "boomi_mcp.patterns"
    ) -> "PatternRegistry":
        module = cls._resolve_package(package)
        registry = cls()
        seen: set[int] = set()
        prefix = module.__name__ + "."
        for _finder, mod_name, _ispkg in pkgutil.walk_packages(module.__path__, prefix):
            try:
                submodule = importlib.import_module(mod_name)
            except Exception as exc:  # noqa: BLE001 — wrap any import failure
                raise PatternRegistryError(
                    error_code="PATTERN_DISCOVERY_FAILED",
                    error=f"Failed to import pattern module {mod_name!r}: {exc}",
                    suggestion="Fix the import error or exclude the broken module.",
                    context={"module": mod_name},
                ) from exc
            for obj in vars(submodule).values():
                if not inspect.isclass(obj):
                    continue
                if id(obj) in seen:
                    continue
                if not issubclass(obj, (ArchetypePattern, PrimitivePattern)):
                    continue
                if obj in _BASE_CLASSES or inspect.isabstract(obj):
                    continue
                seen.add(id(obj))
                registry.register(obj)
        return registry

    def register(self, pattern_cls: PatternClass) -> None:
        self._validate_pattern_class(pattern_cls)
        name = pattern_cls.metadata.name.strip()
        if name in self._by_name:
            existing = self._by_name[name]
            raise PatternRegistryError(
                error_code="DUPLICATE_PATTERN_NAME",
                error=f"Pattern name {name!r} is already registered.",
                suggestion="Rename one of the patterns; names must be globally unique.",
                context={
                    "name": name,
                    "existing": f"{existing.__module__}.{existing.__qualname__}",
                    "incoming": f"{pattern_cls.__module__}.{pattern_cls.__qualname__}",
                },
            )
        self._by_name[name] = pattern_cls

    def get(
        self,
        name: str,
        *,
        kind: PatternKind | str | None = None,
    ) -> PatternClass:
        if not isinstance(name, str):
            raise PatternRegistryError(
                error_code="PATTERN_NOT_FOUND",
                error="Pattern name must be a string.",
                context={"name": repr(name)},
            )
        wanted_kind = self._normalize_kind(kind)
        lookup = name.strip()
        cls = self._by_name.get(lookup)
        if cls is None or (wanted_kind is not None and cls.metadata.kind != wanted_kind):
            raise PatternRegistryError(
                error_code="PATTERN_NOT_FOUND",
                error=f"No pattern registered with name {lookup!r}.",
                suggestion="Call list_patterns() to see available pattern names.",
                context={
                    "name": lookup,
                    "kind": wanted_kind.value if wanted_kind else None,
                },
            )
        return cls

    def list_patterns(
        self,
        *,
        kind: PatternKind | str | None = None,
        query: str | None = None,
        tags: Iterable[str] | None = None,
    ) -> list[PatternClass]:
        wanted_kind = self._normalize_kind(kind)
        needle = query.strip().lower() if query else None
        needed_tags = {t.strip().lower() for t in tags} if tags else set()
        results: list[PatternClass] = []
        for cls in self._by_name.values():
            md: PatternMetadata = cls.metadata
            if wanted_kind is not None and md.kind != wanted_kind:
                continue
            if needle is not None and needle not in self._haystack(md):
                continue
            if needed_tags and not needed_tags.issubset({t.lower() for t in md.tags}):
                continue
            results.append(cls)
        results.sort(
            key=lambda c: (c.metadata.kind.value, c.metadata.name, c.metadata.version)
        )
        return results

    # ---- internal helpers -------------------------------------------------

    @staticmethod
    def _resolve_package(package: str | ModuleType) -> ModuleType:
        if isinstance(package, ModuleType):
            module = package
        else:
            try:
                module = importlib.import_module(package)
            except Exception as exc:  # noqa: BLE001
                raise PatternRegistryError(
                    error_code="PATTERN_DISCOVERY_FAILED",
                    error=f"Failed to import pattern package {package!r}: {exc}",
                    context={"package": package},
                ) from exc
        if not hasattr(module, "__path__"):
            raise PatternRegistryError(
                error_code="PATTERN_DISCOVERY_FAILED",
                error=f"{module.__name__!r} is not a package (no __path__).",
                context={"package": module.__name__},
            )
        return module

    @staticmethod
    def _normalize_kind(kind: PatternKind | str | None) -> PatternKind | None:
        if kind is None:
            return None
        if isinstance(kind, PatternKind):
            return kind
        try:
            return PatternKind(str(kind).strip().lower())
        except ValueError as exc:
            valid = ", ".join(k.value for k in PatternKind)
            raise PatternRegistryError(
                error_code="INVALID_PATTERN_KIND",
                error=f"Unknown pattern kind {kind!r}.",
                suggestion=f"Use one of: {valid}.",
                context={"kind": kind, "valid": [k.value for k in PatternKind]},
            ) from exc

    @staticmethod
    def _haystack(md: PatternMetadata) -> str:
        parts = [md.name, md.description, *md.tags, *md.use_cases, *md.not_for]
        return " \n ".join(p.lower() for p in parts if p)

    @staticmethod
    def _validate_pattern_class(pattern_cls: Any) -> None:
        if not (inspect.isclass(pattern_cls) and issubclass(pattern_cls, PatternBase)):
            raise PatternRegistryError(
                error_code="PATTERN_CONTRACT_INVALID",
                error=f"{pattern_cls!r} is not a PatternBase subclass.",
                context={"class": repr(pattern_cls)},
            )
        if pattern_cls in _BASE_CLASSES or inspect.isabstract(pattern_cls):
            raise PatternRegistryError(
                error_code="PATTERN_CONTRACT_INVALID",
                error=f"{pattern_cls.__qualname__} is abstract or a base class; cannot register.",
                context={
                    "class": f"{pattern_cls.__module__}.{pattern_cls.__qualname__}"
                },
            )
        md = getattr(pattern_cls, "metadata", None)
        if not isinstance(md, PatternMetadata):
            raise PatternRegistryError(
                error_code="PATTERN_CONTRACT_INVALID",
                error=f"{pattern_cls.__qualname__}.metadata must be a PatternMetadata instance.",
                context={"class": pattern_cls.__qualname__},
            )
        if not md.name or not md.name.strip():
            raise PatternRegistryError(
                error_code="PATTERN_CONTRACT_INVALID",
                error=f"{pattern_cls.__qualname__}.metadata.name must be non-empty.",
                context={"class": pattern_cls.__qualname__},
            )
        params_model = getattr(pattern_cls, "parameters_model", None)
        if not (inspect.isclass(params_model) and issubclass(params_model, BaseModel)):
            raise PatternRegistryError(
                error_code="PATTERN_CONTRACT_INVALID",
                error=f"{pattern_cls.__qualname__}.parameters_model must subclass pydantic.BaseModel.",
                context={"class": pattern_cls.__qualname__},
            )
        # Kind must match the concrete subclass family.
        if issubclass(pattern_cls, ArchetypePattern) and md.kind != PatternKind.ARCHETYPE:
            raise PatternRegistryError(
                error_code="PATTERN_CONTRACT_INVALID",
                error=(
                    f"{pattern_cls.__qualname__} extends ArchetypePattern "
                    f"but metadata.kind={md.kind.value!r}."
                ),
                context={
                    "class": pattern_cls.__qualname__,
                    "expected_kind": "archetype",
                },
            )
        if issubclass(pattern_cls, PrimitivePattern) and md.kind != PatternKind.PRIMITIVE:
            raise PatternRegistryError(
                error_code="PATTERN_CONTRACT_INVALID",
                error=(
                    f"{pattern_cls.__qualname__} extends PrimitivePattern "
                    f"but metadata.kind={md.kind.value!r}."
                ),
                context={
                    "class": pattern_cls.__qualname__,
                    "expected_kind": "primitive",
                },
            )
