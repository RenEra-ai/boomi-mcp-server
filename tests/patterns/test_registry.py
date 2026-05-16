"""Tests for the patterns registry (Issue #16)."""

from __future__ import annotations

import sys
import textwrap
from typing import Iterator

import pytest
from pydantic import BaseModel, Field

from boomi_mcp.models.integration_models import (
    IntegrationComponentSpec,
    IntegrationSpecV1,
)
from boomi_mcp.patterns import (
    ArchetypePattern,
    PatternBase,
    PatternClass,
    PatternError,
    PatternIOContract,
    PatternKind,
    PatternMetadata,
    PatternRegistry,
    PatternRegistryError,
    PrimitiveBuildContext,
    PrimitivePattern,
)


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


class _Params(BaseModel):
    integration_name: str = Field(..., description="Integration name")


def _arch(name: str, *, description: str = "", tags=None, use_cases=None, not_for=None):
    """Build a concrete ArchetypePattern subclass with given metadata."""

    md = PatternMetadata(
        name=name,
        version="1.0.0",
        kind=PatternKind.ARCHETYPE,
        description=description or f"Archetype {name}",
        tags=list(tags or []),
        use_cases=list(use_cases or []),
        not_for=list(not_for or []),
    )

    class _Arch(ArchetypePattern):
        metadata = md
        parameters_model = _Params

        @classmethod
        def emit_spec(cls, parameters):
            return IntegrationSpecV1(name=parameters.integration_name)

    _Arch.__name__ = f"Arch_{name}"
    _Arch.__qualname__ = _Arch.__name__
    return _Arch


def _prim(name: str, *, description: str = "", tags=None):
    """Build a concrete PrimitivePattern subclass with given metadata."""

    md = PatternMetadata(
        name=name,
        version="1.0.0",
        kind=PatternKind.PRIMITIVE,
        description=description or f"Primitive {name}",
        tags=list(tags or []),
    )

    class _Prim(PrimitivePattern):
        metadata = md
        parameters_model = _Params
        input_contract = PatternIOContract(name="in")
        output_contract = PatternIOContract(name="out")
        required_builders = ["process_builder"]

        @classmethod
        def emit_components(cls, context, parameters):
            return [
                IntegrationComponentSpec(
                    key=f"{context.component_prefix}-stub", type="process"
                )
            ]

    _Prim.__name__ = f"Prim_{name}"
    _Prim.__qualname__ = _Prim.__name__
    return _Prim


@pytest.fixture
def cleanup_sys_path() -> Iterator[None]:
    """Snapshot sys.path and sys.modules; restore them after the test."""

    original_path = list(sys.path)
    original_modules = set(sys.modules)
    try:
        yield
    finally:
        sys.path[:] = original_path
        for mod_name in list(sys.modules):
            if mod_name not in original_modules:
                sys.modules.pop(mod_name, None)


def _write_pkg(tmp_path, pkg_name: str, modules: dict[str, str]) -> None:
    """Create a Python package on disk with the given submodules.

    ``modules`` maps submodule basename (without ``.py``) to its source.
    An empty ``__init__.py`` is always created.
    """

    pkg_dir = tmp_path / pkg_name
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("")
    for mod_basename, source in modules.items():
        (pkg_dir / f"{mod_basename}.py").write_text(textwrap.dedent(source).lstrip())


# ---------------------------------------------------------------------------
# Manual registration
# ---------------------------------------------------------------------------


def test_register_archetype_and_primitive():
    Arch = _arch("example_arch")
    Prim = _prim("example_prim")
    registry = PatternRegistry([Arch, Prim])

    assert registry.get("example_arch") is Arch
    assert registry.get("example_prim") is Prim
    listed = registry.list_patterns()
    assert len(listed) == 2
    assert {c.metadata.name for c in listed} == {"example_arch", "example_prim"}


def test_list_patterns_deterministic_ordering():
    PrimZ = _prim("z_prim")
    ArchB = _arch("b_arch")
    ArchA = _arch("a_arch")

    # Register out of order.
    registry = PatternRegistry([PrimZ, ArchB, ArchA])
    names = [c.metadata.name for c in registry.list_patterns()]
    # archetypes first (kind.value "archetype" < "primitive"), then alphabetical.
    assert names == ["a_arch", "b_arch", "z_prim"]


def test_get_by_name_returns_class():
    Arch = _arch("example_arch")
    registry = PatternRegistry([Arch])
    assert registry.get("example_arch") is Arch
    # Whitespace around the name is tolerated.
    assert registry.get("  example_arch  ") is Arch


def test_get_with_kind_filter():
    Arch = _arch("dup_name_arch")
    Prim = _prim("dup_name_prim")
    registry = PatternRegistry([Arch, Prim])

    # Matching kind returns the class.
    assert registry.get("dup_name_arch", kind="archetype") is Arch
    assert registry.get("dup_name_arch", kind=PatternKind.ARCHETYPE) is Arch

    # Non-matching kind raises PATTERN_NOT_FOUND.
    with pytest.raises(PatternRegistryError) as excinfo:
        registry.get("dup_name_arch", kind="primitive")
    assert excinfo.value.error_code == "PATTERN_NOT_FOUND"


def test_get_missing_raises_and_converts_to_pattern_error():
    registry = PatternRegistry([_arch("a")])
    with pytest.raises(PatternRegistryError) as excinfo:
        registry.get("does_not_exist")
    assert excinfo.value.error_code == "PATTERN_NOT_FOUND"

    perr = excinfo.value.to_pattern_error()
    assert isinstance(perr, PatternError)
    payload = perr.to_dict()
    assert payload["_success"] is False
    assert payload["error_code"] == "PATTERN_NOT_FOUND"
    assert "does_not_exist" in payload["error"]


def test_duplicate_name_raises():
    Arch = _arch("shared")
    Prim = _prim("shared")
    registry = PatternRegistry([Arch])

    # Same name across archetype/primitive kinds still duplicates.
    with pytest.raises(PatternRegistryError) as excinfo:
        registry.register(Prim)
    assert excinfo.value.error_code == "DUPLICATE_PATTERN_NAME"

    # Re-registering a different class with the same name also duplicates.
    Arch2 = _arch("shared", description="another archetype with the same name")
    with pytest.raises(PatternRegistryError) as excinfo:
        registry.register(Arch2)
    assert excinfo.value.error_code == "DUPLICATE_PATTERN_NAME"


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------


def test_query_filter_is_case_insensitive_and_searches_all_fields():
    in_name = _arch("KEYWORD_NAME")
    in_desc = _arch("p_desc", description="Contains keyword in description")
    in_tags = _arch("p_tags", tags=["KeyWord"])
    in_use = _arch("p_use", use_cases=["use the KEYWORD here"])
    in_not = _arch("p_not", not_for=["never KEYWORD this"])
    decoy = _arch("decoy", description="nothing to see")

    registry = PatternRegistry([in_name, in_desc, in_tags, in_use, in_not, decoy])
    found = registry.list_patterns(query="keyword")
    found_names = {c.metadata.name for c in found}
    assert found_names == {"KEYWORD_NAME", "p_desc", "p_tags", "p_use", "p_not"}

    # Mixed-case query still matches all five.
    found = registry.list_patterns(query="KeYwOrD")
    assert {c.metadata.name for c in found} == found_names


def test_tags_filter_is_case_insensitive_and_ands():
    a = _arch("a", tags=["Alpha", "Beta"])
    b = _arch("b", tags=["alpha"])
    c = _arch("c", tags=["beta"])
    registry = PatternRegistry([a, b, c])

    # AND semantics: both tags required (case-insensitive).
    found = registry.list_patterns(tags=["ALPHA", "beta"])
    assert [cls.metadata.name for cls in found] == ["a"]

    # Single-tag filter matches anything with that tag.
    found = registry.list_patterns(tags=["alpha"])
    assert {cls.metadata.name for cls in found} == {"a", "b"}


def test_invalid_kind_raises():
    registry = PatternRegistry([_arch("a")])
    with pytest.raises(PatternRegistryError) as excinfo:
        registry.list_patterns(kind="bogus")
    assert excinfo.value.error_code == "INVALID_PATTERN_KIND"

    # Also surfaces via get().
    with pytest.raises(PatternRegistryError) as excinfo:
        registry.get("a", kind="bogus")
    assert excinfo.value.error_code == "INVALID_PATTERN_KIND"

    # Converts cleanly to PatternError payload.
    payload = excinfo.value.to_pattern_error().to_dict()
    assert payload["_success"] is False
    assert payload["error_code"] == "INVALID_PATTERN_KIND"


# ---------------------------------------------------------------------------
# Contract validation
# ---------------------------------------------------------------------------


def test_contract_validation_failures():
    registry = PatternRegistry()

    # Not a PatternBase subclass.
    class _NotAPattern:
        pass

    with pytest.raises(PatternRegistryError) as excinfo:
        registry.register(_NotAPattern)  # type: ignore[arg-type]
    assert excinfo.value.error_code == "PATTERN_CONTRACT_INVALID"

    # Blank metadata.name (after strip).
    blank_md = PatternMetadata(
        name="   ", version="1.0.0", kind=PatternKind.ARCHETYPE, description="d"
    )

    class _BlankName(ArchetypePattern):
        metadata = blank_md
        parameters_model = _Params

        @classmethod
        def emit_spec(cls, parameters):
            return IntegrationSpecV1(name="x")

    with pytest.raises(PatternRegistryError) as excinfo:
        registry.register(_BlankName)
    assert excinfo.value.error_code == "PATTERN_CONTRACT_INVALID"

    # metadata is not a PatternMetadata instance.
    class _DictMetadata(ArchetypePattern):
        metadata = {"name": "x"}  # type: ignore[assignment]
        parameters_model = _Params

        @classmethod
        def emit_spec(cls, parameters):
            return IntegrationSpecV1(name="x")

    with pytest.raises(PatternRegistryError) as excinfo:
        registry.register(_DictMetadata)
    assert excinfo.value.error_code == "PATTERN_CONTRACT_INVALID"

    # parameters_model is not a BaseModel subclass.
    class _BadParams(ArchetypePattern):
        metadata = PatternMetadata(
            name="bad_params",
            version="1.0.0",
            kind=PatternKind.ARCHETYPE,
            description="d",
        )
        parameters_model = dict  # type: ignore[assignment]

        @classmethod
        def emit_spec(cls, parameters):
            return IntegrationSpecV1(name="x")

    with pytest.raises(PatternRegistryError) as excinfo:
        registry.register(_BadParams)
    assert excinfo.value.error_code == "PATTERN_CONTRACT_INVALID"

    # Archetype subclass with primitive kind in metadata.
    class _WrongKind(ArchetypePattern):
        metadata = PatternMetadata(
            name="wrong_kind",
            version="1.0.0",
            kind=PatternKind.PRIMITIVE,
            description="d",
        )
        parameters_model = _Params

        @classmethod
        def emit_spec(cls, parameters):
            return IntegrationSpecV1(name="x")

    with pytest.raises(PatternRegistryError) as excinfo:
        registry.register(_WrongKind)
    assert excinfo.value.error_code == "PATTERN_CONTRACT_INVALID"

    # Cannot register the abstract base classes themselves.
    for base in (PatternBase, ArchetypePattern, PrimitivePattern):
        with pytest.raises(PatternRegistryError) as excinfo:
            registry.register(base)  # type: ignore[arg-type]
        assert excinfo.value.error_code == "PATTERN_CONTRACT_INVALID"


# ---------------------------------------------------------------------------
# Discovery via from_package()
# ---------------------------------------------------------------------------


def test_from_package_discovers_concrete_patterns(tmp_path, cleanup_sys_path):
    pkg_name = "discovered_pkg"
    _write_pkg(
        tmp_path,
        pkg_name,
        {
            "my_arch": """
                from pydantic import BaseModel, Field

                from boomi_mcp.models.integration_models import IntegrationSpecV1
                from boomi_mcp.patterns import (
                    ArchetypePattern,
                    PatternKind,
                    PatternMetadata,
                )


                class _DiscParams(BaseModel):
                    integration_name: str = Field(...)


                class DiscoveredArchetype(ArchetypePattern):
                    metadata = PatternMetadata(
                        name="discovered_archetype",
                        version="1.0.0",
                        kind=PatternKind.ARCHETYPE,
                        description="Discovered via from_package",
                    )
                    parameters_model = _DiscParams

                    @classmethod
                    def emit_spec(cls, parameters):
                        return IntegrationSpecV1(name=parameters.integration_name)
                """,
        },
    )
    sys.path.insert(0, str(tmp_path))

    registry = PatternRegistry.from_package(pkg_name)
    listed = registry.list_patterns()
    names = [c.metadata.name for c in listed]
    assert "discovered_archetype" in names


def test_from_package_skips_abstract_and_base_classes(tmp_path, cleanup_sys_path):
    pkg_name = "skip_abstract_pkg"
    _write_pkg(
        tmp_path,
        pkg_name,
        {
            "mixed": """
                from abc import abstractmethod

                from pydantic import BaseModel, Field

                from boomi_mcp.models.integration_models import IntegrationSpecV1
                from boomi_mcp.patterns import (
                    ArchetypePattern,
                    PatternKind,
                    PatternMetadata,
                )


                class _P(BaseModel):
                    integration_name: str = Field(...)


                # Re-export the base class so discovery sees it in vars(module).
                Base = ArchetypePattern


                class StillAbstract(ArchetypePattern):
                    metadata = PatternMetadata(
                        name="still_abstract",
                        version="1.0.0",
                        kind=PatternKind.ARCHETYPE,
                        description="Has an unimplemented extra abstract method",
                    )
                    parameters_model = _P

                    @classmethod
                    def emit_spec(cls, parameters):
                        return IntegrationSpecV1(name=parameters.integration_name)

                    @classmethod
                    @abstractmethod
                    def extra_hook(cls):
                        ...


                class ConcreteOne(ArchetypePattern):
                    metadata = PatternMetadata(
                        name="concrete_one",
                        version="1.0.0",
                        kind=PatternKind.ARCHETYPE,
                        description="Concrete and registrable",
                    )
                    parameters_model = _P

                    @classmethod
                    def emit_spec(cls, parameters):
                        return IntegrationSpecV1(name=parameters.integration_name)


                class _NotAPattern:
                    pass
                """,
        },
    )
    sys.path.insert(0, str(tmp_path))

    registry = PatternRegistry.from_package(pkg_name)
    names = {c.metadata.name for c in registry.list_patterns()}
    assert names == {"concrete_one"}


def test_from_package_import_failure_raises_discovery_failed(
    tmp_path, cleanup_sys_path
):
    pkg_name = "broken_pkg"
    _write_pkg(
        tmp_path,
        pkg_name,
        {
            "broken": """
                raise RuntimeError("intentional discovery failure")
                """,
        },
    )
    sys.path.insert(0, str(tmp_path))

    with pytest.raises(PatternRegistryError) as excinfo:
        PatternRegistry.from_package(pkg_name)
    assert excinfo.value.error_code == "PATTERN_DISCOVERY_FAILED"
    assert excinfo.value.context.get("module") == f"{pkg_name}.broken"


def test_from_package_canonical_path_succeeds():
    # Smoke test: discovery must succeed on the real on-disk patterns package
    # (no validation failures, no import errors). The pattern *count* is
    # intentionally not asserted because future issues will add concrete
    # patterns under this package.
    registry = PatternRegistry.from_package("boomi_mcp.patterns")
    assert isinstance(registry, PatternRegistry)
    assert isinstance(registry.list_patterns(), list)


# ---------------------------------------------------------------------------
# Type alias sanity
# ---------------------------------------------------------------------------


def test_pattern_class_alias_resolves_to_class_type():
    # PatternClass is exported so signatures like ``get() -> PatternClass`` are
    # self-documenting. It must accept any concrete PatternBase subclass.
    cls: PatternClass = _arch("alias_check")
    assert issubclass(cls, PatternBase)
