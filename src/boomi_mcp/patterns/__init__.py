"""Pattern base contracts for V3 integration authoring."""

from .base import (
    ArchetypePattern,
    NoParameters,
    PatternBase,
    PatternExample,
    PatternIOContract,
    PatternKind,
    PatternMetadata,
    PrimitiveBuildContext,
    PrimitivePattern,
)
from .errors import (
    PatternError,
    PatternFieldError,
    pattern_validation_error,
)
from .registry import (
    PatternClass,
    PatternRegistry,
    PatternRegistryError,
)

__all__ = [
    "ArchetypePattern",
    "NoParameters",
    "PatternBase",
    "PatternClass",
    "PatternError",
    "PatternExample",
    "PatternFieldError",
    "PatternIOContract",
    "PatternKind",
    "PatternMetadata",
    "PatternRegistry",
    "PatternRegistryError",
    "PrimitiveBuildContext",
    "PrimitivePattern",
    "pattern_validation_error",
]
