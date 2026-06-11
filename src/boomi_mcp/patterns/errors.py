"""Error contracts for the patterns package."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, ValidationError

from ..errors import PARAM_VALIDATION_FAILED


class PatternFieldError(BaseModel):
    """Single field-level validation error sanitized for MCP responses."""

    field_path: str = Field(..., description="Dotted path to the offending field")
    message: str = Field(..., description="Human-readable validation message")
    error_type: Optional[str] = Field(default=None, description="Pydantic error type tag")


class PatternError(BaseModel):
    """Structured error envelope returned by future pattern MCP tools."""

    error_code: str = Field(..., description="Stable machine-readable error code")
    error: str = Field(..., description="Human-readable error summary")
    suggestion: Optional[str] = Field(default=None, description="Optional remediation hint")
    retryable: bool = Field(default=False, description="Whether retrying with same input could succeed")
    context: Dict[str, Any] = Field(default_factory=dict, description="Optional structured context")
    field_errors: List[PatternFieldError] = Field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        # ``exclude_none=True`` keeps the MCP response shape compact for LLM
        # clients: optional fields like ``suggestion`` and per-field
        # ``error_type`` are dropped when not set.
        return {"_success": False, **self.model_dump(exclude_none=True)}


def pattern_validation_error(
    exc: ValidationError, *, suggestion: Optional[str] = None
) -> PatternError:
    # Strip the raw ``input`` field from every Pydantic error: it can echo back
    # caller-supplied credentials (API keys, passwords) when a primitive wraps a
    # connector. Only the structural fields are propagated.
    field_errors: List[PatternFieldError] = []
    for err in exc.errors():
        field_path = ".".join(str(part) for part in err.get("loc", ()))
        field_errors.append(
            PatternFieldError(
                field_path=field_path,
                message=err.get("msg", ""),
                error_type=err.get("type"),
            )
        )
    return PatternError(
        error_code=PARAM_VALIDATION_FAILED,
        error="Parameter validation failed",
        suggestion=suggestion,
        retryable=False,
        context={},
        field_errors=field_errors,
    )
