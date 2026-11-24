"""Built-in stub recipe: a Stop-only process and zero supporting components (#159 S2).

The typed successor of the legacy ``stub_minimal_integration`` archetype, a
framework smoke test. The legacy archetype emitted a zero-component spec with
two display-only stub endpoints and one no-op flow; this recipe emits ONE
process unit whose root is the grammar's no-connector entry followed by a Stop
(``passthrough -> stop``; a bare ``stop`` does not parse), and nothing else: no
supporting component, no component material, no connection, no capability
claim beyond the process itself.

**Nothing in this module can reach a credential.** The input carries a process
key and names, and the recipe declares no component-material channel, so any
material sent to it is refused by the authoring workflow.
"""

from __future__ import annotations

from typing import Literal, Optional, Tuple

from pydantic import Field

from ...models.process_component import (
    ProcessAuthoringUnitV1,
    ProcessComponentEnvelopeV1,
)
from ...models.process_ir import ProcessIRV1
from ...models.recipe_contributions import (
    ConstraintRequirementV1,
    ProcessUnitContributionV1,
    RecipeComponentKey,
    RequireProcessV1,
)
from ..contracts import RecipeInputBase

_VERSION = "1"

#: The legacy archetype's ``component_prefix`` default.
_DEFAULT_PREFIX = "STUB"
#: The process name suffix when no ``process_name`` is given.
_PROCESS_NAME_SUFFIX = "Stub No-op Process"


class StubMinimalNamingV1(RecipeInputBase):
    component_prefix: str = Field(default=_DEFAULT_PREFIX, min_length=1)
    folder_name: Optional[str] = Field(default=None, min_length=1)
    process_name: Optional[str] = Field(default=None, min_length=1)
    description: str = Field(
        default="",
        description="The description of the process component this recipe creates.",
    )


class StubMinimalInputV1(RecipeInputBase):
    """The stub's whole input: a process key and names.

    The legacy display fields (``goal``, ``source_label``, ``target_label``)
    are not inputs: the served stub endpoints carry fixed labels, so a label
    this recipe accepted could not be served.
    """

    version: Literal["1"]
    process_key: RecipeComponentKey
    naming: StubMinimalNamingV1 = Field(default_factory=StubMinimalNamingV1)


def _root() -> ProcessIRV1:
    return ProcessIRV1.model_validate(
        {
            "version": "1",
            "body": {"kind": "sequence", "steps": [{"kind": "passthrough"}, {"kind": "stop"}]},
        }
    )


def emit_stub_minimal(inp: StubMinimalInputV1) -> Tuple[object, ...]:
    """The PURE executor: one Stop-only process unit, depending on nothing,
    and the requirement that the process exists."""
    naming = inp.naming
    envelope = ProcessComponentEnvelopeV1(
        component_key=inp.process_key,
        name=naming.process_name or f"{naming.component_prefix} {_PROCESS_NAME_SUFFIX}",
        action="create",
        description=naming.description,
        folder_name=naming.folder_name,
        depends_on=(),
    )
    return (
        ProcessUnitContributionV1(
            contribution_kind="process_unit",
            version=_VERSION,
            contribution_id="unit.process",
            unit=ProcessAuthoringUnitV1(envelope=envelope, process_ir=_root()),
        ),
        ConstraintRequirementV1(
            contribution_kind="constraint_requirement",
            version=_VERSION,
            requirement_id="req.process",
            requirement=RequireProcessV1(kind="process", process_key=inp.process_key),
        ),
    )


__all__ = [
    "StubMinimalInputV1",
    "StubMinimalNamingV1",
    "emit_stub_minimal",
]
