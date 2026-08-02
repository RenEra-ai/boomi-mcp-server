"""Aliases with forward references, in a REAL module.

Two constraints shape this file:

* ``exec``-built aliases get ``__module__ = None`` and resolve for the wrong
  reason, so the namespace lookup is never exercised — the alias has to live in a
  real module (issue #145, live QA).
* PEP 695 ``type X = ...`` syntax is a **SyntaxError on Python 3.11**, which both
  Docker stages use, so the aliases are built with
  ``typing_extensions.TypeAliasType`` instead. That is the same object the walk
  recognises — the check is on the type name, which both the ``typing`` and
  ``typing_extensions`` spellings share (issue #145, Codex review).
"""

from typing import List, TypeVar

from pydantic import ConfigDict, model_validator
from typing_extensions import TypeAliasType

from boomi_mcp.recipes import RecipeInputBase

_CONFIG = ConfigDict(extra="forbid", frozen=True)


class AliasLeaf(RecipeInputBase):
    """Converts itself to a dict of only DECLARED keys, so no extra-key check sees it."""

    model_config = _CONFIG
    ok: str = "x"

    @model_validator(mode="before")
    @classmethod
    def _keep_declared(cls, data):
        if isinstance(data, dict):
            return {k: v for k, v in data.items() if k in cls.model_fields}
        return data

    @model_validator(mode="after")
    def _hand_it_back(self):
        return {"ok": self.ok}


#: The forward reference is the point — it must resolve in THIS module's namespace.
ForwardAlias = TypeAliasType("ForwardAlias", List["AliasLeaf"])

_T = TypeVar("_T")
ParameterisedAlias = TypeAliasType(
    "ParameterisedAlias", List[_T], type_params=(_T,)
)


class ForwardAliasInputV1(RecipeInputBase):
    model_config = _CONFIG
    field: ForwardAlias = []


class SubscriptedAliasInputV1(RecipeInputBase):
    model_config = _CONFIG
    field: ParameterisedAlias[AliasLeaf] = []
