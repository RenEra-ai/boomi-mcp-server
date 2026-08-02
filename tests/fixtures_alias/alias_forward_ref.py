"""Aliases with forward references, in a REAL module.

`exec`-built aliases get ``__module__ = None`` and resolve for the wrong reason,
which is how a fixture can produce the expected verdict without exercising the
namespace lookup at all (issue #145, live QA).
"""

from pydantic import ConfigDict, model_validator

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


type ForwardAlias = list['AliasLeaf']
type ParameterisedAlias[T] = list[T]


class ForwardAliasInputV1(RecipeInputBase):
    model_config = _CONFIG
    field: ForwardAlias = []


class SubscriptedAliasInputV1(RecipeInputBase):
    model_config = _CONFIG
    field: ParameterisedAlias[AliasLeaf] = []
