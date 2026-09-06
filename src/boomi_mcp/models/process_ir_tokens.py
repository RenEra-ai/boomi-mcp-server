"""Platform tokens the ProcessIR models and the compiler must agree on.

ONE home for a fact both layers need. The models cannot import the compiler —
``models`` is the authoring surface and ``compiler.process_ir`` imports it, so
the dependency runs one way only — and neither may import the legacy builder
package. Before #156 the caught-error property token therefore existed as two
independent literals: ``compiler.process_ir.contracts.CAUGHT_ERROR_PROPERTY_ID``
and ``process_emitters.rendering._NOTIFY_CAUGHT_ERROR_TOKEN``. Nothing pinned
them to each other; they agreed by inspection.

#156 needs a THIRD reader — ``NotifyNodeV1`` validates that an authored message
template carries the token — and a third hand-copy of a fact whose authority is
the platform is the duplicate-authority defect class ADR-001 §6 removes. This
module is that authority: an import-safe leaf with no dependencies, which both
directions import instead of respelling.

The renderer keeps its own name for the token (it is legacy-facing and its
spelling is part of a frozen golden), but a test pins the two to equality in
both directions, so a divergence is caught rather than merely absent.
"""

#: The platform property that carries a caught error's message on a Try/Catch
#: recovery path. Boomi exposes it as a tracked property, and both the Exception
#: parameter binding and the Notify message substitution key on it.
CAUGHT_ERROR_PROPERTY_ID = "meta.base.catcherrorsmessage"

#: The display name the platform pairs with :data:`CAUGHT_ERROR_PROPERTY_ID`.
CAUGHT_ERROR_PROPERTY_NAME = "Base - Try/Catch Message"

#: The Notify message levels the platform accepts, in the platform's own order.
#:
#: A TUPLE, not a frozenset, because ``Literal[NOTIFY_LEVELS]`` needs a stable
#: order to generate a stable JSON-Schema ``enum`` — and that schema is served.
#: Readers that want set semantics build their own frozenset from it.
#:
#: This is the SECOND fact in this module, and it is here for the reason the
#: structural-fix rule gives rather than by preference. The caught-error token
#: above was one hand-copy of a platform fact; adding a canonical ``level``
#: vocabulary beside the legacy builder's ``_SUPPORTED_NOTIFY_LEVELS`` would have
#: been a second instance of the same (mechanism, runtime-authority) pair — an
#: unpinned hand-model of a Boomi contract. So both consumers now import this
#: instead of spelling the set twice.
NOTIFY_LEVELS = ("INFO", "WARNING", "ERROR")

__all__ = (
    "CAUGHT_ERROR_PROPERTY_ID",
    "CAUGHT_ERROR_PROPERTY_NAME",
    "NOTIFY_LEVELS",
)
