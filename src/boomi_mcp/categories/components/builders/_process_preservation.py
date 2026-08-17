"""The ONE process update-preservation policy (issue #153 / M12.15).

Every builder that emits a Boomi ``process`` component owns exactly the same
subtree — ``bns:object/process`` — and preserves exactly the same siblings. That
fact was hand-written three times in ``process_flow_builder.py`` (the
``ProcessFlowBuilder``, ``WrapperSubprocessBuilder`` and ``SyncPipelineBuilder``
class attributes), and #153's canonical materialization plan needed to record the
same policy a fourth time.

Four hand-copies of one runtime fact is the mechanism this repository's
structural-fix rule names: an unpinned hand-model of a fact whose authority lives
elsewhere. Rather than add the fourth instance and then discover the drift later,
the fact is stated ONCE here and every consumer references it.

**Why sharing a single instance is safe.** Both :class:`PreservationPolicy` and
:class:`OwnedPath` are ``@dataclass(frozen=True)`` and carry only ``str``,
``Optional[str]`` and ``Tuple`` fields — there is no mutable field anywhere in
the graph, so the shared instance cannot be aliased into a surprise by one
consumer mutating it. This was checked before the extraction, not assumed: an
extraction that introduced aliasing would be a silent behaviour change in the
legacy path, which must stay the byte-exact parity oracle until #160.

**What the policy means** (unchanged from the three originals): the builder owns
the entire ``<process>`` subtree — shapes, transitions, everything under it. The
sibling ``<bns:processOverrides>``, which Boomi populates with per-environment
override values through the UI, is deliberately NOT owned, so it survives a
structured update. ``bns:encryptedValues`` and any unknown ``bns:Component``
children are preserved for the same reason. Folder attributes are likewise not
owned: builders emit ``folderName="Home"`` whenever the caller omits a folder,
and owning that attribute would silently move components to Home on every
structured update.
"""

from ._preservation_policy import OwnedPath, PreservationPolicy

#: The path the process builders own, as a named constant so the string itself
#: is not a fourth hand-copy either.
PROCESS_OWNED_PATH = "bns:object/process"

#: The single process preservation policy. Referenced by every process builder
#: and projected onto the #153 materialization plan, so a change here reaches
#: every consumer at once instead of leaving three of four in step.
PROCESS_PRESERVATION_POLICY = PreservationPolicy(
    component_type="process",
    owned_paths=(OwnedPath(path=PROCESS_OWNED_PATH),),
)

__all__ = ["PROCESS_OWNED_PATH", "PROCESS_PRESERVATION_POLICY"]
