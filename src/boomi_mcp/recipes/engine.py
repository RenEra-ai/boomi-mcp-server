"""The recipe execution funnel (issue #145 M12.10).

Every recipe run walks the SAME twelve steps, and none of them is optional:

 1. resolve the descriptor at an exact version
 2. preflight its declared capability requirements
 3. preflight its declared prerequisites — recipe AND execution-context
 4. pre-scan and strictly validate its input
 5. run ONLY the registered callable
 6. re-validate every returned value as a DECLARED contribution type
 7. run it a second time and byte-compare — nondeterminism is a hard failure
 8. order and compose the closed operations
 9. resolve component slots against the private catalog, verifying headers
10. parse / compile / emit / verify every assembled process
11. plan every assembled topology
12. evaluate every declared constraint

The artifacts then go back to the caller. Mutation still goes through
``_build_plan`` / ``_apply_plan``, which this module never calls — those are not
steps of the funnel, they are what happens after it.

**``validation_policy`` is hard-coded ``None`` at the compile call site and there
is no parameter anywhere in this module that could carry one.** That is what makes
"a recipe cannot bypass semantic validation" a structural fact instead of a
convention: the legacy exemptions exist and are reachable, but not from here, and
a test asserts the signature has no seam to add one through.

Step 7 is why executors get nothing but their frozen input. A recipe that read a
clock, an environment variable, or a mutable module global would produce different
bytes on the second run and fail closed — but only if there is no legitimate
channel through which state could differ. Handing an executor a context object
would have opened exactly that channel.
"""

from __future__ import annotations

import array
import sys
from collections import OrderedDict, deque
from dataclasses import dataclass, fields as dataclass_fields, is_dataclass
from decimal import Decimal
from fractions import Fraction
from types import GetSetDescriptorType
from typing import (
    Annotated,
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from typing import NewType, _eval_type  # noqa: PLC2701 — resolving a ForwardRef

from pydantic import BaseModel, TypeAdapter

# ``typing.is_typeddict`` returns False for a ``typing_extensions.TypedDict``
# subclass, and the r42 census recorded BOTH spellings as usable field types — so
# the ``typing`` import judged one and left the other unexamined. The
# ``typing_extensions`` version recognises both (issue #145, live QA).
from typing_extensions import is_typeddict

from ..errors import (
    RECIPE_CONSTRAINT_FAILED,
    RECIPE_REQUEST_INVALID,
    RECIPE_CONTRIBUTION_INVALID,
    RECIPE_INPUT_INVALID,
    RECIPE_OUTPUT_NONDETERMINISTIC,
)
from ..models.integration_models import IntegrationComponentSpec
from ..models.recipe_contributions import (
    RecipeContributionValidationError,
    canonical_recipe_contributions_json,
    scan_forbidden_recipe_shape,
    validate_contribution_object,
)
from .composer import (
    AttributedContributionV1,
    ComposedContributionsV1,
    RecipeInvocationV1,
    compose,
    order_invocations,
)
from .contracts import RecipeDescriptorV1
from .errors import RecipeError, recipe_diagnostic, recipe_error
from .materialization import (
    MaterializationCatalog,
    build_symbol_table,
    placeholder_component_id,
)
from .registry import RecipeRegistry, production_registry


@dataclass(frozen=True)
class RecipeRequestV1:
    """One requested recipe run."""

    recipe_id: str
    invocation_id: str
    raw_input: Mapping[str, Any]
    recipe_version: Optional[str] = None


@dataclass(frozen=True)
class RecipeRunResultV1:
    """Everything a validated recipe run produced."""

    composed: ComposedContributionsV1
    components: Tuple[IntegrationComponentSpec, ...]
    process_artifacts: Tuple[Tuple[str, Any], ...]
    topology_plans: Tuple[Tuple[str, Any], ...]
    provenance: Mapping[str, Any]

    def artifact_for(self, process_key: str) -> Any:
        for key, artifact in self.process_artifacts:
            if key == process_key:
                return artifact
        return None


def _validate_input(descriptor: RecipeDescriptorV1, registry: RecipeRegistry, raw: Mapping[str, Any]):
    """Forbidden-shape scan, THEN the strict model.

    The scan runs first so a credential is rejected as a security failure with a
    value-free diagnostic — not as a pydantic schema error whose message could
    echo the offending value into a log.
    """
    if not isinstance(raw, Mapping):
        raise recipe_error(
            RECIPE_INPUT_INVALID,
            phase="input",
            recipe_ids=(descriptor.recipe_id,),
            recipe_versions=(descriptor.recipe_version,),
        )
    found = scan_forbidden_recipe_shape(dict(raw))
    if found is not None:
        path, _reason = found
        raise recipe_error(
            RECIPE_INPUT_INVALID,
            phase="input",
            path=_pointer(path),
            recipe_ids=(descriptor.recipe_id,),
            recipe_versions=(descriptor.recipe_version,),
        )
    model = registry.input_model_for(descriptor)
    try:
        validated = model.model_validate(dict(raw))
    except Exception:  # noqa: BLE001 — pydantic text can echo an authored value
        raise recipe_error(
            RECIPE_INPUT_INVALID,
            phase="input",
            recipe_ids=(descriptor.recipe_id,),
            recipe_versions=(descriptor.recipe_version,),
        ) from None

    # INSPECT WHAT CAME BACK, rather than classify what ran.
    #
    # Both registration gates read the compiled schema, and the schema faithfully
    # records that a validator runs at a position — but never what it RETURNS,
    # which is a fact about arbitrary Python. ``model_validator(mode="after")``
    # receives the constructed model and its return value IS the result, so it
    # can hand back anything; combined with a ``mode="before"`` that stashes the
    # raw mapping, a model that is genuinely frozen, closed and free of every
    # banned node still delivered the caller's undeclared keys — ``smuggled``,
    # ``password`` — to the executor as a plain dict.
    #
    # Widening the node-type ban is not the fix: ``after`` cannot be banned (a
    # production input model uses it, legitimately, and to enforce a cross-field
    # rule), and each ban is one more classification that the next shape gets to
    # be one node short of. This check is mechanism-independent and closes the
    # class, including the wrap/plain form the ban already covers.
    #
    # EXACT TYPE, not ``isinstance``: a subclass declaring ``extra="allow"``,
    # populated via ``model_construct`` to skip revalidation, satisfies
    # ``isinstance`` while carrying the smuggled keys (issue #145, live QA).
    if type(validated) is not model:
        raise recipe_error(
            RECIPE_INPUT_INVALID,
            phase="input",
            recipe_ids=(descriptor.recipe_id,),
            recipe_versions=(descriptor.recipe_version,),
        )

    # The root check alone is not the boundary — it is only its outermost layer.
    # A ``mode="after"`` validator on a NESTED model, or a ``field_validator``,
    # replaces the value at ITS position and pydantic does not re-check it, so the
    # registered outer type survives intact while a declared field holds the
    # caller's stashed mapping. That lands on the surfaces an ordinary executor
    # reads: ``inp.some_field`` returns it directly.
    #
    # This check uses the ENGINE'S OWN adapters, and that is the whole point.
    #
    # An earlier version asked ``model_dump(warnings="error")`` — delegating the
    # question to the model's serializer. Live QA then defeated it three ways, and
    # the third is the one that settles the design: assigning
    # ``__pydantic_serializer__`` (or ``__pydantic_core_schema__``) AFTER class
    # creation defeats every registration-time gate, because registration runs
    # later and those attributes are plain writable class attributes. Four checks
    # in this issue were retired for the same reason — ``model_config["extra"]``,
    # ``ValidationError.from_exception_data``, ``function.__module__``, and the
    # serializer ban itself. **The class of "read an attribute of the author's
    # class" is exhausted; every one of them is writable.** What is not writable
    # is the value the caller's data actually landed in, checked by an adapter the
    # engine builds. Nothing an input model declares can reach into that.
    #
    # It is checked against ``field.annotation`` WITHOUT its constraint metadata,
    # deliberately: type confusion is the threat here, constraint re-checking is
    # covered by later gates (§12), and re-attaching metadata reintroduces the
    # ``Json[...]`` misfire where the stored value is the parsed object rather than
    # the JSON string the annotation expects (issue #145, live QA).
    try:
        _assert_declared_shape(validated)
    except Exception:  # noqa: BLE001 — the message can echo the offending value
        raise recipe_error(
            RECIPE_INPUT_INVALID,
            phase="input",
            recipe_ids=(descriptor.recipe_id,),
            recipe_versions=(descriptor.recipe_version,),
        ) from None
    return validated


#: Adapters the ENGINE built, keyed by annotation. Never an adapter the input
#: model supplied, and never one derived from a class attribute an author can
#: reassign after class creation.
_DECLARED_SHAPE_ADAPTERS: Dict[Any, Any] = {}


def _declared_shape_adapter(annotation: Any) -> Any:
    try:
        cached = _DECLARED_SHAPE_ADAPTERS.get(annotation)
    except TypeError:  # an unhashable annotation cannot be cached; build each time
        return TypeAdapter(annotation)
    if cached is None:
        cached = TypeAdapter(annotation)
        _DECLARED_SHAPE_ADAPTERS[annotation] = cached
    return cached


#: Both union spellings. ``get_origin(Leaf | str)`` is ``types.UnionType``, NOT
#: ``typing.Union`` — so an earlier version flattened ``Union[Leaf, str]`` and left
#: ``Leaf | str`` as a single opaque option, emptying the class list and disabling
#: the runtime-class check under the ordinary modern spelling.
#:
#: The two are ``==`` and hash-equal, which made it worse than a miss: the memo
#: below returned whichever spelling was computed FIRST for both, so a model
#: written with ``Optional[SecretStr]`` lost its class check whenever some other
#: model in the process used ``SecretStr | None`` earlier. Import-order-dependent
#: security behaviour. Flattening both to the same result is what makes the shared
#: cache entry correct rather than merely consistent (issue #145, live QA).
_UNION_ORIGINS: Tuple[Any, ...]
try:  # pragma: no cover - the fallback is for Python < 3.10
    from types import UnionType as _PEP604Union

    _UNION_ORIGINS = (Union, _PEP604Union)
except ImportError:  # pragma: no cover
    _UNION_ORIGINS = (Union,)


#: Annotation introspection is pure and repeats for every value at a position,
#: so it is memoised. Unhashable annotations fall through and are recomputed.
_UNWRAP_CACHE: Dict[Any, Tuple[Any, ...]] = {}


def _cached(cache: Dict[Any, Any], key: Any, build):
    try:
        hit = cache.get(key, _MISSING)
    except TypeError:
        return build()
    if hit is _MISSING:
        hit = build()
        cache[key] = hit
    return hit


_MISSING = object()


def _resolved_alias_value(alias: Any) -> Any:
    """An alias's ``__value__``, with its forward references resolved.

    Unwrapping alone was a REGRESSION: it exposed the raw ``__value__``, so a
    ``ForwardRef`` inside ``type X = list['Leaf']`` reached an adapter with no
    namespace and raised ``PydanticUserError: not fully defined`` — refusing every
    invocation of an otherwise valid recipe. Closing one miss opened a false
    rejection of exactly the shape the ``Any`` blocker had (issue #145, live QA).

    The alias records the module it was written in, which is the namespace its own
    forward references were written against.
    """
    value = getattr(alias, "__value__", _MISSING)
    if value is _MISSING:
        return alias
    namespace = getattr(sys.modules.get(getattr(alias, "__module__", ""), None), "__dict__", {})
    try:
        return _eval_type(value, namespace, namespace)
    except Exception:  # noqa: BLE001 — see below
        # Returning the raw value does NOT save the walk: an unresolved
        # ``ForwardRef`` inside it still reaches the adapter and raises there.
        # That is acceptable rather than accidental — every case where this fires
        # is one where the annotation is genuinely unresolvable, and refusing an
        # annotation nobody can resolve is the fail-closed answer. Stated because
        # an earlier comment here claimed the fallback avoided raising, which it
        # does not (issue #145, live QA).
        return value


def _unwrap_annotation(annotation: Any) -> Tuple[Any, ...]:
    return _cached(_UNWRAP_CACHE, annotation, lambda: _unwrap_annotation_uncached(annotation))


def _unwrap_annotation_uncached(annotation: Any) -> Tuple[Any, ...]:
    """The concrete options an annotation admits, through ``Annotated`` and unions.

    Deliberately small. This is structural introspection of a DECLARATION — total
    and deterministic for the forms in use — not the "does this validator reach a
    model" question that proved undecidable. Anything it does not recognise is
    returned as-is, and the caller treats unrecognised as "no opinion".
    """
    if get_origin(annotation) is Annotated:
        args = get_args(annotation)
        return _unwrap_annotation(args[0]) if args else ()
    # A form this function does not RECOGNISE used to be returned as its own
    # single option — and then ``_element_candidates`` found no ``get_origin``,
    # returned nothing, and the elements were walked unjudged while the adapter
    # resolved the alias perfectly well. "I do not recognise this" and "there is
    # nothing to check here" were the same value, which is the same defect as a
    # ``None`` element annotation one level up (issue #145, live QA).
    #
    # ``type X = ...`` is the modern spelling and pydantic accepts it as a field
    # annotation; both attributes below are public and documented.
    if type(annotation).__name__ == "TypeAliasType":
        return _unwrap_annotation(_resolved_alias_value(annotation))
    # A SUBSCRIPTED alias — ``A[Leaf]`` where ``type A[T] = list[T]`` — is not a
    # ``TypeAliasType`` itself and carries no ``__value__``; its origin is the
    # alias. Substituting the arguments for the parameters is what makes its
    # elements judgeable at all (issue #145, live QA).
    origin = get_origin(annotation)
    if type(origin).__name__ == "TypeAliasType":
        resolved = _resolved_alias_value(origin)
        parameters = getattr(origin, "__type_params__", ())
        arguments = get_args(annotation)
        if parameters and arguments:
            try:
                resolved = resolved[arguments] if len(arguments) > 1 else resolved[arguments[0]]
            except Exception:  # noqa: BLE001 — leave it unsubstituted rather than guess
                pass
        return _unwrap_annotation(resolved)
    if isinstance(annotation, NewType):
        # GUARDED the same way ``TypeAliasType`` is. An unguarded
        # ``__supertype__`` read unwrapped any object that happened to carry the
        # attribute — including a class that is a perfectly usable field type via
        # ``__get_pydantic_core_schema__``, whose honest instances were then
        # refused (issue #145, live QA).
        return _unwrap_annotation(annotation.__supertype__)

    if get_origin(annotation) in _UNION_ORIGINS:
        options: List[Any] = []
        for arg in get_args(annotation):
            options.extend(_unwrap_annotation(arg))
        return tuple(options)
    return (annotation,)


#: Widenings ``strict=True`` permits where ``isinstance`` is False, enumerated by
#: sweeping twelve scalar annotations against thirteen values. Exempted so the two
#: halves of this gate agree about "declared type" — an earlier version refused an
#: ``int`` literal default in a ``float`` field while the same field passed when a
#: caller supplied the value, which reads as intermittent (issue #145, live QA).
_PERMITTED_WIDENINGS: Dict[type, Tuple[type, ...]] = {
    float: (int, Decimal, Fraction),
    complex: (int, float, Decimal, Fraction),
}


def _assert_runtime_class(value: Any, annotation: Any) -> None:
    """A stored value must BE one of the classes its annotation admits.

    This is where "the adapter accepted it" is misleading. Strict mode still
    CONVERTS: it admits a mapping where a model is declared and then runs that
    model's validators, and it builds a ``SecretStr`` from a raw ``str``. Either
    way conversion succeeds, the adapter's result is discarded, and the stored
    value is not what the field declares.

    Exemptions are ENUMERATED rather than inferred, because two attempts to infer
    them both failed (issue #145, Codex review + live QA):

    * A blanket rule tripped over ``typing.Any`` being a CLASS from Python 3.11 —
      ``isinstance(value, Any)`` raises ``TypeError``, refusing every ``Any``
      field whatever it held, including ``Dict[str, Any]``.
    * Scoping to MODEL positions fixed that but reopened the very bypass this
      check exists for: in ``Union[Leaf, str]`` the ``str`` option made the whole
      check return, discarding the ``Leaf`` already collected, and a dict
      carrying ``smuggled`` reached the executor again.

    So: every class-typed option is checked; ``Any``/``object`` and the numeric
    widenings are named exceptions; and a value matching none of the classes is
    given one last chance against the annotation's NON-class options, which is
    what keeps ``Union[Leaf, Dict[str, str]]`` honest.
    """
    options = _unwrap_annotation(annotation)
    if not options:
        return

    class_options: List[type] = []
    other_options: List[Any] = []
    for option in options:
        if option is Any or option is object:
            return  # admits everything; no opinion to have
        if get_origin(option) is None and isinstance(option, type):
            class_options.append(option)
        else:
            other_options.append(option)

    if not class_options:
        return

    # NOT EVERY CLASS SUPPORTS ``isinstance``. A ``TypedDict`` is a class, passes
    # both registration gates, and pydantic correctly stores a ``dict`` for it —
    # but ``isinstance(value, SomeTypedDict)`` raises ``TypeError``, which
    # ``_validate_input`` turned into ``RECIPE_INPUT_INVALID``, failing EVERY
    # invocation of an otherwise valid recipe.
    #
    # This is the second time a "class" that cannot be instance-checked broke the
    # check — ``typing.Any`` was the first — so the guard is general rather than a
    # ``TypedDict`` special case. If any option cannot be judged, the whole
    # annotation yields no opinion and the adapter above is the check: dropping
    # only the unusable option would false-reject a legitimate value in a mixed
    # union like ``Union[Leaf, SomeTypedDict]`` (issue #145, Codex review).
    verdicts = []
    for cls in class_options:
        try:
            verdicts.append(isinstance(value, cls))
        except Exception:  # noqa: BLE001 — see below
            # ANY failure, not just ``TypeError``. ``__instancecheck__`` is
            # arbitrary user code and a class carrying
            # ``__get_pydantic_core_schema__`` is a legal field type, so a
            # metaclass raising ``ValueError`` reached this and refused every
            # invocation. Enumerating the non-checkable forms is open-ended;
            # attempting the check and catching is the only way to confirm one
            # positively (issue #145, live QA).
            #
            # It abstains for THAT OPTION ONLY, by handing it to the adapter loop
            # below. Returning outright suppressed the whole union: in
            # ``Union[SecretStr, SomeTypedDict]`` the uncheckable ``TypedDict``
            # cancelled the FAILED ``SecretStr`` check and a raw secret string
            # reached the executor. The adapter can still say whether the value
            # is plausibly that option — a ``str`` is not a ``TypedDict``
            # (issue #145, Codex review).
            other_options.append(cls)
    if any(verdicts):
        return
    for cls in class_options:
        if isinstance(value, _PERMITTED_WIDENINGS.get(cls, ())):
            return

    # A mixed union: the value may legitimately be one of the parametrised
    # options. Judged with the ENGINE'S adapter, never the model's.
    for option in other_options:
        try:
            _declared_shape_adapter(option).validate_python(value, strict=True)
            return
        except Exception:  # noqa: BLE001 — this option simply does not match
            continue

    raise ValueError("validated input holds a value of an undeclared class")


def _dataclass_field_types(cls: Any) -> Dict[str, Any]:
    """Resolved field types, or a refusal.

    An earlier version fell back to ``{name: None}``, which walked every field of
    that dataclass with NO annotation — fail-open, and reachable without exotica:
    ``from __future__ import annotations`` plus a ``TYPE_CHECKING``-only import
    leaves ``get_type_hints`` unable to resolve. A pydantic dataclass cannot get
    here (pydantic refuses to build one), but a stdlib dataclass can, and the walk
    handles both. "Learned nothing" fails closed everywhere else in this contract
    (issue #145, live QA).
    """
    try:
        return dict(get_type_hints(cls))
    except Exception:  # noqa: BLE001 — see above
        raise ValueError(
            "validated input holds a dataclass whose field types cannot be resolved"
        )


def _element_candidates(
    annotation: Any, value: Any
) -> Tuple[Tuple[Tuple[Any, Tuple[Any, ...]], ...], bool]:
    """``(origin, args)`` per parametrised arm, and whether any arm MATCHED.

    Returns ``(arms, matched)``. When ``matched`` is true the arms are the ones
    whose origin the value is an instance of; when it is false they are every
    parametrised option. Either way the CALLER decides what its own shape can
    make of them — which is why the ORIGIN travels with the arguments rather
    than being discarded here.

    Discarding it was the defect. A sequence may read arguments POSITIONALLY
    only for a real ``tuple``; for anything else two arguments have no defined
    positional meaning. Handing bare argument tuples to the caller left it
    unable to tell the two apart, so a list-backed ``SecondItems[str, SecretStr]``
    had index 0 checked against ``str`` while its schema used the SECOND
    argument, leaving an unwrapped secret in the field (issue #145, Codex
    review). Returning nothing at all would instead be the "no arm matched means
    nothing to say" conflation this layer kept re-learning.
    """
    candidates = []
    parametrised = []
    for option in _unwrap_annotation(annotation):
        origin = get_origin(option)
        args = get_args(option)
        if origin is None or not args:
            continue
        parametrised.append((origin, args))
        try:
            if isinstance(value, origin):
                candidates.append((origin, args))
        except Exception:  # noqa: BLE001 — a non-class origin, or a metaclass
            # whose ``__instancecheck__`` raises. Catching only ``TypeError``
            # left a legal parametrised custom generic refused on every
            # invocation, the same false rejection ``_assert_runtime_class``
            # was already fixed for (issue #145, Codex review).
            continue
    if candidates:
        return tuple(candidates), True
    return tuple(parametrised), False


def _sequence_arms(annotation: Any, value: Any) -> Tuple[Tuple[Any, ...], ...]:
    """Arms a SEQUENCE can use, or a refusal when their meaning is underivable.

    An arm is usable only where its arguments have a derivable element meaning:

    * ONE argument — every element is that type, whatever the container is;
    * ``Tuple[X, ...]`` — the same uniform statement, spelled with an ellipsis;
    * a MATCHED ``tuple`` — the only origin whose arguments really are positions.

    ``matched`` gates the third clause and nothing else. Reading an *unmatched*
    ``Tuple[str, SecretStr]`` arm positionally would index into a value that arm
    does not describe — one arm's answer used for another arm's value.

    The first version of this rule guarded only the unmatched path, on the
    assumption that a matched arm is self-evidently applicable. It is not: a
    generic that SUBCLASSES its backing list is an instance of its own origin,
    so it matched, skipped the rule entirely, and was read positionally anyway —
    accepting the plaintext secret this rule exists to refuse while rejecting the
    correctly wrapped value (issue #145, live QA #359). A rule with two consumers
    has to be applied at both.
    """
    arms, matched = _element_candidates(annotation, value)
    if not arms:
        return ()
    usable = tuple(
        args
        for origin, args in arms
        if len(args) == 1
        or (len(args) == 2 and args[1] is Ellipsis)
        or (matched and origin is tuple)
    )
    if usable:
        return usable
    raise ValueError("element semantics of this annotation cannot be derived")


def _mapping_arms(annotation: Any, value: Any) -> Tuple[Any, ...]:
    """EVERY arm a mapping can be judged against: TypedDict arms, then pairs.

    A mapping recovers where a sequence cannot: two parameters read as key and
    value, one as the value type — which is what a dict-shaped custom generic
    means by its single parameter. If some generic ever parametrises its KEY
    instead, its honest values are refused rather than accepted, which is the
    wrong-way-safe direction (issue #145, live QA site census).

    Building the two arm sources TOGETHER is load-bearing, and splitting them was
    two defects at once. The refusal below used to live in the pair helper, so it
    fired before the caller could concatenate the ``TypedDict`` arms — and a
    ``Union[Tuple[X, ...], SomeTypedDict]`` was refused on every invocation even
    though the ``TypedDict`` arm described the value exactly (live QA #357). It
    was also suppressed whenever an arm matched by container, so a matched
    3-parameter dict subclass yielded no pairs, raised nothing, and had every
    entry walked unjudged (live QA #360). "I cannot read this arm" does not
    depend on whether the arm matched; it depends on whether anything usable came
    out of the whole annotation.
    """
    typed = _typed_dict_arms(annotation)
    arms, _matched = _element_candidates(annotation, value)
    pairs = []
    for _origin, element in arms:
        if len(element) == 2 and element[1] is not Ellipsis:
            pairs.append((element[0], element[1]))
        elif len(element) == 1:
            pairs.append((None, element[0]))
    if arms and not pairs and not typed:
        raise ValueError("mapping semantics of this annotation cannot be derived")
    return typed + tuple(pairs)


def _assert_one_arm_covers(
    walk: Any, arms: Sequence[Any], _seen: set, _journal: Optional[List[int]] = None
) -> None:
    """ONE arm must describe the WHOLE container, not one arm per element.

    Judging elements independently let a heterogeneous ``[A(), B()]`` satisfy
    ``Union[List[A], List[B]]`` — element 0 chose the ``A`` arm and element 1 the
    ``B`` arm, so the stored value matched neither declared arm while both the
    adapter (which converts, and discards the conversion) and the walk accepted
    it (issue #145, Codex review).

    ``walk`` is called with one arm and must raise if that arm does not cover the
    container. Failed arms roll back by journal, so a partial walk cannot leave
    nodes marked and let a later arm short-circuit past them.
    """
    if not arms:
        # The enclosing journal MUST travel: a nested container with no arms of
        # its own, walked inside an outer trial that later fails, would otherwise
        # leave its marks in the shared cycle guard unrecorded — and the next
        # outer arm would skip those nodes (issue #145, Codex review).
        walk(None, _journal)
        return
    for arm in arms:
        journal: List[int] = []
        try:
            walk(arm, journal)
        except Exception:  # noqa: BLE001 — this arm does not cover the container
            for marker in journal:
                _seen.discard(marker)
            continue
        if _journal is not None:
            _journal.extend(journal)
        return
    raise ValueError("validated input matches no declared arm of its annotation")


def _positional(element: Optional[Tuple[Any, ...]], index: int) -> Any:
    """The annotation for one position of a sequence.

    The final branch — reading argument ``index`` — is reachable only for an arm
    ``_sequence_arms`` certified as genuinely positional, which today means a
    MATCHED ``tuple``. Every other multi-argument arm is refused there rather
    than indexed here (issue #145, live QA #359).
    """
    if not element:
        return None
    if len(element) == 2 and element[1] is Ellipsis:
        return element[0]
    if len(element) == 1:
        return element[0]
    return element[index] if index < len(element) else None


def _typed_dict_arms(annotation: Any) -> Tuple[Dict[str, Any], ...]:
    """Per-KEY annotations for EVERY ``TypedDict`` option, not just the first.

    A ``TypedDict`` has no ``get_origin``, so without this it is judged by nothing
    at all — the class check abstains because it cannot be instance-checked, and
    no parametrised arm matches.

    Returning only the first option's hints was its own leak: in
    ``Union[MetadataTD, SecretTD]`` a value matching the SECOND arm was measured
    against the first, its keys were absent from those hints, and it was walked
    unannotated (issue #145, Codex review).
    """
    arms = []
    for option in _unwrap_annotation(annotation):
        if is_typeddict(option):
            try:
                arms.append(dict(get_type_hints(option)))
            except Exception:  # noqa: BLE001 — unresolvable hints fail closed
                raise ValueError(
                    "validated input declares a TypedDict whose hints cannot be resolved"
                )
    return tuple(arms)


#: The most elements the walk will enumerate in one container.
#:
#: Removing an eager ``list(value)`` stopped ``range(10**9)`` allocating a billion
#: entries — and left it WALKING a billion instead, which ends the process just as
#: surely, only slower. Bounded work is the actual requirement; not copying was
#: only half of it.
#:
#: Over the limit the container is REFUSED, not truncated: walking a prefix and
#: accepting the rest unexamined is the abstention-read-as-permission mistake this
#: layer spent a dozen findings removing. The bound is far above any legitimate
#: recipe input — the largest production container holds a handful of component
#: slots — so it is a guard against a manufactured value, not a real limit
#: (issue #145, Codex review).
_MAX_WALKED_ELEMENTS = 10_000

#: Types whose iteration is KNOWN to be independent and bounded. Enumerated, not
#: inferred: ``abc.Collection`` guarantees only ``__len__``, ``__contains__`` and
#: ``__iter__``, and none of those promises a fresh iterator — a custom collection
#: returning one shared internal iterator passes ``iter(v) is not v`` and is
#: DRAINED by the walk before the executor sees it, and a finite ``__len__`` does
#: not stop ``__iter__`` yielding forever. An interface that does not guarantee
#: replayability is not proof of it (issue #145, Codex review).
_REPLAYABLE_TYPES: Tuple[type, ...] = (
    list,
    tuple,
    set,
    frozenset,
    deque,
    range,
    array.array,
    memoryview,
    type({}.keys()),
    type({}.values()),
    type({}.items()),
)


#: Mapping types whose enumeration is KNOWN safe, same rationale as above.
#: ``OrderedDict`` is here rather than treated as a suspicious ``dict`` subclass
#: because it IS an enumerated known type — refusing it was a false rejection
#: with nothing bought (issue #145, live QA #365).
_REPLAYABLE_MAPPINGS: Tuple[type, ...] = (dict, OrderedDict)

#: What the walk reads, and what an ordinary reader of the same value reads.
#: Split by SHAPE. A single combined list refused a ``NamedTuple`` carrying a
#: field called ``keys`` — on ordinary caller input, with no validator involved —
#: because the generated field descriptor is not ``tuple``'s ``keys``, of which
#: there is none. The sequence walk never calls ``.keys()``, so nothing was
#: bought for it (issue #145, live QA #366).
#: Hooks that supply an accessor WITHOUT appearing under its name. A class-level
#: ``__getattribute__`` answering ``values``/``get`` passed a scan that only
#: looked for those names, and the payload it returned was read by neither the
#: adapter nor the walk — so it was not even confined to shapes the declared type
#: would accept (issue #145, live QA #368). ``__missing__`` is here for the same
#: reason one level along: it MANUFACTURES a value on subscript of an absent key,
#: from a factory the author supplies. That is what refuses ``defaultdict``,
#: which is a correct refusal and not a casualty.
_ATTRIBUTE_HOOKS = ("__getattribute__", "__getattr__", "__missing__")

_SEQUENCE_ENUMERATION = ("__iter__", "__len__", "__getitem__") + _ATTRIBUTE_HOOKS
_MAPPING_ENUMERATION = (
    "__iter__",
    "__len__",
    "__getitem__",
    "items",
    "keys",
    "values",
    "get",
) + _ATTRIBUTE_HOOKS

#: Read the class's OWN storage, bypassing any ``__getattribute__`` the author
#: put on its metaclass. ``getattr(cls, name)`` is answerable by author code;
#: these descriptors are fetched from ``type``, which is not.
_CLASS_VARS = type.__dict__["__dict__"].__get__
_CLASS_MRO = type.__dict__["__mro__"].__get__


def _instance_vars(value: Any) -> Optional[Mapping[str, Any]]:
    """The instance ``__dict__``, or ``None`` if the class redefined how it is READ.

    ``object.__getattribute__(value, "__dict__")`` bypasses a class-level
    ``__getattribute__`` but still performs descriptor lookup — so a ``__dict__``
    PROPERTY in the class body is a data descriptor that wins and hands back an
    empty mapping, while the real instance dict is untouched and its shadowed
    accessor stays live for every reader (issue #145, live QA #369).

    Naming ``__dict__`` in the checked names is NOT the remedy: ``type.__new__``
    puts a ``__dict__`` getset descriptor in every subclass without ``__slots__``,
    so that would refuse ~200 ordinary classes. The descriptor's TYPE is what
    discriminates — compiler-generated, or author-supplied — which is the same
    "read something the author does not own" move ``_CLASS_VARS`` already makes,
    one level down.
    """
    for klass in _CLASS_MRO(type(value)):
        descriptor = _CLASS_VARS(klass).get("__dict__")
        if descriptor is None:
            continue
        if type(descriptor) is not GetSetDescriptorType:
            return None
        try:
            return descriptor.__get__(value)
        except Exception:  # noqa: BLE001 — an unreadable dict is not a readable one
            return None
    return {}  # ``__slots__`` with no instance dict: nothing can be shadowed


def _redefines_enumeration(value: Any, bases: Tuple[type, ...], names: Tuple[str, ...]) -> bool:
    """True when ``value``'s type, or ``value`` itself, rewrote how it enumerates.

    ``_REPLAYABLE_TYPES`` enumerates types whose iteration is KNOWN independent
    and bounded, but membership was tested with ``isinstance``, which admits
    subclasses — and a subclass may replace the very method being vouched for. A
    ``list`` subclass with a one-shot ``__iter__`` was drained by the first union
    arm, after which the second walked zero elements and "covered" the container;
    one whose ``__iter__`` disagrees with its own storage let the walk judge clean
    elements while a subscripting reader got the payload (issue #145, live QA
    #358).

    The FIRST attempt at this asked ``getattr(cls, name)`` whether the method was
    still the base's — and that is reading an attribute of the author's class,
    the surface §7 records as exhausted. Three bypasses, all measured: a metaclass
    ``__getattribute__`` answers with ``list.__iter__`` while the ``tp_iter`` slot
    keeps the override; the same lie hides a ``__len__`` that under-reports, so a
    15,000-element value passed a 10,000 bound; and an INSTANCE attribute
    (``self.items = ...``) shadows ``dict.items`` for the ordinary lookup the walk
    performs, needing no metaclass at all (issue #145, live QA #364).

    So the class is read through ``type``'s own descriptors and the instance
    through ``object``'s, and the MRO is scanned to the first known-safe base
    rather than compared method by method.
    """
    cls = type(value)
    for klass in _CLASS_MRO(cls):
        if klass in bases:
            break
        if any(name in _CLASS_VARS(klass) for name in names):
            return True
    instance_vars = _instance_vars(value)
    if instance_vars is None:  # the class redefined how its own dict is READ
        return True
    return any(name in instance_vars for name in names)


def _assert_mapping_enumeration(value: Any) -> None:
    """Refuse a ``dict`` subclass that rewrote how it enumerates itself.

    The sequence walk gets this from ``_is_walkable_collection``; the mapping
    branch cannot, because every ``Mapping`` is walkable there by construction.

    Only ``dict`` subclasses are checked. A non-``dict`` ``Mapping`` has no
    enumerated base to compare against, and refusing every such implementation
    would be a false rejection far wider than the hole it closes.
    """
    if type(value) in _REPLAYABLE_MAPPINGS:
        return
    if isinstance(value, dict) and _redefines_enumeration(
        value, _REPLAYABLE_MAPPINGS, _MAPPING_ENUMERATION
    ):
        raise ValueError("validated input holds a mapping that redefines its enumeration")


def _replayable_base(value: Any) -> Optional[type]:
    """The enumerated type whose implementation the walk reads THROUGH.

    Reading through the base is what makes the walk's view unforgeable: a
    subclass override, or an instance attribute shadowing one, cannot change what
    ``list.__iter__`` or ``dict.items`` return for the object's real storage.

    DELIBERATELY REDUNDANT with ``_redefines_enumeration``, and the redundancy is
    not dead code. The two answer different readers: reading through the base
    fixes what the WALK sees, while refusing a redefinition is what keeps an
    ordinary EXECUTOR from seeing something the walk never judged. Mutating either
    one alone therefore survives the suite — each is masked by the other — and
    only the PAIRED mutants (``P1``-``P3`` in the round's battery) are killed.
    Every enumeration of forms in this layer has eventually proved incomplete, so
    the second mechanism stays as the bound on the first one's next gap.
    """
    for base in _REPLAYABLE_TYPES:
        if isinstance(value, base):
            return base
    return None


def _walkable_length(value: Any) -> Optional[int]:
    """``len`` read through the base, so an overriding ``__len__`` cannot lie."""
    base = dict if isinstance(value, dict) else _replayable_base(value)
    try:
        if base is not None:
            return base.__len__(value)  # type: ignore[attr-defined]
        return len(value)
    except Exception:  # noqa: BLE001 — unsized values never reach here
        return None


def _assert_walkable_size(value: Any) -> None:
    """Refuse a container too large to enumerate, rather than truncating it."""
    size = _walkable_length(value)
    if size is not None and size > _MAX_WALKED_ELEMENTS:
        raise ValueError("validated input holds a container too large to inspect")


def _is_walkable_collection(value: Any) -> bool:
    """Known-replayable, so the walk can enumerate it without destroying it."""
    if isinstance(value, (str, bytes, bytearray)):
        return False
    if isinstance(value, Mapping):
        return True
    if type(value) in _REPLAYABLE_TYPES:
        return True
    return isinstance(value, _REPLAYABLE_TYPES) and not _redefines_enumeration(
        value, _REPLAYABLE_TYPES, _SEQUENCE_ENUMERATION
    )


def _is_opaque_iterable(value: Any) -> bool:
    if isinstance(value, (str, bytes, bytearray)):
        return False
    if _is_walkable_collection(value):
        return False
    return hasattr(value, "__iter__") or hasattr(value, "__next__")


def _assert_declared_shape(
    value: Any,
    annotation: Any = None,
    _seen: Optional[set] = None,
    _journal: Optional[List[int]] = None,
) -> None:
    """Every value in the tree is what its declaration says, checked value-first.

    VALUE-FIRST, not schema-first, is the load-bearing choice. Walking the model's
    schema means reading something the author can rewrite after the class exists;
    walking the values reads what the caller's data actually landed in.

    Raises anything at all on a mismatch — the caller converts it to a value-free
    diagnostic, because a pydantic message can echo the offending value.
    """
    if _seen is None:
        _seen = set()

    def _mark(node: Any) -> None:
        marker = id(node)
        if marker not in _seen:
            _seen.add(marker)
            if _journal is not None:
                _journal.append(marker)


    if annotation is not None:
        # STRICT, because this runs AFTER validation. Every stored value should
        # already BE its declared type — pydantic did any coercion at
        # ``model_validate`` time — so re-permitting coercion here only re-opens
        # the door: lax mode accepted ``"5"`` for an ``int``, a list for a
        # ``Tuple[str, ...]``, and an ISO string for a ``datetime``.
        #
        # It also stops the check from DAMAGING the value it inspects. Lax mode
        # coerced a generator stored in a ``Tuple[str, ...]`` field into a tuple,
        # reported no mismatch, and left the field holding an exhausted
        # generator — a miss and a destructive side effect in one. Strict refuses
        # it and leaves it unread (issue #145, live QA).
        _declared_shape_adapter(annotation).validate_python(value, strict=True)

        # SUCCESSFUL CONVERSION IS NOT PROOF. Strict mode still admits a mapping
        # where a model is declared, and it then runs that model's validators —
        # so a nested ``after`` returning a dict of only DECLARED keys made the
        # adapter "succeed" and hand back a dict. The adapter's result is
        # discarded, so nothing noticed that the stored value was never a model.
        # What the annotation admits is a fact about the declaration; check it
        # against the runtime class (issue #145, Codex review).
        _assert_runtime_class(value, annotation)

    if id(value) in _seen:
        return

    if isinstance(value, BaseModel):
        _mark(value)
        # Undeclared keys on the INSTANCE. A model whose compiled validator allows
        # extras can be made to look closed at registration by assigning a closed
        # twin's ``__pydantic_core_schema__``; the instance it produces cannot lie
        # about what it is carrying (issue #145, live QA).
        if getattr(value, "__pydantic_extra__", None):
            raise ValueError("validated input carries undeclared keys")
        for name, field in type(value).model_fields.items():
            _assert_declared_shape(getattr(value, name, None), field.annotation, _seen, _journal)
        return

    # A pydantic dataclass is neither a ``BaseModel`` nor a container, so an
    # earlier version walked straight past one holding a model. Its fields carry
    # DECLARED TYPES too — recursing with ``None`` checked nothing, and left a
    # mapping sitting in a ``str`` field reachable at ``inp.holder.label``
    # (issue #145, Codex review).
    if is_dataclass(value) and not isinstance(value, type):
        _mark(value)
        hints = _dataclass_field_types(type(value))
        for field in dataclass_fields(value):
            _assert_declared_shape(
                getattr(value, field.name, None), hints.get(field.name), _seen, _journal
            )
        return

    # RECURSE INTO ANY ELEMENT, not only the ones that are already models. An
    # earlier version tested ``isinstance(item, BaseModel)`` here, which stopped
    # at ONE container level: ``Tuple[Model, ...]`` was covered but
    # ``Dict[str, List[Model]]`` and ``Tuple[Tuple[Model, ...], ...]`` were not,
    # and neither was a model used as a mapping KEY. The elements of a container
    # are not the leaves of the walk (issue #145, live QA).
    #
    # Element ANNOTATIONS are carried down as well. Without them a dict standing
    # in for a ``Leaf`` inside ``Tuple[Leaf, ...]`` was visited but never judged.
    if _is_walkable_collection(value) and not isinstance(value, Mapping):
        _mark(value)
        _assert_walkable_size(value)

        sequence_base = _replayable_base(value)

        def _walk_sequence(arm, journal=None):
            # Iterate the ORIGINAL, THROUGH ITS BASE. ``_REPLAYABLE_TYPES`` exists
            # precisely to guarantee re-iteration is safe, so snapshotting was
            # redundant — and ``range(10**9)`` is a walkable value that a snapshot
            # turns into a billion-element allocation (issue #145, Codex review).
            #
            # Through the base, because ``iter(value)`` reads a slot the author's
            # subclass can replace: what the walk judged then differed from what
            # the executor read. ``list.__iter__(value)`` cannot be redirected
            # (issue #145, live QA #364).
            iterator = (
                sequence_base.__iter__(value)  # type: ignore[union-attr]
                if sequence_base is not None
                else iter(value)
            )
            for index, item in enumerate(iterator):
                element = _positional(arm, index) if arm is not None else None
                if arm is not None and element is None:
                    # This arm is shorter than the value, so it does not describe
                    # it. Previously a ``None`` here meant "no opinion" and the
                    # element was accepted unjudged.
                    raise ValueError("arm does not cover this index")
                _assert_declared_shape(item, element, _seen, journal)

        _assert_one_arm_covers(
            _walk_sequence, _sequence_arms(annotation, value), _seen, _journal
        )
    elif isinstance(value, Mapping):
        _mark(value)
        _assert_mapping_enumeration(value)
        _assert_walkable_size(value)

        def _walk_mapping(arm, journal=None):
            # ``dict.items(value)``, not ``value.items()``: ordinary attribute
            # lookup finds an INSTANCE attribute before the class's method, so
            # ``self.items = ...`` in an author's validator showed the walk clean
            # pairs while the real storage kept the payload — no metaclass needed
            # (issue #145, live QA #364).
            entries = dict.items(value) if isinstance(value, dict) else value.items()
            for key, item in entries:
                if isinstance(arm, dict):  # per-key hints from a TypedDict arm
                    if key not in arm:
                        raise ValueError("arm does not declare this key")
                    _assert_declared_shape(item, arm[key], _seen, journal)
                    continue
                key_annotation, value_annotation = arm if arm else (None, None)
                _assert_declared_shape(key, key_annotation, _seen, journal)
                _assert_declared_shape(item, value_annotation, _seen, journal)

        _assert_one_arm_covers(
            _walk_mapping, _mapping_arms(annotation, value), _seen, _journal
        )
    elif _is_opaque_iterable(value):
        # FAIL CLOSED on anything iterable the walk cannot enumerate safely.
        # ``Iterable[str]`` validates LAZILY — pydantic hands back a
        # ``ValidatorIterator`` without inspecting a single element — so a
        # replayable custom iterable yielding caller mappings reached the
        # executor untouched, twice, identically enough to pass the determinism
        # compare. Consuming it here to look would also destroy it, so the only
        # safe answer is to refuse it (issue #145, Codex review).
        raise ValueError("validated input holds an iterable the gate cannot inspect")


def _pointer(path: Tuple[Any, ...]) -> str:
    if not path:
        return "/"
    escaped = [str(part).replace("~", "~0").replace("/", "~1") for part in path]
    return "/" + "/".join(escaped)


def _preflight_prerequisites(
    descriptor: RecipeDescriptorV1,
    registry: RecipeRegistry,
    catalog: MaterializationCatalog,
    topology_context: Any,
) -> None:
    """Resolve every declared recipe prerequisite, or fail.

    A RECIPE prerequisite is resolved here as defence in depth. Registry
    construction already rejects one that names something unregistered — a
    registration mistake is not a caller's to diagnose — so for any registry built
    through ``RecipeRegistry.__init__`` this branch cannot fire. It stays because
    ``resolve`` is the same call either way and a silent skip is what the earlier
    version got wrong (issue #145).

    Context prerequisites are checked too, against what the caller actually
    supplied. An earlier version said the engine's signature made them
    structurally satisfied; live QA showed all three kinds running unheld while
    ``ExecutionContextPrerequisiteV1``'s own docstring says the engine "must
    hold" them (issue #145). A ``component_catalog`` needs a non-empty catalog, a
    ``topology_context`` needs one to have been passed, and a
    ``process_symbol_catalog`` needs components to build symbols from — which is
    the same non-empty catalog.
    """
    for prerequisite in descriptor.prerequisites:
        kind = getattr(prerequisite, "kind", None)
        if kind == "recipe":
            registry.resolve(prerequisite.recipe_id, prerequisite.recipe_version)
        elif kind in ("component_catalog", "process_symbol_catalog"):
            if not catalog.slots():
                raise recipe_error(
                    RECIPE_CONSTRAINT_FAILED,
                    phase="capability",
                    target=f"execution_context:{kind}",
                    recipe_ids=(descriptor.recipe_id,),
                    recipe_versions=(descriptor.recipe_version,),
                )
        elif kind == "topology_context":
            if topology_context is None:
                raise recipe_error(
                    RECIPE_CONSTRAINT_FAILED,
                    phase="capability",
                    target="execution_context:topology_context",
                    recipe_ids=(descriptor.recipe_id,),
                    recipe_versions=(descriptor.recipe_version,),
                )


def _run_executor(
    descriptor: RecipeDescriptorV1,
    registry: RecipeRegistry,
    validated_input: Any,
) -> Tuple[Any, ...]:
    """Run the registered callable and strictly re-validate everything it returned."""
    executor = registry.executor_for(descriptor)
    try:
        returned = executor(validated_input)
    except RecipeError:
        raise
    except Exception:  # noqa: BLE001 — an executor message can carry a sentinel
        raise recipe_error(
            RECIPE_CONTRIBUTION_INVALID,
            phase="execution",
            recipe_ids=(descriptor.recipe_id,),
            recipe_versions=(descriptor.recipe_version,),
        ) from None

    if not isinstance(returned, tuple):
        raise recipe_error(
            RECIPE_CONTRIBUTION_INVALID,
            phase="execution",
            recipe_ids=(descriptor.recipe_id,),
            recipe_versions=(descriptor.recipe_version,),
        )

    declared = set(descriptor.output_types)
    validated: List[Any] = []
    for index, item in enumerate(returned):
        try:
            checked = validate_contribution_object(item)
        except RecipeContributionValidationError:
            raise recipe_error(
                RECIPE_CONTRIBUTION_INVALID,
                phase="execution",
                recipe_ids=(descriptor.recipe_id,),
                recipe_versions=(descriptor.recipe_version,),
                contribution_indexes=(index,),
            ) from None
        if checked.contribution_kind not in declared:
            raise recipe_error(
                RECIPE_CONTRIBUTION_INVALID,
                phase="execution",
                target=f"undeclared_output:{checked.contribution_kind}",
                recipe_ids=(descriptor.recipe_id,),
                recipe_versions=(descriptor.recipe_version,),
                contribution_indexes=(index,),
            )
        validated.append(checked)

    # ``contribution_id`` is required to be UNIQUE PER INVOCATION, and this is
    # the only scope where that can be decided: an invocation is exactly one
    # executor's returned tuple. Two contributions sharing an id but naming
    # different component keys used to pass here untouched — composition keys on
    # ``component_key``, so nothing downstream ever compared the ids — leaving
    # the id unusable for the one thing it exists for, pointing at a single
    # contribution within its invocation (issue #145, §6 architect review).
    seen_ids: Dict[str, int] = {}
    for index, item in enumerate(validated):
        contribution_id = getattr(item, "contribution_id", None)
        if contribution_id is None:
            continue
        if contribution_id in seen_ids:
            raise recipe_error(
                RECIPE_CONTRIBUTION_INVALID,
                phase="execution",
                target="duplicate_contribution_id",
                recipe_ids=(descriptor.recipe_id,),
                recipe_versions=(descriptor.recipe_version,),
                contribution_indexes=(seen_ids[contribution_id], index),
            )
        seen_ids[contribution_id] = index
    return tuple(validated)


def _execute_deterministically(
    descriptor: RecipeDescriptorV1,
    registry: RecipeRegistry,
    validated_input: Any,
) -> Tuple[Any, ...]:
    first = _run_executor(descriptor, registry, validated_input)
    second = _run_executor(descriptor, registry, validated_input)
    if canonical_recipe_contributions_json(first) != canonical_recipe_contributions_json(
        second
    ):
        raise recipe_error(
            RECIPE_OUTPUT_NONDETERMINISTIC,
            phase="determinism",
            recipe_ids=(descriptor.recipe_id,),
            recipe_versions=(descriptor.recipe_version,),
        )
    return first


def run_recipes(
    requests: Sequence[RecipeRequestV1],
    *,
    catalog: MaterializationCatalog,
    registry: Optional[RecipeRegistry] = None,
    connector_metadata: Optional[Mapping[str, Tuple[Optional[str], Optional[str]]]] = None,
    topology_context: Any = None,
    resolver=placeholder_component_id,
) -> RecipeRunResultV1:
    """Run, compose, and canonically validate a set of recipe requests."""
    active = registry if registry is not None else production_registry()

    seen_invocations = set()
    for request in requests:
        if request.invocation_id in seen_invocations:
            # ``order_invocations`` tracks placement by invocation_id, so a
            # duplicate marks its twin as already placed and can order a recipe
            # BEFORE its declared dependency (issue #145, live QA). The id is the
            # caller's to choose and must be unique.
            # Its OWN code. Reusing RECIPE_INPUT_INVALID told the caller their
            # input carried "credentials, headers, SQL, raw XML" — byte-for-byte
            # the misdiagnosis a sibling fix had just removed (issue #145).
            raise recipe_error(
                RECIPE_REQUEST_INVALID,
                phase="input",
                target="duplicate_invocation_id",
                recipe_ids=(request.recipe_id,),
            )
        seen_invocations.add(request.invocation_id)

    invocations: List[RecipeInvocationV1] = []
    for request in requests:
        descriptor = active.resolve(request.recipe_id, request.recipe_version)
        active.preflight_capabilities(descriptor)
        _preflight_prerequisites(descriptor, active, catalog, topology_context)
        validated_input = _validate_input(descriptor, active, request.raw_input)
        invocations.append(
            RecipeInvocationV1(
                invocation_id=request.invocation_id,
                descriptor=descriptor,
                validated_input=validated_input,
            )
        )

    ordered = order_invocations(invocations)

    attributed: List[AttributedContributionV1] = []
    # Keyed by (id, VERSION). Keying on the id alone let two versions of one
    # recipe share a policy lookup, so the merge decision read one version's
    # rules for both writers — accepting a merge neither declared, or raising a
    # spurious conflict between two that did (issue #145, live QA).
    descriptors: Dict[Tuple[str, str], RecipeDescriptorV1] = {}
    for invocation in ordered:
        descriptors[
            (invocation.descriptor.recipe_id, invocation.descriptor.recipe_version)
        ] = invocation.descriptor
        contributions = _execute_deterministically(
            invocation.descriptor, active, invocation.validated_input
        )
        for index, contribution in enumerate(contributions):
            attributed.append(
                AttributedContributionV1(
                    invocation_id=invocation.invocation_id,
                    recipe_id=invocation.descriptor.recipe_id,
                    recipe_version=invocation.descriptor.recipe_version,
                    index=index,
                    contribution=contribution,
                )
            )

    composed = compose(attributed, descriptors)

    components = _resolve_components(composed, catalog)
    process_artifacts = _compile_processes(composed, components, connector_metadata, resolver)
    topology_plans = _plan_topologies(composed, components, topology_context)
    _evaluate_constraints(composed, components, active)

    return RecipeRunResultV1(
        composed=composed,
        components=tuple(components),
        process_artifacts=tuple(process_artifacts),
        topology_plans=tuple(topology_plans),
        provenance={
            "registry_revision": active.registry_revision,
            "recipes": [
                {
                    "recipe_id": inv.descriptor.recipe_id,
                    "recipe_version": inv.descriptor.recipe_version,
                    "entry_kind": inv.descriptor.entry_kind,
                    "implementation_sha256": inv.descriptor.provenance.implementation_sha256,
                    "invocation_id": inv.invocation_id,
                }
                for inv in ordered
            ],
        },
    )


def _resolve_components(
    composed: ComposedContributionsV1, catalog: MaterializationCatalog
) -> List[IntegrationComponentSpec]:
    """Slot -> real component, in contribution order, with headers verified."""
    resolved: List[IntegrationComponentSpec] = []
    for item in composed.component_slots:
        contribution = item.contribution
        resolved.append(
            catalog.resolve(
                contribution.materializer_slot,
                component_key=contribution.component_key,
                component_type=contribution.component_type,
                materialization_mode=contribution.materialization_mode,
            )
        )
    return resolved


def _compile_processes(
    composed: ComposedContributionsV1,
    components: Sequence[IntegrationComponentSpec],
    connector_metadata: Optional[Mapping[str, Tuple[Optional[str], Optional[str]]]],
    resolver,
) -> List[Tuple[str, Any]]:
    """The canonical chain, per assembled process. No exemption is reachable."""
    from ..compiler.process_ir.diagnostics import ProcessIRCompileError
    from ..compiler.process_ir.emitter_registry import emit_process
    from ..compiler.process_ir.pipeline import compile_process_ir_v1
    from ..models.process_ir import parse_process_ir_v1

    symbols = build_symbol_table(
        components, connector_metadata=connector_metadata, resolver=resolver
    )
    artifacts: List[Tuple[str, Any]] = []
    for process_key, root in composed.process_roots:
        try:
            reparsed = parse_process_ir_v1(root.model_dump(mode="json"))
            # validation_policy is NOT a parameter of run_recipes and is pinned
            # to None here. A legacy dialect's exemptions are unreachable from
            # the recipe path by construction.
            _cfg, plan = compile_process_ir_v1(reparsed, symbols, validation_policy=None)
            artifacts.append((process_key, emit_process(plan, symbols)))
        except ProcessIRCompileError as exc:
            raise RecipeError(
                (
                    recipe_diagnostic(
                        RECIPE_CONSTRAINT_FAILED,
                        phase="validation",
                        target=f"process:{process_key}",
                        cause_codes=tuple(d.code for d in exc.diagnostics),
                    ),
                )
            ) from None
        except Exception as exc:  # noqa: BLE001
            codes = tuple(
                getattr(d, "code", "") for d in getattr(exc, "diagnostics", ())
            )
            raise RecipeError(
                (
                    recipe_diagnostic(
                        RECIPE_CONSTRAINT_FAILED,
                        phase="validation",
                        target=f"process:{process_key}",
                        cause_codes=tuple(c for c in codes if c),
                    ),
                )
            ) from None
    return artifacts


def _project_topology_context(
    composed: ComposedContributionsV1,
    components: Sequence[IntegrationComponentSpec],
    topology_context: Any,
) -> Any:
    """Add THIS RUN's component symbols to the caller's resolution context.

    A topology contributed by a recipe routinely names components contributed by
    the same run. Those components do not exist in the account yet and are not in
    the caller's snapshot, so without projection the only way to resolve them was
    for the caller to redundantly assert symbols for components the run itself
    had just decided to build — asking the caller to restate what the engine
    already knows, and to get it right, or the plan reports an unresolvable
    reference (issue #145, §6 architect review).

    The run's own symbols WIN over a caller-supplied symbol for the same key.
    The run is the authority on what it materializes; a caller assertion about a
    component this run is building is at best a duplicate and at worst stale.
    That is implemented by FILTERING, not by ordering.

    Caller symbols for every other key are preserved, and keep their order
    relative to one another — but not their position, since they follow the
    whole projected block. Nothing depends on either: ``prepare_topology_context``
    sorts ``component_plan_symbols``, so no order this function produces reaches
    the planner (issue #145, live QA).
    """
    from ..compiler.system_topology.context import project_component_plan_symbols
    from ..models.integration_models import IntegrationSpecV1

    projected = project_component_plan_symbols(
        IntegrationSpecV1(name="recipe-run", components=list(components)),
        process_ir_keys=frozenset(key for key, _ in composed.process_roots),
    )
    contributed_keys = {symbol.component_key for symbol in projected}
    existing = tuple(getattr(topology_context, "component_plan_symbols", ()) or ())
    merged = projected + tuple(
        symbol
        for symbol in existing
        if symbol.component_key not in contributed_keys
    )
    return topology_context.model_copy(update={"component_plan_symbols": merged})


def _plan_topologies(
    composed: ComposedContributionsV1,
    components: Sequence[IntegrationComponentSpec],
    topology_context: Any,
) -> List[Tuple[str, Any]]:
    """Parse and PLAN every assembled topology. Plan only — there is no apply."""
    if not composed.topologies:
        return []
    if topology_context is None:
        raise recipe_error(
            RECIPE_CONSTRAINT_FAILED,
            phase="validation",
            target="topology_context_missing",
        )

    from ..compiler.system_topology.pipeline import plan_system_topology
    from ..models.system_topology import (
        SystemTopologyValidationError,
        parse_system_topology_v1,
    )

    topology_context = _project_topology_context(composed, components, topology_context)

    plans: List[Tuple[str, Any]] = []
    for topology_id, payload in composed.topologies:
        try:
            spec = parse_system_topology_v1(payload)
        except SystemTopologyValidationError as exc:
            raise RecipeError(
                (
                    recipe_diagnostic(
                        RECIPE_CONSTRAINT_FAILED,
                        phase="validation",
                        target=f"topology:{topology_id}",
                        cause_codes=tuple(d.code for d in exc.diagnostics),
                    ),
                )
            ) from None
        plan = plan_system_topology(spec, topology_context, "plan")
        if plan.blockers:
            raise RecipeError(
                (
                    recipe_diagnostic(
                        RECIPE_CONSTRAINT_FAILED,
                        phase="validation",
                        target=f"topology:{topology_id}",
                        cause_codes=tuple(b.code for b in plan.blockers),
                    ),
                )
            )
        plans.append((topology_id, plan))
    return plans


def _evaluate_constraints(
    composed: ComposedContributionsV1,
    components: Sequence[IntegrationComponentSpec],
    registry: RecipeRegistry,
) -> None:
    """Evaluate every declared requirement against the canonical artifacts.

    A constraint can only ADD an obligation. There is no path here by which one
    could satisfy itself, suppress a validator, or mark a violation safe — the
    artifacts were already produced by the canonical chain above, and this loop
    only reads them.
    """
    by_key = {component.key: component for component in components}
    process_keys = {key for key, _ in composed.process_roots}
    topologies = {topology_id: payload for topology_id, payload in composed.topologies}

    failures: List[str] = []
    for item in composed.constraints:
        requirement = item.contribution.requirement
        kind = requirement.kind
        if kind == "component":
            component = by_key.get(requirement.component_key)
            if component is None or component.type != requirement.component_type:
                failures.append("component")
        elif kind == "process":
            if requirement.process_key not in process_keys:
                failures.append("process")
        elif kind == "topology_object":
            payload = topologies.get(requirement.topology_id)
            if payload is None or not any(
                obj["key"] == requirement.object_key
                and obj["kind"] == requirement.object_kind
                for obj in payload["objects"]
            ):
                failures.append("topology_object")
        elif kind == "topology_relation":
            payload = topologies.get(requirement.topology_id)
            if payload is None or not any(
                rel["key"] == requirement.relation_key
                and rel["kind"] == requirement.relation_kind
                for rel in payload["relations"]
            ):
                failures.append("topology_relation")
        elif kind == "capability":
            from .contracts import RecipeCapabilityRequirementV1

            probe = RecipeCapabilityRequirementV1(
                authority=requirement.authority,
                subject=requirement.subject,
                required_state=requirement.required_state,
            )
            if not registry.capability_satisfied(probe):
                failures.append("capability")

    if failures:
        raise RecipeError(
            (
                recipe_diagnostic(
                    RECIPE_CONSTRAINT_FAILED,
                    phase="validation",
                    target="constraint_requirement",
                    cause_codes=tuple(f"requirement:{name}" for name in failures),
                ),
            )
        )


__all__ = [
    "RecipeRequestV1",
    "RecipeRunResultV1",
    "run_recipes",
]
