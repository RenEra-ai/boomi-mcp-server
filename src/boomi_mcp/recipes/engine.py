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
from collections import deque
from dataclasses import dataclass, fields as dataclass_fields, is_dataclass
from decimal import Decimal
from fractions import Fraction
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

from pydantic import BaseModel, TypeAdapter

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
            return
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
    try:
        return dict(get_type_hints(cls))
    except Exception:  # noqa: BLE001 — unresolvable hints must not pass silently
        return {field.name: None for field in dataclass_fields(cls)}


def _element_annotations(annotation: Any, value: Any) -> Optional[Tuple[Any, ...]]:
    """The element parameters of the union arm whose CONTAINER matches this value.

    An earlier version returned the first option that had args at all, whatever
    container it belonged to, which broke both ways (issue #145, live QA):

    * ``Union[List[Leaf], List[Other]]`` holding ``[Other()]`` was REFUSED — the
      ``Leaf`` arm's parameters were applied to the other arm's value.
    * ``Union[Tuple[Any, ...], Dict[str, Leaf]]`` holding a mapping was ACCEPTED —
      the tuple arm's args are ``(Any, Ellipsis)``, so ``_mapping_annotations``
      saw a shape it could not read as key/value and dropped BOTH annotations,
      leaving a dict standing in for ``Leaf`` unjudged.

    Abstains when no arm matches, and when several do — with two candidate
    parameter sets there is no basis to prefer one, and guessing is what caused
    the defect.
    """
    candidates = []
    for option in _unwrap_annotation(annotation):
        origin = get_origin(option)
        args = get_args(option)
        if origin is None or not args:
            continue
        try:
            if isinstance(value, origin):
                candidates.append(args)
        except TypeError:  # a non-class origin cannot be matched against a value
            continue
    if len(candidates) == 1:
        return candidates[0]
    return None


def _positional(element: Optional[Tuple[Any, ...]], index: int) -> Any:
    """The annotation for one position of a sequence."""
    if not element:
        return None
    if len(element) == 2 and element[1] is Ellipsis:
        return element[0]
    if len(element) == 1:
        return element[0]
    return element[index] if index < len(element) else None


def _mapping_annotations(annotation: Any, value: Any) -> Tuple[Any, Any]:
    """The (key, value) annotations for a mapping, or no opinion."""
    element = _element_annotations(annotation, value)
    if element and len(element) == 2 and element[1] is not Ellipsis:
        return element[0], element[1]
    return None, None


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


def _is_walkable_collection(value: Any) -> bool:
    """Known-replayable, so the walk can enumerate it without destroying it."""
    if isinstance(value, (str, bytes, bytearray)):
        return False
    if isinstance(value, Mapping):
        return True
    return isinstance(value, _REPLAYABLE_TYPES)


def _is_opaque_iterable(value: Any) -> bool:
    if isinstance(value, (str, bytes, bytearray)):
        return False
    if _is_walkable_collection(value):
        return False
    return hasattr(value, "__iter__") or hasattr(value, "__next__")


def _assert_declared_shape(value: Any, annotation: Any = None, _seen: Optional[set] = None) -> None:
    """Every value in the tree is what its declaration says, checked value-first.

    VALUE-FIRST, not schema-first, is the load-bearing choice. Walking the model's
    schema means reading something the author can rewrite after the class exists;
    walking the values reads what the caller's data actually landed in.

    Raises anything at all on a mismatch — the caller converts it to a value-free
    diagnostic, because a pydantic message can echo the offending value.
    """
    if _seen is None:
        _seen = set()

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
        _seen.add(id(value))
        # Undeclared keys on the INSTANCE. A model whose compiled validator allows
        # extras can be made to look closed at registration by assigning a closed
        # twin's ``__pydantic_core_schema__``; the instance it produces cannot lie
        # about what it is carrying (issue #145, live QA).
        if getattr(value, "__pydantic_extra__", None):
            raise ValueError("validated input carries undeclared keys")
        for name, field in type(value).model_fields.items():
            _assert_declared_shape(getattr(value, name, None), field.annotation, _seen)
        return

    # A pydantic dataclass is neither a ``BaseModel`` nor a container, so an
    # earlier version walked straight past one holding a model. Its fields carry
    # DECLARED TYPES too — recursing with ``None`` checked nothing, and left a
    # mapping sitting in a ``str`` field reachable at ``inp.holder.label``
    # (issue #145, Codex review).
    if is_dataclass(value) and not isinstance(value, type):
        _seen.add(id(value))
        hints = _dataclass_field_types(type(value))
        for field in dataclass_fields(value):
            _assert_declared_shape(
                getattr(value, field.name, None), hints.get(field.name), _seen
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
        _seen.add(id(value))
        element = _element_annotations(annotation, value)
        for index, item in enumerate(value):
            _assert_declared_shape(item, _positional(element, index), _seen)
    elif isinstance(value, Mapping):
        _seen.add(id(value))
        key_annotation, value_annotation = _mapping_annotations(annotation, value)
        for key, item in value.items():
            _assert_declared_shape(key, key_annotation, _seen)
            _assert_declared_shape(item, value_annotation, _seen)
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
