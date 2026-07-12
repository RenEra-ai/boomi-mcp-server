"""Fail-closed resolution of the corpus embedding contract.

One pure resolver is the single source of embedding-model identity for the
four call sites that would otherwise each hardcode it: cheap startup
validation, the deferred heavy build, the Docker image preload, and the
kb://boomi-docs/corpus resource. Pure stdlib — safe to import without the ML
stack installed.

Two manifest generations are supported:

* Legacy schema-v1 kb-24 manifests carry no ``embedding_contract``. They are
  accepted only for the one model they were ever built with
  (``all-MiniLM-L6-v2``) and mapped to a pinned revision that is verified
  behavior-compatible with kb-24 (not asserted to be the uniquely proven
  historical builder checkout). Unknown unpinned legacy models are rejected.
* Schema-v2 kb-25 manifests carry an explicit ``embedding_contract``. A
  present contract must include every required field with a valid type and
  allowed value; a malformed contract NEVER falls back to the legacy mapping.
"""
import re
from dataclasses import dataclass

from .errors import KbStartupError

PINNED_MODEL_ID = "all-MiniLM-L6-v2"
# Verified behavior-compatible with kb-24.
KB24_COMPATIBLE_REVISION = "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"
CONTRACT_VERSION = 1
ALLOWED_DISTANCE_METRICS = ("cosine", "l2", "ip")
LEGACY_EMBEDDING_TEXT_VERSION = "raw-v1"

_REVISION_RE = re.compile(r"^[0-9a-f]{40}$")


@dataclass(frozen=True)
class EmbeddingContract:
    """Resolved, validated embedding identity for one corpus."""

    model_id: str
    revision: str
    max_seq_length: int
    distance_metric: str
    normalize_embeddings: bool
    embedding_text_version: str
    s7_enabled: bool
    source: str  # "contract" (schema v2) or "legacy-kb24" (mapped v1)


def _require(condition, field, detail):
    if not condition:
        raise KbStartupError(
            f"KB manifest embedding_contract field {field!r} is invalid: {detail}"
        )


def _resolve_from_contract(raw):
    if not isinstance(raw, dict):
        raise KbStartupError(
            "KB manifest embedding_contract must be a JSON object, "
            f"got {type(raw).__name__}"
        )

    required = (
        "version", "model_id", "revision", "max_seq_length", "distance_metric",
        "normalize_embeddings", "embedding_text_version", "s7_enabled",
    )
    missing = [f for f in required if f not in raw]
    if missing:
        raise KbStartupError(
            f"KB manifest embedding_contract is missing required field(s): {missing}"
        )

    version = raw["version"]
    _require(
        isinstance(version, int) and not isinstance(version, bool)
        and version == CONTRACT_VERSION,
        "version", f"expected {CONTRACT_VERSION}, got {version!r}",
    )
    model_id = raw["model_id"]
    _require(
        model_id == PINNED_MODEL_ID,
        "model_id", f"expected {PINNED_MODEL_ID!r}, got {model_id!r}",
    )
    revision = raw["revision"]
    _require(
        isinstance(revision, str) and bool(_REVISION_RE.match(revision)),
        "revision", f"expected a 40-char lowercase hex sha, got {revision!r}",
    )
    max_seq_length = raw["max_seq_length"]
    _require(
        isinstance(max_seq_length, int) and not isinstance(max_seq_length, bool)
        and max_seq_length > 0,
        "max_seq_length", f"expected a positive integer, got {max_seq_length!r}",
    )
    distance_metric = raw["distance_metric"]
    _require(
        distance_metric in ALLOWED_DISTANCE_METRICS,
        "distance_metric",
        f"expected one of {list(ALLOWED_DISTANCE_METRICS)}, got {distance_metric!r}",
    )
    normalize = raw["normalize_embeddings"]
    _require(
        isinstance(normalize, bool),
        "normalize_embeddings", f"expected a boolean, got {normalize!r}",
    )
    text_version = raw["embedding_text_version"]
    _require(
        isinstance(text_version, str) and text_version.strip() != "",
        "embedding_text_version", f"expected a non-empty string, got {text_version!r}",
    )
    s7_enabled = raw["s7_enabled"]
    _require(
        isinstance(s7_enabled, bool),
        "s7_enabled", f"expected a boolean, got {s7_enabled!r}",
    )

    return EmbeddingContract(
        model_id=model_id,
        revision=revision,
        max_seq_length=max_seq_length,
        distance_metric=distance_metric,
        normalize_embeddings=normalize,
        embedding_text_version=text_version,
        s7_enabled=s7_enabled,
        source="contract",
    )


def resolve_embedding_contract(manifest):
    """Resolve a manifest to a validated EmbeddingContract, or raise.

    Raises KbStartupError for a malformed present contract (never falls back),
    for an unknown unpinned legacy model, and for a top-level embedding_model
    that does not match the contract's model_id.
    """
    raw = manifest.get("embedding_contract")
    if raw is None:
        embedding_model = manifest.get("embedding_model")
        if embedding_model != PINNED_MODEL_ID:
            raise KbStartupError(
                "KB manifest has no embedding_contract and its embedding_model "
                f"{embedding_model!r} is not a known pinned legacy model "
                f"(only {PINNED_MODEL_ID!r} kb-24 corpora are accepted without "
                "a contract)"
            )
        return EmbeddingContract(
            model_id=PINNED_MODEL_ID,
            revision=KB24_COMPATIBLE_REVISION,
            max_seq_length=256,
            distance_metric="cosine",
            normalize_embeddings=False,
            embedding_text_version=LEGACY_EMBEDDING_TEXT_VERSION,
            s7_enabled=False,
            source="legacy-kb24",
        )

    contract = _resolve_from_contract(raw)
    embedding_model = manifest.get("embedding_model")
    if embedding_model != contract.model_id:
        raise KbStartupError(
            f"KB manifest top-level embedding_model {embedding_model!r} does not "
            f"equal embedding_contract.model_id {contract.model_id!r}"
        )
    return contract


def assert_model_seq_length(model, contract):
    """Fail closed unless the LOADED model's max_seq_length matches the contract.

    ``model`` is the SentenceTransformer instance (it exposes max_seq_length).
    A mismatch means the loaded weights/config are not the corpus's embedding
    identity — serving would silently rank with a different token window.
    """
    actual = getattr(model, "max_seq_length", None)
    if actual != contract.max_seq_length:
        raise KbStartupError(
            f"Loaded embedding model max_seq_length {actual!r} does not match "
            f"the corpus contract max_seq_length {contract.max_seq_length!r}"
        )


def assert_collection_metric(collection, contract):
    """Fail closed unless the collection's hnsw space equals the declared metric.

    Chroma exposes the space via the legacy ``metadata['hnsw:space']`` key and,
    on newer persisted collections, via ``configuration_json['hnsw']['space']``.
    Indeterminate is a failure: a corpus whose metric cannot be verified must
    not serve distances that the confidence thresholds assume are cosine.
    """
    space = None
    metadata = getattr(collection, "metadata", None)
    if isinstance(metadata, dict):
        space = metadata.get("hnsw:space")
    if space is None:
        try:
            space = collection.configuration_json["hnsw"]["space"]
        except (AttributeError, KeyError, TypeError):
            space = None
    if space != contract.distance_metric:
        raise KbStartupError(
            f"KB collection distance metric {space!r} does not match the corpus "
            f"contract distance_metric {contract.distance_metric!r}"
        )
