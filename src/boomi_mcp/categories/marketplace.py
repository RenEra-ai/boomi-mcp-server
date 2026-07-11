"""search_marketplace_recipes — read-only Boomi Marketplace recipe discovery (M7.4, issue #84).

Queries Boomi's PUBLIC, UNAUTHENTICATED GraphQL catalog
(``POST https://platform.boomi.com/graphql``) and returns PUBLISHED Recipe
listings as reference patterns. Read-only and open-world: it reaches an EXTERNAL
endpoint, sends NO credentials/profile/cookies/Authorization header, and exposes
NO install or mutation path (install is deliberately deferred — see issue #84).

Every response — success and every error branch — carries
``read_only=True`` / ``boomi_mutation=False`` / ``open_world=True`` (mirrors the
``_REUSE_FLAGS`` contract in ``components/connection_reuse.py``) so the advertised
contract holds unconditionally. Endpoint failures return a structured
``MARKETPLACE_GRAPHQL_UNAVAILABLE`` envelope with ``recipes=[]`` and NEVER echo
upstream response bodies or GraphQL error text (an unbounded surface).

Recipes are positioned as REFERENCE PATTERNS to review/adapt (Boomi's own
framing), not production-ready components — the tool installs nothing.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import httpx

# ---------------------------------------------------------------------------
# Contract constants
# ---------------------------------------------------------------------------

_MARKETPLACE_GRAPHQL_URL = "https://platform.boomi.com/graphql"
_ENDPOINT_TIMEOUT_SECONDS = 30

# top_k clamp — a discovery tool never needs the whole catalog (1,648+ listings).
_TOP_K_MIN = 1
_TOP_K_MAX = 25
_TOP_K_DEFAULT = 10

MARKETPLACE_GRAPHQL_UNAVAILABLE = "MARKETPLACE_GRAPHQL_UNAVAILABLE"

_MARKETPLACE_FLAGS = {"read_only": True, "boomi_mutation": False, "open_world": True}

# The MANDATORY Recipe asset-type clause — Recipe is the only listingType that
# carries an installable ``artifactSourceId`` (verbatim from the checked-in
# reference boomi_companion/bc-marketplace/skills/boomi-marketplace/references/
# graphql-api.md). Always ANDed with any user-supplied tag filter.
_RECIPE_TAG_FILTER = (
    "(listingTags.listingTag.categoryCode = 'solution_asset_type' "
    "and listingTags.listingTag.name = 'Recipe')"
)

_GRAPHQL_QUERY = (
    "query SearchMarketplaceRecipes($input: CatalogListingsSearchInput) {\n"
    "  catalogListings(input: $input) {\n"
    "    totalCount\n"
    "    currentPageSize\n"
    "    catalogListings {\n"
    "      slug\n"
    "      catalogListingStatus\n"
    "      numberOfInstalls\n"
    "      listingMetaData { name description }\n"
    "      listingArtifact { listingType artifactSourceId }\n"
    "      listingTags { listingTag { id name categoryCode } }\n"
    "    }\n"
    "  }\n"
    "}"
)

_PUBLISHED = "PUBLISHED"
_RECIPE = "RECIPE"

_GUIDANCE = (
    "Recipes are reference patterns — review and adapt them before use; "
    "this tool does not install anything."
)


# ---------------------------------------------------------------------------
# Input normalization
# ---------------------------------------------------------------------------

def _clamp_top_k(top_k: Any) -> int:
    """Coerce ``top_k`` to an int (default on garbage) and clamp to 1..25."""
    try:
        value = int(top_k)
    except (TypeError, ValueError):
        value = _TOP_K_DEFAULT
    return min(max(value, _TOP_K_MIN), _TOP_K_MAX)


def _normalize_tags(tags: Any) -> List[str]:
    """Accept a list of tag names; strip, drop empties/non-str, dedupe preserving
    first-seen order and ORIGINAL casing (the catalog filter matches tag names
    case-sensitively). Anything that is not a list yields ``[]``."""
    if not isinstance(tags, list):
        return []
    seen: set = set()
    out: List[str] = []
    for tag in tags:
        if not isinstance(tag, str):
            continue
        cleaned = tag.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        out.append(cleaned)
    return out


def _quote_literal(value: str) -> str:
    """Escape a filter string literal by doubling single quotes so a tag value
    can never break out of (or inject into) the ``catalogListingTagFilter``."""
    return value.replace("'", "''")


def _build_tag_filter(tags: List[str]) -> str:
    """The mandatory Recipe clause, optionally ANDed with an OR-list of the
    user's tag names (``... in ('A', 'B')``)."""
    if not tags:
        return _RECIPE_TAG_FILTER
    quoted = ", ".join("'" + _quote_literal(t) + "'" for t in tags)
    return (
        _RECIPE_TAG_FILTER
        + " and (listingTags.listingTag.name in (" + quoted + "))"
    )


def _build_variables(
    query: Optional[str], tags: List[str], top_k: int
) -> Dict[str, Any]:
    """Assemble the GraphQL ``$input`` variables. ``searchTerm`` is included only
    for a non-blank query; ``limit`` is an int (first page only)."""
    input_obj: Dict[str, Any] = {
        "offset": 0,
        "limit": top_k,
        "catalogListingStatus": [_PUBLISHED],
        "catalogListingTagFilter": _build_tag_filter(tags),
    }
    if query:
        input_obj["searchTerm"] = query
    return {"input": input_obj}


# ---------------------------------------------------------------------------
# Response normalization
# ---------------------------------------------------------------------------

def _normalize_listing(item: Any) -> Optional[Dict[str, Any]]:
    """Normalize one catalog listing to the public recipe shape, or return None
    to defensively DISCARD it (not PUBLISHED, not a RECIPE, or malformed) even
    though the server request already filters these. Nullable fields stay ``None``
    (never coerced to ``""``/``0``)."""
    if not isinstance(item, dict):
        return None
    if item.get("catalogListingStatus") != _PUBLISHED:
        return None

    artifact = item.get("listingArtifact")
    artifact = artifact if isinstance(artifact, dict) else {}
    if artifact.get("listingType") != _RECIPE:
        return None

    meta = item.get("listingMetaData")
    meta = meta if isinstance(meta, dict) else {}

    tags: List[Dict[str, Any]] = []
    raw_tags = item.get("listingTags")
    # A non-list listingTags (e.g. a bare int/str from a malformed response) is
    # NOT iterable-as-entries — guard it to [] so one weird listing degrades to
    # empty tags instead of raising, consistent with the artifact/meta guards.
    for entry in raw_tags if isinstance(raw_tags, list) else []:
        if not isinstance(entry, dict):
            continue
        tag = entry.get("listingTag")
        if not isinstance(tag, dict):
            continue
        tags.append({
            "id": tag.get("id"),
            "name": tag.get("name"),
            "category_code": tag.get("categoryCode"),
        })

    return {
        "slug": item.get("slug"),
        "name": meta.get("name"),
        "description": meta.get("description"),
        "tags": tags,
        "install_count": item.get("numberOfInstalls"),
        "artifact_source_id": artifact.get("artifactSourceId"),
    }


def _matches_requested_tags(
    recipe_tags: List[Dict[str, Any]], requested: List[str]
) -> bool:
    """True when no tags were requested, else any requested tag name appears
    among the recipe's tag names (defensive any-match atop the server filter)."""
    if not requested:
        return True
    names = {t.get("name") for t in recipe_tags}
    return any(name in names for name in requested)


# ---------------------------------------------------------------------------
# Structured error envelope
# ---------------------------------------------------------------------------

def _error_envelope(
    failure_kind: str, http_status: Optional[int] = None
) -> Dict[str, Any]:
    """A leak-proof structured failure. Never carries upstream body/GraphQL error
    text — only a bounded ``failure_kind`` and (when known) the numeric HTTP
    status."""
    return {
        "_success": False,
        "error_code": MARKETPLACE_GRAPHQL_UNAVAILABLE,
        "error": "Boomi Marketplace recipe search is temporarily unavailable.",
        "failure_kind": failure_kind,
        "http_status": http_status,
        "recipes": [],
        **_MARKETPLACE_FLAGS,
    }


# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------

def search_marketplace_recipes_action(
    query: Optional[str] = None,
    tags: Optional[list] = None,
    top_k: int = _TOP_K_DEFAULT,
) -> Dict[str, Any]:
    """Search Boomi's public Marketplace catalog for PUBLISHED Recipe listings.

    Read-only and open-world: sends NO credentials, cookies, profile, Boomi SDK
    client, or Authorization header. Returns normalized reference-pattern results
    or a structured ``MARKETPLACE_GRAPHQL_UNAVAILABLE`` envelope on any failure.
    Exposes no install/mutation path.
    """
    top_k_int = _clamp_top_k(top_k)
    norm_tags = _normalize_tags(tags)
    norm_query = (
        query.strip() if isinstance(query, str) and query.strip() else None
    )
    variables = _build_variables(norm_query, norm_tags, top_k_int)

    # --- Fetch (no auth, no cookies, no profile) ---
    # Handler order matters: TimeoutException < TransportError < HTTPError, so the
    # timeout branch must precede the general transport branch.
    try:
        with httpx.Client(timeout=_ENDPOINT_TIMEOUT_SECONDS) as client:
            resp = client.post(
                _MARKETPLACE_GRAPHQL_URL,
                json={"query": _GRAPHQL_QUERY, "variables": variables},
            )
    except httpx.TimeoutException:
        return _error_envelope("timeout")
    except httpx.HTTPError:
        return _error_envelope("transport")
    except Exception:
        return _error_envelope("unexpected")

    status = getattr(resp, "status_code", None)
    if not isinstance(status, int) or not (200 <= status <= 299):
        return _error_envelope(
            "http", http_status=status if isinstance(status, int) else None
        )

    try:
        payload = resp.json()
    except (ValueError, json.JSONDecodeError):
        return _error_envelope("invalid_response")
    except Exception:
        return _error_envelope("invalid_response")

    if not isinstance(payload, dict):
        return _error_envelope("invalid_response")

    # A 200 with GraphQL-level errors: never echo the messages (unbounded surface).
    if payload.get("errors"):
        return _error_envelope("graphql")

    data = payload.get("data")
    outer = data.get("catalogListings") if isinstance(data, dict) else None
    if not isinstance(outer, dict):
        return _error_envelope("invalid_response")

    raw_items = outer.get("catalogListings")
    if raw_items is None:
        raw_items = []
    if not isinstance(raw_items, list):
        return _error_envelope("invalid_response")

    # Belt-and-suspenders: _normalize_listing already guards the known nested
    # shapes, but wrapping the whole normalization pass guarantees the handler's
    # structured-error contract holds for ANY unforeseen malformed envelope shape
    # rather than raising out of the handler.
    try:
        recipes: List[Dict[str, Any]] = []
        for item in raw_items:
            normalized = _normalize_listing(item)
            if normalized is None:
                continue
            if not _matches_requested_tags(normalized["tags"], norm_tags):
                continue
            recipes.append(normalized)
        total_count = outer.get("totalCount")
    except Exception:
        return _error_envelope("unexpected")

    return {
        "_success": True,
        **_MARKETPLACE_FLAGS,
        "query": norm_query,
        "tags": norm_tags,
        "top_k": top_k_int,
        "total_count": total_count,
        "returned_count": len(recipes),
        "recipes": recipes,
        "guidance": _GUIDANCE,
    }


__all__ = ["search_marketplace_recipes_action"]
