"""Issue #84 (M7.4): handler tests for ``search_marketplace_recipes_action``.

Mocks ``httpx.Client`` at the point of use to exercise the fixed unauthenticated
endpoint, the mandatory Recipe filter + user-tag OR clause, published-recipe
filtering, tag safe-quoting, top_k clamping, nullable-field preservation, empty
results, and every structured failure branch — without any network access.
"""

import json
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import httpx

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.categories.marketplace import search_marketplace_recipes_action

_MODULE = "boomi_mcp.categories.marketplace"
_URL = "https://platform.boomi.com/graphql"


# ---------------------------------------------------------------------------
# Fixtures / mock helpers
# ---------------------------------------------------------------------------

def _resp(status=200, payload=None, json_exc=None):
    resp = MagicMock()
    resp.status_code = status
    if json_exc is not None:
        resp.json.side_effect = json_exc
    else:
        resp.json.return_value = payload if payload is not None else {}
    return resp


def _client_class(post_return=None, post_side_effect=None):
    """Build a MagicMock standing in for ``httpx.Client`` (the class). Returns the
    class mock plus the inner client mock (context-manager ``__enter__`` returns
    itself, matching real httpx)."""
    client = MagicMock()
    if post_side_effect is not None:
        client.post.side_effect = post_side_effect
    else:
        client.post.return_value = post_return
    client.__enter__.return_value = client
    client.__exit__.return_value = False
    return MagicMock(return_value=client), client


def _tag(name="Salesforce", tid="t1", category="solution_app"):
    return {"id": tid, "name": name, "categoryCode": category}


def _listing(
    slug="recipe-a",
    status="PUBLISHED",
    listing_type="RECIPE",
    name="Recipe A",
    description="Does A",
    installs=5,
    artifact_source_id="bundle-a",
    tags=None,
):
    if tags is None:
        tags = [_tag()]
    return {
        "slug": slug,
        "catalogListingStatus": status,
        "numberOfInstalls": installs,
        "listingMetaData": {"name": name, "description": description},
        "listingArtifact": {
            "listingType": listing_type,
            "artifactSourceId": artifact_source_id,
        },
        "listingTags": [{"listingTag": t} for t in tags],
    }


def _payload(listings, total_count=None):
    if total_count is None:
        total_count = len(listings)
    return {
        "data": {
            "catalogListings": {
                "totalCount": total_count,
                "currentPageSize": len(listings),
                "catalogListings": listings,
            }
        }
    }


def _input_of(client):
    return client.post.call_args.kwargs["json"]["variables"]["input"]


def _ws(s):
    """Collapse all whitespace runs so query documents compare structurally."""
    return " ".join(s.split())


# The full GraphQL document the handler MUST send — pinned independently of the
# module (already whitespace-normalized) so dropping any field selection fails
# the request-shape test.
_EXPECTED_QUERY = (
    "query SearchMarketplaceRecipes($input: CatalogListingsSearchInput) { "
    "catalogListings(input: $input) { "
    "totalCount currentPageSize catalogListings { "
    "slug catalogListingStatus numberOfInstalls "
    "listingMetaData { name description } "
    "listingArtifact { listingType artifactSourceId } "
    "listingTags { listingTag { id name categoryCode } } "
    "} } }"
)

_RECIPE_FILTER = (
    "(listingTags.listingTag.categoryCode = 'solution_asset_type' "
    "and listingTags.listingTag.name = 'Recipe')"
)


# ---------------------------------------------------------------------------
# Request shape / no-auth
# ---------------------------------------------------------------------------

def test_posts_to_fixed_endpoint_no_auth():
    cls, client = _client_class(post_return=_resp(payload=_payload([_listing()])))
    with patch(f"{_MODULE}.httpx.Client", cls):
        out = search_marketplace_recipes_action(query="orders")

    cls.assert_called_once_with(timeout=30)
    client.post.assert_called_once()
    args, kwargs = client.post.call_args
    assert args[0] == _URL
    body = kwargs["json"]
    # The FULL GraphQL document must match the pinned expected document exactly
    # (whitespace-normalized) — dropping any field selection fails here.
    assert _ws(body["query"]) == _EXPECTED_QUERY
    # EXACT variables: no missing keys, no extra keys. First page (offset 0),
    # default limit 10, published-only, mandatory Recipe filter, searchTerm.
    assert body["variables"] == {
        "input": {
            "offset": 0,
            "limit": 10,
            "catalogListingStatus": ["PUBLISHED"],
            "catalogListingTagFilter": _RECIPE_FILTER,
            "searchTerm": "orders",
        }
    }
    # No authentication surface of any kind.
    assert "auth" not in kwargs and "cookies" not in kwargs
    headers = kwargs.get("headers") or {}
    assert not any(k.lower() in ("authorization", "cookie") for k in headers)
    assert out["_success"] is True


def test_success_envelope_flags_and_guidance():
    cls, client = _client_class(post_return=_resp(payload=_payload([_listing()])))
    with patch(f"{_MODULE}.httpx.Client", cls):
        out = search_marketplace_recipes_action()
    assert out["read_only"] is True
    assert out["boomi_mutation"] is False
    assert out["open_world"] is True
    assert "reference pattern" in out["guidance"].lower()
    # No install/mutation surface leaks into the envelope.
    assert "artifact_source_id" in out["recipes"][0]
    assert "install" not in {k.lower() for k in out} or "returned_count" in out


# ---------------------------------------------------------------------------
# Published-recipe filtering
# ---------------------------------------------------------------------------

def test_published_recipe_filtering():
    listings = [
        _listing(slug="pub-recipe", status="PUBLISHED", listing_type="RECIPE"),
        _listing(slug="draft-recipe", status="DRAFT", listing_type="RECIPE"),
        _listing(
            slug="pub-accel",
            status="PUBLISHED",
            listing_type="ACCELERATOR",
            artifact_source_id=None,
        ),
    ]
    cls, client = _client_class(
        post_return=_resp(payload=_payload(listings, total_count=3))
    )
    with patch(f"{_MODULE}.httpx.Client", cls):
        out = search_marketplace_recipes_action()
    assert out["returned_count"] == 1
    assert [r["slug"] for r in out["recipes"]] == ["pub-recipe"]
    assert out["total_count"] == 3


# ---------------------------------------------------------------------------
# Tag filtering
# ---------------------------------------------------------------------------

def test_tag_filter_recipe_clause_and_in_list():
    cls, client = _client_class(post_return=_resp(payload=_payload([])))
    with patch(f"{_MODULE}.httpx.Client", cls):
        search_marketplace_recipes_action(
            tags=["Salesforce", "ServiceNow", "Salesforce"]
        )
    filt = _input_of(client)["catalogListingTagFilter"]
    assert filt.startswith(
        "(listingTags.listingTag.categoryCode = 'solution_asset_type' "
        "and listingTags.listingTag.name = 'Recipe')"
    )
    assert (
        "and (listingTags.listingTag.name in ('Salesforce', 'ServiceNow'))"
        in filt
    )
    # dedupe, order-preserving: Salesforce appears once in the OR-list.
    assert filt.count("'Salesforce'") == 1


def test_requested_tag_defensive_filter():
    keep = _listing(slug="keep", tags=[_tag("Salesforce")])
    drop = _listing(slug="drop", tags=[_tag("Workday")])
    cls, client = _client_class(post_return=_resp(payload=_payload([keep, drop])))
    with patch(f"{_MODULE}.httpx.Client", cls):
        out = search_marketplace_recipes_action(tags=["Salesforce"])
    assert [r["slug"] for r in out["recipes"]] == ["keep"]


def test_tag_literal_quoting_is_safe():
    cls, client = _client_class(post_return=_resp(payload=_payload([])))
    with patch(f"{_MODULE}.httpx.Client", cls):
        search_marketplace_recipes_action(tags=["O'Brien"])
    filt = _input_of(client)["catalogListingTagFilter"]
    assert "'O''Brien'" in filt


def test_no_tag_filter_is_recipe_clause_only():
    cls, client = _client_class(post_return=_resp(payload=_payload([])))
    with patch(f"{_MODULE}.httpx.Client", cls):
        search_marketplace_recipes_action()
    filt = _input_of(client)["catalogListingTagFilter"]
    assert filt == (
        "(listingTags.listingTag.categoryCode = 'solution_asset_type' "
        "and listingTags.listingTag.name = 'Recipe')"
    )


# ---------------------------------------------------------------------------
# searchTerm handling
# ---------------------------------------------------------------------------

def test_search_term_present_for_nonblank_query():
    cls, client = _client_class(post_return=_resp(payload=_payload([])))
    with patch(f"{_MODULE}.httpx.Client", cls):
        search_marketplace_recipes_action(query="  orders  ")
    assert _input_of(client)["searchTerm"] == "orders"


def test_search_term_absent_for_blank_query():
    for q in (None, "", "   "):
        cls, client = _client_class(post_return=_resp(payload=_payload([])))
        with patch(f"{_MODULE}.httpx.Client", cls):
            search_marketplace_recipes_action(query=q)
        assert "searchTerm" not in _input_of(client)


# ---------------------------------------------------------------------------
# Empty results / clamping / nullable fields
# ---------------------------------------------------------------------------

def test_empty_results():
    cls, client = _client_class(
        post_return=_resp(payload=_payload([], total_count=0))
    )
    with patch(f"{_MODULE}.httpx.Client", cls):
        out = search_marketplace_recipes_action()
    assert out["_success"] is True
    assert out["recipes"] == []
    assert out["returned_count"] == 0
    assert out["total_count"] == 0
    assert out["read_only"] is True
    assert out["boomi_mutation"] is False
    assert out["open_world"] is True


def test_top_k_clamped_high():
    cls, client = _client_class(post_return=_resp(payload=_payload([])))
    with patch(f"{_MODULE}.httpx.Client", cls):
        out = search_marketplace_recipes_action(top_k=100)
    assert _input_of(client)["limit"] == 25
    assert out["top_k"] == 25


def test_top_k_clamped_low():
    cls, client = _client_class(post_return=_resp(payload=_payload([])))
    with patch(f"{_MODULE}.httpx.Client", cls):
        out = search_marketplace_recipes_action(top_k=0)
    assert _input_of(client)["limit"] == 1
    assert out["top_k"] == 1


def test_top_k_garbage_defaults_to_ten():
    cls, client = _client_class(post_return=_resp(payload=_payload([])))
    with patch(f"{_MODULE}.httpx.Client", cls):
        out = search_marketplace_recipes_action(top_k="lots")
    assert _input_of(client)["limit"] == 10
    assert out["top_k"] == 10


def test_nullable_fields_stay_none():
    listing = {
        "slug": "recipe-x",
        "catalogListingStatus": "PUBLISHED",
        # numberOfInstalls missing
        "listingMetaData": {"name": "X"},  # description missing
        "listingArtifact": {"listingType": "RECIPE"},  # artifactSourceId missing
        "listingTags": [],
    }
    cls, client = _client_class(post_return=_resp(payload=_payload([listing])))
    with patch(f"{_MODULE}.httpx.Client", cls):
        out = search_marketplace_recipes_action()
    r = out["recipes"][0]
    assert r["slug"] == "recipe-x"
    assert r["name"] == "X"
    assert r["description"] is None
    assert r["artifact_source_id"] is None
    assert r["install_count"] is None
    assert r["tags"] == []


def test_tags_normalized_to_snake_case_keys():
    listing = _listing(tags=[_tag(name="Stripe", tid="s1", category="solution_app")])
    cls, client = _client_class(post_return=_resp(payload=_payload([listing])))
    with patch(f"{_MODULE}.httpx.Client", cls):
        out = search_marketplace_recipes_action()
    tag = out["recipes"][0]["tags"][0]
    assert tag == {"id": "s1", "name": "Stripe", "category_code": "solution_app"}


# ---------------------------------------------------------------------------
# Failure branches (never leak upstream text)
# ---------------------------------------------------------------------------

def _assert_error(out, failure_kind, http_status="__unset__"):
    assert out["_success"] is False
    assert out["error_code"] == "MARKETPLACE_GRAPHQL_UNAVAILABLE"
    assert out["failure_kind"] == failure_kind
    assert out["recipes"] == []
    assert out["read_only"] is True
    assert out["boomi_mutation"] is False
    assert out["open_world"] is True
    if http_status != "__unset__":
        assert out["http_status"] == http_status


def test_timeout_failure():
    cls, client = _client_class(post_side_effect=httpx.TimeoutException("timed out"))
    with patch(f"{_MODULE}.httpx.Client", cls):
        out = search_marketplace_recipes_action()
    _assert_error(out, "timeout")


def test_transport_failure():
    cls, client = _client_class(post_side_effect=httpx.ConnectError("refused"))
    with patch(f"{_MODULE}.httpx.Client", cls):
        out = search_marketplace_recipes_action()
    _assert_error(out, "transport")


def test_http_503():
    cls, client = _client_class(post_return=_resp(status=503, payload={}))
    with patch(f"{_MODULE}.httpx.Client", cls):
        out = search_marketplace_recipes_action()
    _assert_error(out, "http", http_status=503)


def test_graphql_errors_200_do_not_leak():
    payload = {"errors": [{"message": "secret-token-xyz"}], "data": None}
    cls, client = _client_class(post_return=_resp(status=200, payload=payload))
    with patch(f"{_MODULE}.httpx.Client", cls):
        out = search_marketplace_recipes_action()
    _assert_error(out, "graphql")
    assert "secret-token-xyz" not in json.dumps(out)


def test_invalid_json():
    cls, client = _client_class(
        post_return=_resp(status=200, json_exc=ValueError("no json"))
    )
    with patch(f"{_MODULE}.httpx.Client", cls):
        out = search_marketplace_recipes_action()
    _assert_error(out, "invalid_response")


def test_malformed_envelope_missing_catalog_listings():
    cls, client = _client_class(post_return=_resp(status=200, payload={"data": {}}))
    with patch(f"{_MODULE}.httpx.Client", cls):
        out = search_marketplace_recipes_action()
    _assert_error(out, "invalid_response")


def test_malformed_envelope_catalog_listings_not_dict():
    cls, client = _client_class(
        post_return=_resp(status=200, payload={"data": {"catalogListings": []}})
    )
    with patch(f"{_MODULE}.httpx.Client", cls):
        out = search_marketplace_recipes_action()
    _assert_error(out, "invalid_response")


def test_malformed_listing_tags_non_iterable_does_not_raise():
    # A non-iterable listingTags (e.g. a bare int) must NOT raise out of the
    # handler — it degrades that listing to empty tags and the search succeeds,
    # honoring the "every failure mode returns a structured envelope" contract.
    listing = {
        "slug": "recipe-bad-tags",
        "catalogListingStatus": "PUBLISHED",
        "numberOfInstalls": 3,
        "listingMetaData": {"name": "Bad Tags", "description": "d"},
        "listingArtifact": {"listingType": "RECIPE", "artifactSourceId": "b1"},
        "listingTags": 1,  # malformed: not a list
    }
    cls, client = _client_class(post_return=_resp(payload=_payload([listing])))
    with patch(f"{_MODULE}.httpx.Client", cls):
        out = search_marketplace_recipes_action()
    assert out["_success"] is True
    assert out["returned_count"] == 1
    assert out["recipes"][0]["slug"] == "recipe-bad-tags"
    assert out["recipes"][0]["tags"] == []


def test_malformed_listing_entry_not_dict_is_skipped():
    # A listingTags entry that is not a dict is skipped, not fatal.
    listing = {
        "slug": "recipe-mixed",
        "catalogListingStatus": "PUBLISHED",
        "numberOfInstalls": 3,
        "listingMetaData": {"name": "Mixed", "description": "d"},
        "listingArtifact": {"listingType": "RECIPE", "artifactSourceId": "b1"},
        "listingTags": ["not-a-dict", {"listingTag": _tag("Stripe")}],
    }
    cls, client = _client_class(post_return=_resp(payload=_payload([listing])))
    with patch(f"{_MODULE}.httpx.Client", cls):
        out = search_marketplace_recipes_action()
    assert out["_success"] is True
    assert [t["name"] for t in out["recipes"][0]["tags"]] == ["Stripe"]


def test_normalization_exception_returns_structured_envelope():
    # Belt-and-suspenders backstop: if normalization raises for any unforeseen
    # reason, the handler returns a structured envelope, not a raise.
    cls, client = _client_class(
        post_return=_resp(payload=_payload([_listing()]))
    )
    with (
        patch(f"{_MODULE}.httpx.Client", cls),
        patch(f"{_MODULE}._normalize_listing", side_effect=RuntimeError("boom")),
    ):
        out = search_marketplace_recipes_action()
    _assert_error(out, "unexpected")
