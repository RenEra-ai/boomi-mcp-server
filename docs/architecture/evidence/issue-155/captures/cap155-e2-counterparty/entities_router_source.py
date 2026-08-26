"""Router factory for the top-level entities: persons, users, clients, matters."""
from fastapi import APIRouter, Body, HTTPException, Query, Response

from shared import store
from shared.common import error_message, http_404


def build_entity_router(reg: dict, *, auth_deps: list | None = None) -> APIRouter:
    et = reg["entity_type"]
    pid = reg["public_id_field"]
    collection = reg["collection_path"]
    item = collection + "/{key}"
    router = APIRouter(tags=[reg["tag"]], dependencies=auth_deps or [])

    @router.get(collection, summary=f"List {et}")
    def list_items(  # noqa: ANN202
        skip: int = 0,
        limit: int = 50,
        search: str | None = None,
        fields: str | None = None,
        expand: str | None = Query(None, alias="$expand"),
    ):
        return store.list_documents(et, skip=skip, limit=limit, search=search)

    @router.get(item, summary=f"Get a {et[:-1]} by key or public id")
    def get_item(  # noqa: ANN202
        key: str,
        getBy: str | None = None,  # noqa: N803 - matches the CDS query param name
        expand: str | None = Query(None, alias="$expand"),
    ):
        doc = store.get_document(et, key, get_by=getBy)
        if doc is None:
            raise http_404(f"{reg['tag']} record '{key}' not found")
        return doc

    @router.patch(item, summary=f"Upsert a {et[:-1]} (create if missing, else merge)")
    def patch_item(  # noqa: ANN202
        key: str,
        body: dict = Body(...),
        getBy: str | None = None,  # noqa: N803
    ):
        return store.upsert_document(et, key, body, pid, get_by=getBy)

    @router.patch(collection, summary=f"Bulk upsert {et}")
    def patch_collection(body: list = Body(...)):  # noqa: ANN202
        return store.bulk_upsert(et, body, pid)

    # --- the remaining HTTP verbs -------------------------------------------
    #
    # Added so an integration platform can be OBSERVED exercising each verb
    # against a counterparty that actually accepts it. Before this, everything
    # but GET and PATCH answered 405, and a 405 tells you the call was made —
    # never what it did. The three write verbs are deliberately given DIFFERENT
    # replay behaviour, because that difference is the whole observation:
    #
    #   POST   create-only      — a replayed POST conflicts (409)
    #   PUT    whole-body replace — a replayed PUT converges on the same state
    #   DELETE remove           — a replayed DELETE finds nothing (404)
    #
    # HEAD/OPTIONS/TRACE answer 2xx with no body and touch no state, so a
    # readback before and after is identical.

    @router.post(collection, status_code=201, summary=f"Create a {et[:-1]}")
    def post_collection(body: dict = Body(...)):  # noqa: ANN202
        created = store.create_document(et, body, pid)
        if created is None:
            raise HTTPException(
                status_code=409,
                detail=error_message(
                    f"{reg['tag']} record already exists", 409,
                    "a record with this key is already stored; use PUT or PATCH",
                ),
            )
        return created

    @router.put(item, summary=f"Replace a {et[:-1]} in full")
    def put_item(  # noqa: ANN202
        key: str,
        body: dict = Body(...),
        getBy: str | None = None,  # noqa: N803
    ):
        replaced = store.replace_document(et, key, body, pid, get_by=getBy)
        if replaced is None:
            raise http_404(f"{reg['tag']} record '{key}' not found")
        return replaced

    @router.delete(item, status_code=204, summary=f"Delete a {et[:-1]}")
    def delete_item(  # noqa: ANN202
        key: str,
        getBy: str | None = None,  # noqa: N803
    ):
        if not store.delete_document(et, key, get_by=getBy):
            raise http_404(f"{reg['tag']} record '{key}' not found")
        return Response(status_code=204)

    @router.head(item, status_code=200, summary=f"Existence check for a {et[:-1]}")
    def head_item(  # noqa: ANN202
        key: str,
        getBy: str | None = None,  # noqa: N803
    ):
        if store.get_document(et, key, get_by=getBy) is None:
            raise http_404(f"{reg['tag']} record '{key}' not found")
        return Response(status_code=200)

    @router.options(item, status_code=204, summary=f"Allowed methods for a {et[:-1]}")
    def options_item(key: str):  # noqa: ANN202
        return Response(
            status_code=204,
            headers={"Allow": "GET, HEAD, PUT, PATCH, DELETE, OPTIONS, TRACE"},
        )

    @router.api_route(item, methods=["TRACE"], status_code=200,
                      summary=f"Echo the request line for a {et[:-1]}")
    def trace_item(key: str):  # noqa: ANN202
        # message/http per RFC 9110, and deliberately body-less of any stored
        # document: TRACE must reflect the request, never resource state.
        return Response(status_code=200, media_type="message/http", content="")

    return router
