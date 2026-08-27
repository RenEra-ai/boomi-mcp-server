# Correlation — cap155-e4-head-status

**Execution id**: `execution-487d4ceb-32e5-4f1e-95ec-9a2d64475607-2026.08.27`
**Boomi-issued request**: `HEAD /admin/cdscm/api/v1/clients/71cb4f7c-c1a7-482e-97a3-7d7a168b667b`
**Access-log line (verbatim, `docker logs -t cds-mock`)**:

    2026-08-27T13:32:00.272311262Z INFO:     192.168.65.1:41958 - "HEAD /admin/cdscm/api/v1/clients/71cb4f7c-c1a7-482e-97a3-7d7a168b667b HTTP/1.1" 200 OK

**Observed status**: `200 OK`

## Why this line belongs to this execution

1. **The (method, path) pair is UNIQUE in the container entire log.**
   The target resource key was created minutes before the run, and the only stored copy of
   that path is the connector-action component this execution used.  Occurrences of
   `"HEAD /admin/cdscm/api/v1/clients/71cb4f7c-c1a7-482e-97a3-7d7a168b667b HTTP/1.1"` in the whole `docker logs` stream since container start:
   **1**; occurrences inside this execution's window: **1**.
   
2. **It sits inside the execution window** `2026-08-27T13:31:51Z` ..
   `2026-08-27T13:32:20Z`, milliseconds after the same execution's fetch-stage
   GET on the template resource:

    2026-08-27T13:32:00.262104179Z INFO:     192.168.65.1:18528 - "GET /admin/cdscm/api/v1/clients/5f3c1fd6-dbfc-434c-a7fb-93a50f50998b HTTP/1.1" 200 OK

3. **The platform's own record agrees with the counterparty's response.**  The
   GenericConnectorRecord for operation `_TEST_155E4_HEAD_head_20260827132830` carries
   `response_date = 'Thu, 27 Aug 2026 13:31:59 GMT'` (the counterparty's `Date` header) against an access-log
   timestamp of `2026-08-27T13:32:00.272311262Z` — skew 1.272 s
   (uvicorn caches the `Date` header at 1-second granularity), and
   `response_content-length = '0'`.

**Limitation, stated rather than papered over**: the log's peer address does NOT
discriminate the atom from this host.  Both the `renera-local-atom` container (via
`host.docker.internal:8081`) and the QA process (via `localhost:8081`) reach the mock
through Docker Desktop's gateway and are logged as `192.168.65.1`.  Attribution rests on
the (method, path) uniqueness above, not on the peer address.  This run issued only
`GET` requests directly; it never issued `HEAD` against `/admin/cdscm/api/v1/clients/71cb4f7c-c1a7-482e-97a3-7d7a168b667b`.

## Other lines inside the window

Fetch-stage GET (same execution):

    INFO:     192.168.65.1:18528 - "GET /admin/cdscm/api/v1/clients/5f3c1fd6-dbfc-434c-a7fb-93a50f50998b HTTP/1.1" 200 OK

Everything else in the window (this process's readbacks, all `GET`):

    INFO:     192.168.65.1:20296 - "GET /admin/cdscm/api/v1/clients/71cb4f7c-c1a7-482e-97a3-7d7a168b667b HTTP/1.1" 200 OK
    INFO:     192.168.65.1:17948 - "GET /admin/cdscm/api/v1/clients/71cb4f7c-c1a7-482e-97a3-7d7a168b667b HTTP/1.1" 200 OK

## What the platform-side record says about the same call

| field | value |
| --- | --- |
| ExecutionRecord `status` | `COMPLETE` |
| ExecutionRecord `inboundErrorDocumentCount` | `0` |
| ExecutionConnector row `errorCount` for the send stage | `[0]` |
| GenericConnectorRecord `status` | `SUCCESS` |
| GenericConnectorRecord `errorMessage` | `None` |
| connectorField names served | `['response_content-length', 'response_date', 'response_server']` |
| an HTTP status anywhere in the served record | **no** |

`download_connector_document` returns `status_code` = `[202, 202, 202]`
for every document in this execution — that is the platform's document-retrieval status,
identical for a 2xx and for a 4xx counterparty response, and is NOT the counterparty status.
