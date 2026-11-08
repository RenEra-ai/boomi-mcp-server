# Correlation — cap155-e4-negative-control

**Execution id**: `execution-a800fe72-6589-4ac9-8093-4a478af4925d-2026.08.27`
**Boomi-issued request**: `DELETE /admin/cdscm/api/v1/clients`
**Access-log line (verbatim, `docker logs -t cds-mock`)**:

    2026-08-27T13:32:47.672556131Z INFO:     192.168.65.1:64117 - "DELETE /admin/cdscm/api/v1/clients HTTP/1.1" 405 Method Not Allowed

**Observed status**: `405 Method Not Allowed`

## Why this line belongs to this execution

1. **The (method, path) pair is NOT unique in the container entire log — disambiguated by the window instead, see below.**
   The target resource key was created minutes before the run, and the only stored copy of
   that path is the connector-action component this execution used.  Occurrences of
   `"DELETE /admin/cdscm/api/v1/clients HTTP/1.1"` in the whole `docker logs` stream since container start:
   **2**; occurrences inside this execution's window: **1**.
   The other 1 occurrence(s) lie OUTSIDE this window and are this QA run own direct probes, issued before the capture began; the in-window occurrence is therefore still unambiguous, but the uniqueness leg alone does not carry it here.
2. **It sits inside the execution window** `2026-08-27T13:32:41Z` ..
   `2026-08-27T13:33:01Z`, milliseconds after the same execution's fetch-stage
   GET on the template resource:

    2026-08-27T13:32:47.665311548Z INFO:     192.168.65.1:17860 - "GET /admin/cdscm/api/v1/clients/5f3c1fd6-dbfc-434c-a7fb-93a50f50998b HTTP/1.1" 200 OK

3. **The platform's own record agrees with the counterparty's response.**  The
   GenericConnectorRecord for operation `_TEST_155E4_DELETE_negctl_20260827132830` carries
   `response_date = 'Thu, 27 Aug 2026 13:32:46 GMT'` (the counterparty's `Date` header) against an access-log
   timestamp of `2026-08-27T13:32:47.672556131Z` — skew 1.673 s
   (uvicorn caches the `Date` header at 1-second granularity), and
   `response_content-length = '135'`.

**Limitation, stated rather than papered over**: the log's peer address does NOT
discriminate the atom from this host.  Both the `renera-local-atom` container (via
`host.docker.internal:8081`) and the QA process (via `localhost:8081`) reach the mock
through Docker Desktop's gateway and are logged as `192.168.65.1`.  Attribution rests on
the (method, path) uniqueness above, not on the peer address.  This run issued only
`GET` requests directly; it never issued `DELETE` against `/admin/cdscm/api/v1/clients`.

## Other lines inside the window

Fetch-stage GET (same execution):

    INFO:     192.168.65.1:17860 - "GET /admin/cdscm/api/v1/clients/5f3c1fd6-dbfc-434c-a7fb-93a50f50998b HTTP/1.1" 200 OK

Everything else in the window (this process's readbacks, all `GET`):

    INFO:     192.168.65.1:40750 - "GET /admin/cdscm/api/v1/clients/6186425e-a59a-4a19-b616-b4e18a58e0ce HTTP/1.1" 200 OK
    INFO:     192.168.65.1:23624 - "GET /admin/cdscm/api/v1/clients HTTP/1.1" 200 OK
    INFO:     192.168.65.1:63024 - "GET /admin/cdscm/api/v1/clients HTTP/1.1" 200 OK

## What the platform-side record says about the same call

| field | value |
| --- | --- |
| ExecutionRecord `status` | `COMPLETE` |
| ExecutionRecord `inboundErrorDocumentCount` | `0` |
| ExecutionConnector row `errorCount` for the send stage | `[0]` |
| GenericConnectorRecord `status` | `SUCCESS` |
| GenericConnectorRecord `errorMessage` | `None` |
| connectorField names served | `['response_allow', 'response_content-length', 'response_content-type', 'response_date', 'response_server']` |
| an HTTP status anywhere in the served record | **no** |

`download_connector_document` returns `status_code` = `[202, 202, 202]`
for every document in this execution — that is the platform's document-retrieval status,
identical for a 2xx and for a 4xx counterparty response, and is NOT the counterparty status.
