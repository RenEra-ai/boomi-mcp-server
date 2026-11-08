# Correlation — cap155-e4-options-status

**Execution id**: `execution-88a1330f-52a2-4a68-ba95-7d4698618bda-2026.08.27`
**Boomi-issued request**: `OPTIONS /admin/cdscm/api/v1/clients/61ae0e89-bed1-466f-9cc0-5909c05204a8`
**Access-log line (verbatim, `docker logs -t cds-mock`)**:

    2026-08-27T13:32:19.916824799Z INFO:     192.168.65.1:20806 - "OPTIONS /admin/cdscm/api/v1/clients/61ae0e89-bed1-466f-9cc0-5909c05204a8 HTTP/1.1" 204 No Content

**Observed status**: `204 No Content`

## Why this line belongs to this execution

1. **The (method, path) pair is UNIQUE in the container entire log.**
   The target resource key was created minutes before the run, and the only stored copy of
   that path is the connector-action component this execution used.  Occurrences of
   `"OPTIONS /admin/cdscm/api/v1/clients/61ae0e89-bed1-466f-9cc0-5909c05204a8 HTTP/1.1"` in the whole `docker logs` stream since container start:
   **1**; occurrences inside this execution's window: **1**.
   
2. **It sits inside the execution window** `2026-08-27T13:32:14Z` ..
   `2026-08-27T13:32:31Z`, milliseconds after the same execution's fetch-stage
   GET on the template resource:

    2026-08-27T13:32:19.908105007Z INFO:     192.168.65.1:60687 - "GET /admin/cdscm/api/v1/clients/5f3c1fd6-dbfc-434c-a7fb-93a50f50998b HTTP/1.1" 200 OK

3. **The platform's own record agrees with the counterparty's response.**  The
   GenericConnectorRecord for operation `_TEST_155E4_OPTIONS_options_20260827132830` carries
   `response_date = 'Thu, 27 Aug 2026 13:32:19 GMT'` (the counterparty's `Date` header) against an access-log
   timestamp of `2026-08-27T13:32:19.916824799Z` — skew 0.917 s
   (uvicorn caches the `Date` header at 1-second granularity), and
   `response_content-length = None`.

**Limitation, stated rather than papered over**: the log's peer address does NOT
discriminate the atom from this host.  Both the `renera-local-atom` container (via
`host.docker.internal:8081`) and the QA process (via `localhost:8081`) reach the mock
through Docker Desktop's gateway and are logged as `192.168.65.1`.  Attribution rests on
the (method, path) uniqueness above, not on the peer address.  This run issued only
`GET` requests directly; it never issued `OPTIONS` against `/admin/cdscm/api/v1/clients/61ae0e89-bed1-466f-9cc0-5909c05204a8`.

## Other lines inside the window

Fetch-stage GET (same execution):

    INFO:     192.168.65.1:60687 - "GET /admin/cdscm/api/v1/clients/5f3c1fd6-dbfc-434c-a7fb-93a50f50998b HTTP/1.1" 200 OK

Everything else in the window (this process's readbacks, all `GET`):

    INFO:     192.168.65.1:17948 - "GET /admin/cdscm/api/v1/clients/71cb4f7c-c1a7-482e-97a3-7d7a168b667b HTTP/1.1" 200 OK
    INFO:     192.168.65.1:60782 - "GET /admin/cdscm/api/v1/clients/61ae0e89-bed1-466f-9cc0-5909c05204a8 HTTP/1.1" 200 OK
    INFO:     192.168.65.1:26139 - "GET /admin/cdscm/api/v1/clients/61ae0e89-bed1-466f-9cc0-5909c05204a8 HTTP/1.1" 200 OK

## What the platform-side record says about the same call

| field | value |
| --- | --- |
| ExecutionRecord `status` | `COMPLETE` |
| ExecutionRecord `inboundErrorDocumentCount` | `0` |
| ExecutionConnector row `errorCount` for the send stage | `[0]` |
| GenericConnectorRecord `status` | `SUCCESS` |
| GenericConnectorRecord `errorMessage` | `None` |
| connectorField names served | `['response_allow', 'response_date', 'response_server']` |
| an HTTP status anywhere in the served record | **no** |

`download_connector_document` returns `status_code` = `[202, 202, 202]`
for every document in this execution — that is the platform's document-retrieval status,
identical for a 2xx and for a 4xx counterparty response, and is NOT the counterparty status.
