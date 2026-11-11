# Provenance — Boomi canvas Arrange fixtures (#188)

These are the recorded before/after pairs the server's Arrange emulator (`src/boomi_mcp/arrange/`)
is tested against (`tests/test_arrange.py`, `tests/_arrange_evaluate.py`). They are byte contracts:
never re-write them through a parser or an editor; `SHA256SUMS` covers every file here except
itself, and `.gitattributes` disables line-ending conversion for this tree.

## Origin

| Item | Value |
|---|---|
| Source | A handoff package (`boomi_autoformat_mcp.zip`, folder `boomi_autoformat_mcp_handoff_2026-09-30/`, 85 files) written on 2026-09-30 by another Claude session while it built processes for a separate project |
| Source sha256 | `07f3a788d2cafa214e2d9711a75ccfeaefaf3c6fb3fa5c93d563c7597bdfadf0` |
| Imported and sanitized | 2026-10-20 to 2026-10-22, kept in the maintainers' local development archive |
| Vendored here | 2026-11-11, for #188 |

## How the pairs were recorded

- 36 synthetic `AFL …` process bodies were generated and created in a disposable Boomi demo account
  (folder `root/AutoFormat Lab`) on 2026-09-30.
- Each was opened in the Build editor; **Arrange** was clicked, the unsaved-changes prompt was answered
  **Don't Save**, and the process was then saved. Bodies were read back through the Component API.
- `after/` files are therefore the platform's own output — a clean-room reference for the emulator.
- `p17_orphan_cycle` has no `after/` file: Boomi's Arrange crashes on it (a loop Start cannot reach), and
  the emulator raises `UnarrangeableProcess` for the same input.

## Inventory

| Source (handoff) | Destination |
|---|---|
| `lab/before/*.saved.xml` (36 files) | `before/` — byte-identical |
| `lab/after/*.saved.xml` (35 files) | `after/` — byte-identical |
| `boomi_autoformat/*.py` | `src/boomi_mcp/arrange/` (defects fixed while vendoring; see #188) |
| `tests/test_autoformat.py` | `tests/test_arrange.py` |
| `lab/evaluate.py` | `tests/_arrange_evaluate.py` |
| `lab/generate*.py`, `lab/page_helper.js` | not vendored (recording kit, kept in the local archive) |

## Changes from the original delivery

- `createdBy` / `modifiedBy` held the demo account's API-user mailbox; before import it was replaced by
  the reserved placeholder `demo-api-user@example.invalid` in every file. Layout data is untouched.
- Every other attribute is as recorded: component ids, folder id/path, branch id, dates, versions,
  `processcall processId` values (demo-account processes), and generator-chosen `userlabel` words.
  None of them feeds the layout: the emulator reads shape names, shape types, dragpoint order,
  `toShape`, and the prior `y` of shapes with unset dragpoints.

## Shape matrix (`shape_matrix/`) — recorded 2026-11-11 for #188

The handoff's lab corpus did not cover every shape type the server emits. These 9 pairs close
that gap; the emulator reproduces every one of them exactly, so no per-type offset was added.

| Pair | Covers |
|---|---|
| `m1_shape_types` | `connectoraction`, `map`, `dataprocess`, `doccacheload`, `doccacheretrieve`, `doccacheremove` — each with an empty caption (row 0), a long caption that wraps over several lines (branch path 1) and a short caption (branch path 2); a Branch; Stop with and without a caption; 22 shapes |
| `s_noaction`, `s_noaction_long` | Start — No Data, empty and long caption |
| `s_passthrough`, `s_passthrough_long` | Start — Data Passthrough |
| `s_wss`, `s_wss_long` | Start — Web Services Server Listen (connector Start, `actionType="Listen"`) |
| `s_connector`, `s_connector_long` | Start — REST connector Get (connector Start with a connection) |

**How they were recorded.**
- The `before/` bodies were generated with scattered coordinates and created in the disposable demo
  account (folder `AutoFormat Lab 188`) directly through the Component API — not through this
  server — so their bytes are exactly as written. Boomi validates references on create, so the
  map, document cache, JSON profile, WSS Listen operation, REST connection and REST operation they
  point at are real components in that account.
- Each was opened in the Build editor (in-app browser, signed in by the account owner), **Arrange**
  was clicked once the canvas had rendered every shape, the unsaved-changes prompt was answered
  **Don't Save**, and the process was saved (version 1 → 2), with the handoff's recording helper.
- `before/` is the stored version 1 and `after/` the stored version 2, both read back through the
  Component API. `after/` is therefore the platform's own Arrange output.

**Scrubbed before vendoring** (layout data untouched): `createdBy`/`modifiedBy` (the account's API
login) → `demo-api-user@example.invalid`; `folderFullPath`'s account-name segment → `Demo-Account`.
Component ids, folder id, branch id, dates and versions are as recorded.
