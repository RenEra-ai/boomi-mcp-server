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
