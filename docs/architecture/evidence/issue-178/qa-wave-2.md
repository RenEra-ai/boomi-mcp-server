# Stage-1 QA — wave-2, issue #178 (validating the wave correction)

Attested `8cc8cfafca55b27af936a1203247fa1c2edcfb9c`, verified by diff: one file, change confined to
`_plain_scalar` / `_SCALAR_SLOTS`. Tree `clean` at PRE and `worktree_moved=False` at ATTEST on every
run. Account back to 22 / 24 / 22, licences untouched, repo untouched.

**Verdict: CLEAN. Closure verdict stands — land it.**

## Both properties, on the same values — 9/9

| Carrier | Result type | Value preserved | Hooks stripped |
| --- | --- | --- | --- |
| plain `"1"` | `str` | yes | yes |
| `class V(str, Enum): ONE = "1"` | `str` | **yes -> `"1"`** | yes |
| `int`-Enum | `int` | yes | yes |
| `str` subclass, lying `__str__` | `str` | yes | yes |
| `int` / `float` subclass, lying `__int__` / `__float__` | `int` / `float` | yes | yes |
| `bytes` subclass | `bytes` | yes | yes |
| `bool True` | `bool` | yes | yes |
| subclass whose `__str__` returns another subclass | `str` | yes -> `"1"` | yes |

The value check calls `expect_type.__eq__(out, expected)` rather than `==`, so a hostile `__eq__`
cannot fake a pass. End to end: the parser ACCEPTS `V.ONE` and the compile entry now COMPILES it, so
parity holds; the control (`int`-Enum in a `str` field) agrees in the refuse direction.

**QA corrected its own scoring.** It first marked the nested-subclass row a defect, carrying forward
an expectation from an older docstring that described a refusal path. It is not a defect: the slot
bypasses `__str__` entirely, so the override never runs. Measured side by side — old
`str(Nested("1"))` -> `'EVIL'` still carrying hooks; new -> plain `'1'`. Strictly stronger, not
merely equivalent.

## No security property was traded away

Forgery battery **0 breaches across 11 attacks (A0–A8)**; 8/8 real refusals verbatim. Escape battery
**0 problems** (8/8 `Exception`-derived converted, 4/4 `BaseException`-only correctly not swallowed).
Inert battery: variant 5 still closed, legal control still compiles, accepted-limitation carriers
behave exactly as recorded.

## Live sweep and shared state

Five digests exact for the NINTH round, both M8 oracles byte-for-byte, E-matrix leaf-diff one stamp,
apply matrix green, `B1`/`B2` inventory 22 -> 22 with `mutation_performed:false` against five real
creates. `_QA_FIXTURE_noop` at v3 executes **COMPLETE, 0 error documents**.

## `QA-178-r7-01` closed the way QA recommended

QA grepped the OLD string rather than trusting the new text:
`"builtin-derived scalar reaching the parser is a plain builtin"` is absent. The replacement names
the true set and lists `complex`/`bytearray` alongside `datetime` as by-design pass-throughs. The
list was narrowed, not extended — no sixth instance patch.

## Standing condition on closure

The scope decision holds BECAUSE `ProcessIRV1` is reachable only by in-process construction. If a
deserialisation or plugin-loading path ever instantiates caller-named classes, reopen it.
