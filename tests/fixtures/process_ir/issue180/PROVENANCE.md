# Issue #180 — effect-declaration golden provenance

Three goldens, all `process-component-v1`, all rendered through the PUBLIC chain
(`compile_authoring_request_v1` → `materialize_canonical_process_xml`) rather
than through the emitter alone.

**These bytes are not their own oracle.** They are rendered by the ProcessIR
compiler and the canonical materializer, so they are REGRESSION PINS. Stating
that plainly is the same standing `tests/fixtures/process_ir/issue154/PROVENANCE.md`
records for the grammar goldens, and it is why the table below names what does
stand behind each case.

| Golden | Case | What the declaration gates | Oracle | Provenance class |
| --- | --- | --- | --- | --- |
| `issue180_map_declared_effect.xml` | `issue180:map_declared_effect` | `set_dpp ECHO` reads `dpp:OUT`, which only the map writes | the map's own function mapping, re-derived server-side by `derive_map_effect` from the authored config — the declaration supplies identity, never content | server-derived authority, independent of the declaration |
| `issue180_subprocess_declared_effect.xml` | `issue180:subprocess_declared_effect` | a Branch leg reads `dpp:OUT`, which only the earlier leg's `process_call` establishes | the child root's own derived summary (`derive_subprocess_effect`), including the replay-UNSAFE verdict its connector forces | server-derived authority, independent of the declaration |
| `issue180_external_writer_declared_effect.xml` | `issue180:external_writer_declared_effect` | a `cache_get` over a cache nothing in the request writes | the authored `external_writer` flag on the `cache_get` node itself — the declaration may only vouch for a writer the ROOT already claims | authored-graph identity, checked against the symbol table |

## Why a `process-component-v1` renderer

The full component envelope is the artifact apply actually writes. A bare
`process-xml-v1` element would have frozen the emitter's output and skipped the
materializer — which is precisely the layer #180 found was broken.

## What these goldens do NOT attest

- **Platform acceptance.** No Boomi account has executed or even stored these
  components. `verify_process_graph` accepts the XML and the compiler's own
  invariants hold; neither is a platform oracle.
- **That the emitted bytes differ with and without the declaration.** They do
  not differ — they do not EXIST without it. Each root is refused at compile
  when its declaration is absent (asserted at the public boundary in
  `tests/test_process_ir_effect_declarations.py`), so the property frozen here
  is reachability, not a byte diff. `test_a_declared_map_effect_reaches_compile_and_emit_on_the_legacy_topology`
  already records the case where a declaration changes no emitted byte at all.
- **The registered-script family.** It has no golden because it has no
  non-inert production path: `PRODUCTION_VETTED_SCRIPTS` ships empty by design.
  `test_a_script_declaration_reaches_the_public_boundary_and_is_inert_here`
  pins that to the registry itself and fails loudly if a script is ever vetted.

## Regeneration

There is no `--write` flag, by design. To re-mint after an intended change:

```bash
PYTHONPATH=src .venv/bin/python -c "
import sys, pathlib
sys.path.insert(0, 'tests')
import _wave_gate_golden_corpus as c
for case in ('map_declared_effect', 'subprocess_declared_effect',
             'external_writer_declared_effect'):
    b = c.render_golden_case('issue180:' + case, 'process-component-v1')
    b = b if isinstance(b, bytes) else b.encode('utf-8')
    pathlib.Path('tests/fixtures/golden_xml/issue180_%s.xml' % case).write_bytes(b)
"
```

The bytes carry **no trailing newline**; an editor that adds one breaks the
byte comparison.
