# boomi_autoformat

Offline emulator of the Boomi process canvas **Arrange** (Auto Format) button. Give it a process component XML
and it writes every shape and dragpoint coordinate exactly where Boomi's Arrange would, without opening the UI.
Standard library only (Python 3.10+).

```
python -m boomi_autoformat process.xml -o arranged.xml
python -m boomi_autoformat process.xml --check boomi_arranged.xml    # exit 1 on any mismatch
```

```python
from boomi_autoformat import autoformat_xml
xml = autoformat_xml(xml)          # before manage_component create/update
```

The canvas is the first `<shapes>` element (in document order) that is the document root or a direct child of a
`<process>` — in a Boomi component that is `bns:object/process/shapes`. Only the `x`/`y` attributes of its direct
`<shape>` children and their `<dragpoints>/<dragpoint>`s change (added if missing); every other byte is kept —
declaration, labels, configuration, comments, CDATA, quoting and line ends. Other processes or process-like markup
elsewhere in the document are not touched. Any valid XML spelling is accepted (single or double quotes, character
references and raw whitespace in names, namespaced elements). A DOCTYPE is refused (`ValueError`): Boomi component XML
never has one, and refusing it rules out entity-expansion tricks when the emulator runs in a server.

`--check` is strict: it reports shapes or dragpoints present on only one side, `toShape` differences and every
coordinate difference (dragpoints are matched by position; the Boomi editor renames them on save). Exit codes: 0 match,
1 mismatch, 2 the process cannot be arranged, 3 invalid input XML.

Processes Boomi's own Arrange cannot handle raise `UnarrangeableProcess`: a loop that Start cannot reach, or a
one-output shape pointing at a shape that does not exist. A process without a Start shape is returned unchanged.

## In the build flow

Generate the process XML with any coordinates, run `autoformat_xml`, then create or update the component. The saved
canvas then looks exactly like the one Boomi shows after clicking Arrange and saving, so no manual Arrange pass is
needed. For generated processes:

- List each shape's dragpoints in the order the paths should stack (Branch 1, 2, 3 …): Arrange follows XML order.
- Unset dragpoints are fine, but their saved height depends on the shape's position before Arrange (Boomi behaves the
  same); give shapes grid positions if that matters.

## Known limit

Boomi offsets a shape's y by `-trunc((rendered height mod 16) / 2)`. The rendered height is only known in the browser;
the emulator uses -2 for Start and 0 for everything else, which held for every shape type recorded so far (see
`docs/arrange_rules.md`). A shape type whose widget renders at another height would sit 1–7 units off.

## How it was derived

See [docs/arrange_rules.md](../docs/arrange_rules.md) (the algorithm, transcribed from the Boomi web client and
checked against 35 arranged lab processes) and [docs/arrange_observations.md](../docs/arrange_observations.md) (UI flow).
Tests: `python -m pytest tests -q`; score report: `python lab/evaluate.py` (exit 1 on any difference, or when the
recorded corpus is incomplete).
