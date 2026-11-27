"""The two committed authoring-contract snapshots: one renderer for the test and the producer.

Not a ``test_*`` module, so pytest never collects it. The freshness tests in
``tests/test_process_ir_authoring_contract_parity.py`` compare these renders against the
committed bytes; ``scripts/regen_baselines.py`` writes them with ``--write``. Both go through
:data:`RENDERERS`, so the bytes a refresh writes are by construction the bytes the test
expects — the producer is not a second hand-copy of the rendering rule.

    PYTHONPATH=src .venv/bin/python tests/_authoring_contract_snapshots.py --write
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

#: The committed paths, repeated from `tests/_revision_corpus.py` (`CONTRACT_SNAPSHOT`,
#: `SCHEMA_SNAPSHOT`) because this module must import without pytest;
#: `tests/test_regen_baselines.py` pins the two spellings together.
CONTRACT_SNAPSHOT = "tests/fixtures/authoring_contract/process_ir_authoring_v1.contract.json"
SCHEMA_SNAPSHOT = "tests/fixtures/authoring_contract/process_ir_authoring_v1.schema.json"


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def render_contract():
    """The served authoring contract's revision payload, as committed."""
    from boomi_mcp.authoring.process_ir_projection import process_ir_authoring_revision_payload

    return _canonical(process_ir_authoring_revision_payload())


def render_schema():
    """The authoring contract page's JSON Schema, as committed."""
    from boomi_mcp.models.process_ir_authoring import (
        process_ir_authoring_contract_v1_json_schema,
    )

    return _canonical(process_ir_authoring_contract_v1_json_schema())


RENDERERS = {CONTRACT_SNAPSHOT: render_contract, SCHEMA_SNAPSHOT: render_schema}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--write", action="store_true", help="rewrite both snapshots")
    action.add_argument("--check", action="store_true", help="exit 1 if either is stale")
    args = parser.parse_args(argv)
    stale = []
    for relative, render in RENDERERS.items():
        path = ROOT / relative
        rendered = render()
        if path.read_text(encoding="utf-8") == rendered:
            continue
        stale.append(relative)
        if args.write:
            path.write_text(rendered, encoding="utf-8")
    verb = "rewrote" if args.write else "stale"
    for relative in stale:
        print("{0} {1}".format(verb, relative))
    return 1 if (stale and args.check) else 0


if __name__ == "__main__":
    sys.exit(main())
