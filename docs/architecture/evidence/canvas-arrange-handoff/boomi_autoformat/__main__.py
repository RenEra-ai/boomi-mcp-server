"""python -m boomi_autoformat in.xml [-o out.xml] [--check expected.xml] [--ignore-unset]

Exit codes: 0 ok / match, 1 mismatch, 2 Boomi cannot arrange this process, 3 invalid input XML.
"""
import argparse
import sys
from pathlib import Path

from . import UnarrangeableProcess, autoformat_xml, compare


def main(argv=None):
    ap = argparse.ArgumentParser(prog="boomi_autoformat", description=__doc__)
    ap.add_argument("input", help="process component XML")
    ap.add_argument("-o", "--output", help="write formatted XML here (default: stdout unless --check)")
    ap.add_argument("--check", metavar="EXPECTED", help="compare the result with XML arranged by Boomi")
    ap.add_argument("--ignore-unset", action="store_true", help="skip unconnected dragpoints in --check")
    a = ap.parse_args(argv)
    with open(a.input, encoding="utf-8", newline="") as f:  # keep CRLF / LF exactly as given
        src = f.read()
    try:
        out = autoformat_xml(src)
    except UnarrangeableProcess as e:
        print(f"cannot arrange: {e}", file=sys.stderr)
        return 2
    except (ValueError, SyntaxError) as e:  # ET.ParseError is a SyntaxError
        print(f"invalid process XML: {e}", file=sys.stderr)
        return 3
    if a.output:
        Path(a.output).write_text(out, encoding="utf-8", newline="")
    if a.check:
        with open(a.check, encoding="utf-8", newline="") as f:
            expected = f.read()
        diffs = compare(out, expected, include_unset=not a.ignore_unset)
        for kind, name, got, want in diffs:
            print(f"{kind:9} {name:28} emulator={got} boomi={want}")
        print(f"{len(diffs)} mismatches")
        return 1 if diffs else 0
    if not a.output:
        sys.stdout.buffer.write(out.encode("utf-8"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
