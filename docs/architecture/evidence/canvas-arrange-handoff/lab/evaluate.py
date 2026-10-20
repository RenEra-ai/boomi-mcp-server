"""Score the emulator against every recorded Boomi Arrange result. Exit code 1 if any case differs.

    python lab/evaluate.py [-v] [--allow-missing] [name-filter]
Group:  lab = lab/before/<n>.saved.xml -> lab/after/<n>.saved.xml (demo account, see PROVENANCE.md)
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from boomi_autoformat import UnarrangeableProcess, autoformat_xml, compare  # noqa: E402

EXPECTED_COUNTS = {"lab": 35}  # recorded cases each group must contain


def pairs():
    """(group, name, before path, after path, arranged?)"""
    for after in sorted((ROOT / "lab" / "after").glob("*.saved.xml")):
        name = after.name[:-len(".saved.xml")]
        before = ROOT / "lab" / "before" / after.name
        if before.exists():
            arranged = after.read_text(encoding="utf-8") != before.read_text(encoding="utf-8")
            yield "lab", name, before, after, arranged


def check(group, name, before, after):
    """Differences that count as failures."""
    try:
        got = autoformat_xml(before.read_text(encoding="utf-8"))
    except UnarrangeableProcess as e:
        return [("unarrangeable", name, str(e), None)]
    return compare(got, after.read_text(encoding="utf-8"))


def main(argv=None):
    """Exit 1 if any case differs, or if a corpus is missing or short (unless --allow-missing)."""
    argv = sys.argv[1:] if argv is None else argv
    verbose = "-v" in argv
    allow_missing = "--allow-missing" in argv
    filt = next((a for a in argv if not a.startswith("-")), "")
    totals, failed = {}, 0
    for group, name, before, after, arranged in pairs():
        if filt not in name:
            continue
        if not arranged:
            print(f"SKIP {group:6} {name:48} (not arranged in Boomi: after equals before)")
            continue
        diffs = check(group, name, before, after)
        t = totals.setdefault(group, [0, 0])
        t[0] += 1
        t[1] += not diffs
        failed += bool(diffs)
        kinds = {}
        for d in diffs:
            kinds[d[0]] = kinds.get(d[0], 0) + 1
        detail = " ".join(f"{k} {v}" for k, v in sorted(kinds.items()))
        print(f"{'OK ' if not diffs else 'BAD'}  {group:6} {name:48} {detail}")
        if verbose:
            for d in diffs[:40]:
                print("      ", d)
    for g, (n, ok) in totals.items():
        print(f"{g}: {ok}/{n} exact")
    if not filt:
        for g, want in EXPECTED_COUNTS.items():
            have = totals.get(g, [0])[0]
            if have < want:
                print(f"{'WARN' if allow_missing else 'FAIL'} {g}: {have} of {want} recorded cases found")
                failed += not allow_missing
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
