"""Second round: probes for rules read from Boomi's Arrange code, plus blind random processes.

    python lab/generate_extra.py
Writes lab/specs + lab/before for p15..p19 and b01..b03, and commits the emulator's predictions
for every one of them to lab/predictions/ before anything is arranged in Boomi.
"""
import random
import sys
import zlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from generate import LAB, Spec, emit, random_big, to_xml  # noqa: E402
from boomi_autoformat import autoformat_xml  # noqa: E402


def probes():
    out = []

    def P(name):
        s = Spec(name, 0)
        s.add("start")
        out.append(s)
        return s

    s = P("p15_unset_middle_path")          # branch: 1 -> A, 2 unset, 3 -> B  => B two rows down
    b = s.add("branch", nbranch=3); s.link(0, 0, b)
    s.chain(b, 0, ["message", "stop"])
    s.chain(b, 2, ["notify", "stop"])

    s = P("p16_orphan_chain_reversed")      # unreachable X -> Y -> Z listed Z, Y, X in XML
    s.chain(0, 0, ["message", "stop"])
    z = s.add("stop", "Orphan end")
    y = s.add("notify", "Orphan middle"); s.link(y, 0, z)
    x = s.add("message", "Orphan root"); s.link(x, 0, y)

    s = P("p17_orphan_cycle")               # unreachable A <-> B, plus a separate orphan root
    s.chain(0, 0, ["message", "stop"])
    a = s.add("message", "Cycle A")
    bb = s.add("decision", "Cycle B")
    s.link(a, 0, bb); s.link(bb, 0, a)
    s.chain(bb, 1, ["stop"])
    s.add("notify", "Lonely")

    s = P("p18_first_out_to_placed")        # decision true -> earlier shape, false -> new chain
    m = s.chain(0, 0, ["message"])
    d = s.add("decision", "Again?"); s.link(m, 0, d)
    s.link(d, 0, m)
    s.chain(d, 1, ["notify", "stop"])

    s = P("p19_branch_all_merge")           # every branch path meets in one shape
    b = s.add("branch", nbranch=4); s.link(0, 0, b)
    j = s.add("message", "Join")
    for k in range(4):
        if k % 2:
            s.link(s.chain(b, k, ["notify"]), 0, j)
        else:
            s.link(b, k, j)
    s.chain(j, 0, ["stop"])
    return out


def main():
    names = []
    for s in probes():
        names.append(emit(s, to_xml(s, random.Random(zlib.crc32(s.name.encode())))))
    for k in range(3):
        seed = 9001 + k
        rng = random.Random(seed)
        s = random_big(f"b{k + 1:02d}", seed, rng.randint(50, 110))
        names.append(emit(s, to_xml(s, rng)))
    pred = LAB / "predictions"
    pred.mkdir(exist_ok=True)
    for n in names:
        xml = (LAB / "before" / f"{n}.xml").read_text(encoding="utf-8")
        (pred / f"{n}.xml").write_text(autoformat_xml(xml), encoding="utf-8", newline="")
        print(n, xml.count("<shape "))


if __name__ == "__main__":
    main()
