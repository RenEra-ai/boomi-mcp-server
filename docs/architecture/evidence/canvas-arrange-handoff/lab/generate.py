"""Synthetic Boomi process generator for Auto Format (Arrange) experiments.

Writes topology specs (lab/specs/*.json) and full process component XML with
randomized shape and dragpoint coordinates (lab/before/*.xml). Only shapes that
need no connection/profile components are used, so every process saves cleanly.

    python lab/generate.py            # probes + big randoms
    python lab/generate.py --blind 3 --seed 9001 --prefix blind
"""
import argparse
import json
import os
import random
import zlib
from pathlib import Path
from xml.sax.saxutils import escape, quoteattr

LAB = Path(__file__).resolve().parent
FOLDER = "AutoFormat Lab"
# Lab-account components the generated processes reference. Set them before generating:
#   AFL_FOLDER_ID, AFL_CHILD_WITH_RETURN (a child process ending in Return Documents),
#   AFL_CHILD_RETURN_SHAPE (that child's Return Documents shape name), AFL_CHILD_NO_RETURN.
FOLDER_ID = os.environ.get("AFL_FOLDER_ID", "<folder-id>")
CHILD_WITH_RETURN = (os.environ.get("AFL_CHILD_WITH_RETURN", "<child-with-return-id>"),
                     os.environ.get("AFL_CHILD_RETURN_SHAPE", "shape14"))
CHILD_NO_RETURN = os.environ.get("AFL_CHILD_NO_RETURN", "<child-no-return-id>")

# type -> fixed output identifiers (None = one plain output, [] = terminal)
OUTPUTS = {
    "start": None, "message": None, "notify": None, "documentproperties": None,
    "flowcontrol": None, "decision": ["true", "false"], "catcherrors": ["default", "error"],
    "processcall": ["return"], "processcall_noreturn": [], "stop": [], "exception": [],
    "returndocuments": [], "branch": "N",
}
IMAGE = {
    "start": "start", "message": "message_icon", "notify": "notify_icon",
    "documentproperties": "documentproperties_icon", "flowcontrol": "flowcontrol_icon",
    "decision": "decision_icon", "catcherrors": "catcherrors_icon", "processcall": "processcall_icon",
    "processcall_noreturn": "processcall_icon", "stop": "stop_icon", "exception": "exception_icon",
    "returndocuments": "returndocuments_icon", "branch": "branch_icon",
}
WORDS = ("order customer invoice sync lookup resolve check validate enrich map split route cache "
         "partner client matter address billing status record update create fetch retry log").split()


class Spec:
    """Process topology: nodes[i] = {type, label, outs: [target index | None], extra}."""

    def __init__(self, name, seed):
        self.name, self.seed, self.nodes = name, seed, []

    def add(self, typ, label="", nbranch=None):
        outs = OUTPUTS[typ]
        n = {"type": typ, "label": label, "outs": []}
        if outs is None:
            n["outs"] = [None]
        elif outs == "N":
            n["outs"] = [None] * nbranch
        else:
            n["outs"] = [None] * len(outs)
        self.nodes.append(n)
        return len(self.nodes) - 1

    def link(self, a, slot, b):
        self.nodes[a]["outs"][slot] = b

    def chain(self, src, slot, types, rng=None):
        """Append a straight chain of shape types after src.outs[slot]; return last index."""
        cur, cs = src, slot
        for t in types:
            i = self.add(t, rand_label(rng) if rng else "")
            self.link(cur, cs, i)
            cur, cs = i, 0
        return cur

    def to_json(self):
        return {"name": self.name, "seed": self.seed, "nodes": self.nodes}


def rand_label(rng):
    r = rng.random()
    if r < 0.35:
        return ""
    if r < 0.8:
        return " ".join(rng.choice(WORDS) for _ in range(rng.randint(1, 3))).capitalize()
    return " ".join(rng.choice(WORDS) for _ in range(rng.randint(6, 12))).capitalize()


# ---------------------------------------------------------------- XML writer

def f(v):
    return f"{float(v):.1f}"


def shape_config(n, name):
    t, label = n["type"], n["label"]
    if t == "start":
        return "<noaction/>"
    if t == "message":
        return f'<message combined="false"><msgTxt>{escape(name)}</msgTxt><msgParameters/></message>'
    if t == "notify":
        return ('<notify disableEvent="true" enableUserLog="false" perExecution="false" title="">'
                f'<notifyMessage>{escape(name)}</notifyMessage><notifyMessageLevel>INFO</notifyMessageLevel>'
                '<notifyParameters/></notify>')
    if t == "documentproperties":
        return "<documentproperties/>"
    if t == "flowcontrol":
        return '<flowcontrol chunkStyle="threadOnly" forEachCount="1"/>'
    if t == "decision":
        return (f'<decision comparison="equals" name={quoteattr(label)}>'
                '<decisionvalue valueType="track"><trackparameter defaultValue="" '
                'propertyId="dynamicdocument.DDP_LAB" propertyName="Dynamic Document Property - DDP_LAB"/>'
                '</decisionvalue><decisionvalue valueType="static"><staticparameter staticproperty="x"/>'
                '</decisionvalue></decision>')
    if t == "catcherrors":
        return '<catcherrors catchAll="true" retryCount="0"/>'
    if t == "branch":
        return f'<branch numBranches="{len(n["outs"])}"/>'
    if t == "processcall":
        pid, ret = CHILD_WITH_RETURN
        return (f'<processcall abort="true" processId="{pid}" wait="true"><parameters/>'
                f'<returnpaths><returnpaths childShapeName="{ret}"/></returnpaths></processcall>')
    if t == "processcall_noreturn":
        return f'<processcall abort="true" processId="{CHILD_NO_RETURN}" wait="true"><parameters/><returnpaths/></processcall>'
    if t == "stop":
        return '<stop continue="true"/>'
    if t == "exception":
        return (f'<exception stopProcessReturnSingleDoc="false" stopsingledoc="false" title={quoteattr(label or "Lab")}>'
                f'<exMessage>{escape(name)}</exMessage><exParameters/></exception>')
    if t == "returndocuments":
        return "<returndocuments/>"
    raise ValueError(t)


def dragpoint_attrs(n, slot):
    t = n["type"]
    if t == "decision":
        return {"identifier": ["true", "false"][slot], "text": ["True", "False"][slot]}
    if t == "catcherrors":
        return {"identifier": ["default", "error"][slot], "text": ["Try", "Catch"][slot]}
    if t == "branch":
        return {"identifier": str(slot + 1), "text": str(slot + 1)}
    if t == "processcall":
        return {"identifier": CHILD_WITH_RETURN[1]}
    return {}


def to_xml(spec, rng, order=None, dp_order=None, coord_box=(0, 3000, 0, 2000)):
    """order: permutation of node indexes for XML shape order; dp_order: {node: [slot order]}."""
    x0, x1, y0, y1 = coord_box
    names = [f"shape{i + 1}" for i in range(len(spec.nodes))]
    order = order or list(range(len(spec.nodes)))
    parts = []
    for i in order:
        n = spec.nodes[i]
        name = names[i]
        sx, sy = rng.randint(x0, x1), rng.randint(y0, y1)
        label_attr = f' userlabel={quoteattr(n["label"])}'
        dps = []
        slots = (dp_order or {}).get(i, list(range(len(n["outs"]))))
        for k, slot in enumerate(slots):
            tgt = n["outs"][slot]
            attrs = dragpoint_attrs(n, slot)
            a = "".join(f' {key}={quoteattr(v)}' for key, v in (("identifier", attrs.get("identifier")),) if v)
            a += f' name="{name}.dragpoint{k + 1}"'
            if attrs.get("text"):
                a += f' text={quoteattr(attrs["text"])}'
            a += f' toShape="{names[tgt] if tgt is not None else "unset"}"'
            a += f' x="{f(rng.randint(x0, x1))}" y="{f(rng.randint(y0, y1))}"'
            dps.append(f"<dragpoint{a}/>")
        dragpoints = f"<dragpoints>{''.join(dps)}</dragpoints>" if dps else "<dragpoints/>"
        parts.append(f'<shape image="{IMAGE[n["type"]]}" name="{name}" '
                     f'shapetype="{"processcall" if n["type"].startswith("processcall") else n["type"]}"'
                     f'{label_attr} x="{f(sx)}" y="{f(sy)}"><configuration>{shape_config(n, name)}</configuration>'
                     f'{dragpoints}</shape>')
    comp_name = f"AFL {spec.name}"
    return ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            f'xmlns:bns="http://api.platform.boomi.com/" name={quoteattr(comp_name)} type="process" '
            f'folderFullPath="root/{FOLDER}" folderId="{FOLDER_ID}"><bns:encryptedValues/><bns:description>Auto Format lab '
            f'(seed {spec.seed}). Safe to delete.</bns:description><bns:object>'
            '<process xmlns="" allowSimultaneous="false" enableUserLog="false" processLogOnErrorOnly="false" '
            'purgeDataImmediately="false" stopProcessingIfZeroDocuments="true" updateRunDates="true" '
            f'workload="general"><shapes>{"".join(parts)}</shapes></process></bns:object>'
            '<bns:processOverrides/></bns:Component>')


# ---------------------------------------------------------------- probes

def probes():
    out = []

    def P(name):
        s = Spec(name, 0)
        s.add("start")
        out.append(s)
        return s

    s = P("p01_linear")
    s.chain(0, 0, ["message", "notify", "documentproperties", "message", "stop"])

    s = P("p02_branch3_unequal")
    b = s.add("branch", nbranch=3); s.link(0, 0, b)
    s.chain(b, 0, ["message", "stop"])
    s.chain(b, 1, ["message", "notify", "message", "notify", "stop"])
    s.chain(b, 2, ["message", "notify", "stop"])

    s = P("p03_decision")
    d = s.add("decision", "Is valid?"); s.link(0, 0, d)
    s.chain(d, 0, ["message", "notify", "stop"])
    s.chain(d, 1, ["exception"])

    s = P("p04_nested_branch")
    b = s.add("branch", nbranch=2); s.link(0, 0, b)
    m = s.chain(b, 0, ["message"])
    b2 = s.add("branch", nbranch=3); s.link(m, 0, b2)
    for k in range(3):
        s.chain(b2, k, ["notify"] * (k + 1) + ["stop"])
    s.chain(b, 1, ["message", "stop"])

    s = P("p05_trycatch")
    t = s.add("catcherrors"); s.link(0, 0, t)
    s.chain(t, 0, ["message", "message", "stop"])
    s.chain(t, 1, ["notify", "exception"])

    s = P("p06_merge2")
    d = s.add("decision", "Route?"); s.link(0, 0, d)
    a = s.chain(d, 0, ["message"])
    c = s.chain(d, 1, ["notify", "notify"])
    m = s.add("message", "Merge here"); s.link(a, 0, m); s.link(c, 0, m)
    s.chain(m, 0, ["stop"])

    s = P("p07_merge3_depths")
    b = s.add("branch", nbranch=3); s.link(0, 0, b)
    e1 = s.chain(b, 0, ["message"])
    e2 = s.chain(b, 1, ["message", "message", "message"])
    e3 = s.chain(b, 2, ["message", "message"])
    m = s.add("notify", "Join"); [s.link(e, 0, m) for e in (e1, e2, e3)]
    s.chain(m, 0, ["stop"])

    s = P("p08_unconnected")
    s.chain(0, 0, ["message", "stop"])
    for t in ("message", "notify", "stop", "decision"):
        s.add(t, "Orphan " + t)

    s = P("p09_unset_dragpoints")
    d = s.add("decision", "Half wired"); s.link(0, 0, d)
    m = s.chain(d, 0, ["message"])  # decision false left unset
    b = s.add("branch", nbranch=3)  # paths 1 and 3 left unset
    s.link(m, 0, b)
    s.chain(b, 1, ["stop"])

    s = P("p10_branch6")
    b = s.add("branch", nbranch=6); s.link(0, 0, b)
    for k in range(6):
        s.chain(b, k, ["message"] * (k % 3 + 1) + ["stop"])

    s = P("p11_labels")
    s.chain(0, 0, ["message", "notify", "message", "stop"])
    s.nodes[1]["label"] = "A very long label that should wrap across several lines on the canvas"
    s.nodes[2]["label"] = "x"
    s.nodes[3]["label"] = "Medium length label here"

    s = P("p12_processcall")
    pc = s.add("processcall", "Call child"); s.link(0, 0, pc)
    s.chain(pc, 0, ["message", "stop"])
    s.add("processcall_noreturn", "Call noop")

    s = P("p13_cycle")
    a = s.chain(0, 0, ["message", "decision"])
    s.nodes[a]["label"] = "Loop?"
    s.link(a, 0, 1)  # back edge to first message
    s.chain(a, 1, ["stop"])

    s = P("p14_decision_chain")
    cur, slot = 0, 0
    for k in range(4):
        d = s.add("decision", f"Check {k + 1}"); s.link(cur, slot, d)
        s.chain(d, 1, ["notify", "stop"])
        cur, slot = d, 0
    s.chain(cur, slot, ["message", "stop"])
    return out


# ---------------------------------------------------------------- random big

WEIGHTS = [("message", 22), ("notify", 14), ("documentproperties", 10), ("flowcontrol", 4),
           ("decision", 12), ("branch", 8), ("catcherrors", 5), ("processcall", 3)]


def random_big(name, seed, size):
    rng = random.Random(seed)
    s = Spec(name, seed)
    s.add("start")
    frontier = [(0, 0)]
    while frontier:
        src, slot = frontier.pop(rng.randrange(len(frontier)) if rng.random() < 0.3 else 0)
        r = rng.random()
        if len(s.nodes) >= size or (r < 0.04 and len(s.nodes) > 15):
            if r < 0.02:
                continue  # leave unset
            i = s.add(rng.choice(["stop", "stop", "stop", "exception", "returndocuments"]), rand_label(rng))
            s.link(src, slot, i)
            continue
        if r < 0.12 and len(s.nodes) > 4:
            anc = ancestors(s, src)
            cands = [j for j in range(1, len(s.nodes)) if j not in anc and j != src
                     and s.nodes[j]["type"] not in ("start",)]
            if cands:
                s.link(src, slot, rng.choice(cands))  # merge
                continue
        typ = rng.choices([w[0] for w in WEIGHTS], [w[1] for w in WEIGHTS])[0]
        i = s.add(typ, rand_label(rng), nbranch=rng.randint(2, 5))
        s.link(src, slot, i)
        frontier.extend((i, k) for k in range(len(s.nodes[i]["outs"])))
    for _ in range(rng.randint(0, 3)):
        s.add(rng.choice(["message", "notify", "stop"]), rand_label(rng))  # orphans
    return s


def ancestors(s, node):
    preds = {}
    for i, n in enumerate(s.nodes):
        for t in n["outs"]:
            if t is not None:
                preds.setdefault(t, set()).add(i)
    seen, stack = set(), [node]
    while stack:
        for p in preds.get(stack.pop(), ()):
            if p not in seen:
                seen.add(p)
                stack.append(p)
    return seen


# ---------------------------------------------------------------- main

def emit(spec, xml, variant=""):
    name = spec.name + variant
    (LAB / "specs").mkdir(exist_ok=True)
    (LAB / "before").mkdir(exist_ok=True)
    (LAB / "specs" / f"{name}.json").write_text(json.dumps(spec.to_json(), indent=1))
    (LAB / "before" / f"{name}.xml").write_text(xml.replace(f'name="AFL {spec.name}"', f'name="AFL {name}"'),
                                               encoding="utf-8")
    return name


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=1234)
    ap.add_argument("--blind", type=int, default=0, help="only emit N random processes with --prefix")
    ap.add_argument("--prefix", default="r")
    ap.add_argument("--big", type=int, default=10)
    a = ap.parse_args()
    written = []
    if a.blind:
        for k in range(a.blind):
            rng = random.Random(a.seed + k)
            s = random_big(f"{a.prefix}{k + 1:02d}", a.seed + k, rng.randint(40, 110))
            written.append(emit(s, to_xml(s, rng)))
    else:
        for s in probes():
            rng = random.Random(zlib.crc32(s.name.encode()))
            written.append(emit(s, to_xml(s, rng)))
        # variants of p06 that only change input coordinates / XML order / dragpoint order
        base = next(p for p in probes() if p.name == "p06_merge2")
        written.append(emit(base, to_xml(base, random.Random(777)), "_coordsB"))
        order = list(range(len(base.nodes)))
        random.Random(5).shuffle(order)
        written.append(emit(base, to_xml(base, random.Random(778), order=order), "_shuffled"))
        written.append(emit(base, to_xml(base, random.Random(779), dp_order={1: [1, 0]}), "_dpswap"))
        b2 = next(p for p in probes() if p.name == "p02_branch3_unequal")
        written.append(emit(b2, to_xml(b2, random.Random(780), dp_order={1: [2, 0, 1]}), "_dpswap"))
        for k in range(a.big):
            rng = random.Random(a.seed + k)
            s = random_big(f"r{k + 1:02d}", a.seed + k, rng.randint(40, 120))
            written.append(emit(s, to_xml(s, rng)))
    for w in written:
        spec = json.loads((LAB / "specs" / f"{w}.json").read_text()) if (LAB / "specs" / f"{w}.json").exists() else None
        print(w, len(spec["nodes"]) if spec else "")


if __name__ == "__main__":
    main()
