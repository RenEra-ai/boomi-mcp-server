"""Reassemble HTTP requests (inbound to :8081) from the sidecar's wire.jsonl.

requests(path, t0, t1) -> list of {ts, client, method, target, headers, body} in arrival order.
Segments are grouped per TCP flow, ordered by sequence number, de-duplicated,
concatenated, and split into HTTP/1.1 messages by Content-Length.
"""
import json, base64, sys

PORT = 8081


def _segments(path, t0=None, t1=None, direction="in"):
    flows = {}
    with open(path) as f:
        for line in f:
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if "b64" not in r:
                continue
            if t0 is not None and r["ts"] < t0:
                continue
            if t1 is not None and r["ts"] > t1:
                continue
            inbound = r["dport"] == PORT
            if (direction == "in") != inbound:
                continue
            key = (r["src"], r["sport"], r["dst"], r["dport"])
            payload = base64.b64decode(r["b64"])
            if not payload:
                continue
            flows.setdefault(key, {})
            if r["seq"] not in flows[key]:
                flows[key][r["seq"]] = (r["ts"], payload)
    return flows


def _parse_stream(data, first_ts):
    out = []
    i = 0
    while i < len(data):
        he = data.find(b"\r\n\r\n", i)
        if he < 0:
            break
        head = data[i:he].decode("latin-1")
        lines = head.split("\r\n")
        start = lines[0]
        hdrs = {}
        for l in lines[1:]:
            if ":" in l:
                k, v = l.split(":", 1)
                hdrs[k.strip().lower()] = v.strip()
        n = int(hdrs.get("content-length", "0") or 0)
        body = data[he + 4: he + 4 + n]
        parts = start.split(" ")
        out.append({"start_line": start, "method": parts[0] if parts else "",
                    "target": parts[1] if len(parts) > 1 else "", "headers": hdrs,
                    "body": body.decode("utf-8", "replace"), "body_bytes": len(body),
                    "flow_first_ts": first_ts})
        i = he + 4 + n
    return out


def requests(path, t0=None, t1=None, direction="in"):
    flows = _segments(path, t0, t1, direction)
    reqs = []
    for key, segs in flows.items():
        ordered = sorted(segs.items(), key=lambda kv: kv[0])
        # handle 32-bit wrap crudely: sort by ts if seq span is huge
        if ordered and (ordered[-1][0] - ordered[0][0]) > 2 ** 31:
            ordered = sorted(segs.items(), key=lambda kv: kv[1][0])
        data = b"".join(p for _, (ts, p) in ordered)
        first_ts = min(ts for _, (ts, p) in ordered)
        for rq in _parse_stream(data, first_ts):
            rq["client"] = f"{key[0]}:{key[1]}"
            reqs.append(rq)
    reqs.sort(key=lambda r: r["flow_first_ts"])
    return reqs


if __name__ == "__main__":
    for r in requests(sys.argv[1]):
        print(json.dumps({k: r[k] for k in ("client", "start_line", "body")}))
