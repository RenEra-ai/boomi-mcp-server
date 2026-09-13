"""Counterparty wire capture for cds-mock (runs INSIDE a sidecar sharing cds-mock's netns).

The mock's own access log records only the request line, not the body. This
captures raw TCP segments to/from port 8081 on eth0 of the mock's network
namespace (AF_PACKET, ETH_P_ALL), writing one JSON line per segment to
/cap/wire.jsonl. It does not touch the mock process or its code.
"""
import socket, struct, json, time, base64, sys, os

PORT = int(os.environ.get("CAP_PORT", "8081"))
OUT = os.environ.get("CAP_OUT", "/cap/wire.jsonl")
s = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.ntohs(3))
s.bind(("eth0", 0))
f = open(OUT, "a", buffering=1)
f.write(json.dumps({"event": "start", "ts": time.time(), "port": PORT}) + "\n")
while True:
    pkt, addr = s.recvfrom(65535)
    if len(pkt) < 34:
        continue
    eth_type = struct.unpack("!H", pkt[12:14])[0]
    if eth_type != 0x0800:
        continue
    ip = pkt[14:]
    ihl = (ip[0] & 0x0F) * 4
    total_len = struct.unpack("!H", ip[2:4])[0]
    if ip[9] != 6:
        continue
    src = socket.inet_ntoa(ip[12:16]); dst = socket.inet_ntoa(ip[16:20])
    tcp = ip[ihl:total_len]
    if len(tcp) < 20:
        continue
    sport, dport, seq, ack = struct.unpack("!HHII", tcp[:12])
    if PORT not in (sport, dport):
        continue
    off = (tcp[12] >> 4) * 4
    flags = tcp[13]
    payload = tcp[off:]
    if not payload and not (flags & 0x03):
        continue
    f.write(json.dumps({"ts": time.time(), "src": src, "sport": sport, "dst": dst, "dport": dport,
                        "seq": seq, "flags": flags, "pkttype": addr[2],
                        "b64": base64.b64encode(payload).decode()}) + "\n")
