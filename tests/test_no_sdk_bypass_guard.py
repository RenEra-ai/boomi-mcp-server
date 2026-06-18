"""Static guard: no un-justified SDK transport bypasses in SDK-covered code.

SDK-covered operations must route through the SDK — typed methods, the SDK 3.0.1
component-family / SharedWebServer ``*_json`` methods, or the XML-only generic
``/Component`` raw methods. Direct ``Serializer(...)`` construction and
``service.send_request(...)`` calls are allowed only on a line carrying an
explicit ``# sdk-bypass-ok:`` justification. After the SDK 3.0.1 JSON-transport
adoption the ONLY remaining justified bypass is the ``invoke_boomi_api`` raw
escape hatch — this keeps stale raw bypasses from creeping back into SDK-covered
paths.
"""
import pathlib
import re

_SRC = pathlib.Path(__file__).resolve().parent.parent / "src" / "boomi_mcp"
_PATTERN = re.compile(r"\bSerializer\s*\(|\.send_request\s*\(")
_MARKER = "sdk-bypass-ok"


def _marked_bypasses():
    """Return [(relpath, lineno, stripped_line)] for every justified bypass."""
    marked = []
    for path in sorted(_SRC.rglob("*.py")):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if _PATTERN.search(line) and _MARKER in line:
                rel = path.relative_to(_SRC.parent.parent)
                marked.append((str(rel), lineno, line.strip()))
    return marked


def test_no_unjustified_sdk_bypasses():
    offenders = []
    for path in sorted(_SRC.rglob("*.py")):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if _PATTERN.search(line) and _MARKER not in line:
                rel = path.relative_to(_SRC.parent.parent)
                offenders.append(f"{rel}:{lineno}: {line.strip()}")
    assert not offenders, (
        "Direct SDK transport bypass without a '# sdk-bypass-ok:' justification:\n"
        + "\n".join(offenders)
        + "\n\nRoute SDK-covered operations through the SDK, or add a "
        "'# sdk-bypass-ok: <reason>' comment if this is a documented escape hatch."
    )


def test_bypass_inventory_is_the_documented_set():
    """Pin the justified-bypass inventory. After adopting the SDK 3.0.1 JSON
    methods the only documented bypasses are:

      * meta_tools.py (2): the invoke_boomi_api raw escape hatch (Serializer +
        send_request);
      * shared_resources.py (2): the lossless raw-JSON read for the channel
        update merge (_get_channel_raw_json) — the component-family endpoints got
        no lossless dict GET in 3.0.1 (unlike SharedWebServer), and the hydrating
        *_json GET drops nested config on a _map() round-trip.

    Any other marker is an accidental bypass that should route through the SDK.
    """
    marked = _marked_bypasses()
    by_file = {}
    for rel, _ln, _line in marked:
        by_file[rel] = by_file.get(rel, 0) + 1
    assert by_file == {
        "src/boomi_mcp/categories/meta_tools.py": 2,
        "src/boomi_mcp/categories/shared_resources.py": 2,
    }, f"Unexpected sdk-bypass-ok inventory: {marked}"
    # Each marker must name its sanctioned reason.
    for rel, _ln, line in marked:
        ok = "invoke_boomi_api" in line or "channel" in line.lower()
        assert ok, f"Bypass marker with an unrecognized justification: {rel}: {line}"


def test_guard_actually_detects_a_bypass(tmp_path):
    """Sanity check: the pattern flags an unmarked Serializer/send_request line."""
    assert _PATTERN.search("ser = Serializer(url, auth)")
    assert _PATTERN.search("svc.send_request(req)")
    assert not _PATTERN.search("# Serializer is the SDK transport primitive")
    assert not _PATTERN.search("svc.send_request_raw(req)  # raw bytes")
