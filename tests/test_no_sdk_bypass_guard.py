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


def test_only_invoke_boomi_api_bypass_remains():
    """Pin the justified-bypass inventory: after adopting the SDK 3.0.1 JSON
    methods, the component-family and SharedWebServer JSON transports are gone,
    leaving only the two invoke_boomi_api raw-escape-hatch lines (Serializer +
    send_request) in meta_tools.py."""
    marked = _marked_bypasses()
    non_invoke = [m for m in marked if "invoke_boomi_api" not in m[2]]
    assert not non_invoke, (
        "Unexpected '# sdk-bypass-ok:' markers remain — only the invoke_boomi_api "
        f"raw escape hatch is allowed: {non_invoke}"
    )
    assert all(m[0].endswith("meta_tools.py") for m in marked), (
        f"invoke_boomi_api bypass markers should live only in meta_tools.py: {marked}"
    )
    assert len(marked) == 2, (
        f"Expected exactly 2 invoke_boomi_api bypass markers (Serializer + "
        f"send_request), got {len(marked)}: {marked}"
    )


def test_guard_actually_detects_a_bypass(tmp_path):
    """Sanity check: the pattern flags an unmarked Serializer/send_request line."""
    assert _PATTERN.search("ser = Serializer(url, auth)")
    assert _PATTERN.search("svc.send_request(req)")
    assert not _PATTERN.search("# Serializer is the SDK transport primitive")
    assert not _PATTERN.search("svc.send_request_raw(req)  # raw bytes")
