"""Static guard: no un-justified SDK transport bypasses in SDK-covered code.

SDK-covered operations must route through the SDK — typed methods, the documented
component-family / SharedWebServer JSON transports, or the XML-only generic
``/Component`` raw methods. Direct ``Serializer(...)`` construction and
``service.send_request(...)`` calls are allowed only on a line carrying an
explicit ``# sdk-bypass-ok:`` justification (the ``invoke_boomi_api`` raw escape
hatch and the documented JSON transports). This keeps stale raw bypasses from
creeping back into SDK-covered paths — the core goal of the SDK 3.0.0 cleanup.
"""
import pathlib
import re

_SRC = pathlib.Path(__file__).resolve().parent.parent / "src" / "boomi_mcp"
_PATTERN = re.compile(r"\bSerializer\s*\(|\.send_request\s*\(")
_MARKER = "sdk-bypass-ok"


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


def test_guard_actually_detects_a_bypass(tmp_path):
    """Sanity check: the pattern flags an unmarked Serializer/send_request line."""
    assert _PATTERN.search("ser = Serializer(url, auth)")
    assert _PATTERN.search("svc.send_request(req)")
    assert not _PATTERN.search("# Serializer is the SDK transport primitive")
    assert not _PATTERN.search("svc.send_request_raw(req)  # raw bytes")
