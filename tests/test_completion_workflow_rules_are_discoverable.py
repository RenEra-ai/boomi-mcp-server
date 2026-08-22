"""A standing workflow rule must be reachable from a fresh clone, not just written down.

`CLAUDE.md` and `AGENTS.md` are the files an agent reads, and both are gitignored
(`.gitignore:100-101`). A rule written only there governs one working copy: no clone, no CI
run, and no other engineer ever sees it. #177 recorded a "standing repo rule" that way and
the Stage-2 commit review caught it — the same defect class the slice itself is about, a
durable claim with nothing behind it.

So the rule lives in a TRACKED document, and this test keeps the link honest from both ends:
the tracked document must exist and carry the rule, and any local instruction file that is
present must cite it. It cannot make an ignored file exist in a clone — nothing can — which
is why the tracked README also names the document as the entry point.
"""

import pathlib

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_RULES = _ROOT / "docs" / "architecture" / "COMPLETION_WORKFLOW_RULES.md"
_RELATIVE = "docs/architecture/COMPLETION_WORKFLOW_RULES.md"

#: Instruction files an agent loads. Gitignored, so they may legitimately be ABSENT — the
#: test asserts the citation only where the file exists.
_INSTRUCTION_FILES = ("CLAUDE.md", "AGENTS.md")


def test_the_tracked_rules_document_exists_and_carries_the_rule():
    assert _RULES.is_file(), _RELATIVE
    text = _RULES.read_text(encoding="utf-8")
    # The rule this document was created to carry. Named explicitly so deleting the rule
    # while keeping the file fails here rather than passing on an empty shell.
    assert "capped at THREE evaluations" in text, "the architect cap is missing"
    assert "CRITICAL" in text, "the cap's non-relaxation carve-outs are missing"


def test_the_tracked_readme_names_the_rules_document():
    """The only tracked entry point a fresh clone actually reads."""
    readme = (_ROOT / "README.md").read_text(encoding="utf-8")
    assert _RELATIVE in readme, "README does not point at " + _RELATIVE


def test_any_local_instruction_file_cites_the_tracked_rules():
    """Where the ignored instruction files exist, they must defer to the tracked source.

    Asserted per file that is PRESENT. A checkout without them is not a failure — it is the
    normal state of a fresh clone, and precisely why the rule is not kept there.
    """
    missing = []
    for name in _INSTRUCTION_FILES:
        path = _ROOT / name
        if not path.is_file():
            continue
        if _RELATIVE not in path.read_text(encoding="utf-8"):
            missing.append(name)
    assert missing == [], missing
