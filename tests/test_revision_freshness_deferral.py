"""The development-only revision-freshness deferral, ``BOOMI_DEFER_REVISION_GATES``.

The rule is `docs/architecture/COMPLETION_WORKFLOW_RULES.md`; the mechanism is the corpus
plugin, `tests/_revision_corpus.py`. Pinned here: which values the switch accepts, the
two-way bind between a ``revision_freshness`` marker and the comparison it labels, that
every committed freshness artifact is compared by some collected test, that every spelling
of the variable is the plugin's, and that the inventory producers refuse to run under it.
The corpus guard's own behaviour under the switch is pinned beside its strict witnesses in
`tests/test_issue_184_revision_corpus.py`; the gate's refusal in `tests/test_wave_gate.py`.
"""

from __future__ import annotations

import importlib.util
import re
import sys
import textwrap
from pathlib import Path

import pytest

import _revision_corpus as plugin

ROOT = Path(__file__).resolve().parents[1]


def _load(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# the switch
# ---------------------------------------------------------------------------


def test_absent_is_strict_and_only_the_exact_value_1_defers():
    assert plugin.deferral_requested({}) is False
    assert plugin.deferral_requested({plugin.DEFER_ENV: "1"}) is True


@pytest.mark.parametrize("value", ["", "0", " 1", "1 ", "true", "yes", "01"],
                         ids=["empty", "zero", "leading-space", "trailing-space", "true", "yes",
                              "zero-one"])
def test_any_other_value_is_a_usage_error_rather_than_a_quiet_answer(value):
    with pytest.raises(pytest.UsageError, match=plugin.DEFER_ENV):
        plugin.deferral_requested({plugin.DEFER_ENV: value})


def test_every_spelling_of_the_variable_is_the_plugins():
    """Three standalone scripts refuse the variable and cannot import the plugin to learn its
    name, so each spells it again; a respelling in one would leave that script blind to it."""
    gate = _load("_wave_gate_for_deferral", "scripts/wave_gate.py")
    rebaseline = _load("_rebaseline_for_deferral", "scripts/rebaseline_m12_12_inventory.py")
    import _m12_12_legacy_inventory as inv

    assert gate.DEFER_ENV == rebaseline.DEFER_ENV == inv._DEFER_ENV == plugin.DEFER_ENV


def test_the_inventory_artifact_is_the_one_its_producers_write():
    import _m12_12_legacy_inventory as inv

    rebaseline = _load("_rebaseline_for_deferral", "scripts/rebaseline_m12_12_inventory.py")
    assert plugin.INVENTORY_BASELINE == inv.FIXTURE_RELPATH
    assert rebaseline.INVENTORY == ROOT / plugin.INVENTORY_BASELINE
    assert all((ROOT / artifact).is_file() for artifact in plugin.FRESHNESS_ARTIFACTS)


# ---------------------------------------------------------------------------
# the marker's two-way bind
# ---------------------------------------------------------------------------

_BIND = textwrap.dedent('''
    import pytest
    import _revision_corpus as rc

    @pytest.mark.revision_freshness(artifact=rc.SCHEMA_SNAPSHOT)
    def test_marked_and_reads_its_artifact():
        assert rc.committed_artifact(rc.SCHEMA_SNAPSHOT)

    @pytest.mark.revision_freshness(artifact=rc.SCHEMA_SNAPSHOT)
    def test_marked_but_reads_nothing():
        pass

    def test_reads_without_a_marker():
        rc.committed_artifact(rc.SCHEMA_SNAPSHOT)

    @pytest.mark.revision_freshness(artifact=rc.SCHEMA_SNAPSHOT)
    def test_marked_for_one_artifact_reads_another():
        rc.committed_artifact(rc.CONTRACT_SNAPSHOT)
''')


def _outcomes(output):
    found = {}
    for line in output.splitlines():
        match = re.match(r"^(FAILED|SKIPPED|PASSED) \S*::(\w+)", line)
        if match:
            found[match.group(2)] = match.group(1)
    return found


def test_a_marker_and_its_comparison_imply_each_other(tmp_path):
    """Strict run: only the test whose marker names the artifact it reads passes. A marker
    with no read, a read with no marker, and a marker naming a different artifact than the
    one read each FAIL — so the marker can neither claim a comparison that is not there nor
    leave one the deferral would not skip."""
    module = tmp_path / "test_bind_witness.py"
    module.write_text(_BIND, encoding="utf-8")
    process = plugin.run_pytest([str(module), "-rA"])
    output = process.stdout + process.stderr
    assert _outcomes(output) == {
        "test_marked_and_reads_its_artifact": "PASSED",
        "test_marked_but_reads_nothing": "FAILED",
        "test_reads_without_a_marker": "FAILED",
        "test_marked_for_one_artifact_reads_another": "FAILED",
    }, output
    assert "never read it through" in output and "is marked revision_freshness" in output


def test_a_deferred_run_skips_exactly_the_marked_tests(tmp_path):
    """Deferred: the three MARKED tests are skipped whatever their bodies would do; the
    unmarked read still fails, because a comparison the marker does not declare is never the
    deferral's to skip."""
    module = tmp_path / "test_bind_witness.py"
    module.write_text(_BIND, encoding="utf-8")
    process = plugin.run_pytest([str(module), "-rA"], env={plugin.DEFER_ENV: "1"})
    output = process.stdout + process.stderr
    # pytest's own SKIPPED lines carry a location, not a name; the deferral's summary names them.
    assert _outcomes(output) == {"test_reads_without_a_marker": "FAILED"}, output
    skipped = re.findall(r"freshness check skipped: \S*::(\w+) ", output)
    assert sorted(skipped) == ["test_marked_and_reads_its_artifact",
                               "test_marked_but_reads_nothing",
                               "test_marked_for_one_artifact_reads_another"], output
    assert "1 failed, 3 skipped" in output
    assert "PARTIAL VALIDATION — NOT COMPLETION" in output


def test_a_marker_naming_an_unknown_artifact_refuses_the_collection(tmp_path):
    module = tmp_path / "test_unknown_artifact.py"
    module.write_text(textwrap.dedent('''
        import pytest

        @pytest.mark.revision_freshness(artifact="tests/fixtures/not_a_freshness_artifact.json")
        def test_it():
            pass
    '''), encoding="utf-8")
    process = plugin.run_pytest([str(module)])
    assert process.returncode == pytest.ExitCode.USAGE_ERROR, process.stdout + process.stderr
    assert "must name artifact= one of" in process.stdout + process.stderr


def test_every_freshness_artifact_is_compared_by_a_collected_test():
    """The other direction of the closed set, read from pytest's own collection of the suite
    the gate runs (``tests --ignore=tests/kb``) rather than from a list: every committed
    freshness artifact is named by at least one collected marker. (Every marker naming a
    MEMBER is refused at collection, above.) A deferred ``--collect-only`` lists exactly the
    tests the deferral would skip."""
    process = plugin.run_pytest(["--collect-only", "tests", "--ignore=tests/kb"],
                                env={plugin.DEFER_ENV: "1"})
    output = process.stdout + process.stderr
    assert process.returncode == 0, output[-4000:]
    skipped = re.findall(r"freshness check skipped: (\S+) \((\S+)\)", output)
    assert skipped, output[-4000:]
    assert {artifact for _node, artifact in skipped} == set(plugin.FRESHNESS_ARTIFACTS)
    assert len({node for node, _artifact in skipped}) == len(skipped)


# ---------------------------------------------------------------------------
# the inventory producers refuse to run under it
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", ["1", "0", ""], ids=["one", "zero", "empty"])
def test_the_inventory_producer_refuses_before_deriving_anything(monkeypatch, capsys, value):
    import _m12_12_legacy_inventory as inv

    def _never():
        raise AssertionError("the inventory was derived under the deferral")

    monkeypatch.setattr(inv, "build_inventory", _never)
    monkeypatch.setenv(plugin.DEFER_ENV, value)
    for argv in (["--check"], ["--emit-markdown"], ["--write", "/dev/null"]):
        assert inv._main(argv) == 2
        assert "refusing: " + plugin.DEFER_ENV in capsys.readouterr().err


@pytest.mark.parametrize("value", ["1", "0", ""], ids=["one", "zero", "empty"])
def test_the_rebaseline_refuses_before_deriving_or_writing_anything(monkeypatch, value):
    rebaseline = _load("_rebaseline_for_deferral", "scripts/rebaseline_m12_12_inventory.py")

    def _never(*_args):
        raise AssertionError("the rebaseline ran the emitter under the deferral")

    monkeypatch.setattr(rebaseline, "_run", _never)
    monkeypatch.setattr(sys, "argv", ["rebaseline_m12_12_inventory.py"])
    monkeypatch.setenv(plugin.DEFER_ENV, value)
    with pytest.raises(SystemExit, match="refusing: " + plugin.DEFER_ENV):
        rebaseline.main()
