"""SEED 2 (do not merge): an import failure that terminates collection."""
import nonexistent_module_that_cannot_possibly_exist_xyz  # noqa: F401


def test_never_runs():
    assert True
