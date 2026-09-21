"""The suite's only conftest: it registers the #184 behaviour-corpus guard, nothing else.

There is deliberately no fixture, no path manipulation and no import here — every test
module in this repo does its own ``sys.path`` insert, and that stays true. ``pytest_plugins``
is only legal in a ROOTDIR conftest, and a plugin declared in one test module loads only when
that module is collected: with the declaration there, a developer running
``pytest tests/test_issue_184_child_state_transfer.py`` alone got a green run while the
packaged corpus was stale, and only CI (which runs the whole suite) refused it
(CDX-184-r5-CORPUS-06). Here the guard is on for every run under ``tests/``.

The plugin itself does nothing until a COVERED module (``_revision_corpus.covered_targets``)
is collected: it imports the package and reads the corpus only then, so a run that collects
no covered test pays nothing for this file.
"""

pytest_plugins = ["_revision_corpus"]
