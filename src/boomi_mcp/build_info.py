"""Trusted build/source provenance (issue #145 M12.10).

The recipe registry needs to answer one question honestly: *is the code running
here the code the checkout has?* Semantic versions cannot answer it — two
registries can agree on ``api_to_api_sync@0.1.0`` and run different bytes, which
is exactly the skew the issue calls out. So provenance is derived from the code
itself, never from a request field.

Two sources, in priority order:

1. **The image build revision.** Cloud Build passes ``$COMMIT_SHA`` as a Docker
   build argument; the Dockerfile writes it to a read-only file in the image.
   Reading a file the build wrote is trustworthy in a way that reading a request
   header or shelling out to ``git`` at runtime is not — the deployed container
   has no ``.git`` directory, and a caller can set a header.
2. **A source digest**, for local and test execution, where no image exists.
   ``source-sha256:<hex>`` over the exact source of the registered recipe
   modules. It is not a commit id and never pretends to be one — the prefix says
   so — but it *does* change whenever the registered code changes, which is the
   property skew detection actually needs.

Pure-stdlib on purpose (no pydantic, no SDK), so it is import-safe from both the
``boomi_mcp.*`` and ``src.boomi_mcp.*`` namespaces — the same rule ``errors.py``
follows.
"""

from __future__ import annotations

import hashlib
import importlib
import inspect
import os
import re
from typing import Iterable, Mapping, Optional, Tuple

PACKAGE_NAME = "boomi_mcp"

#: Where the Dockerfile writes the build revision. Read-only in the image.
BUILD_REVISION_PATH = "/app/BUILD_REVISION"

#: A build revision is a git object id: hex, 7-40 characters. Validated on the
#: way IN (in the Dockerfile) and again on the way OUT, so a malformed file
#: cannot become provenance a skew comparison would then treat as authoritative.
_REVISION_RE = re.compile(r"^[0-9a-f]{7,40}$")

_SOURCE_DIGEST_PREFIX = "source-sha256:"


def package_version() -> str:
    """The installed package version."""
    from . import __version__

    return __version__


def image_build_revision(path: Optional[str] = None) -> Optional[str]:
    """The build revision written into the image, or ``None`` outside one.

    ``None`` is the honest answer for a source checkout, and callers must treat
    it that way rather than substituting a plausible-looking stand-in. Any read
    or shape failure is also ``None``: a half-read or malformed revision is not
    weaker evidence, it is *no* evidence, and reporting it as a commit id would
    make a skew comparison confidently wrong.
    """
    target = path or BUILD_REVISION_PATH
    try:
        with open(target, "r", encoding="utf-8") as handle:
            value = handle.read().strip()
    except OSError:
        return None
    if not _REVISION_RE.match(value):
        return None
    return value


def source_digest(modules: Iterable[str]) -> str:
    """``source-sha256:<hex>`` over the exact source of the named modules.

    Sorted by module name so the digest does not depend on iteration or import
    order. Fails CLOSED: if a module cannot be imported or its source cannot be
    read (a zipimport, a stripped deployment), this raises rather than hashing a
    shorter list — a digest that silently omits a module would report ``match``
    for two registries whose code differs in exactly that module.
    """
    hasher = hashlib.sha256()
    for name in sorted(set(modules)):
        module = importlib.import_module(name)
        try:
            source = inspect.getsource(module)
        except (OSError, TypeError) as exc:  # pragma: no cover - environment
            raise RuntimeError(
                f"recipe provenance requires readable source for {name!r}"
            ) from exc
        hasher.update(name.encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(source.encode("utf-8"))
        hasher.update(b"\0")
    return _SOURCE_DIGEST_PREFIX + hasher.hexdigest()


def source_revision(modules: Iterable[str], *, path: Optional[str] = None) -> str:
    """The image build revision if there is one, else the source digest."""
    revision = image_build_revision(path)
    if revision is not None:
        return revision
    return source_digest(modules)


def is_source_digest(value: str) -> bool:
    """Whether a revision string is a local source digest rather than a commit."""
    return value.startswith(_SOURCE_DIGEST_PREFIX)


def build_metadata(
    modules: Iterable[str], *, path: Optional[str] = None
) -> Mapping[str, str]:
    """The provenance block published on discovery/capability responses."""
    revision = source_revision(modules, path=path)
    return {
        "package_name": PACKAGE_NAME,
        "package_version": package_version(),
        "source_revision": revision,
        "source_revision_kind": "source_digest" if is_source_digest(revision) else "build_revision",
    }


def implementation_digest(parts: Tuple[str, ...]) -> str:
    """SHA-256 over an ordered tuple of already-canonical strings.

    Length-prefixed rather than concatenated: ``("ab", "c")`` and ``("a", "bc")``
    must not hash alike, or two different registrations could claim one
    implementation hash.
    """
    hasher = hashlib.sha256()
    for part in parts:
        encoded = part.encode("utf-8")
        hasher.update(str(len(encoded)).encode("ascii"))
        hasher.update(b":")
        hasher.update(encoded)
    return hasher.hexdigest()


__all__ = [
    "BUILD_REVISION_PATH",
    "PACKAGE_NAME",
    "build_metadata",
    "image_build_revision",
    "implementation_digest",
    "is_source_digest",
    "package_version",
    "source_digest",
    "source_revision",
]
