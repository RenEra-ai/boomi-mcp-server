#!/usr/bin/env python3
"""
Extend durable refresh-token recovery alias TTLs to the 30-day window.

Why this exists:
    The durable recovery alias ledger (`mcp-rt-recovery`, written by
    `rt_recovery_backend` / `refresh_token_recovery_patch`) originally capped each
    alias TTL at 7 days (BOOMI_RT_RECOVERY_MAX_AGE_SECONDS=604800). The auth
    hardening follow-up raises that cap to 30 days (2592000) to match the sliding
    refresh-token lifetime -- but a Mongo TTL is fixed at write time, so existing
    aliases keep their original 7-day expiry and get swept early. A client
    returning after 8-30 days would still fail recovery until natural re-rotation
    writes a fresh 30-day alias.

What it does:
    Enumerates every alias key in the recovery collection, reads it through the
    SAME MongoDBStore + FernetEncryptionWrapper + STORAGE_ENCRYPTION_KEY stack as
    server.py, and (with --apply) re-writes each decryptable, non-expired alias
    with TTL min(BOOMI_RT_RECOVERY_MAX_AGE_SECONDS, successor_expires_at - now).
    Defaults to a DRY RUN that only reports counts.

    Never prints token values or full token hashes -- only a non-reversible
    fingerprint of each alias key.

Usage (from project root with MONGODB_URI and STORAGE_ENCRYPTION_KEY set in .env
or directly in the environment):
    .venv/bin/python scripts/extend_rt_recovery_alias_ttl.py            # dry run
    .venv/bin/python scripts/extend_rt_recovery_alias_ttl.py --apply    # write

Exit code is 0 on a clean run, 1 on a missing prerequisite or any write failure.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ENV = _REPO_ROOT / ".env"
if _ENV.exists():
    try:
        from dotenv import load_dotenv

        load_dotenv(_ENV)
    except ImportError:
        pass

# Make `rt_recovery_backend` importable when the script is run from anywhere.
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from rt_recovery_backend import (  # noqa: E402 — after sys.path/.env setup
    DEFAULT_RECOVERY_COLLECTION,
    alias_ttl_seconds,
)

DB_NAME = "boomi_mcp"
_THIRTY_DAYS = 60 * 60 * 24 * 30  # 2592000; matches the recovery max-age default


def _build_fernet(storage_encryption_key: str):
    """Mirror server.py's STORAGE_ENCRYPTION_KEY parsing (newest key first)."""
    from cryptography.fernet import Fernet, MultiFernet

    keys = [k.strip() for k in storage_encryption_key.split(",") if k.strip()]
    if not keys:
        raise SystemExit("STORAGE_ENCRYPTION_KEY must contain at least one Fernet key")
    fernets = [Fernet(k.encode()) for k in keys]
    return (fernets[0] if len(fernets) == 1 else MultiFernet(fernets)), len(fernets)


def _fingerprint(key: str) -> str:
    """Non-reversible short id for logs -- never the raw alias key/token hash."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:8]


def _default_collection() -> str:
    """The recovery collection the server/backend uses (env-driven).

    rt_recovery_backend resolves the collection from BOOMI_RT_RECOVERY_COLLECTION,
    so the script must too -- hardcoding the constant would silently scan the
    wrong collection on deployments that override it.
    """
    return os.getenv("BOOMI_RT_RECOVERY_COLLECTION", DEFAULT_RECOVERY_COLLECTION)


async def _enumerate_keys(mongodb_uri: str, collection: str) -> list[str]:
    """Return all alias document keys in the recovery collection."""
    from motor.motor_asyncio import AsyncIOMotorClient

    client = AsyncIOMotorClient(mongodb_uri)
    try:
        db = client[DB_NAME]
        cursor = db[collection].find({}, projection={"key": 1, "_id": 0})
        keys: list[str] = []
        async for doc in cursor:
            key = doc.get("key")
            if key:
                keys.append(key)
        return keys
    finally:
        client.close()


async def extend_aliases(
    wrapped, keys, *, max_age: int, apply: bool, now: float | None = None
) -> dict:
    """Read each alias and extend the live ones. Returns a counts dict.

    The buckets partition the scan:
        total_scanned == would_extend + skipped_expired + skipped_malformed
        decryptable   == would_extend + skipped_expired   (have a usable
                         successor_expires_at)
    On --apply, ``extended`` counts successful re-writes and ``write_failed``
    counts put errors. ``skipped_malformed`` folds in decryption/read failures,
    deleted docs, and records lacking a usable ``successor_expires_at``.

    Never logs token values or full hashes -- only a key fingerprint.
    """
    if now is None:
        now = time.time()
    counts = {
        "total_scanned": len(keys),
        "decryptable": 0,
        "skipped_expired": 0,
        "skipped_malformed": 0,
        "would_extend": 0,
        "extended": 0,
        "write_failed": 0,
    }
    for key in keys:
        try:
            value = await wrapped.get(key=key)
        except Exception as exc:  # noqa: BLE001 — DecryptionError/Deserialization/etc.
            counts["skipped_malformed"] += 1
            print(f"  [skip malformed] alias={_fingerprint(key)} ({type(exc).__name__})")
            continue
        if value is None:
            # Deleted or TTL-swept between enumerate and read.
            counts["skipped_malformed"] += 1
            continue
        successor_expires_at = value.get("successor_expires_at") if isinstance(value, dict) else None
        # Must be a positive real number: a corrupted record could carry a
        # string/object/bool, which would raise on the comparison/arithmetic
        # below. Treat anything non-numeric or non-positive as malformed.
        if (
            not isinstance(successor_expires_at, (int, float))
            or isinstance(successor_expires_at, bool)
            or successor_expires_at <= 0
        ):
            counts["skipped_malformed"] += 1
            print(f"  [skip malformed] alias={_fingerprint(key)} (missing/invalid successor_expires_at)")
            continue
        counts["decryptable"] += 1
        if successor_expires_at <= now:
            counts["skipped_expired"] += 1
            continue
        counts["would_extend"] += 1
        if not apply:
            continue
        new_ttl = alias_ttl_seconds(successor_expires_at, max_age, now)
        try:
            record = dict(value)
            record["updated_at"] = now
            await wrapped.put(key=key, value=record, ttl=new_ttl)
            counts["extended"] += 1
        except Exception as exc:  # noqa: BLE001 — surface but keep going
            counts["write_failed"] += 1
            print(f"  [write failed]  alias={_fingerprint(key)} ({type(exc).__name__})")
    return counts


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually re-write alias TTLs. Without this flag the script only reports (dry run).",
    )
    parser.add_argument(
        "--collection",
        default=_default_collection(),
        help=(
            "Recovery alias collection. Defaults to $BOOMI_RT_RECOVERY_COLLECTION "
            f"or {DEFAULT_RECOVERY_COLLECTION}, matching server.py."
        ),
    )
    args = parser.parse_args()

    mongodb_uri = os.getenv("MONGODB_URI")
    storage_encryption_key = os.getenv("STORAGE_ENCRYPTION_KEY")
    if not mongodb_uri:
        print("ERROR: MONGODB_URI not set", file=sys.stderr)
        return 1
    if not storage_encryption_key:
        print("ERROR: STORAGE_ENCRYPTION_KEY not set", file=sys.stderr)
        return 1

    max_age = int(os.getenv("BOOMI_RT_RECOVERY_MAX_AGE_SECONDS", str(_THIRTY_DAYS)))
    fernet, key_count = _build_fernet(storage_encryption_key)
    mode = "APPLY" if args.apply else "DRY RUN"
    print(
        f"[{mode}] recovery alias TTL extension "
        f"(collection={args.collection}, max_age={max_age}s, fernet_keys={key_count})"
    )

    keys = await _enumerate_keys(mongodb_uri, args.collection)
    print(f"Found {len(keys)} alias document(s) in {args.collection}")
    if not keys:
        return 0

    from key_value.aio.stores.mongodb import MongoDBStore
    from key_value.aio.wrappers.encryption import FernetEncryptionWrapper

    store = MongoDBStore(url=mongodb_uri, db_name=DB_NAME, default_collection=args.collection)
    wrapped = FernetEncryptionWrapper(key_value=store, fernet=fernet)

    counts = await extend_aliases(wrapped, keys, max_age=max_age, apply=args.apply)

    print()
    print(
        "Summary: total_scanned={total_scanned} decryptable={decryptable} "
        "would_extend={would_extend} skipped_expired={skipped_expired} "
        "skipped_malformed={skipped_malformed} extended={extended} "
        "write_failed={write_failed}".format(**counts)
    )
    if not args.apply:
        print("Dry run: no aliases were modified. Re-run with --apply to extend them.")
    return 1 if counts["write_failed"] else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
