#!/usr/bin/env python3
"""
Re-encrypt all OAuth client documents under the newest STORAGE_ENCRYPTION_KEY.

Why this exists:
    MultiFernet (used by Fix C.3 of the OAuth hardening plan) decrypts under
    any listed key but only re-encrypts on PUT. The `mcp-oauth-proxy-clients`
    collection holds long-lived Dynamic Client Registration (DCR) documents
    that are written exactly once at registration and never naturally
    rewritten by token refresh, runtime CIMD lookup notwithstanding.

    Without this script, dropping OLD_KEY from STORAGE_ENCRYPTION_KEY after
    the documented 30-day wait will still 401 any DCR client whose
    registration document was written before the rotation -- with Fix C.2
    enabled, the corrupted doc gets self-healed (deleted) and the user is
    forced into Dynamic Client Registration again.

What it does:
    For every document in `mcp-oauth-proxy-clients`:
      1. GET via FernetEncryptionWrapper backed by MultiFernet (succeeds
         under either the new or the old key).
      2. PUT via the same wrapper (forces re-encryption under the first
         key in the list = the newest key).
    Reports counts: total / re-wrapped / decrypt_failed / unchanged.

When to run:
    Run after rotating STORAGE_ENCRYPTION_KEY to the comma-separated
    "NEW,OLD" form and BEFORE dropping OLD. See
    docs/oauth-migration-runbook.md "Token refresh hardening" section.

Usage (from project root with MONGODB_URI and STORAGE_ENCRYPTION_KEY set
either in .env or in the environment directly):
    .venv/bin/python scripts/rewrap_oauth_clients.py [--dry-run] [--collection <name>]

Exit code is 0 on a clean rewrap, 1 on partial failure.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ENV = _REPO_ROOT / ".env"
if _ENV.exists():
    try:
        from dotenv import load_dotenv

        load_dotenv(_ENV)
    except ImportError:
        pass

DB_NAME = "boomi_mcp"
DEFAULT_COLLECTION = "mcp-oauth-proxy-clients"


def _build_fernet(storage_encryption_key: str):
    """Mirrors the parsing logic in server.py."""
    from cryptography.fernet import Fernet, MultiFernet

    keys = [k.strip() for k in storage_encryption_key.split(",") if k.strip()]
    if not keys:
        raise SystemExit("STORAGE_ENCRYPTION_KEY must contain at least one Fernet key")
    fernets = [Fernet(k.encode()) for k in keys]
    return fernets[0] if len(fernets) == 1 else MultiFernet(fernets), len(fernets)


async def _enumerate_keys(mongodb_uri: str, collection: str) -> list[str]:
    """Return all document keys in the given collection."""
    from motor.motor_asyncio import AsyncIOMotorClient

    client = AsyncIOMotorClient(mongodb_uri)
    try:
        db = client[DB_NAME]
        cursor = db[collection].find({}, projection={"key": 1, "_id": 0})
        keys = []
        async for doc in cursor:
            key = doc.get("key")
            if key:
                keys.append(key)
        return keys
    finally:
        client.close()


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--dry-run", action="store_true", help="Read each doc but do not write back")
    parser.add_argument("--collection", default=DEFAULT_COLLECTION,
                        help=f"Collection to rewrap (default: {DEFAULT_COLLECTION})")
    args = parser.parse_args()

    mongodb_uri = os.getenv("MONGODB_URI")
    storage_encryption_key = os.getenv("STORAGE_ENCRYPTION_KEY")
    if not mongodb_uri:
        print("ERROR: MONGODB_URI not set", file=sys.stderr)
        return 1
    if not storage_encryption_key:
        print("ERROR: STORAGE_ENCRYPTION_KEY not set", file=sys.stderr)
        return 1

    fernet, key_count = _build_fernet(storage_encryption_key)
    print(f"Loaded {key_count} Fernet key(s); writes will use the newest key.")
    if key_count == 1:
        print("WARNING: only one key configured. Rewrap is a no-op (same key in/out).")

    keys = await _enumerate_keys(mongodb_uri, args.collection)
    print(f"Found {len(keys)} document(s) in {args.collection}")
    if not keys:
        return 0

    from key_value.aio.stores.mongodb import MongoDBStore
    from key_value.aio.wrappers.encryption import FernetEncryptionWrapper

    store = MongoDBStore(url=mongodb_uri, db_name=DB_NAME, coll_name=args.collection)
    wrapped = FernetEncryptionWrapper(key_value=store, fernet=fernet)

    total = len(keys)
    rewrapped = 0
    decrypt_failed: list[str] = []
    other_failed: list[tuple[str, str]] = []

    for key in keys:
        try:
            value = await wrapped.get(key=key)
        except Exception as exc:  # DecryptionError, DeserializationError, etc.
            decrypt_failed.append(key)
            print(f"  [FAIL decrypt] key={key[:16]}... {type(exc).__name__}: {exc}")
            continue
        if value is None:
            # The wrapper returned None (e.g. doc was deleted between
            # enumerate and read). Skip silently.
            continue
        if args.dry_run:
            rewrapped += 1
            continue
        try:
            await wrapped.put(key=key, value=value)
            rewrapped += 1
        except Exception as exc:
            other_failed.append((key, f"{type(exc).__name__}: {exc}"))
            print(f"  [FAIL write]   key={key[:16]}... {type(exc).__name__}: {exc}")

    action = "would re-encrypt" if args.dry_run else "re-encrypted"
    print()
    print(f"Summary: total={total} {action}={rewrapped} "
          f"decrypt_failed={len(decrypt_failed)} other_failed={len(other_failed)}")
    if decrypt_failed:
        print("Keys that failed to decrypt under ANY listed key:")
        for k in decrypt_failed:
            print(f"  {k}")
        print("These are unrecoverable. Either restore the missing historical key "
              "and re-run, or delete the docs (the clients will re-register on next DCR).")

    return 0 if not decrypt_failed and not other_failed else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
