#!/usr/bin/env python3
"""
Diagnostic script: inspect OAuth storage in MongoDB Atlas.

Connects to MongoDB, lists documents in all OAuth-related collections,
attempts Fernet decryption, and reports the state of stored data.

Usage (from project root, with .env containing MONGODB_URI and STORAGE_ENCRYPTION_KEY):
    python scripts/diagnose_oauth_storage.py

Or pass env vars directly:
    MONGODB_URI=... STORAGE_ENCRYPTION_KEY=... python scripts/diagnose_oauth_storage.py

Added 2026-04-11 to investigate token refresh 401 failures.
"""

import asyncio
import base64
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Load .env if available
env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
    from dotenv import load_dotenv
    load_dotenv(env_path)


MONGODB_URI = os.getenv("MONGODB_URI")
STORAGE_ENCRYPTION_KEY = os.getenv("STORAGE_ENCRYPTION_KEY")
DB_NAME = "boomi_mcp"

COLLECTIONS = [
    # New upstream v3.1.1 collection names (hyphenated)
    "mcp-oauth-proxy-clients",
    "mcp-upstream-tokens",
    "mcp-jti-mappings",
    "mcp-authorization-codes",
    "mcp-oauth-transactions",
    "mcp-refresh-tokens",
    # Refresh-token grace cache + distributed lock (Fix D / D.2)
    "mcp-rt-grace",
    "mcp-rt-inflight-locks",
    # Old vendored FastMCP collection names (underscore + hash suffix)
    "mcp_oauth_proxy_clients-4db71f6a",
    "mcp_upstream_tokens-064b3cac",
    "mcp_jti_mappings-a0131f3f",
    "mcp_authorization_codes-62ca573a",
    "mcp_oauth_transactions-6f3deda7",
    # Catch-all collection a key_value op falls back to when given no
    # collection -- should stay empty once grace-cache routing is fixed.
    "default_collection",
]


def try_decrypt(encrypted_data_b64: str, fernet) -> tuple[bool, str]:
    """Attempt to decrypt a base64-encoded Fernet ciphertext."""
    try:
        encrypted_bytes = base64.b64decode(encrypted_data_b64)
        decrypted = fernet.decrypt(encrypted_bytes)
        data = json.loads(decrypted)
        # Sanitize: remove actual secret values
        sanitized = {}
        for k, v in data.items():
            if any(s in k.lower() for s in ("secret", "token", "key", "password")):
                sanitized[k] = f"[REDACTED len={len(str(v))}]"
            elif isinstance(v, str) and len(v) > 50:
                sanitized[k] = v[:20] + "..." + v[-10:]
            else:
                sanitized[k] = v
        return True, json.dumps(sanitized, indent=2, default=str)
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


async def expiry_spread(db, coll_name, now):
    """Aggregate TTL summary for one collection (read-only)."""
    expiries = []
    async for doc in db[coll_name].find({}, {"expires_at": 1}):
        ea = doc.get("expires_at")
        if isinstance(ea, datetime):
            if ea.tzinfo is None:
                ea = ea.replace(tzinfo=timezone.utc)
            expiries.append(ea)
    if not expiries:
        print(f"  {coll_name}: no expires_at timestamps")
        return
    expiries.sort()
    past = sum(1 for e in expiries if e < now)
    soon24 = sum(1 for e in expiries if now <= e < now + timedelta(hours=24))
    soon72 = sum(1 for e in expiries if now <= e < now + timedelta(hours=72))
    print(
        f"  {coll_name}: {len(expiries)} docs | "
        f"earliest={expiries[0].isoformat()} latest={expiries[-1].isoformat()} | "
        f"expired={past} <24h={soon24} <72h={soon72}"
    )


async def main():
    if not MONGODB_URI:
        print("ERROR: MONGODB_URI not set. Load from .env or pass as env var.")
        sys.exit(1)
    if not STORAGE_ENCRYPTION_KEY:
        print("ERROR: STORAGE_ENCRYPTION_KEY not set. Load from .env or pass as env var.")
        sys.exit(1)

    from cryptography.fernet import Fernet
    from motor.motor_asyncio import AsyncIOMotorClient

    fernet = Fernet(STORAGE_ENCRYPTION_KEY.encode())
    client = AsyncIOMotorClient(MONGODB_URI)
    db = client[DB_NAME]

    import hashlib
    key_fingerprint = hashlib.sha256(STORAGE_ENCRYPTION_KEY.encode()).hexdigest()[:12]
    print(f"Connected to MongoDB: {DB_NAME}")
    print(f"Encryption key loaded (sha256:{key_fingerprint})")
    print()

    # List all collections in the database
    existing_collections = await db.list_collection_names()
    print(f"Collections in {DB_NAME}: {existing_collections}")
    print()

    now = datetime.now(timezone.utc)

    for coll_name in COLLECTIONS:
        if coll_name not in existing_collections:
            print(f"=== {coll_name} === (does not exist)")
            print()
            continue

        coll = db[coll_name]
        count = await coll.count_documents({})
        print(f"=== {coll_name} === ({count} documents)")

        # Check indexes
        indexes = await coll.index_information()
        for idx_name, idx_info in indexes.items():
            if "expireAfterSeconds" in idx_info:
                print(f"  TTL index: {idx_name} -> expireAfterSeconds={idx_info['expireAfterSeconds']}")

        async for doc in coll.find().limit(20):
            key = doc.get("key", "?")
            key_display = key[:16] + "..." if len(str(key)) > 16 else key
            created_at = doc.get("created_at")
            expires_at = doc.get("expires_at")

            # Check TTL status
            ttl_status = ""
            if expires_at:
                if isinstance(expires_at, datetime):
                    if expires_at.tzinfo is None:
                        expires_at = expires_at.replace(tzinfo=timezone.utc)
                    remaining = (expires_at - now).total_seconds()
                    if remaining < 0:
                        ttl_status = f" EXPIRED {abs(remaining)/3600:.1f}h ago"
                    else:
                        ttl_status = f" expires in {remaining/3600:.1f}h"

            # Try to decrypt
            value = doc.get("value", {})
            encrypted_data = None
            if isinstance(value, dict):
                encrypted_data = value.get("__encrypted_data__")

            if encrypted_data:
                ok, detail = try_decrypt(encrypted_data, fernet)
                status = "OK" if ok else "DECRYPT_FAILED"
                print(f"  key={key_display} created={created_at}{ttl_status} [{status}]")
                if ok:
                    # Show key fields only
                    try:
                        parsed = json.loads(detail)
                        interesting = {k: v for k, v in parsed.items()
                                     if k in ("client_id", "client_name", "scope",
                                              "token_endpoint_auth_method", "grant_types",
                                              "redirect_uris", "upstream_token_id",
                                              "jti", "created_at", "expires_at")}
                        if interesting:
                            print(f"    {json.dumps(interesting, default=str)}")
                    except Exception:
                        pass
                else:
                    print(f"    {detail}")
            else:
                print(f"  key={key_display} created={created_at}{ttl_status} [NOT_ENCRYPTED]")
                # Show raw value (sanitized)
                sanitized = {}
                for k, v in (value.items() if isinstance(value, dict) else []):
                    if any(s in k.lower() for s in ("secret", "token", "key", "password")):
                        sanitized[k] = "[REDACTED]"
                    else:
                        sanitized[k] = v
                if sanitized:
                    print(f"    {json.dumps(sanitized, default=str)[:200]}")

        print()

    # --- Aggregate TTL health + grace-cache routing check (read-only) ---
    print("=== Expiry spread (TTL health) ===")
    for summary_coll in ("mcp-refresh-tokens", "mcp-upstream-tokens", "mcp-jti-mappings"):
        if summary_coll in existing_collections:
            await expiry_spread(db, summary_coll, now)
    print()

    # `default_collection` should stay empty once grace-cache routing is fixed
    # (rt_grace_shared_backend now passes default_collection=mcp-rt-grace).
    if "default_collection" in existing_collections:
        dc_count = await db["default_collection"].count_documents({})
        if dc_count:
            print(
                f"NOTE: 'default_collection' has {dc_count} doc(s) -- pre-fix "
                "grace-cache records, or another collection-less writer."
            )
        else:
            print("OK: 'default_collection' is empty.")
    print()

    client.close()
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
