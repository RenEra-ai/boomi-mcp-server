#!/usr/bin/env python3
"""Bake the corpus's exact embedding model into the image's HF cache.

Thin CLI over boomi_mcp.kb.embedding_contract.preload_model — the SAME
resolver the runtime warmup uses, run against the downloaded corpus manifest.
Invoked by the Dockerfile (network-online stage, before the offline flags are
set) so the runtime's offline model lookup is a guaranteed cache hit for the
pinned revision.
"""
import argparse
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Preload the KB embedding model resolved from the corpus manifest"
    )
    parser.add_argument(
        "--db-path", default="/app/kb/boomi_knowledge_db",
        help="Corpus directory containing manifest.json",
    )
    args = parser.parse_args()

    from boomi_mcp.kb.embedding_contract import preload_model

    try:
        contract = preload_model(args.db_path)
    except Exception as e:
        print(f"[ERROR] KB model preload failed: {e}")
        return 1
    print(
        f"[INFO] KB model preloaded: {contract.model_id} "
        f"revision={contract.revision} (source={contract.source})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
