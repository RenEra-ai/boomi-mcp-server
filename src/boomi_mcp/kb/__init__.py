"""Boomi documentation knowledge base retrieval (optional, feature-gated).

Importing this package is cheap and safe. The heavy chromadb /
sentence-transformers dependencies are imported only inside
``boomi_mcp.kb.service.build_kb_service``, which ``server.py`` calls solely
when ``BOOMI_DOCS_ENABLED`` is true. Do not add ``from .service import ...``
here — that would pull the ML stack in on every server start.
"""
