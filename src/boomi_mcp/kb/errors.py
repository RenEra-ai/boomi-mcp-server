"""Narrow exception types for the Boomi docs knowledge base."""


class KbStartupError(Exception):
    """Raised when the KB corpus cannot be loaded or validated at startup.

    server.py catches this, logs it, and exits the process — a server
    configured with BOOMI_DOCS_ENABLED=true must not run with a broken corpus.
    """


class KbQueryError(Exception):
    """Raised when a Chroma query fails at request time.

    The tool layer catches this and converts it into a structured error
    response so it never propagates out of an MCP tool call.
    """
