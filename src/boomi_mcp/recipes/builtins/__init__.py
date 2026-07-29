"""Built-in recipes. Exported as a STATIC catalog — no package scanning.

Importing this package must not discover anything: what is registered is exactly
what ``catalog.PRODUCTION_REGISTRATIONS`` lists, so the registry revision is a
property of the code rather than of the filesystem.
"""

from .catalog import PRODUCTION_REGISTRATIONS

__all__ = ["PRODUCTION_REGISTRATIONS"]
