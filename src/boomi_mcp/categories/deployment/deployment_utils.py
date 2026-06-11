"""Shared deployment helpers: environment-account signal detection and atom-attachment
deprecation metadata.

Pure-Python on purpose (no SDK imports) so it is import-safe from both the ``boomi_mcp.*``
and ``src.boomi_mcp.*`` namespaces.
"""

from typing import Any, Dict, Optional

ENVIRONMENT_ACCOUNT_ATOM_ATTACHMENT_UNSUPPORTED = "ENVIRONMENT_ACCOUNT_ATOM_ATTACHMENT_UNSUPPORTED"
DEPRECATED_ATOM_ATTACHMENT_ACTION = "DEPRECATED_ATOM_ATTACHMENT_ACTION"

# Boomi rejects a direct process<->atom (ProcessAtomAttachment) binding on environment-enabled
# accounts, surfacing a message that names "environments" / "ComponentEnvironmentAttachment"
# (e.g. "This account uses environments. Please use ComponentEnvironmentAttachment"). On such
# accounts the process<->environment binding plus the runtime<->environment binding already make
# the process runnable on the runtime via the environment, so the direct process<->runtime leg is
# not required and must NOT be treated as a failure. Detect the signal narrowly so any other
# list/attach failure still fails closed.
_ENVIRONMENT_ACCOUNT_SIGNALS = ("uses environments", "componentenvironmentattachment")


def is_environment_account_signal(message: Optional[str]) -> bool:
    """True when an error indicates the account uses environments (no direct atom attach)."""
    if not message:
        return False
    lowered = message.lower()
    return any(signal in lowered for signal in _ENVIRONMENT_ACCOUNT_SIGNALS)


_ATOM_ATTACHMENT_REPLACEMENTS = {
    "attach_component_atom": [
        "attach_component_environment", "manage_runtimes(action='attach')",
    ],
    "detach_component_atom": [
        "detach_component_environment", "manage_runtimes(action='detach')",
    ],
    "list_component_atom_attachments": [
        "list_component_environment_attachments", "manage_runtimes(action='list_attachments')",
    ],
    "attach_process_atom": [
        "attach_process_environment", "manage_runtimes(action='attach')",
    ],
    "detach_process_atom": [
        "detach_process_environment", "manage_runtimes(action='detach')",
    ],
    "list_process_atom_attachments": [
        "list_process_environment_attachments", "manage_runtimes(action='list_attachments')",
    ],
}

_ATOM_DEPRECATION_NOTE = (
    "Direct atom attachments are deprecated: environment-enabled accounts reject attach_* "
    "and return misleadingly empty list_* results. Use the environment-attachment actions "
    "for the component/process<->environment leg and manage_runtimes for the "
    "runtime<->environment leg."
)


def atom_attachment_deprecation_metadata(action: str) -> Dict[str, Any]:
    """Deprecation metadata for the six direct atom-attachment actions; {} for any other."""
    replacements = _ATOM_ATTACHMENT_REPLACEMENTS.get(action)
    if not replacements:
        return {}
    return {
        "deprecated": True,
        "deprecation": {
            "error_code": DEPRECATED_ATOM_ATTACHMENT_ACTION,
            "deprecated_action": action,
            "replacement_actions": list(replacements),
            "note": _ATOM_DEPRECATION_NOTE,
        },
    }


def environment_account_remediation(action: str) -> Optional[str]:
    """Action-specific remediation for an env-account rejection of a direct atom action.

    Environment-enabled accounts reject not just attach_*_atom creates but also the
    list_*_atom_attachments queries themselves, so every atom action needs a working-path
    pointer. Returns None for non-atom actions.
    """
    replacements = _ATOM_ATTACHMENT_REPLACEMENTS.get(action)
    if not replacements:
        return None
    return (
        f"This account uses environments; direct atom attachments are not supported. "
        f"Use {replacements[0]} for the component/process<->environment leg and "
        f"{replacements[1]} for the runtime<->environment leg."
    )
