"""Error message sanitization to prevent information leaks."""

import re


def sanitize_error_msg(msg: str) -> str:
    """Strip URLs and file paths from error messages."""
    msg = re.sub(r'https?://[^\s\'")\]}>]+', '<redacted-url>', msg)
    msg = re.sub(r'(/[a-zA-Z0-9_./-]{3,})', '<redacted-path>', msg)
    return msg
