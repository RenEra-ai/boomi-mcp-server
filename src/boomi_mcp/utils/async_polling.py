"""
Shared async polling helper for Boomi MCP tools.

Many Boomi API operations are asynchronous: the initial call returns an
async token, and the caller must poll a token endpoint until the result
is ready or a timeout is reached.  This module centralises that logic.
"""

import time
from typing import Any, Callable, Optional


def poll_async_result(
    initiate_fn: Callable[[], Any],
    poll_fn: Callable[[str], Any],
    timeout: int = 60,
    interval: int = 2,
    resource_label: str = "async operation",
) -> dict:
    """Execute an async Boomi operation and poll for the result.

    Args:
        initiate_fn: Callable that initiates the async operation.
            Must return an object with ``.async_token.token``.
        poll_fn: Callable(token: str) that polls for the result.
            Should return the final result when ready, or raise/return
            an in-progress indicator while still processing.
        timeout: Maximum seconds to wait before giving up.
        interval: Seconds between poll attempts.
        resource_label: Human-readable label for error messages.

    Returns:
        On success: the raw result object from poll_fn.
        On timeout: raises a TimeoutError.
        On failure during initiation: propagates the original exception.
    """
    token_result = initiate_fn()

    if not hasattr(token_result, "async_token") or not token_result.async_token:
        raise ValueError(f"Failed to get async token for {resource_label}")

    token = token_result.async_token.token

    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = poll_fn(token)

            # Some endpoints return a result with a list of items
            if hasattr(response, "result") and response.result is not None:
                return response

            # Some endpoints return a single object directly
            if response is not None:
                # Check for 202 / still-processing status codes
                status = getattr(response, "response_status_code", None)
                if status and status != 200:
                    time.sleep(interval)
                    continue
                return response

        except Exception as poll_error:
            err_str = str(poll_error).lower()
            if "202" in err_str or "still processing" in err_str or "not ready" in err_str:
                time.sleep(interval)
                continue
            raise

        time.sleep(interval)

    raise TimeoutError(f"Timeout after {timeout}s waiting for {resource_label}")
