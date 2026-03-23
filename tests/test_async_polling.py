"""Tests for the shared async polling helper."""

import time
from unittest.mock import MagicMock, patch
import pytest

from boomi_mcp.utils.async_polling import poll_async_result


def _make_token_result(token="test-token-123"):
    """Create a mock async token result."""
    token_result = MagicMock()
    token_result.async_token.token = token
    return token_result


class TestPollAsyncResult:
    """Tests for poll_async_result."""

    def test_immediate_success(self):
        """Poll returns result on first attempt."""
        token_result = _make_token_result()
        expected = MagicMock()
        expected.result = [{"id": "1"}]

        result = poll_async_result(
            initiate_fn=lambda: token_result,
            poll_fn=lambda token: expected,
            timeout=10,
            interval=1,
        )
        assert result is expected

    def test_success_after_retries(self):
        """Poll returns None twice then succeeds."""
        token_result = _make_token_result()
        expected = MagicMock()
        expected.result = [{"id": "1"}]

        call_count = {"n": 0}
        def poll_fn(token):
            call_count["n"] += 1
            if call_count["n"] < 3:
                return None
            return expected

        with patch("boomi_mcp.utils.async_polling.time.sleep"):
            result = poll_async_result(
                initiate_fn=lambda: token_result,
                poll_fn=poll_fn,
                timeout=60,
                interval=1,
            )
        assert result is expected

    def test_timeout(self):
        """Raises TimeoutError when poll never succeeds."""
        token_result = _make_token_result()

        with patch("boomi_mcp.utils.async_polling.time.sleep"):
            with patch("boomi_mcp.utils.async_polling.time.time") as mock_time:
                # Simulate time progression past timeout
                mock_time.side_effect = [0, 0, 5, 10, 15, 20, 25, 30, 35]
                with pytest.raises(TimeoutError, match="Timeout after 30s"):
                    poll_async_result(
                        initiate_fn=lambda: token_result,
                        poll_fn=lambda token: None,
                        timeout=30,
                        interval=2,
                        resource_label="test operation",
                    )

    def test_no_async_token(self):
        """Raises ValueError when initiate returns no token."""
        bad_result = MagicMock(spec=[])  # no async_token attr

        with pytest.raises(ValueError, match="Failed to get async token"):
            poll_async_result(
                initiate_fn=lambda: bad_result,
                poll_fn=lambda token: None,
                timeout=10,
            )

    def test_null_async_token(self):
        """Raises ValueError when async_token is None."""
        bad_result = MagicMock()
        bad_result.async_token = None

        with pytest.raises(ValueError, match="Failed to get async token"):
            poll_async_result(
                initiate_fn=lambda: bad_result,
                poll_fn=lambda token: None,
                timeout=10,
            )

    def test_still_processing_exception_retries(self):
        """Retries when poll raises 'still processing' exception."""
        token_result = _make_token_result()
        expected = MagicMock()
        expected.result = [{"id": "1"}]

        call_count = {"n": 0}
        def poll_fn(token):
            call_count["n"] += 1
            if call_count["n"] < 3:
                raise Exception("Request is still processing")
            return expected

        with patch("boomi_mcp.utils.async_polling.time.sleep"):
            result = poll_async_result(
                initiate_fn=lambda: token_result,
                poll_fn=poll_fn,
                timeout=60,
                interval=1,
            )
        assert result is expected

    def test_202_exception_retries(self):
        """Retries when poll raises exception containing '202'."""
        token_result = _make_token_result()
        expected = MagicMock()
        expected.result = [{"id": "1"}]

        call_count = {"n": 0}
        def poll_fn(token):
            call_count["n"] += 1
            if call_count["n"] < 2:
                raise Exception("HTTP 202 Accepted")
            return expected

        with patch("boomi_mcp.utils.async_polling.time.sleep"):
            result = poll_async_result(
                initiate_fn=lambda: token_result,
                poll_fn=poll_fn,
                timeout=60,
                interval=1,
            )
        assert result is expected

    def test_non_retryable_exception_propagates(self):
        """Non-retryable exceptions propagate immediately."""
        token_result = _make_token_result()

        def poll_fn(token):
            raise RuntimeError("Connection refused")

        with pytest.raises(RuntimeError, match="Connection refused"):
            poll_async_result(
                initiate_fn=lambda: token_result,
                poll_fn=poll_fn,
                timeout=60,
            )

    def test_single_object_response(self):
        """Returns single object response (no .result attribute)."""
        token_result = _make_token_result()
        expected = MagicMock(spec=["some_field"])
        expected.some_field = "value"
        # No .result attribute, but response is not None

        result = poll_async_result(
            initiate_fn=lambda: token_result,
            poll_fn=lambda token: expected,
            timeout=10,
        )
        assert result is expected

    def test_passes_token_to_poll_fn(self):
        """Verifies the token from initiate is passed to poll_fn."""
        token_result = _make_token_result("my-special-token")
        captured_tokens = []

        expected = MagicMock()
        expected.result = ["data"]

        def poll_fn(token):
            captured_tokens.append(token)
            return expected

        poll_async_result(
            initiate_fn=lambda: token_result,
            poll_fn=poll_fn,
            timeout=10,
        )
        assert captured_tokens == ["my-special-token"]
