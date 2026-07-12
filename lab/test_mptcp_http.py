import io
import json
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import mptcp_http


class MptcpHttpTests(unittest.TestCase):
    def test_duration_end_partials_are_valid_for_parallel_duration_cohort(self):
        args = SimpleNamespace(
            label="mptcp-parallel-test",
            target="127.0.0.1:8080",
            timeout=1.0,
            load_duration=1.0,
            interval_seconds=0.2,
            parallel_downloads=3,
        )

        def one_partial_request(_args, started, _deadline, state, lock):
            mptcp_http.record_chunk(started, state, lock, 100)
            return False, 200, "deadline"

        stdout = io.StringIO()
        with (
            mock.patch("mptcp_http.check_support"),
            mock.patch(
                "mptcp_http.download_once", side_effect=one_partial_request
            ) as download_once,
            redirect_stdout(stdout),
        ):
            exit_code = mptcp_http.download(args)

        row = json.loads(stdout.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(download_once.call_count, 3)
        self.assertEqual(row["parallel_downloads"], 3)
        self.assertEqual(row["requests"], 3)
        self.assertEqual(row["partial_requests"], 3)
        self.assertEqual(row["duration_end_partial_requests"], 3)
        self.assertEqual(row["premature_partial_requests"], 0)
        self.assertEqual(row["bytes"], 300)
        self.assertEqual(row["status"], "ok")

    def test_premature_eof_with_bytes_is_loss(self):
        args = SimpleNamespace(
            label="mptcp-premature-eof-test",
            target="127.0.0.1:8080",
            timeout=2.0,
            load_duration=1.0,
            interval_seconds=0.2,
            parallel_downloads=1,
        )

        def one_truncated_request(_args, started, _deadline, state, lock):
            mptcp_http.record_chunk(started, state, lock, 100)
            return False, 200, "eof"

        stdout = io.StringIO()
        with (
            mock.patch("mptcp_http.check_support"),
            mock.patch(
                "mptcp_http.download_once", side_effect=one_truncated_request
            ),
            redirect_stdout(stdout),
        ):
            exit_code = mptcp_http.download(args)

        row = json.loads(stdout.getvalue())
        self.assertEqual(exit_code, 1)
        self.assertEqual(row["status"], "loss")
        self.assertFalse(row["complete"])
        self.assertEqual(row["duration_end_partial_requests"], 0)
        self.assertEqual(row["premature_partial_requests"], 1)
        self.assertEqual(row["bytes"], 100)

    def test_timeout_before_requested_duration_is_not_a_valid_duration_end(self):
        args = SimpleNamespace(
            label="mptcp-timeout-cap-test",
            target="127.0.0.1:8080",
            timeout=1.0,
            load_duration=2.0,
            interval_seconds=0.2,
            parallel_downloads=1,
        )

        def one_timeout_request(_args, started, _deadline, state, lock):
            mptcp_http.record_chunk(started, state, lock, 100)
            return False, 200, "deadline"

        stdout = io.StringIO()
        with (
            mock.patch("mptcp_http.check_support"),
            mock.patch("mptcp_http.download_once", side_effect=one_timeout_request),
            redirect_stdout(stdout),
        ):
            exit_code = mptcp_http.download(args)

        row = json.loads(stdout.getvalue())
        self.assertEqual(exit_code, 1)
        self.assertEqual(row["status"], "loss")
        self.assertEqual(row["deadline_reason"], "timeout")
        self.assertEqual(row["premature_partial_requests"], 1)

    def test_redirect_is_non_success_http_status(self):
        args = SimpleNamespace(
            target="127.0.0.1:8080",
            path="/large.bin",
            timeout=1.0,
            chunk_bytes=4096,
        )
        sock = mock.MagicMock()
        sock.__enter__.return_value = sock
        sock.recv.return_value = (
            b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        )

        with mock.patch("mptcp_http.mptcp_socket", return_value=sock):
            result = mptcp_http.download_once(
                args,
                mptcp_http.time.monotonic(),
                mptcp_http.time.monotonic() + 1.0,
                {},
                mock.MagicMock(),
            )

        self.assertEqual(result, (False, 302, "http_status"))

    def test_non_success_http_status_is_failed_not_partial(self):
        args = SimpleNamespace(
            label="mptcp-http-status-test",
            target="127.0.0.1:8080",
            timeout=2.0,
            load_duration=1.0,
            interval_seconds=0.2,
            parallel_downloads=1,
        )

        stdout = io.StringIO()
        with (
            mock.patch("mptcp_http.check_support"),
            mock.patch(
                "mptcp_http.download_once",
                return_value=(False, 302, "http_status"),
            ),
            redirect_stdout(stdout),
        ):
            exit_code = mptcp_http.download(args)

        row = json.loads(stdout.getvalue())
        self.assertEqual(exit_code, 1)
        self.assertEqual(row["status"], "fail")
        self.assertEqual(row["requests"], 1)
        self.assertEqual(row["complete_requests"], 0)
        self.assertEqual(row["partial_requests"], 0)
        self.assertEqual(row["failed_requests"], 1)
        self.assertEqual(row["premature_partial_requests"], 0)

    def test_zero_byte_premature_eof_is_fail(self):
        args = SimpleNamespace(
            label="mptcp-zero-byte-eof-test",
            target="127.0.0.1:8080",
            timeout=2.0,
            load_duration=1.0,
            interval_seconds=0.2,
            parallel_downloads=1,
        )

        stdout = io.StringIO()
        with (
            mock.patch("mptcp_http.check_support"),
            mock.patch(
                "mptcp_http.download_once", return_value=(False, 200, "eof")
            ),
            redirect_stdout(stdout),
        ):
            exit_code = mptcp_http.download(args)

        row = json.loads(stdout.getvalue())
        self.assertEqual(exit_code, 1)
        self.assertEqual(row["status"], "fail")
        self.assertFalse(row["complete"])
        self.assertEqual(row["bytes"], 0)
        self.assertEqual(row["premature_partial_requests"], 1)


if __name__ == "__main__":
    unittest.main()
