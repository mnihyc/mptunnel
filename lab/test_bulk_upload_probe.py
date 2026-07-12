import socket
import socketserver
import sys
import threading
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bulk_upload_probe import (
    IPPROTO_MPTCP,
    connect_socks5,
    connect_target,
    interval_upload,
)


class ScriptedAckHandler(socketserver.BaseRequestHandler):
    def handle(self):
        with self.server.results_lock:
            stream_index = len(self.server.stream_totals)
            self.server.stream_totals.append(None)
            action = self.server.actions[stream_index]

        total = 0
        while True:
            data = self.request.recv(1024 * 1024)
            if not data:
                break
            total += len(data)

        with self.server.results_lock:
            self.server.stream_totals[stream_index] = total

        if action == "exact":
            response = f"ACK {total}\nOK {total}\n"
        elif action == "progress":
            response = f"ACK {max(1, total // 3)}\n"
        elif action == "mismatched-final":
            confirmed = max(1, total // 2)
            response = f"ACK {confirmed}\nOK {confirmed}\n"
        elif action == "decreasing":
            response = f"ACK {total}\nACK {max(0, total - 1)}\nOK {total}\n"
        elif action == "none":
            response = ""
        else:
            raise AssertionError(f"unknown action: {action}")

        if response:
            self.request.sendall(response.encode("ascii"))


class ScriptedAckServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, actions):
        super().__init__(("127.0.0.1", 0), ScriptedAckHandler)
        self.actions = actions
        self.results_lock = threading.Lock()
        self.stream_totals = []


class SlowSocksHandler(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.recv(3)
        self.server.handshake_started.set()
        self.server.release_handshake.wait(timeout=1.0)


class SlowSocksServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), SlowSocksHandler)
        self.handshake_started = threading.Event()
        self.release_handshake = threading.Event()


@contextmanager
def scripted_ack_server(actions):
    server = ScriptedAckServer(actions)
    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.01},
        daemon=True,
    )
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1.0)


@contextmanager
def slow_socks_server():
    server = SlowSocksServer()
    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.01},
        daemon=True,
    )
    thread.start()
    try:
        yield server
    finally:
        server.release_handshake.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=1.0)


def probe_args(server, parallel_uploads=1):
    host, port = server.server_address
    return SimpleNamespace(
        label="receiver-accounting-test",
        protocol="tcp-upload",
        proxy=None,
        target=f"{host}:{port}",
        failover_after=-1,
        timeout=1.0,
        chunk_bytes=4096,
        load_duration=0.08,
        parallel_uploads=parallel_uploads,
        interval_seconds=0.02,
        drain_timeout=0.1,
        started_file=None,
    )


class BulkUploadProbeTests(unittest.TestCase):
    def assert_v2_accounting(self, result):
        self.assertEqual(result["upload_metric_version"], 2)
        self.assertEqual(result["upload_accounting_source"], "target_sink_ack")

    def test_exact_final_confirmation_is_ok(self):
        with scripted_ack_server(["exact"]) as server:
            result = interval_upload(probe_args(server))

        self.assert_v2_accounting(result)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["bytes"], server.stream_totals[0])
        self.assertEqual(result["local_accepted_bytes"], server.stream_totals[0])
        self.assertEqual(result["complete_streams"], 1)
        self.assertEqual(result["failed_streams"], 0)
        self.assertEqual(result["upload_probe_errors"], [])
        self.assertTrue(result["upload_ack_accounting_valid"])

    def test_progress_without_final_confirmation_is_loss(self):
        with scripted_ack_server(["progress"]) as server:
            result = interval_upload(probe_args(server))

        confirmed = max(1, server.stream_totals[0] // 3)
        expected_goodput = confirmed * 8 / result["time_s"] / 1_000_000
        self.assert_v2_accounting(result)
        self.assertEqual(result["status"], "loss")
        self.assertEqual(result["bytes"], confirmed)
        self.assertEqual(result["local_accepted_bytes"], server.stream_totals[0])
        self.assertLess(result["bytes"], result["local_accepted_bytes"])
        self.assertAlmostEqual(
            result["upload_goodput_mbps"], expected_goodput, delta=0.01
        )
        self.assertEqual(result["complete_streams"], 0)
        self.assertEqual(result["failed_streams"], 1)
        self.assertEqual(result["upload_probe_errors"], [])
        self.assertTrue(result["upload_ack_accounting_valid"])

    def test_no_receiver_confirmation_is_fail(self):
        with scripted_ack_server(["none"]) as server:
            result = interval_upload(probe_args(server))

        self.assert_v2_accounting(result)
        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["bytes"], 0)
        self.assertGreater(result["local_accepted_bytes"], 0)
        self.assertEqual(result["upload_goodput_mbps"], 0)
        self.assertEqual(result["complete_streams"], 0)
        self.assertEqual(result["failed_streams"], 1)
        self.assertEqual(result["upload_probe_errors"], [])
        self.assertTrue(result["upload_ack_accounting_valid"])

    def test_final_total_must_exactly_match_local_acceptance(self):
        with scripted_ack_server(["mismatched-final"]) as server:
            result = interval_upload(probe_args(server))

        confirmed = max(1, server.stream_totals[0] // 2)
        self.assert_v2_accounting(result)
        self.assertEqual(result["status"], "loss")
        self.assertEqual(result["bytes"], confirmed)
        self.assertEqual(result["complete_streams"], 0)
        self.assertEqual(result["failed_streams"], 1)
        self.assertEqual(result["upload_probe_errors"], [])
        self.assertTrue(result["upload_ack_accounting_valid"])

    def test_non_monotonic_progress_cannot_become_complete(self):
        with scripted_ack_server(["decreasing"]) as server:
            result = interval_upload(probe_args(server))

        self.assert_v2_accounting(result)
        self.assertEqual(result["status"], "loss")
        self.assertEqual(result["bytes"], server.stream_totals[0])
        self.assertEqual(result["complete_streams"], 0)
        self.assertEqual(result["failed_streams"], 1)
        self.assertFalse(result["upload_ack_accounting_valid"])
        self.assertEqual(result["interval_goodput_raw_mbps"], [])
        self.assertEqual(len(result["upload_probe_errors"]), 1)
        self.assertIn("acknowledgement decreased", result["upload_probe_errors"][0])

    def test_parallel_streams_aggregate_receiver_confirmations(self):
        with scripted_ack_server(["exact", "progress"]) as server:
            result = interval_upload(probe_args(server, parallel_uploads=2))

        exact_total, progress_total = server.stream_totals
        expected_confirmed = exact_total + max(1, progress_total // 3)
        self.assert_v2_accounting(result)
        self.assertEqual(result["status"], "loss")
        self.assertEqual(result["bytes"], expected_confirmed)
        self.assertEqual(result["local_accepted_bytes"], exact_total + progress_total)
        self.assertEqual(result["streams"], 2)
        self.assertEqual(result["complete_streams"], 1)
        self.assertEqual(result["failed_streams"], 1)
        self.assertEqual(result["upload_probe_errors"], [])
        self.assertTrue(result["upload_ack_accounting_valid"])

    def test_slow_socks_handshake_uses_load_deadline(self):
        with slow_socks_server() as server:
            host, port = server.server_address
            args = probe_args(server)
            args.proxy = f"{host}:{port}"
            args.target = "127.0.0.1:9"
            args.load_duration = 0.08
            args.timeout = 2.0
            started = time.monotonic()
            result = interval_upload(args)
            elapsed = time.monotonic() - started

            self.assertTrue(server.handshake_started.wait(timeout=0.5))

        self.assertLess(elapsed, 0.5)
        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["streams"], 1)
        self.assertEqual(result["failed_streams"], 1)
        self.assertFalse(result["upload_ack_accounting_valid"])
        self.assertEqual(len(result["upload_probe_errors"]), 1)
        self.assertIn("timed out", result["upload_probe_errors"][0])

    def test_socks_handshake_refreshes_only_remaining_deadline(self):
        class FakeSocket:
            def __init__(self):
                self.responses = bytearray(
                    b"\x05\x00\x05\x00\x00\x01\x7f\x00\x00\x01\x1f\x90"
                )
                self.timeouts = []

            def settimeout(self, timeout):
                self.timeouts.append(timeout)

            def sendall(self, _data):
                return None

            def recv(self, size):
                data = bytes(self.responses[:1])
                del self.responses[: min(size, 1)]
                return data

            def close(self):
                return None

        fake_socket = FakeSocket()
        clock = [100.0]

        def advancing_clock():
            current = clock[0]
            clock[0] += 0.01
            return current

        with (
            mock.patch(
                "bulk_upload_probe.socket.create_connection",
                return_value=fake_socket,
            ) as create_connection,
            mock.patch("bulk_upload_probe.time.monotonic", side_effect=advancing_clock),
        ):
            connected = connect_socks5(
                "proxy.example:1080", "target.example:443", deadline=101.0
            )

        self.assertIs(connected, fake_socket)
        connect_timeout = create_connection.call_args.kwargs["timeout"]
        self.assertGreater(connect_timeout, 0)
        self.assertTrue(fake_socket.timeouts)
        self.assertTrue(all(timeout > 0 for timeout in fake_socket.timeouts))
        self.assertEqual(
            fake_socket.timeouts, sorted(fake_socket.timeouts, reverse=True)
        )
        self.assertTrue(
            all(timeout < connect_timeout for timeout in fake_socket.timeouts)
        )

    def test_direct_connect_uses_remaining_load_deadline(self):
        class FakeSocket:
            def __init__(self):
                self.timeout = None

            def settimeout(self, timeout):
                self.timeout = timeout

            def close(self):
                return None

        args = SimpleNamespace(proxy=None, target="127.0.0.1:443", timeout=30.0)
        fake_socket = FakeSocket()
        with (
            mock.patch(
                "bulk_upload_probe.socket.create_connection",
                return_value=fake_socket,
            ) as create_connection,
            mock.patch("bulk_upload_probe.time.monotonic", side_effect=[200.0, 200.1]),
        ):
            connected = connect_target(args, deadline=201.0)

        self.assertIs(connected, fake_socket)
        self.assertAlmostEqual(
            create_connection.call_args.kwargs["timeout"], 1.0, places=6
        )
        self.assertAlmostEqual(fake_socket.timeout, 0.9, places=6)

    def test_mptcp_direct_connect_uses_mptcp_protocol_on_socket(self):
        class FakeSocket:
            def __init__(self):
                self.connected = None
                self.timeouts = []
                self.closed = False

            def settimeout(self, timeout):
                self.timeouts.append(timeout)

            def connect(self, address):
                self.connected = address

            def close(self):
                self.closed = True

        args = SimpleNamespace(
            proxy=None,
            target="127.0.0.1:443",
            timeout=30.0,
            mptcp=True,
        )
        fake_socket = FakeSocket()
        with (
            mock.patch(
                "bulk_upload_probe.socket.socket",
                return_value=fake_socket,
            ) as socket_factory,
            mock.patch(
                "bulk_upload_probe.time.monotonic", side_effect=[200.0, 200.1]
            ),
        ):
            connected = connect_target(args, deadline=201.0)

        self.assertIs(connected, fake_socket)
        socket_factory.assert_called_once_with(
            socket.AF_INET, socket.SOCK_STREAM, IPPROTO_MPTCP
        )
        self.assertEqual(fake_socket.connected, ("127.0.0.1", 443))
        self.assertEqual(len(fake_socket.timeouts), 2)
        self.assertAlmostEqual(fake_socket.timeouts[0], 1.0, places=6)
        self.assertAlmostEqual(fake_socket.timeouts[1], 0.9, places=6)
        self.assertFalse(fake_socket.closed)

    def test_mptcp_direct_connect_rejects_proxy_fallback(self):
        args = SimpleNamespace(
            proxy="127.0.0.1:1080",
            target="127.0.0.1:443",
            timeout=30.0,
            mptcp=True,
        )

        with self.assertRaisesRegex(ValueError, "cannot be combined with a proxy"):
            connect_target(args, deadline=time.monotonic() + 1.0)

    def test_workers_share_one_load_and_drain_deadline(self):
        server = SimpleNamespace(server_address=("127.0.0.1", 9))
        args = probe_args(server, parallel_uploads=3)
        observed_deadlines = []

        def capture_deadlines(
            _args,
            _started,
            load_deadline,
            drain_deadline,
            _state,
            lock,
            _payload,
        ):
            with lock:
                observed_deadlines.append((load_deadline, drain_deadline))
            return {
                "complete": False,
                "confirmed_bytes": 0,
                "local_accepted_bytes": 0,
            }

        with mock.patch(
            "bulk_upload_probe.upload_one_stream", side_effect=capture_deadlines
        ):
            result = interval_upload(args)

        self.assertEqual(len(observed_deadlines), 3)
        self.assertEqual(len(set(observed_deadlines)), 1)
        load_deadline, drain_deadline = observed_deadlines[0]
        self.assertAlmostEqual(
            drain_deadline - load_deadline, args.drain_timeout, places=6
        )
        self.assertEqual(result["streams"], 3)
        self.assertEqual(result["failed_streams"], 3)

    def test_partial_delivery_survives_worker_connect_error(self):
        server = SimpleNamespace(server_address=("127.0.0.1", 9))
        args = probe_args(server, parallel_uploads=2)

        def partial_or_failed_stream(
            _args,
            _started,
            _load_deadline,
            _drain_deadline,
            state,
            lock,
            _payload,
        ):
            if threading.current_thread().name == "upload-worker-0":
                with lock:
                    state["bytes"] += 512
                    state["local_accepted_bytes"] += 512
                    state["streams_with_delivery"] += 1
                    state["interval_bytes"][0] = 512
                return {
                    "complete": False,
                    "confirmed_bytes": 512,
                    "local_accepted_bytes": 512,
                }
            raise ConnectionRefusedError("mocked connect failure")

        with mock.patch(
            "bulk_upload_probe.upload_one_stream",
            side_effect=partial_or_failed_stream,
        ):
            result = interval_upload(args)

        self.assertEqual(result["status"], "loss")
        self.assertEqual(result["exit_code"], 0)
        self.assertEqual(result["bytes"], 512)
        self.assertEqual(result["streams"], 2)
        self.assertEqual(result["failed_streams"], 2)
        self.assertEqual(len(result["upload_probe_errors"]), 1)
        self.assertFalse(result["upload_ack_accounting_valid"])
        self.assertEqual(result["interval_goodput_raw_mbps"], [])

    def test_result_is_not_read_until_non_daemon_worker_terminates(self):
        server = SimpleNamespace(server_address=("127.0.0.1", 9))
        args = probe_args(server)
        worker_started = threading.Event()
        release_worker = threading.Event()
        observed_workers = []
        result_holder = {}

        def blocked_stream(*_args):
            observed_workers.append(threading.current_thread())
            worker_started.set()
            release_worker.wait()
            return {
                "complete": False,
                "confirmed_bytes": 0,
                "local_accepted_bytes": 0,
            }

        def run_probe():
            result_holder["result"] = interval_upload(args)

        probe_thread = threading.Thread(target=run_probe)
        with mock.patch(
            "bulk_upload_probe.upload_one_stream", side_effect=blocked_stream
        ):
            probe_thread.start()
            try:
                self.assertTrue(worker_started.wait(timeout=1.0))
                self.assertEqual(len(observed_workers), 1)
                self.assertFalse(observed_workers[0].daemon)
                self.assertTrue(observed_workers[0].is_alive())
                self.assertNotIn("result", result_holder)
            finally:
                release_worker.set()
                probe_thread.join(timeout=1.0)

        self.assertFalse(probe_thread.is_alive())
        self.assertFalse(observed_workers[0].is_alive())
        self.assertIn("result", result_holder)


if __name__ == "__main__":
    unittest.main()
