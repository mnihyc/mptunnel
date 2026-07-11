import json
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
import sys
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tcp_sink import ACK_BYTES, SinkHandler, ThreadingTcpServer


class SizedChunk:
    def __init__(self, size):
        self.size = size

    def __len__(self):
        return self.size


class FakeRequest:
    def __init__(self, chunks):
        self.chunks = iter(chunks)
        self.responses = []

    def recv(self, _size):
        return next(self.chunks, b"")

    def sendall(self, data):
        self.responses.append(data)


class BlockingRequest(FakeRequest):
    def __init__(self, chunks):
        super().__init__(chunks)
        self.waiting_for_eof = threading.Event()
        self.release_eof = threading.Event()

    def recv(self, size):
        try:
            return next(self.chunks)
        except StopIteration:
            self.waiting_for_eof.set()
            if not self.release_eof.wait(timeout=1.0):
                raise RuntimeError("test did not release the fake sink EOF")
            return b""


class FailingSendRequest(FakeRequest):
    def sendall(self, _data):
        raise OSError("feedback channel is closed")


class IgnoringShutdownRequest:
    def shutdown(self, _how):
        pass


def read_snapshot(path):
    return json.loads(path.read_text(encoding="utf-8"))


class TcpSinkTests(unittest.TestCase):
    def test_reports_monotonic_progress_and_exact_final_total(self):
        request = FakeRequest([b"abc", SizedChunk(ACK_BYTES)])

        SinkHandler(request, ("127.0.0.1", 12345), object())

        self.assertEqual(
            request.responses,
            [
                b"ACK 3\n",
                f"ACK {ACK_BYTES + 3}\n".encode("ascii"),
                f"OK {ACK_BYTES + 3}\n".encode("ascii"),
            ],
        )

    def test_progress_file_resets_and_tracks_connections_atomically(self):
        with tempfile.TemporaryDirectory() as directory:
            progress_file = Path(directory) / "sink-progress.json"
            progress_file.write_text("stale", encoding="utf-8")
            server = ThreadingTcpServer(
                ("127.0.0.1", 0),
                SinkHandler,
                progress_file=str(progress_file),
                snapshot_interval_seconds=60.0,
            )
            try:
                initial = read_snapshot(progress_file)
                first = server.allocate_connection()
                second = server.allocate_connection()
                server.record_progress(first, 11, final=False)
                server.record_progress(second, 17, final=True)
                self.assertTrue(server.flush_progress())
                snapshot = read_snapshot(progress_file)
            finally:
                server.server_close()

            self.assertEqual(initial["version"], 2)
            self.assertFalse(initial["quiesced"])
            self.assertFalse(initial["finalized"])
            self.assertEqual(initial["connections"], {})
            self.assertGreater(initial["updated_wall_time_ns"], 0)
            self.assertEqual(
                {
                    connection_id: {
                        "bytes": connection["bytes"],
                        "final": connection["final"],
                    }
                    for connection_id, connection in snapshot["connections"].items()
                },
                {
                    "0": {"bytes": 11, "final": False},
                    "1": {"bytes": 17, "final": True},
                },
            )
            self.assertTrue(
                all(
                    connection["updated_wall_time_ns"] > 0
                    for connection in snapshot["connections"].values()
                )
            )
            self.assertEqual(list(Path(directory).glob("*.tmp.*")), [])

    def test_progress_file_captures_active_bytes_below_ack_cadence(self):
        with tempfile.TemporaryDirectory() as directory:
            progress_file = Path(directory) / "sink-progress.json"
            server = ThreadingTcpServer(
                ("127.0.0.1", 0),
                SinkHandler,
                progress_file=str(progress_file),
                snapshot_interval_seconds=60.0,
            )
            request = BlockingRequest([b"abc", b"defgh"])
            handler = threading.Thread(
                target=SinkHandler,
                args=(request, ("127.0.0.1", 12345), server),
                daemon=True,
            )
            with mock.patch.object(
                server,
                "_atomic_write_snapshot",
                wraps=server._atomic_write_snapshot,
            ) as writer:
                handler.start()
                try:
                    self.assertTrue(request.waiting_for_eof.wait(timeout=1.0))
                    self.assertEqual(writer.call_count, 0)
                    self.assertTrue(server.flush_progress())
                    self.assertEqual(writer.call_count, 1)
                    active = read_snapshot(progress_file)
                    self.assertEqual(active["connections"]["0"]["bytes"], 8)
                    self.assertFalse(active["connections"]["0"]["final"])
                finally:
                    request.release_eof.set()
                    handler.join(timeout=1.0)
                self.assertTrue(server.flush_progress())
                self.assertEqual(writer.call_count, 2)

            final = read_snapshot(progress_file)
            server.server_close()
            self.assertFalse(handler.is_alive())
            self.assertEqual(final["connections"]["0"]["bytes"], 8)
            self.assertTrue(final["connections"]["0"]["final"])
            self.assertEqual(request.responses, [b"ACK 3\n", b"OK 8\n"])

    def test_background_snapshot_flushes_dirty_progress_on_cadence(self):
        with tempfile.TemporaryDirectory() as directory:
            progress_file = Path(directory) / "sink-progress.json"
            server = ThreadingTcpServer(
                ("127.0.0.1", 0),
                SinkHandler,
                progress_file=str(progress_file),
                snapshot_interval_seconds=0.01,
            )
            request = BlockingRequest([b"periodic"])
            handler = threading.Thread(
                target=SinkHandler,
                args=(request, ("127.0.0.1", 12345), server),
                daemon=True,
            )
            handler.start()
            try:
                self.assertTrue(request.waiting_for_eof.wait(timeout=1.0))
                deadline = time.monotonic() + 1.0
                while True:
                    snapshot = read_snapshot(progress_file)
                    if snapshot["connections"].get("0", {}).get("bytes") == 8:
                        break
                    if time.monotonic() >= deadline:
                        self.fail("background snapshot did not publish progress")
                    time.sleep(0.01)
            finally:
                request.release_eof.set()
                handler.join(timeout=1.0)
                server.server_close()

        self.assertFalse(handler.is_alive())

    def test_finalized_connection_cannot_regress_or_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            server = ThreadingTcpServer(
                ("127.0.0.1", 0),
                SinkHandler,
                progress_file=str(Path(directory) / "sink-progress.json"),
                snapshot_interval_seconds=60.0,
            )
            try:
                connection_id = server.allocate_connection()
                server.record_progress(connection_id, 12, final=True)
                with self.assertRaisesRegex(RuntimeError, "decreased"):
                    server.record_progress(connection_id, 11, final=True)
                with self.assertRaisesRegex(RuntimeError, "finalized"):
                    server.record_progress(connection_id, 12, final=False)
            finally:
                server.server_close()

    def test_observer_finalizes_when_feedback_channel_is_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            progress_file = Path(directory) / "sink-progress.json"
            server = ThreadingTcpServer(
                ("127.0.0.1", 0),
                SinkHandler,
                progress_file=str(progress_file),
                snapshot_interval_seconds=60.0,
            )
            try:
                SinkHandler(
                    FailingSendRequest([b"delivered"]),
                    ("127.0.0.1", 12345),
                    server,
                )
                server.flush_progress()
                snapshot = read_snapshot(progress_file)
            finally:
                server.server_close()

        self.assertEqual(snapshot["connections"]["0"]["bytes"], 9)
        self.assertTrue(snapshot["connections"]["0"]["final"])

    def test_quiesce_flushes_natural_final_state_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            progress_file = Path(directory) / "sink-progress.json"
            server = ThreadingTcpServer(
                ("127.0.0.1", 0),
                SinkHandler,
                progress_file=str(progress_file),
                snapshot_interval_seconds=60.0,
            )
            serving = threading.Thread(target=server.serve_forever, daemon=True)
            serving.start()
            client = socket.create_connection(server.server_address, timeout=1.0)
            try:
                client.sendall(b"delivered")
                client.shutdown(socket.SHUT_WR)
                response = bytearray()
                while b"OK 9\n" not in response:
                    chunk = client.recv(128)
                    if not chunk:
                        break
                    response.extend(chunk)
                self.assertIn(b"OK 9\n", response)

                self.assertTrue(server.quiesce(timeout=1.0))
                first_final = read_snapshot(progress_file)
                self.assertFalse(server.quiesce(timeout=1.0))
                second_final = read_snapshot(progress_file)
            finally:
                client.close()
                server.server_close()
                serving.join(timeout=1.0)

            self.assertFalse(serving.is_alive())
            self.assertEqual(first_final, second_final)
            self.assertEqual(first_final["version"], 2)
            self.assertTrue(first_final["quiesced"])
            self.assertTrue(first_final["finalized"])
            self.assertEqual(first_final["connections"]["0"]["bytes"], 9)
            self.assertTrue(first_final["connections"]["0"]["final"])
            self.assertEqual(list(Path(directory).glob("*.tmp.*")), [])

    def test_forced_quiesce_keeps_active_connection_not_final(self):
        with tempfile.TemporaryDirectory() as directory:
            progress_file = Path(directory) / "sink-progress.json"
            server = ThreadingTcpServer(
                ("127.0.0.1", 0),
                SinkHandler,
                progress_file=str(progress_file),
                snapshot_interval_seconds=60.0,
            )
            serving = threading.Thread(target=server.serve_forever, daemon=True)
            serving.start()
            client = socket.create_connection(server.server_address, timeout=1.0)
            try:
                client.sendall(b"active")
                deadline = time.monotonic() + 1.0
                while True:
                    server.flush_progress()
                    snapshot = read_snapshot(progress_file)
                    if snapshot["connections"].get("0", {}).get("bytes") == 6:
                        break
                    if time.monotonic() >= deadline:
                        self.fail("sink did not observe active bytes")
                    time.sleep(0.01)

                self.assertTrue(server.quiesce(timeout=1.0))
                final = read_snapshot(progress_file)
            finally:
                client.close()
                server.server_close()
                serving.join(timeout=1.0)

            self.assertFalse(serving.is_alive())
            self.assertTrue(final["quiesced"])
            self.assertTrue(final["finalized"])
            self.assertEqual(final["connections"]["0"]["bytes"], 6)
            self.assertFalse(final["connections"]["0"]["final"])

    def test_quiesce_failure_is_explicit_and_not_finalized(self):
        with tempfile.TemporaryDirectory() as directory:
            progress_file = Path(directory) / "sink-progress.json"
            server = ThreadingTcpServer(
                ("127.0.0.1", 0),
                SinkHandler,
                progress_file=str(progress_file),
                snapshot_interval_seconds=60.0,
            )
            server.allocate_connection(IgnoringShutdownRequest())
            try:
                with self.assertRaisesRegex(RuntimeError, "handlers did not stop"):
                    server.quiesce(timeout=0.01)
                failed = read_snapshot(progress_file)
                with self.assertRaisesRegex(RuntimeError, "previously failed"):
                    server.quiesce(timeout=0.01)
            finally:
                server.server_close()

            self.assertFalse(failed["quiesced"])
            self.assertFalse(failed["finalized"])


if __name__ == "__main__":
    unittest.main()
