#!/usr/bin/env python3
import argparse
import json
import os
import signal
import socket
import socketserver
import sys
import threading
import time


ACK_INTERVAL_SECONDS = 0.2
ACK_BYTES = 64 * 1024 * 1024
SNAPSHOT_INTERVAL_SECONDS = 0.05
QUIESCE_TIMEOUT_SECONDS = 5.0
IPPROTO_MPTCP = getattr(socket, "IPPROTO_MPTCP", 262)


def acknowledgement(kind, total):
    return f"{kind} {total}\n".encode("ascii")


class ConnectionProgress:
    def __init__(self, connection_id):
        self.connection_id = connection_id
        self.lock = threading.Lock()
        self.bytes = 0
        self.final = False
        self.updated_wall_time_ns = time.time_ns()
        self.generation = 0
        self.flushed_generation = -1


class SinkHandler(socketserver.BaseRequestHandler):
    def handle(self):
        allocate_connection = getattr(self.server, "allocate_connection", None)
        connection = allocate_connection(self.request) if allocate_connection else None
        total = 0
        last_ack_total = 0
        last_ack_at = time.monotonic()
        progress_available = True
        natural_eof = False
        try:
            while True:
                try:
                    data = self.request.recv(1024 * 1024)
                except OSError:
                    break
                if not data:
                    is_quiescing = getattr(self.server, "is_quiescing", lambda: False)
                    natural_eof = not is_quiescing()
                    break
                total += len(data)
                if connection is not None:
                    self.server.record_progress(connection, total, final=False)
                now = time.monotonic()
                if (
                    last_ack_total == 0
                    or total - last_ack_total >= ACK_BYTES
                    or now - last_ack_at >= ACK_INTERVAL_SECONDS
                ):
                    if progress_available:
                        try:
                            self.request.sendall(acknowledgement("ACK", total))
                        except OSError:
                            progress_available = False
                    last_ack_total = total
                    last_ack_at = now
        finally:
            if connection is not None:
                self.server.finish_connection(connection, total, final=natural_eof)

        if natural_eof:
            try:
                self.request.sendall(acknowledgement("OK", total))
            except OSError:
                pass


class ThreadingTcpServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        server_address,
        handler_class,
        progress_file=None,
        snapshot_interval_seconds=SNAPSHOT_INTERVAL_SECONDS,
        socket_protocol=0,
    ):
        self.progress_file = progress_file
        self.snapshot_interval_seconds = max(0.001, snapshot_interval_seconds)
        self.connections_lock = threading.Lock()
        self.active_condition = threading.Condition(self.connections_lock)
        self.progress_connections = {}
        self.active_requests = {}
        self.request_connections = {}
        self.next_connection_id = 0
        self.lifecycle_condition = threading.Condition()
        self.lifecycle_state = "running"
        self.quiesced = False
        self.finalized = False
        self.lifecycle_generation = 0
        self.flushed_lifecycle_generation = -1
        self.flush_lock = threading.Lock()
        self.snapshot_stop = threading.Event()
        self.snapshot_thread = None
        self.snapshot_error = None
        self.serving_lock = threading.Lock()
        self.serving = False
        self.serving_thread_id = None
        super().__init__(server_address, handler_class, bind_and_activate=False)
        if socket_protocol:
            # A client-only MPTCP socket may fall back to TCP. Opting in on the
            # listener keeps the baseline's transport identity unambiguous.
            self.socket.close()
            self.socket = socket.socket(
                self.address_family, self.socket_type, socket_protocol
            )
        try:
            self.server_bind()
            self.server_activate()
        except Exception:
            self.server_close()
            raise
        self.flush_progress(force=True)
        if self.progress_file:
            self.snapshot_thread = threading.Thread(
                target=self._snapshot_loop,
                name="tcp-sink-snapshot",
                daemon=True,
            )
            self.snapshot_thread.start()

    def serve_forever(self, poll_interval=0.1):
        with self.serving_lock:
            self.serving = True
            self.serving_thread_id = threading.get_ident()
        try:
            super().serve_forever(poll_interval=poll_interval)
        finally:
            with self.serving_lock:
                self.serving = False
                self.serving_thread_id = None

    def is_quiescing(self):
        with self.lifecycle_condition:
            return self.lifecycle_state != "running"

    def allocate_connection(self, request=None):
        with self.lifecycle_condition:
            quiescing = self.lifecycle_state != "running"
            with self.active_condition:
                if request is not None:
                    existing = self.request_connections.get(id(request))
                    if existing is not None:
                        return existing
                connection_id = self.next_connection_id
                self.next_connection_id += 1
                connection = ConnectionProgress(connection_id)
                self.progress_connections[connection_id] = connection
                if request is not None:
                    self.active_requests[connection_id] = request
                    self.request_connections[id(request)] = connection
        if quiescing and request is not None:
            self._shutdown_request(request)
        return connection

    def process_request(self, request, client_address):
        connection = self.allocate_connection(request)
        try:
            super().process_request(request, client_address)
        except Exception:
            self.finish_connection(connection, 0, final=False)
            self._shutdown_request(request)
            raise

    def _resolve_connection(self, connection):
        if isinstance(connection, ConnectionProgress):
            return connection
        with self.connections_lock:
            return self.progress_connections[connection]

    def record_progress(self, connection, total, final):
        connection = self._resolve_connection(connection)
        with connection.lock:
            if total < connection.bytes:
                raise RuntimeError("TCP sink byte count decreased")
            if connection.final and (not final or total != connection.bytes):
                raise RuntimeError("TCP sink changed a finalized connection")
            changed = total != connection.bytes or bool(final) != connection.final
            connection.bytes = total
            connection.final = bool(final)
            if changed:
                connection.updated_wall_time_ns = time.time_ns()
                connection.generation += 1

    def finish_connection(self, connection, total, final):
        connection = self._resolve_connection(connection)
        try:
            self.record_progress(connection, total, final=final)
        finally:
            with self.active_condition:
                request = self.active_requests.pop(connection.connection_id, None)
                if request is not None:
                    self.request_connections.pop(id(request), None)
                self.active_condition.notify_all()

    def _snapshot_data(self):
        with self.connections_lock:
            connections = list(self.progress_connections.values())
        connection_rows = {}
        connection_generations = {}
        dirty = False
        for connection in connections:
            with connection.lock:
                connection_rows[str(connection.connection_id)] = {
                    "bytes": connection.bytes,
                    "final": connection.final,
                    "updated_wall_time_ns": connection.updated_wall_time_ns,
                }
                connection_generations[connection.connection_id] = connection.generation
                dirty = dirty or (
                    connection.generation != connection.flushed_generation
                )
        with self.lifecycle_condition:
            quiesced = self.quiesced
            finalized = self.finalized
            lifecycle_generation = self.lifecycle_generation
            dirty = dirty or (lifecycle_generation != self.flushed_lifecycle_generation)
        return (
            {
                "version": 2,
                "quiesced": quiesced,
                "finalized": finalized,
                "connections": connection_rows,
                "updated_wall_time_ns": time.time_ns(),
            },
            connections,
            connection_generations,
            lifecycle_generation,
            dirty,
        )

    def _atomic_write_snapshot(self, snapshot):
        temporary = f"{self.progress_file}.tmp.{os.getpid()}"
        try:
            with open(temporary, "w", encoding="utf-8") as handle:
                json.dump(snapshot, handle, sort_keys=True)
                handle.write("\n")
            os.replace(temporary, self.progress_file)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    def flush_progress(self, force=False):
        if not self.progress_file:
            return False
        with self.flush_lock:
            (
                snapshot,
                connections,
                connection_generations,
                lifecycle_generation,
                dirty,
            ) = self._snapshot_data()
            if not force and not dirty:
                return False
            self._atomic_write_snapshot(snapshot)
            for connection in connections:
                with connection.lock:
                    generation = connection_generations[connection.connection_id]
                    if connection.generation == generation:
                        connection.flushed_generation = generation
            with self.lifecycle_condition:
                if self.lifecycle_generation == lifecycle_generation:
                    self.flushed_lifecycle_generation = lifecycle_generation
                self.snapshot_error = None
            return True

    def _snapshot_loop(self):
        while not self.snapshot_stop.wait(self.snapshot_interval_seconds):
            try:
                self.flush_progress()
            except Exception as exc:
                with self.lifecycle_condition:
                    self.snapshot_error = exc

    def _stop_snapshot_thread(self):
        self.snapshot_stop.set()
        if self.snapshot_thread is not None:
            self.snapshot_thread.join(timeout=1.0)
            if self.snapshot_thread.is_alive():
                raise RuntimeError("TCP sink snapshot thread did not stop")

    @staticmethod
    def _shutdown_request(request):
        try:
            request.shutdown(socket.SHUT_RDWR)
        except (AttributeError, OSError):
            pass

    def _stop_accepting(self):
        with self.serving_lock:
            serving = self.serving
            serving_thread_id = self.serving_thread_id
        if serving:
            if serving_thread_id == threading.get_ident():
                raise RuntimeError("TCP sink cannot quiesce from its serving thread")
            self.shutdown()

    def quiesce(self, timeout=QUIESCE_TIMEOUT_SECONDS):
        with self.lifecycle_condition:
            while self.lifecycle_state == "quiescing":
                self.lifecycle_condition.wait()
            if self.lifecycle_state == "quiesced":
                return False
            if self.lifecycle_state == "failed":
                raise RuntimeError("TCP sink quiesce previously failed")
            self.lifecycle_state = "quiescing"

        try:
            self._stop_accepting()
            with self.active_condition:
                active_requests = list(self.active_requests.values())
            for request in active_requests:
                self._shutdown_request(request)

            deadline = time.monotonic() + max(0.0, timeout)
            with self.active_condition:
                while self.active_requests:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise RuntimeError(
                            "TCP sink handlers did not stop during quiesce"
                        )
                    self.active_condition.wait(timeout=remaining)

            self._stop_snapshot_thread()
            with self.lifecycle_condition:
                self.quiesced = True
                self.finalized = True
                self.lifecycle_generation += 1
            self.flush_progress(force=True)
        except Exception:
            try:
                self._stop_snapshot_thread()
                self.flush_progress(force=True)
            finally:
                with self.lifecycle_condition:
                    self.lifecycle_state = "failed"
                    self.lifecycle_condition.notify_all()
            raise

        with self.lifecycle_condition:
            self.lifecycle_state = "quiesced"
            self.lifecycle_condition.notify_all()
        return True

    def server_close(self):
        if self.lifecycle_state == "running":
            self._stop_snapshot_thread()
        super().server_close()


class GracefulShutdown(Exception):
    pass


def parse_bind(value):
    host, port = value.rsplit(":", 1)
    return host, int(port)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", default="0.0.0.0:10023")
    parser.add_argument("--progress-file")
    parser.add_argument(
        "--mptcp",
        action="store_true",
        help="listen with IPPROTO_MPTCP instead of a TCP socket",
    )
    args = parser.parse_args()
    server = ThreadingTcpServer(
        parse_bind(args.bind),
        SinkHandler,
        progress_file=args.progress_file,
        socket_protocol=IPPROTO_MPTCP if args.mptcp else 0,
    )

    def request_shutdown(_signum, _frame):
        raise GracefulShutdown

    signal.signal(signal.SIGTERM, request_shutdown)
    exit_code = 0
    try:
        server.serve_forever()
    except (GracefulShutdown, KeyboardInterrupt):
        pass
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        try:
            server.quiesce()
        except Exception as exc:
            print(f"TCP sink quiesce failed: {exc}", file=sys.stderr)
            exit_code = 1
        server.server_close()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
