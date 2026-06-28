#!/usr/bin/env python3
import argparse
import socketserver


class SinkHandler(socketserver.BaseRequestHandler):
    def handle(self):
        total = 0
        while True:
            data = self.request.recv(1024 * 1024)
            if not data:
                break
            total += len(data)
        try:
            self.request.sendall(f"OK {total}\n".encode("ascii"))
        except OSError:
            pass


class ThreadingTcpServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


def parse_bind(value):
    host, port = value.rsplit(":", 1)
    return host, int(port)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", default="0.0.0.0:10023")
    args = parser.parse_args()
    server = ThreadingTcpServer(parse_bind(args.bind), SinkHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
