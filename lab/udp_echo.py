#!/usr/bin/env python3
import argparse
import socket


def parse_host_port(value):
    host, port = value.rsplit(":", 1)
    return host, int(port)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", default="0.0.0.0:9090")
    args = parser.parse_args()

    bind = parse_host_port(args.bind)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(bind)
    while True:
        payload, peer = sock.recvfrom(65535)
        sock.sendto(payload, peer)


if __name__ == "__main__":
    main()
