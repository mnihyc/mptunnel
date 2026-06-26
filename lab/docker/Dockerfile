FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      bash \
      ca-certificates \
      curl \
      iperf3 \
      iproute2 \
      procps \
      python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
