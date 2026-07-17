FROM ubuntu:24.04

ARG MPTUNNEL_LAB_INSTALL_WINE=0

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

# Wine is opt-in because only the portable Windows client cohort needs it.
RUN if [ "$MPTUNNEL_LAB_INSTALL_WINE" = "1" ]; then \
      apt-get update \
      && apt-get install -y --no-install-recommends wine wine64 \
      && rm -rf /var/lib/apt/lists/*; \
    fi

WORKDIR /workspace
