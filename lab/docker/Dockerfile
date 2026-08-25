FROM ubuntu:24.04 AS iproute2-builder

# Ubuntu 24.04 ships iproute2 6.1, which predates tc-netem's `seed`
# userspace support.  A seeded packet process is part of the lab evidence
# contract, so build a checksum-pinned tc from upstream instead of silently
# falling back to an unreplayable loss/corruption stream.
ARG IPROUTE2_VERSION=6.15.0
ARG IPROUTE2_SHA256=8041854a882583ad5263466736c9c8c68c74b1a35754ab770d23343f947528fb

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      bison \
      build-essential \
      ca-certificates \
      curl \
      flex \
      libbpf-dev \
      libcap-dev \
      libelf-dev \
      libmnl-dev \
      pkg-config \
      xz-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

RUN curl --fail --location --show-error --silent \
      "https://www.kernel.org/pub/linux/utils/net/iproute2/iproute2-${IPROUTE2_VERSION}.tar.xz" \
      --output iproute2.tar.xz \
    && echo "${IPROUTE2_SHA256}  iproute2.tar.xz" | sha256sum --check --strict \
    && mkdir iproute2 \
    && tar --extract --file iproute2.tar.xz --strip-components=1 --directory iproute2 \
    && cd iproute2 \
    && ./configure \
    && make -j2

FROM ubuntu:24.04

ARG MPTUNNEL_LAB_INSTALL_WINE=0

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      bash \
      ca-certificates \
      curl \
      iperf3 \
      iptables \
      iproute2 \
      procps \
      python3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=iproute2-builder /src/iproute2/tc/tc /usr/local/sbin/tc
COPY --from=iproute2-builder /src/iproute2/netem/*.dist /usr/lib/tc/

ENV PATH="/usr/local/sbin:${PATH}"

RUN tc -V | grep -F 'iproute2-6.15.0' \
    && test -r /usr/lib/tc/normal.dist

# Wine is opt-in because only the portable Windows client cohort needs it.
RUN if [ "$MPTUNNEL_LAB_INSTALL_WINE" = "1" ]; then \
      apt-get update \
      && apt-get install -y --no-install-recommends wine wine64 \
      && rm -rf /var/lib/apt/lists/*; \
    fi

WORKDIR /workspace
