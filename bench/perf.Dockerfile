FROM debian:bookworm-slim@sha256:7ff6ba31527b9e336162fa061746c366c4b2032f4d2cff2078bea43f78efe3f8

LABEL org.opencontainers.image.title="Cairn native perf collector"
LABEL org.opencontainers.image.description="ARM64 Debian packaged tooling only; no database or source build"

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
      linux-perf libdw1 binutils \
 && rm -rf /var/lib/apt/lists/* \
 && dpkg --print-architecture | grep -x arm64 \
 && mkdir -p /opt/collector-evidence \
 && cp /etc/os-release /opt/collector-evidence/os-release \
 && dpkg-query -W -f='${binary:Package}\t${Version}\t${Architecture}\n' > /opt/collector-evidence/packages.tsv \
 && perf version --build-options > /opt/collector-evidence/perf-build-options.txt \
 && sha256sum /usr/bin/perf /usr/bin/readelf /usr/bin/addr2line > /opt/collector-evidence/tool-sha256.txt

ENTRYPOINT ["perf"]
