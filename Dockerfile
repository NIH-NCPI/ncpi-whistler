# syntax=docker/dockerfile:1
#
# NCPI Whistler + Google Whistle (v1) in a single, batch-friendly image.
#
# Designed for Cavatica / Seven Bridges (and any CWL runner):
#   * real base OS with bash/coreutils (the platform wraps commands in a shell)
#   * no ENTRYPOINT (the CWL baseCommand is run as-is)
#   * runs as any UID, writes nothing into the image at runtime
#
# Build from the root of the ncpi-whistler repository:
#   docker build --platform linux/amd64 -t ncpi-whistler:latest .

###############################################################################
# Stage 1: build Whistle v1 (same recipe as NIH-NCPI/dockerized-whistle)
###############################################################################
FROM golang:1.13-buster AS whistle-builder

# Whistler is not yet compatible with Whistle v2, so pin the last v1 commit.
ARG WHISTLE_COMMIT=fa0d4ac3a5bb07ca071fbfd625c9293dbf78102e

# Buster is EOL; packages live on archive.debian.org.
RUN sed -i 's|security.debian.org|archive.debian.org|g; s|deb.debian.org|archive.debian.org|g; /buster-updates/d' /etc/apt/sources.list && \
    echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/99archive && \
    apt-get -y update && \
    apt-get -y install --no-install-recommends \
        libclang1-11 \
        libprotobuf-dev \
        openjdk-11-jre \
        protobuf-compiler && \
    rm -rf /var/lib/apt/lists/* && \
    # mapping.proto imports google/protobuf/any.proto (from libprotobuf-dev).
    # Whistle's generate_protos.sh ignores protoc failures, so check up front.
    test -f /usr/include/google/protobuf/any.proto

RUN git clone https://github.com/GoogleCloudPlatform/healthcare-data-harmonization /go/hdh && \
    cd /go/hdh && \
    git checkout "${WHISTLE_COMMIT}" && \
    sh build_all.sh && \
    install -D -m 0755 mapping_engine/main/main /dist/whistle

###############################################################################
# Stage 2: install NCPI Whistler into a self-contained virtualenv
###############################################################################
FROM python:3.12-slim-bookworm AS py-builder

# git is needed only to resolve the ncpi_fhir_client git dependency
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/whistler
ENV PATH=/opt/whistler/bin:$PATH

COPY pyproject.toml README.md MANIFEST.in LICENSE /src/
COPY wstlr /src/wstlr

# pyjwt[crypto] pulls in `cryptography`, required for RS256 signing used by
# the GCP service-account auth modules in ncpi_fhir_client.
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir /src "pyjwt[crypto]"

###############################################################################
# Stage 3: runtime
###############################################################################
FROM python:3.12-slim-bookworm

LABEL org.opencontainers.image.title="ncpi-whistler" \
      org.opencontainers.image.description="NCPI Whistler pipeline with Google Whistle v1, for Cavatica/CWL workflows" \
      org.opencontainers.image.source="https://github.com/NIH-NCPI/ncpi-whistler" \
      org.opencontainers.image.licenses="MIT"

# HOME=/tmp: platforms may run as an arbitrary UID with no home directory.
# COLUMNS: keeps rich's console output readable in non-TTY task logs.
ENV PATH=/opt/whistler/bin:$PATH \
    HOME=/tmp \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    COLUMNS=160

COPY --from=whistle-builder /dist/whistle /usr/local/bin/whistle
COPY --from=py-builder /opt/whistler /opt/whistler
COPY docker/whistler-run /usr/local/bin/whistler-run

# Fail the build (not the Cavatica task) if anything is missing.
# The whistle binary is built on glibc 2.28 and runs on bookworm's 2.36.
RUN chmod 0755 /usr/local/bin/whistler-run && \
    ! ldd /usr/local/bin/whistle | grep -q "not found" && \
    command -v which && \
    which whistle play && \
    cd /tmp && python -c "import wstlr.play, cryptography"

# No ENTRYPOINT on purpose: Cavatica supplies the full command line.
CMD ["bash"]
