# syntax=docker/dockerfile:1
# quic-interop-runner endpoint image for poseidon-http-client (client role).
#
# Build from the REPOSITORY ROOT, not from this directory — the client is a
# nested module whose replace directive points at the parent:
#
#   docker buildx build -f test/interop/quic/Dockerfile -t poseidon-interop:local .
#
# The runner requires linux/amd64 and linux/arm64. The builder stage is pinned to
# the BUILD platform and cross-compiles with GOARCH, so the arm64 image is
# produced by the native toolchain rather than under qemu emulation.

FROM --platform=$BUILDPLATFORM golang:1.25.13-bookworm AS builder

ARG TARGETARCH
WORKDIR /src
COPY . .
WORKDIR /src/test/interop/quic
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} \
    go build -trimpath -o /out/interop-client .

# The base image the runner's network simulator requires: it supplies /setup.sh
# (routing, checksum offload, the /logs tree) and /wait-for-it.sh.
FROM martenseemann/quic-network-simulator-endpoint:latest

WORKDIR /poseidon
COPY --from=builder /out/interop-client ./interop-client
# --chmod instead of the canonical `COPY … .` + `RUN chmod +x`: a RUN in this
# stage executes in the TARGET platform's rootfs, so on an amd64 host the arm64
# half of the build needs qemu binfmt installed just to run one chmod, and fails
# with exit 255 without it. COPY runs on the builder, so this produces the same
# 0755 entrypoint on both platforms with no emulation anywhere.
COPY --chmod=0755 test/interop/quic/run_endpoint.sh .
ENTRYPOINT [ "./run_endpoint.sh" ]
