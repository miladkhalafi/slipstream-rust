# syntax=docker/dockerfile:1
# Slipstream DNS tunnel - Docker build
# Builds slipstream-client and slipstream-server binaries

FROM rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y \
    cmake \
    pkg-config \
    libssl-dev \
    python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy workspace and ensure submodules are available (GitHub Actions checks out with submodules)
COPY . .
# Initialize submodules if not present (e.g. when building locally)
RUN git submodule update --init --recursive || true

# Build both binaries in release mode
RUN cargo build -p slipstream-client -p slipstream-server --release

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/slipstream-client /usr/local/bin/
COPY --from=builder /build/target/release/slipstream-server /usr/local/bin/

# Run either: docker run image slipstream-server ... or docker run image slipstream-client ...
CMD ["slipstream-server", "--help"]
