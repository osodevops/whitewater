FROM rust:1.90-bookworm AS builder
WORKDIR /workspace
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && printf 'pub fn dependency_cache_marker() {}\n' > src/lib.rs \
    && printf 'fn main() {}\n' > src/main.rs \
    && cargo build --locked --release \
    && rm -rf src
COPY src ./src
RUN touch src/lib.rs src/main.rs \
    && cargo build --locked --release

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 whitewater \
    && useradd --system --uid 10001 --gid whitewater --home-dir /var/lib/whitewater whitewater \
    && mkdir -p /var/lib/whitewater \
    && chown whitewater:whitewater /var/lib/whitewater
COPY --from=builder /workspace/target/release/whitewater /usr/local/bin/whitewater
COPY --from=builder /workspace/target/release/wwctl /usr/local/bin/wwctl
USER whitewater
EXPOSE 7070
VOLUME ["/var/lib/whitewater"]
ENV WHITEWATER_BIND_ADDR=0.0.0.0:7070 \
    WHITEWATER_DATA_DIR=/var/lib/whitewater \
    RUST_LOG=whitewater=info,tower_http=info
HEALTHCHECK --interval=5s --timeout=2s --start-period=5s --retries=12 CMD ["curl", "--fail", "--silent", "http://127.0.0.1:7070/health"]
ENTRYPOINT ["/usr/local/bin/whitewater"]
