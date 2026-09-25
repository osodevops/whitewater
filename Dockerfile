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
    && groupadd --system --gid 10001 finnstream \
    && useradd --system --uid 10001 --gid finnstream --home-dir /var/lib/finnstream finnstream \
    && mkdir -p /var/lib/finnstream \
    && chown finnstream:finnstream /var/lib/finnstream
COPY --from=builder /workspace/target/release/finnstream /usr/local/bin/finnstream
COPY --from=builder /workspace/target/release/wwctl /usr/local/bin/wwctl
USER finnstream
EXPOSE 7070
VOLUME ["/var/lib/finnstream"]
ENV FINNSTREAM_BIND_ADDR=0.0.0.0:7070 \
    FINNSTREAM_DATA_DIR=/var/lib/finnstream \
    RUST_LOG=finnstream=info,tower_http=info
HEALTHCHECK --interval=5s --timeout=2s --start-period=5s --retries=12 CMD ["curl", "--fail", "--silent", "http://127.0.0.1:7070/health"]
ENTRYPOINT ["/usr/local/bin/finnstream"]
