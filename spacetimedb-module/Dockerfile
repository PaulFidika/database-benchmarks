FROM rust:1.75 as builder

# Install spacetime CLI
RUN curl -fsSL https://install.spacetimedb.com | bash
ENV PATH="/root/.spacetime/bin:${PATH}"

# Install wasm target
RUN rustup target add wasm32-unknown-unknown

WORKDIR /app
COPY Cargo.toml .
COPY src ./src

# Build the module
RUN spacetime build

# The output will be in target/wasm32-unknown-unknown/release/benchmark_module.wasm
# We'll copy this out in a multi-stage build or use it directly

FROM debian:bookworm-slim
COPY --from=builder /app/target/wasm32-unknown-unknown/release/benchmark_module.wasm /module.wasm
CMD ["cat", "/module.wasm"]
