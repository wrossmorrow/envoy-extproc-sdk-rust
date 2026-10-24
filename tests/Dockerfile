ARG RUST_VERSION=1.99.1

# Build the extproc server binary (the tester-backed implementation of the SDK).
FROM rust:${RUST_VERSION} AS builder

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --bin extproc-tester

FROM rust:${RUST_VERSION}

COPY --from=builder /build/target/release/extproc-tester /extproc

EXPOSE 50051
EXPOSE 9090

ENTRYPOINT [ "/extproc" ]
