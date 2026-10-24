ARG RUST_VERSION=1.99.1

# build
FROM rust:${RUST_VERSION} AS builder

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --bin extproc-example

# application
FROM rust:${RUST_VERSION}

COPY --from=builder /build/target/release/extproc-example /extproc-example

EXPOSE 50051
EXPOSE 9090

ARG EXAMPLE=noop
ENTRYPOINT [ "/extproc-example" ]
CMD [ "noop" ]
