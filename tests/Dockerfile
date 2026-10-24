ARG RUST_VERSION=1.99.1
ARG GO_VERSION_TAG=1.25-alpine

# Build the extproc server binary (the tester-backed implementation of the SDK).
FROM rust:${RUST_VERSION} AS builder

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests

RUN cargo build --release --bin extproc-tester

FROM golang:${GO_VERSION_TAG} AS healthcheck

RUN go install github.com/grpc-ecosystem/grpc-health-probe@v0.4.39

FROM rust:${RUST_VERSION}

COPY --from=builder /build/target/release/extproc-tester /extproc
COPY --from=healthcheck /go/bin/grpc-health-probe /grpc-health-probe

EXPOSE 50051
EXPOSE 9090

ENTRYPOINT [ "/extproc" ]
