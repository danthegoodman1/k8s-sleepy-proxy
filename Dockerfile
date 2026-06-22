# syntax=docker/dockerfile:1.7

ARG RUST_IMAGE=rust:1-bookworm
ARG RUNTIME_IMAGE=gcr.io/distroless/cc-debian12:nonroot

FROM ${RUST_IMAGE} AS build
WORKDIR /workspace

ARG BIN
RUN test -n "${BIN}"

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/workspace/target \
    cargo build --locked --release -p "${BIN}" --bin "${BIN}" && \
    mkdir -p /out/usr/local/bin && \
    cp "target/release/${BIN}" "/out/usr/local/bin/${BIN}" && \
    ln -s "${BIN}" /out/usr/local/bin/sleepypods

FROM ${RUNTIME_IMAGE}

ARG BIN
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/ /

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/sleepypods"]
