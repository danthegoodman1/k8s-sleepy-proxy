FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/controller ./cmd/controller
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/lb ./cmd/lb
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/tcplb ./cmd/tcplb
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/sidecar ./cmd/sidecar
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/tcpsidecar ./cmd/tcpsidecar
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/echo ./cmd/echo

FROM gcr.io/distroless/static-debian12 AS controller
COPY --from=build /out/controller /controller
ENTRYPOINT ["/controller"]

FROM gcr.io/distroless/static-debian12 AS lb
COPY --from=build /out/lb /lb
ENTRYPOINT ["/lb"]

FROM gcr.io/distroless/static-debian12 AS tcplb
COPY --from=build /out/tcplb /tcplb
ENTRYPOINT ["/tcplb"]

FROM gcr.io/distroless/static-debian12 AS sidecar
COPY --from=build /out/sidecar /sidecar
ENTRYPOINT ["/sidecar"]

FROM gcr.io/distroless/static-debian12 AS tcpsidecar
COPY --from=build /out/tcpsidecar /tcpsidecar
ENTRYPOINT ["/tcpsidecar"]

FROM gcr.io/distroless/static-debian12 AS echo
COPY --from=build /out/echo /echo
ENTRYPOINT ["/echo"]
