# syntax=docker/dockerfile:1

# One image with all three binaries; docker-compose picks the entrypoint.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
COPY deploy/scenario.yaml /etc/throttling/scenario.yaml
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/evaluator"]
