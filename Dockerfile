# The Go version that builds the program. CI runs the tests with this
# version too, and checks that the two are the same.
ARG GO_VERSION=1.27

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION} AS build
WORKDIR /app

# The modules first, in a layer of their own, so that a change of the
# source does not fetch them again
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY pkg ./pkg

# The builder keeps the cache of the compiler between builds, so a change
# of the source compiles only what has changed
ARG CI_COMMIT_SHORT_SHA
ARG TARGETOS TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags "-X main.GitCommit=$CI_COMMIT_SHORT_SHA" -o ./bin/nanit ./cmd/nanit/

FROM debian:trixie-slim

# ca-certificates: the program checks the certificates of the Nanit servers.
# bash, curl and jq: scripts/init-nanit.sh needs them.
RUN apt-get -yqq update && \
    apt-get install -yq --no-install-recommends ca-certificates bash curl jq && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /app/bin/nanit /app/bin/nanit
COPY scripts /app/scripts

RUN mkdir -p /data && \
    chmod +x /app/scripts/*.sh

WORKDIR /app
ENTRYPOINT ["/app/bin/nanit"]
