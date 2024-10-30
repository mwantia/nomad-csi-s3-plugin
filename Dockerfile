FROM golang:1.22-alpine AS gobuild

ARG TARGETOS
ARG TARGETARCH

WORKDIR /build
ADD . /build

RUN go get -d -v ./...
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o ./driver ./cmd/driver

FROM debian:bullseye-slim

ARG TARGETOS
ARG TARGETARCH

RUN apt update \
    && apt install -yqq jq s3fs bindfs \
    && apt clean -yqq
RUN rm -rf /var/lib/apt/lists/*

COPY --from=gobuild /build/driver /driver
ENTRYPOINT ["/driver"]