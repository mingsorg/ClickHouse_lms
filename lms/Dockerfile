FROM golang:1.24.2-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -o /out/logd ./cmd/logd && \
    CGO_ENABLED=0 go build -trimpath -o /out/lmsctl ./cmd/lmsctl

FROM alpine:3.22
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 lms
COPY --from=build /out/logd /usr/local/bin/logd
COPY --from=build /out/lmsctl /usr/local/bin/lmsctl
USER lms
ENTRYPOINT ["/usr/local/bin/logd"]
