FROM golang:1.26.1-bookworm AS source
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

FROM source AS test
ENV CGO_ENABLED=1
CMD ["go", "test", "./..."]

FROM source AS build
RUN CGO_ENABLED=0 go build -trimpath -o /out/api ./cmd/api

FROM debian:bookworm-slim AS runtime
WORKDIR /app
COPY --from=build /out/api /app/queue-api
COPY openapi.yaml /app/openapi.yaml
USER 65532:65532
EXPOSE 8082
ENTRYPOINT ["/app/queue-api"]
