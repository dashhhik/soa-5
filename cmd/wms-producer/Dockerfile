FROM golang:1.23-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/wms-producer ./cmd/wms-producer

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl
COPY --from=builder /out/wms-producer /usr/local/bin/wms-producer
ENTRYPOINT ["wms-producer"]
