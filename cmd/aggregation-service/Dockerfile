FROM golang:1.23-alpine AS build

WORKDIR /src

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/aggregation-service ./cmd/aggregation-service

FROM alpine:3.20

RUN apk add --no-cache ca-certificates curl

COPY --from=build /out/aggregation-service /usr/local/bin/aggregation-service

EXPOSE 8082

ENTRYPOINT ["/usr/local/bin/aggregation-service"]
