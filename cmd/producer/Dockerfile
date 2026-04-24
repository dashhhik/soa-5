FROM golang:1.23-alpine AS build

WORKDIR /src

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/movie-producer ./cmd/producer

FROM alpine:3.20

RUN apk add --no-cache ca-certificates curl

COPY --from=build /out/movie-producer /usr/local/bin/movie-producer

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/movie-producer"]
