FROM golang:1.23-alpine

RUN apk add --no-cache ca-certificates

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

CMD ["go", "test", "./...", "-count=1", "-v"]
