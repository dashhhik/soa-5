FROM golang:1.23-alpine
WORKDIR /tests
COPY go.mod go.sum ./
RUN go mod download
COPY . .
CMD ["go", "test", "-v", "./...", "-count=1"]
