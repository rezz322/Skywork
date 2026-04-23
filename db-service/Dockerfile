FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o db-service .

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/db-service .
EXPOSE 8000
CMD ["./db-service"]
