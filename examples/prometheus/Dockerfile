FROM golang:1.25-alpine AS builder

WORKDIR /src

# Copy root module (the library)
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build from the examples workspace
WORKDIR /src/examples
RUN go mod download
RUN CGO_ENABLED=0 go build -o /kharon-prom ./prometheus/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /kharon-prom /usr/local/bin/kharon-prom
ENTRYPOINT ["kharon-prom"]
