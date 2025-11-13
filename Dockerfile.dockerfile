# === Stage 1: Build ===
FROM golang:1.21-alpine AS builder

# Install git for module fetching
RUN apk add --no-cache git

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code (only main.go)
COPY main.go ./

# Build the TCP server binary
RUN go build -ldflags="-w -s" -o out main.go

# === Stage 2: Runtime ===
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk add --no-cache ca-certificates

# Create a non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/out .

# Change ownership to non-root user
USER appuser

# Expose TCP port
EXPOSE 5027

# Start the server
ENTRYPOINT ["./out"]
