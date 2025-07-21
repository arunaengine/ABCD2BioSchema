# === Build-Stage ===
FROM rust:1.88-slim as builder

WORKDIR /app

# System Dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy Cargo files first for caching
COPY Cargo.toml Cargo.lock ./

# Dummy File to cache dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo fetch
RUN cargo build --release
RUN rm -rf src

COPY src ./src/

# Build the application
RUN touch src/main.rs
RUN cargo build --release

# === Runtime-Stage ===
FROM debian:bookworm-slim

# Runtime Dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    file \
    strace \
    && rm -rf /var/lib/apt/lists/*

# Non-root User
RUN useradd -r -s /bin/false gfbio

# Create working and temp directory for runtime
WORKDIR /app
RUN mkdir -p temp && chown gfbio:gfbio temp

# Copy Binaries
COPY --from=builder /app/target/release/abcd2bioschema /app/abcd2bioschema

# Change ownership of the application directory
RUN chown gfbio:gfbio /app/abcd2bioschema
RUN chmod +x /app/abcd2bioschema

# Change to non-root user
USER gfbio

RUN ls -la /app/abcd2bioschema
RUN file /app/abcd2bioschema

EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:3000/health || exit 1

CMD ["./abcd2bioschema"]