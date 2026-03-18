# DUPer Docker Image
# Multi-stage build for smaller final image

# Build stage
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN pip install --no-cache-dir hatchling build

# Copy project files
COPY pyproject.toml .
COPY duper/ duper/

# Build wheel
RUN python -m build --wheel

# Runtime stage
FROM python:3.11-slim

WORKDIR /app

# Create non-root user
RUN useradd -m -s /bin/bash duper && \
    mkdir -p /data /config && \
    chown -R duper:duper /data /config

# Copy wheel from builder
COPY --from=builder /app/dist/*.whl /tmp/

# Install the wheel
RUN pip install --no-cache-dir /tmp/*.whl && \
    rm -rf /tmp/*.whl

# Switch to non-root user
USER duper

# Environment variables
ENV DUPER_DATA_DIR=/data
ENV DUPER_CONFIG_DIR=/config

# Expose default port
EXPOSE 8420

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://localhost:8420/api/health')" || exit 1

# Default command
CMD ["duper", "serve", "--host", "0.0.0.0", "--port", "8420"]

# Labels
LABEL org.opencontainers.image.title="DUPer"
LABEL org.opencontainers.image.description="Duplicate file finder and manager with web UI"
LABEL org.opencontainers.image.source="https://github.com/eurrl/DUPer"
