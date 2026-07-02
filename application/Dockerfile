# ============================================================================
# Multi-stage build for production Kafka microservices
# ============================================================================
# Rationale:
# - Alpine base: smaller image, faster startup, reduced attack surface.
# - Multi-stage: separates build dependencies from runtime.
# - Non-root user: improves security (prevent container escapes to host).
# - No ENTRYPOINT, use CMD: allows flexible testing and debugging.
# ============================================================================

FROM python:3.11-alpine as builder

# Install build dependencies for confluent-kafka (needs librdkafka-dev).
RUN apk add --no-cache \
    build-base \
    librdkafka-dev \
    linux-headers

# Create virtual environment.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements and install.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ============================================================================
# Runtime stage
# ============================================================================

FROM python:3.11-alpine

# Install runtime dependency: librdkafka (compiled library, no build tools).
RUN apk add --no-cache librdkafka

# Copy virtual environment from builder.
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Create non-root user for security.
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

# Set working directory.
WORKDIR /app

# Copy application code.
COPY --chown=appuser:appgroup producer.py .
COPY --chown=appuser:appgroup consumer.py .

# Expose ports for health endpoints.
EXPOSE 8080 8081

# Health check (for Docker and Kubernetes).
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health' if __import__('os').getenv('SERVICE', 'producer') == 'producer' else 'http://localhost:8081/health').read()"

# Run the application (set SERVICE env var to 'producer' or 'consumer').
CMD ["python", "-u", "-c", "import sys; exec(open('producer.py' if __import__('os').getenv('SERVICE', 'producer') == 'producer' else 'consumer.py').read())"]
