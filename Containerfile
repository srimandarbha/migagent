# syntax=docker/dockerfile:1
# Multi-stage production container for Migration Failure Agent (MFA)
# Optimized for Red Hat OpenShift Container Platform (OCP)

# --- Stage 1: Build & Dependency Resolution ---
FROM registry.access.redhat.com/ubi9/python-311:latest AS builder

USER 0

WORKDIR /app

# Install strictly pinned dependencies from lockfile
COPY pyproject.toml requirements.lock /app/

# Create isolated virtualenv for clean distribution
RUN python3 -m venv /opt/app-root/venv && \
    /opt/app-root/venv/bin/pip install --no-cache-dir --upgrade pip setuptools wheel && \
    /opt/app-root/venv/bin/pip install --no-cache-dir -r requirements.lock

# --- Stage 2: Minimal Distroless / Hardened Runtime ---
FROM registry.access.redhat.com/ubi9/python-311:latest AS runtime

LABEL maintainer="OpenShift Virtualization SRE Team" \
      name="migration-failure-agent" \
      version="2.12.3" \
      summary="Automated VMware to OpenShift Virtualization migration failure diagnostic agent" \
      description="Production LangGraph & deterministic rule-based orchestration layer for MTV/Forklift failures"

USER 0

# Copy pre-built virtual environment from builder
COPY --from=builder /opt/app-root/venv /opt/app-root/venv

ENV PATH="/opt/app-root/venv/bin:$PATH" \
    PYTHONPATH="/app" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    MFA_ENV=production \
    MFA_METRICS_PORT=8080

WORKDIR /app

# Copy application codebase
COPY --chown=1001:0 config/ /app/config/
COPY --chown=1001:0 engine/ /app/engine/
COPY --chown=1001:0 persistence/ /app/persistence/
COPY --chown=1001:0 policies/ /app/policies/
COPY --chown=1001:0 skills/ /app/skills/
COPY --chown=1001:0 scripts/ /app/scripts/
COPY --chown=1001:0 VERSION /app/VERSION
COPY --chown=1001:0 pyproject.toml /app/pyproject.toml

# Set permissions for OpenShift arbitrary UID support (group 0 writeable)
RUN chown -R 1001:0 /app && \
    chmod -R g=u /app && \
    mkdir -p /tmp/mfa && chmod -R 777 /tmp/mfa

# Non-root user 1001
USER 1001

EXPOSE 8080

# Health check using the internal HTTP probe server
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/livez')" || exit 1

ENTRYPOINT ["python3", "scripts/run_kafka_agent.py"]
