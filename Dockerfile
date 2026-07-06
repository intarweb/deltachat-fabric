# Delta Chat Fabric — generic engine image.
# ZERO fleet identity is baked in: domain, roster, a2a-directory, ports, dirs all inject
# at DEPLOY via env / a mounted roster (see README + docker-compose.yml).
FROM python:3.12-slim

# deltachat-rpc-server ships as a self-contained Rust binary wheel (manylinux) — no Rust
# toolchain needed at build. Keep the image slim; add nothing beyond the Python deps.
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DATA_DIR=/data \
    DELTA_BACKUP_DIR=/backup

WORKDIR /app

# Bake pinned deps at BUILD (never runtime-pip). requirements.txt includes
# deltachat-rpc-server (the Rust binary wheel) + deltachat2 + the HTTP stack.
# (Hash-locking is applied by CI's frozen lockfile — see requirements.txt.)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# App code (generic engine only — no roster, no identity).
COPY app/ ./app/

# LOCAL account-DB dir + backup dir. 🔴 /data MUST be a LOCAL volume, never NFS:
# deltachat uses SQLCipher and NFS corrupts it. Both are declared VOLUMEs so a plain
# `docker run` gets writable local storage; a compose/deploy binds real local paths.
RUN mkdir -p /data /data/accounts /backup
VOLUME ["/data", "/backup"]

# The relay's internal HTTP contract (/send + channels/contacts/react). Overridable via
# RELAY_PORT/PORT env; EXPOSE documents the default.
EXPOSE 8080

# One process: uvicorn + reconciler loop + relay inbound loop + nightly backup loop.
CMD ["python", "-m", "app.main"]
