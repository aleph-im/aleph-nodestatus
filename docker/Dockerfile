# Stage 1: Build wheels (includes compiler toolchain)
FROM python:3.9-slim AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential libleveldb-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .
RUN pip wheel --no-cache-dir --wheel-dir /wheels .

# Stage 2: Runtime (no compiler, no sources)
FROM python:3.9-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends cron libleveldb1d && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /wheels /wheels
RUN rm -f /wheels/setuptools-*.whl && \
    pip install --no-cache-dir /wheels/*.whl && \
    rm -rf /wheels
