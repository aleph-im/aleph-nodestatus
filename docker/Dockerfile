# Stage 1: Build wheels (includes compiler toolchain)
# Python 3.12: aleph-sdk-python>=2.3.2 (cursor pagination) requires >=3.10,
# and 3.9 hit EOL in 2025-10.
FROM python:3.12-slim AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential libleveldb-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .
RUN pip wheel --no-cache-dir --wheel-dir /wheels .

# Stage 2: Runtime (no compiler, no sources)
FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends cron libleveldb1d && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /wheels /wheels
RUN rm -f /wheels/setuptools-*.whl && \
    pip install --no-cache-dir /wheels/*.whl && \
    rm -rf /wheels
