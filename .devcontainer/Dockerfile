FROM ghcr.io/astral-sh/uv:0.12.19@sha256:04d046b13e60d6bcec73cbc5e1cad25d680dea90c8573340950a0ac2d1aef424 AS uv

FROM mcr.microsoft.com/devcontainers/base:ubuntu-24.04@sha256:d94c97dd9cacf183d0a6fd12a8e87b526e9e928307674ae9c94139139c0c6eae

ARG DEBIAN_FRONTEND=noninteractive

USER root

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        binutils \
    && rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /usr/local/bin/uv

ENV PATH="/home/vscode/.local/bin:${PATH}" \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_TOOL_BIN_DIR=/home/vscode/.local/bin \
    UV_PYTHON_DOWNLOADS=never

USER vscode
