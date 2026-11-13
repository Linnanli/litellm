FROM public.ecr.aws/docker/library/rust:1.98.0-slim-bookworm@sha256:1469a27c125cb5a3aebfa4f4e4665d935b02fb72cc093b2c974b3d740e43f157 AS rust
FROM public.ecr.aws/docker/library/node:22.23.0-bookworm-slim@sha256:d9f850096136edbc402debdd8729579a288aac64574ada0ff4db26b6ae58b0b2 AS node
FROM public.ecr.aws/docker/library/python:3.12.13-slim-bookworm@sha256:4766d8b510c428e595d74b9cc5bbb2fae8e26316fffb4adc89908d79aacd58a2 AS cargo-deps

WORKDIR /app
ENV PATH="/app/.venv/bin:/opt/cargo/bin:${PATH}" \
    RUSTUP_HOME=/opt/rustup \
    CARGO_HOME=/opt/cargo \
    RUSTUP_TOOLCHAIN=1.98.0 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    PRISMA_BINARY_CACHE_DIR=/opt/prisma/binaries

COPY --from=rust /usr/local/cargo /opt/cargo
COPY --from=rust /usr/local/rustup /opt/rustup
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/npm
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && apt-get update \
    && apt-get install -y --no-install-recommends build-essential pkg-config libssl-dev libsndfile1 \
    && rm -rf /var/lib/apt/lists/* \
    && python -m pip install --no-cache-dir uv==0.11.7 \
    && rustc --version && cargo --version

COPY pyproject.toml uv.lock ./
COPY litellm-proxy-extras/pyproject.toml litellm-proxy-extras/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-install-workspace --no-default-groups \
    --extra proxy --extra extra_proxy --extra proxy-runtime

COPY .cargo .cargo
COPY rust-toolchain.toml ./
COPY litellm-rust litellm-rust
RUN --mount=type=cache,target=/opt/cargo/registry,sharing=locked \
    cargo fetch --locked --manifest-path litellm-rust/Cargo.toml \
    --target "$(rustc -vV | sed -n 's/^host: //p')"

FROM cargo-deps AS build
COPY LICENSE LICENSE
COPY LICENSES LICENSES
COPY README.md README.md
COPY model_prices_and_context_window.json schema.prisma ./
COPY litellm litellm
COPY litellm_cli litellm_cli
COPY litellm-proxy-extras litellm-proxy-extras
COPY daswork_enterprise daswork_enterprise
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-default-groups --no-editable --no-install-project \
    --extra proxy --extra extra_proxy --extra proxy-runtime
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=cache,target=/opt/cargo/registry,sharing=locked \
    --mount=type=cache,target=/app/litellm-rust/target,sharing=locked \
    uvx --from maturin==1.15.0 maturin build --locked --release \
    --interpreter /app/.venv/bin/python --out /tmp/litellm-wheel \
    && uv pip install --no-deps /tmp/litellm-wheel/*.whl \
    && rm -r /tmp/litellm-wheel

RUN prisma generate --schema=/app/schema.prisma \
    && npm install --prefix /opt/prisma/schema-tool --no-audit --no-fund prisma@6.19.0 \
    && /opt/prisma/schema-tool/node_modules/.bin/prisma migrate diff \
       --from-empty --to-schema-datamodel /app/schema.prisma --script > /app/schema.generated.sql \
    && test -s /app/schema.generated.sql

FROM public.ecr.aws/docker/library/python:3.12.13-slim-bookworm@sha256:4766d8b510c428e595d74b9cc5bbb2fae8e26316fffb4adc89908d79aacd58a2
WORKDIR /app
ENV PATH="/app/.venv/bin:${PATH}" \
    LITELLM_MODE=PRODUCTION \
    LITELLM_LOCAL_MODEL_COST_MAP=True \
    PRISMA_BINARY_CACHE_DIR=/opt/prisma/binaries \
    PRISMA_OFFLINE_MODE=true \
    PRISMA_CLI_PATH=/opt/prisma/schema-tool/node_modules/.bin/prisma

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 libsndfile1 tzdata \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/node /usr/local/bin/node
COPY --from=build /app/.venv /app/.venv
COPY --from=build /opt/prisma /opt/prisma
COPY --from=build /app/schema.prisma /app/schema.generated.sql /app/LICENSE /app/
COPY --from=build /app/LICENSES /app/LICENSES
RUN python -c 'from importlib.metadata import version; from importlib.util import find_spec; from litellm.proxy.proxy_server import app; from daswork_enterprise.app import create_app; assert version("litellm") and app and create_app; assert all(find_spec(name) is None for name in ("enterprise", "litellm_enterprise"))'
ENTRYPOINT ["python", "-m", "uvicorn", "daswork_enterprise.app:create_app", "--factory"]
CMD ["--host", "0.0.0.0", "--port", "4000", "--workers", "1"]
