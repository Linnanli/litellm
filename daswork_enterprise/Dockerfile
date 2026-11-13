FROM python:3.12.13-slim-bookworm AS build

WORKDIR /src
COPY LICENSE LICENSE
COPY LICENSES LICENSES
COPY litellm litellm
COPY packaging/daswork-core packaging/daswork-core
COPY daswork_enterprise daswork_enterprise
RUN python -m pip install --no-cache-dir uv==0.10.9 \
    && uv sync --project daswork_enterprise --locked --no-dev --no-editable \
    && /src/daswork_enterprise/.venv/bin/python -c 'import daswork_enterprise.app, litellm; from importlib.metadata import version; assert version("daswork-litellm-oss")'

FROM python:3.12.13-slim-bookworm
ENV LITELLM_MODE=PRODUCTION \
    LITELLM_LOCAL_MODEL_COST_MAP=True \
    PATH=/src/daswork_enterprise/.venv/bin:$PATH
COPY --from=build /src/daswork_enterprise/.venv /src/daswork_enterprise/.venv
WORKDIR /srv
ENTRYPOINT ["python", "-m", "uvicorn", "daswork_enterprise.app:create_app", "--factory", "--workers", "1", "--host", "0.0.0.0"]
CMD ["--port", "4000"]
