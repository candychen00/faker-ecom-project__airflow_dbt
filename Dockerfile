FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.5.14 /uv /usr/local/bin/uv

WORKDIR /app/dbt

# install deps first so this layer is cached unless the lockfile changes
COPY dbt/pyproject.toml dbt/uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY dbt/ ./

ENV PATH="/app/dbt/.venv/bin:$PATH"

WORKDIR /app/dbt/ecom_dbt_pj

RUN dbt deps

CMD ["dbt", "build"]
