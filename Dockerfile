FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.5.14 /uv /usr/local/bin/uv

WORKDIR /app/dbt/

# install uv packages, without installing the project itself
# destination: /app/dbt/
COPY dbt/pyproject.toml dbt/uv.lock /app/dbt/
RUN uv sync --frozen --no-dev --no-install-project --no-cache

COPY dbt/ /app/dbt/

ENV PATH="/app/dbt/.venv/bin:$PATH"

WORKDIR /app/dbt/ecom_dbt_pj/

RUN dbt deps

CMD ["dbt", "build"]
