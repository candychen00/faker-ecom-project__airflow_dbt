FROM python:3.12-slim

WORKDIR /app

COPY dbt/ /app/dbt/

WORKDIR /app/dbt

CMD ["dbt", "build"]