FROM apache/airflow:3.3.2-python3.12

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt"

WORKDIR /opt/airflow/

ENV PYTHONPATH="/opt/airflow"