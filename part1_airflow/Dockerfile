FROM apache/airflow:2.7.3-python3.10

COPY requirements.txt /requirements.txt
# apache-airflow в команде нужен, чтобы pip не подтянул другую версию вместе с провайдерами
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
