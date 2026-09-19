# Образ Airflow для проекта: официальный образ 2.7.3 + зависимости из requirements.txt
FROM apache/airflow:2.7.3-python3.10

# Устанавливаем провайдеры и библиотеки, которые используют DAG и плагины.
# Версию самого Airflow фиксируем через ${AIRFLOW_VERSION} (переменная задана в базовом образе),
# чтобы pip не обновил его при установке зависимостей.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
