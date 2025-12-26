
FROM apache/airflow:2.7.2-python3.11

USER root

# Instalar Java 17 (Requerido por PySpark 3.5+)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Instalar PySpark y dependencias del proyecto
COPY requirements.txt .
RUN pip install --no-cache-dir "apache-airflow==2.7.2" -r requirements.txt
