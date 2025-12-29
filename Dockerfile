
FROM apache/airflow:2.7.2-python3.11

USER root

# Instalar Java 17 (Requerido por PySpark 3.5+) y Google Cloud SDK
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    curl \
    gnupg \
    lsb-release && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update && \
    apt-get install -y google-cloud-sdk && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Instalar PySpark y dependencias del proyecto
COPY requirements.txt .
RUN pip install --no-cache-dir "apache-airflow==2.7.2" -r requirements.txt

