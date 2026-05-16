FROM apache/airflow:3.2.1
USER root
RUN apt-get update && apt-get install -y default-jdk
USER airflow
COPY requirements.txt .
RUN pip install -r requirements.txt