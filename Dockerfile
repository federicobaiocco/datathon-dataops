FROM apache/airflow:1.10.10

WORKDIR /dataops

COPY requirements.txt .
RUN pip install -r requirements.txt --user