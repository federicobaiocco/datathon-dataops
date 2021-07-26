from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from utils.process_movies_data import run_pipeline


with DAG(
    dag_id="etl_movies_dag",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
) as dag:

    init_task = DummyOperator(task_id="init")

    pipeline_task = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
        dag=dag,
    )

    init_task >> pipeline_task
