from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from collect_data import collect_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'data_collection_dag',  
    default_args=default_args,
    description='DAG to run the collect_data PySpark job',
    schedule_interval='@daily', 
    start_date=datetime(2023, 11, 15), 
    catchup=False  
) as dag:

 
    run_collect_data = PythonOperator(
        task_id='run_collect_data_task',
        python_callable=collect_data,
    )

    run_collect_data
