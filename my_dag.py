
from airflow import DAG
from modules import test1
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG("my_dag", start_date=datetime(2023,6,7),
          schedule_interval="@daily", catchup = False) as dag:
        

        etl_task= PythonOperator(
        
                task_id="etl_task",
                python_callable = test1.etl_function,
                dag=dag
        )      
        
        etl_task


       
