from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

current_folder = os.getcwd()


with DAG(dag_id="flight_pipeline",
         start_date=datetime(2023,10,10),
         schedule_interval="0 0 * * *",  # Run every day at midnight
         catchup=False) as dag:
  
        

        load_data = BashOperator(
            task_id='load_data',
            bash_command=f'{current_folder}/spark/bin/spark-submit --master local[*] {current_folder}/scripts/data_loading.py',
            dag=dag
        )

        analyse_data = BashOperator(
            task_id='analyse_data',
            bash_command=f'{current_folder}/spark/bin/spark-submit --master local[*] {current_folder}/scripts/data_analysing.py',
            dag=dag
        )

load_data >> analyse_data
