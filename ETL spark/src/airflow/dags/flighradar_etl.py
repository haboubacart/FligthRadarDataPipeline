import sys
sys.path.append('../../')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from extract import main
def main2():
    print("hello")

default_args = {
    'owner': 'votre_nom',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Initialisez votre DAG
dag = DAG(
    'mon_dag',
    default_args=default_args,
    description='Un simple DAG qui exécute une fonction Python',
    schedule_interval=None,
)

python_operator = PythonOperator(
    task_id='execution_fonction_python',
    python_callable=main2,
    dag=dag,
)

spark_submit_task = BashOperator(
    task_id='spark_submit_task',
    bash_command='docker exec spark-submit --master spark://172.22.0.2:7077 /spark/data/Transform/clean_and_transform.py',
    dag=dag,
)

python_operator >> spark_submit_task 