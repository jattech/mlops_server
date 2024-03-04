from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


#Comment
def helloWorld():
    print('Hello World')

def byeWorld():
    print('Bye World')

with DAG("01_python_example",
         start_date=datetime(2023,10,1),
         schedule_interval="@hourly",
         catchup=False
) as dag:
    
    task1 = PythonOperator(
        task_id="hello_world",
        python_callable=helloWorld)
    
    task2 = PythonOperator(
        task_id="bye_world",
        python_callable=byeWorld)
    
    
    task1 >> task2