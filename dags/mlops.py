from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    '03_mlops_example',
    default_args=default_args,
    description='Ejemplo de MLOps con Airflow',
    schedule_interval=timedelta(days=1),
    catchup=False
)



# Tarea 1: Descargar los datos
t1 = BashOperator(
    task_id='download_data',
    bash_command='python download_data.py',
    dag=dag,
)

# Tarea 2: Preprocesar los datos
t2 = BashOperator(
    task_id='preprocess_data',
    bash_command='python preprocess_data.py',
    dag=dag,
)

# Tarea 3: Entrenar el modelo
t3 = BashOperator(
    task_id='train_model',
    bash_command='python train_model.py',
    dag=dag,
)

# Tarea 4: Evaluar el modelo
t4 = BashOperator(
    task_id='evaluate_model',
    bash_command='python evaluate_model.py',
    dag=dag,
)

# Tarea 5: Desplegar el modelo
t5 = BashOperator(
    task_id='deploy_model',
    bash_command='python deploy_model.py',
    dag=dag,
)

# Tarea 6: Enviar una notificación
def send_notification():
    print('El DAG ha finalizado con éxito')

t6 = PythonOperator(
    task_id='send_notification',
    python_callable=send_notification,
    dag=dag,
)