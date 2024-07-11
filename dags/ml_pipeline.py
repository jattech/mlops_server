from airflow.models import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime

from utils.load_data import load_data
from utils.preprocess_data import preprocess_data
from utils.experiment import experiment
from utils.track_experiments_info import track_experiments_info
from utils.fit_best_model import fit_best_model
from utils.save_batch_data import save_batch_data


default_args= {
    'owner': 'Data Scientist',
    'email_on_failure': False,
    'email': ['ds@mymail.com'],
    'start_date': datetime(2023, 10, 1)
}

with DAG(
    "04_ml_pipeline",
    description='End-to-end ML pipeline example',
    schedule_interval='@daily',
    default_args=default_args,
    template_searchpath = '/home/jovyan/work/dags/sql', 
    catchup=False
) as dag:

    # Group Task: 1 -> CREATE AUX TABLES IN DB
    with TaskGroup('creating_storage_structures') as creating_storage_structures:

        # task: 1.1 -> Create table experiments in db relate to connection postgres_postgres_local_connection
        creating_experiment_tracking_table = PostgresOperator(
            task_id="creating_experiment_tracking_table",
            postgres_conn_id='postgres_local_connection',
            sql='sql/create_experiments.sql'
        )

        # task: 1.2 -> Create batch data table in db relate to connection postgres_postgres_local_connection
        creating_batch_data_table = PostgresOperator(
            task_id="creating_batch_data_table",
            postgres_conn_id='postgres_local_connection',
            sql='sql/create_batch_data_table.sql'
        )

    # task: 2 -> Execute file load_data 
    fetching_data = PythonOperator(
        task_id='fetching_data',
        python_callable=load_data

    )
    
    # task: 3
    with TaskGroup('preparing_data') as preparing_data:

        # task: 3.1 -> Split dataset in train and test 
        preprocessing = PythonOperator(
            task_id='preprocessing',
            python_callable=preprocess_data
        )

        # task: 3.2 -> Save data in database (batch table) 
        saving_batch_data = PythonOperator(
            task_id='saving_batch_data',
            python_callable=save_batch_data
        )
        
    # task: 4 -> Hyperparam tuning
    hyperparam_tuning = PythonOperator(
        task_id='hyperparam_tuning',
        python_callable=experiment
    )

    # task: 5
    with TaskGroup('after_crossvalidation') as after_crossvalidation:

        # =======
        # task: 5.1  --> Save data in experiments table      
        saving_results = PythonOperator(
            task_id='saving_results',
            python_callable=track_experiments_info
        )

        # task: 5.2
        fitting_best_model = PythonOperator(
            task_id='fitting_best_model',
            python_callable=fit_best_model
        )    

    creating_storage_structures >> fetching_data >> preparing_data >> hyperparam_tuning >> after_crossvalidation