from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator


default_args={
    'owner':'douglaszickuhr',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'catchup': False,
    'email_on_retry': False,
    'start_date': datetime.now()
}

dag = DAG('udacity-dend-capstone-dz',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1
        )

start_operator = DummyOperator(
    dag=DAG,
    task_id='start_operator'
)

