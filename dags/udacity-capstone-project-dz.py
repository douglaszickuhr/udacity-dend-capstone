from airflow import DAG
from datetime import datetime, timedelta
import json
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import S3ToRedshiftOperator

default_args={
    'owner':'douglaszickuhr',
    'depends_on_past': False,
    #'retries': 3,
    #'retry_delay': timedelta(minutes=3),
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
    dag=dag,
    task_id='start_operator'
)

copy_tips_to_redshift = S3ToRedshiftOperator(
    dag=dag,
    task_id='copy_tips_to_redshift',
    redshift_conn_id='redshift',
    redshift_schema='dev',
    table='tips_staging',
    s3_conn_id='aws_credentials',
    s3_bucket='udacity-dend-capstone-douglas',
    s3_key='yelp_academic_dataset_tip.json',
    load_type='truncate',
    origin_schema=[{'name':'user_id',
                      'type':'varchar'},
                      {'name':'business_id',
                      'type':'varchar'},
                      {'name':'text',
                      'type':'varchar(65535)'},
                      {'name':'date',
                      'type':'timestamp'},
                      {'name':'compliment_count',
                      'type':'int4'}],
    schema_location = 'Local'
)

start_operator >> copy_tips_to_redshift
