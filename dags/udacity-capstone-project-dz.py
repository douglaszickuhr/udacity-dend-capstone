from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from subdags.copy_to_redshift import get_s3_to_redshift
#from airflow.operators import S3ToRedshiftOperator


start_date = datetime.utcnow()

default_args = {
    'owner': 'douglaszickuhr',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'catchup': False,
    'email_on_retry': False,
    'start_date': start_date
}

dag = DAG('udacity-dend-capstone-dz',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1)

start_operator = DummyOperator(
    dag=dag,
    task_id='start_operator'
)

definitions = [{'table_name': 'tips',
                's3_key': 'yelp_academic_dataset_tip.json',
                'copy_params': ["JSON 'auto'"],
                'origin_schema': [{'name': 'user_id', 'type': 'varchar'},
                                  {'name': 'business_id', 'type': 'varchar'},
                                  {'name': 'text', 'type': 'varchar(65535)'},
                                  {'name': 'date', 'type': 'timestamp'},
                                  {'name': 'compliment_count', 'type': 'int4'}]},
               {'table_name': 'business',
                's3_key': 'yelp_academic_dataset_business_short.csv',
                'copy_params': ['FORMAT AS CSV', "IGNOREHEADER 1"],
                'origin_schema': [{'name': 'business_id', 'type': 'varchar'},
                                  {'name': 'name', 'type': 'varchar'},
                                  {'name': 'categories', 'type': 'varchar'},
                                  {'name': 'type', 'type': 'varchar'},
                                  {'name': 'latitude', 'type': 'float8'},
                                  {'name': 'longitude', 'type': 'float8'},
                                  {'name': 'review_count', 'type': 'int4'},
                                  {'name': 'state', 'type': 'varchar'},
                                  {'name': 'city', 'type': 'varchar'},
                                  {'name': 'full_address', 'type': 'varchar(65535)'},
                                  {'name': 'open', 'type': 'bool'}]},
               {'table_name': 'users',
                's3_key': 'yelp_academic_dataset_user.json',
                'copy_params': ["JSON 'auto'"],
                'origin_schema': [{'name': 'user_id', 'type': 'varchar'},
                                  {'name': 'name', 'type': 'varchar'},
                                  {'name': 'review_count', 'type': 'int4'},
                                  {'name': 'yelping_since', 'type': 'datetime'},
                                  {'name': 'useful', 'type': 'int4'},
                                  {'name': 'funny', 'type': 'int4'},
                                  {'name': 'cool', 'type': 'int4'},
                                  {'name': 'elite', 'type': 'varchar'},
                                  {'name': 'fans', 'type': 'int4'},
                                  {'name': 'average_stars', 'type': 'float8'},
                                  {'name': 'compliment_hot', 'type': 'int4'},
                                  {'name': 'compliment_more', 'type': 'int4'},
                                  {'name': 'compliment_profile', 'type': 'int4'},
                                  {'name': 'compliment_cute', 'type': 'int4'},
                                  {'name': 'compliment_list', 'type': 'int4'},
                                  {'name': 'compliment_note', 'type': 'int4'},
                                  {'name': 'compliment_plain', 'type': 'int4'},
                                  {'name': 'compliment_cool', 'type': 'int4'},
                                  {'name': 'compliment_funny', 'type': 'int4'},
                                  {'name': 'compliment_writer', 'type': 'int4'},
                                  {'name': 'compliment_photos', 'type': 'int4'}]},
               {'table_name': 'reviews',
                's3_key': 'yelp_academic_dataset_review.json',
                'copy_params': ["JSON 'auto'"],
                'origin_schema': [{'name': 'review_id', 'type': 'varchar'},
                                  {'name': 'user_id', 'type': 'varchar'},
                                  {'name': 'business_id', 'type': 'varchar'},
                                  {'name': 'stars', 'type': 'float8'},
                                  {'name': 'useful', 'type': 'int4'},
                                  {'name': 'funny', 'type': 'int4'},
                                  {'name': 'cool', 'type': 'int4'},
                                  {'name': 'text', 'type': 'varchar(max)'},
                                  {'name': 'date', 'type': 'datetime'}]}]

subdag_id = 'copy_data_to_redshift_subdag'
copy_data_to_redshift = SubDagOperator(
    subdag=get_s3_to_redshift(
        parent_dag_name='udacity-dend-capstone-dz',
        task_id=subdag_id,
        tables_definition=definitions,
        redshift_conn_id='redshift',
        redshift_schema='public',
        s3_conn_id='aws_credentials',
        s3_bucket='udac-dend-capstone-dz',
        load_type='truncate',
        schema_location='Local',
        start_date=start_date),
    task_id=subdag_id,
    dag=dag,
)

start_operator >> copy_data_to_redshift
