from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.subdag_operator import SubDagOperator
# from subdags.copy_to_redshift import get_s3_to_redshift
from airflow.operators import S3ToRedshiftOperator
import yaml


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

# Read table definitions from YAML file
with open('dags/configuration/copy_from_s3_to_redshift.yml', 'r') as file:
    copy_definitions = yaml.safe_load(file)

with dag:
    start_operator = DummyOperator(
        task_id='start_operator')

    for table in copy_definitions:
        copy_task = S3ToRedshiftOperator(
            task_id=f"copy_{table.get('table_name',None)}_to_redshift",
            redshift_conn_id='redshift',
            redshift_schema='public',
            table=f"staging_{table.get('table_name',None)}",
            s3_conn_id='aws_credentials',
            s3_bucket='udac-dend-capstone-dz',
            s3_key=table.get('s3_key', None),
            load_type='rebuild',
            copy_params=table.get('copy_params', None),
            origin_schema=table.get('origin_schema', None),
            schema_location='Local')

        copy_task.set_upstream(start_operator)
