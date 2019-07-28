from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from subdags.copy_to_redshift import get_s3_to_redshift
from airflow.operators.postgres_operator import PostgresOperator
from airflow.executors import GetDefaultExecutor
import yaml

start_date = datetime.utcnow()

default_args = {
    'owner': 'douglaszickuhr',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'catchup': False,
    'email_on_retry': False,
    'start_date': start_date,
    'template_searchpath': './dags'
}

dag = DAG('udacity-dend-capstone-dz',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1)

# Read table definitions from YAML file
with open('dags/configuration/copy_from_s3_to_redshift.yml', 'r') as file:
    copy_definitions = yaml.safe_load(file)

with open('dags/configuration/dimensions.yml', 'r') as file:
    dimensions_definitions = yaml.safe_load(file)

with dag:
    start_operator = DummyOperator(
        task_id='start_operator')

    subdag_id = 'copy_data_to_redshift'
    copy_data_to_redshift = SubDagOperator(
        subdag=get_s3_to_redshift(
            parent_dag_name='udacity-dend-capstone-dz',
            task_id=subdag_id,
            tables_definition=copy_definitions,
            redshift_conn_id='redshift',
            redshift_schema='public',
            s3_conn_id='aws_credentials',
            s3_bucket='udac-dend-capstone-dz',
            load_type='truncate',
            schema_location='Local',
            start_date=start_date),
        task_id=subdag_id,
        dag=dag,
        executor=GetDefaultExecutor())
    copy_data_to_redshift.set_upstream(start_operator)

    for dimension in dimensions_definitions:
        dimension_task = PostgresOperator(
            task_id=f"process_{dimension.get('task_name')}",
            sql=dimension.get('sql', None),
            postgres_conn_id='redshift')
        dimension_task.set_upstream(copy_data_to_redshift)
