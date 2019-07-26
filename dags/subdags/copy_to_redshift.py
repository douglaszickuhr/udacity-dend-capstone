# from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import S3ToRedshiftOperator


def get_s3_to_redshift(
    parent_dag_name,
    task_id,
    tables_definition,
    redshift_conn_id,
    redshift_schema,
    s3_conn_id,
    s3_bucket,
    load_type,
    schema_location,
    *args, **kwargs):

    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    for table in tables_definition:
        S3ToRedshiftOperator(
            dag=dag,
            task_id=f"copy_{table.get('table_name',None)}_to_redshift",
            redshift_conn_id=redshift_conn_id,
            redshift_schema=redshift_schema,
            table=f"staging_{table.get('table_name',None)}",
            s3_conn_id=s3_conn_id,
            s3_bucket=s3_bucket,
            s3_key=table.get('s3_key', None),
            load_type=load_type,
            copy_params=table.get('copy_params', None),
            origin_schema=table.get('origin_schema', None),
            schema_location=schema_location)

    return dag
