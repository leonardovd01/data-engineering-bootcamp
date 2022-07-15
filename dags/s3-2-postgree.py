"""Database Ingestion Workflow
Author: Enrique Olivares <enrique.olivares@wizeline.com>
Description: Ingests the data from a S3 bucket into a postgres table.
"""

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# General constants
DAG_ID = "aws_database_ingestion_workflow"
STABILITY_STATE = "stable"
CLOUD_PROVIDER = "aws"

# AWS constants
AWS_CONN_ID = "project_s3_conn"
S3_BUCKET_NAME = "s3-data-bootcamp-leo20220715182015278000000005"
S3_KEY_NAME = "user_purchase.csv"

# Postgres constants
POSTGRES_CONN_ID = "project_post_conn"
POSTGRES_SCHEMA_NAME = "users_purchase_data"
POSTGRES_TABLE_NAME = "user_purchase"


def csv_to_postgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='conn_postgress')
    get_postgres_conn = PostgresHook(postgres_conn_id='conn_postgress').get_conn()
    curr = get_postgres_conn.cursor("cursor")
    # CSV loading to table.

    # Getting the current work directory (cwd)
    table_dir = os.getcwd()
    # r=root, d=directories, f=files
    for r, d, f in os.walk(table_dir):
        for file in f:
            if file.endswith("user_purchase.csv"):
                table_path = os.path.join(r, file)

    print(table_path)

    file = table_path
    with open(file, 'r') as f:
        next(f)
        curr.copy_from(f, 'user_purchase', sep=',')
        get_postgres_conn.commit()


with DAG(
    dag_id=DAG_ID,
    schedule_interval="@once",
    start_date=days_ago(1),
    tags=[CLOUD_PROVIDER, STABILITY_STATE],
) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")

    verify_key_existence = S3KeySensor(
        task_id="verify_key_existence",
        aws_conn_id=AWS_CONN_ID,
        bucket_name=S3_BUCKET_NAME,
        bucket_key=S3_KEY_NAME,
    )

    create_table_entity = PostgresOperator(
        task_id="create_table_entity",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE_NAME} (
                invoice_number varchar(10),
                stock_code varchar(20),
                detail varchar(1000),
                quantity int,
                invoice_date timestamp,
                unit_price numeric(8,3),
                customer_id int,
                country varchar(20)
            );
        """,
    )

    clear_table = PostgresOperator(
        task_id="clear_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"DELETE FROM {POSTGRES_TABLE_NAME}",
    )
    continue_process = DummyOperator(task_id="continue_process")

    ingest_data = PythonOperator(task_id='csv_to_database',
                   provide_context=True,
                   python_callable=csv_to_postgres,
                   dag=dag)

    validate_data = BranchSQLOperator(
        task_id="validate_data",
        conn_id=POSTGRES_CONN_ID,
        sql=f"SELECT COUNT(*) AS total_rows FROM {POSTGRES_TABLE_NAME}",
        follow_task_ids_if_false=[continue_process.task_id],
        follow_task_ids_if_true=[clear_table.task_id],
    )

    end_workflow = DummyOperator(task_id="end_workflow")

    (
        start_workflow
        >> verify_key_existence
        >> create_table_entity
        >> validate_data
    )
    validate_data >> [clear_table, continue_process] >> ingest_data
    ingest_data >> end_workflow

    dag.doc_md = __doc__