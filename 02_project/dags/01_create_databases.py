from airflow import DAG
from datetime import datetime
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator


with DAG("01_build_databases_mssql", start_date=datetime(2022, 1, 1),
         schedule_interval=None, catchup=False, tags=['airflow_etl'],
         template_searchpath="/opt/airflow/dags/project_scripts/01_build_databases/") as dag:
    # SQL server connection: (SQL Server Configuration Manager -> Network Configuration -> Protocols -> TCP/IP -> IP8
    # SQL Authentication (set login and password in SSMS)
    # Host: 192.168.0.103
    # port: 1433

    create_NewStoreDB = MsSqlOperator(
        task_id="create_NewStoreDB",
        mssql_conn_id="ms_sql_conn",
        autocommit=True,
        sql="create_NewStoreDB.sql"
    )

    create_NewStoreDW = MsSqlOperator(
        task_id="create_NewStoreDW",
        mssql_conn_id="ms_sql_conn",
        autocommit=True,
        sql="create_NewStoreDW.sql"
    )

    create_etl_job_summary_table = PostgresOperator(
        # host: postgres
        # login: airflow
        # password: airflow
        # Port: 5432
        task_id='create_etl_job_summary_table',
        postgres_conn_id='postgres_etl_conn',
        autocommit=True,
        sql='etl_job_summary_table.sql'
    )

    create_hdfs_path = BashOperator(
        task_id="create_hdfs_path",
        bash_command="""
        hdfs dfs -mkdir -p /NewStoreRawData    
        """,
        do_xcom_push=False

    )