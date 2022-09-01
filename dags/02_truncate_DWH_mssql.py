from airflow import DAG
from datetime import datetime
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator


with DAG("02_truncate_DWH_mssql", start_date=datetime(2022, 1, 1),
         schedule_interval=None, catchup=False, tags=['airflow_etl'],
         template_searchpath="/opt/airflow/dags/project_scripts/02_truncate_DWH_mssql/") as dag:

    truncate_DWH_mssql = MsSqlOperator(
        task_id="truncate_DWH_mssql",
        mssql_conn_id="ms_sql_conn",
        autocommit=True,
        sql='truncate_DWH_mssql.sql'
    )