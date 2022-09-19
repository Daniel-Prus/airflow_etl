from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

INGEST_DATE = '2022-07-04'

with DAG("02_spark_test", start_date=datetime(2022, 1, 1),
         schedule_interval=None, catchup=False, tags=['airflow_etl']) as dag:
    spark_test = SparkSubmitOperator(
        task_id="spark_test",
        conn_id="spark_conn",
        application="/opt/airflow/dags/project_scripts/01_mssql_test.py",
        name="Get_raw_data",
        jars="/opt/airflow/dags/project_scripts/lib/enu/mssql-jdbc-11.2.0.jre8.jar",
        application_args=[INGEST_DATE],
        verbose=False,
    )

# "/opt/airflow/dags/project_scripts/01_build_databases/"
