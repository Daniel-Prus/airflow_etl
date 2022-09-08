from airflow import DAG
from datetime import datetime, date, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.utils.edgemodifier import Label
from airflow.operators.python import PythonOperator
from project_scripts.incremental_load_scd2_mssql.utils import cleanup_xcom
from project_scripts.incremental_load_scd2_mssql.custom_operators import MsSqlCustomOperator
from project_scripts.incremental_load_scd2_mssql.custom_sensors import CustomSqlSensor

# Daily data pipeline by OrderDate:
DAY_1 = "2022-07-04"
DAY_2 = '2022-07-05'
DAY_3 = '2022-07-06'

# Date variable to manipulate local calendar:
date_yesterday = date.today() - timedelta(1)
DAY_VAR = date_yesterday.strftime("%Y-%m-%d")
INGEST_DATE = DAY_1

# Data pipeline interval (pass to 'custom operator' for etl_job_summary table):
INTERVAL = 'daily_10:00_AM'

# Source MSSQL database variables:
SOURCE_DB = "NewStoreDB"
SOURCE_TABLE = "dbo.Orders"

# Target MSSQL database:
NEW_STORE_DWH = "NewStoreDW"
RAW_DATA_DWH_TABLE = "dbo.NewStoreRawData"
STG_DIM_CUSTOMERS = "dbo.STG_DimCustomers"
STG_FACT_ORDERS = "dbo.STG_FactOrders"
DW_DIM_CUSTOMERS = "dbo.DW_DimCustomers"
"0 10 * * *"
with DAG("03_incremental_load_scd2_mssql", start_date=datetime(2022, 1, 1),
         schedule_interval=None, catchup=False, tags=['airflow_etl'],
         template_searchpath="/opt/airflow/dags/project_scripts/incremental_load_scd2_mssql/sql") as dag:
    start = DummyOperator(task_id="start")

    clear_xcom = PythonOperator(
        task_id="clear_xcom",
        python_callable=cleanup_xcom,
        op_kwargs={
            "dag_str": "03_incremental_load_scd2_mssql"
        }
    )

    with TaskGroup("process_raw_data") as process_raw_data:
        # etl raw data from mssql NewStoreDB to msql NewStoreDW.dbo.NewStoreRawData by OrderDate
        extract_load_rawdata = MsSqlCustomOperator(
            task_id="extract_load_rawdata",
            conn_id="ms_sql_conn",
            source_db=SOURCE_DB,
            source_table=SOURCE_TABLE,
            destination_db=NEW_STORE_DWH,
            destination_table=RAW_DATA_DWH_TABLE,
            sql='etl_raw_data.sql',
            schedule_interval=INTERVAL,
            autocommit=True,
            params={"ingest_date": INGEST_DATE}
        )

    with TaskGroup("process_staging") as process_staging:
        raw_data_sql_sensor = CustomSqlSensor(
            task_id="raw_data_sql_sensor",
            conn_id="ms_sql_conn",
            sql="raw_data_sql_sensor.sql",
            params={"ingest_date": INGEST_DATE},
            xcom_task_id="process_raw_data.extract_load_rawdata",
            xcom_task_id_key="NewStoreDW.dbo.NewStoreRawData_rows_affected",
            fail_on_empty=False,
            poke_interval=20,
            mode="reschedule",  # free the worker
            timeout=60 * 2,
            soft_fail=False,
            exponential_backoff=True
        )

        load_stg_DimCustomers = MsSqlCustomOperator(
            task_id="stg_DimCustomers",
            conn_id="ms_sql_conn",
            sql="stg_dim_customers.sql",
            source_db=NEW_STORE_DWH,
            source_table=RAW_DATA_DWH_TABLE,
            destination_db=NEW_STORE_DWH,
            destination_table=STG_DIM_CUSTOMERS,
            schedule_interval=INTERVAL,
            autocommit=True,
            params={"ingest_date": INGEST_DATE}
        )

        load_fact_orders = MsSqlCustomOperator(
            task_id="stg_FactOrders",
            conn_id="ms_sql_conn",
            sql="stg_fact_orders.sql",
            source_db=NEW_STORE_DWH,
            source_table=RAW_DATA_DWH_TABLE,
            destination_db=NEW_STORE_DWH,
            destination_table=STG_FACT_ORDERS,
            schedule_interval=INTERVAL,
            autocommit=True,
            params={"ingest_date": INGEST_DATE}
        )
        raw_data_sql_sensor >> load_stg_DimCustomers >> load_fact_orders

    with TaskGroup("process_dwh") as process_dwh:
        load_dw_DimCustomers = MsSqlCustomOperator(
            task_id="dw_DimCustomers",
            conn_id="ms_sql_conn",
            sql="stg_dim_customers.sql",
            source_db=NEW_STORE_DWH,
            source_table=STG_DIM_CUSTOMERS,
            destination_db=NEW_STORE_DWH,
            destination_table=DW_DIM_CUSTOMERS,
            schedule_interval=INTERVAL,
            autocommit=True,
            params={"ingest_date": INGEST_DATE}
        )


start >> clear_xcom >> Label("etl raw data") >> process_raw_data >> process_staging
