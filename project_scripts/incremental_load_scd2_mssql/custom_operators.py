from airflow.models import BaseOperator
from project_scripts.incremental_load_scd2_mssql.custom_hooks import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from project_scripts.incremental_load_scd2_mssql.mssql_query import MsSqlQuerySupportSCD2
from psycopg2.sql import SQL
from typing import Any, Optional
import pandas as pd


class MsSqlCustomOperator(BaseOperator):
    # class variables to keep track of etl process
    etl_job_conn = "postgres_etl_conn"
    etl_job_db = "airflow"
    etl_job_table = "etl_job_summary"
    target_fields_if_success = ('dag_id', 'dag_start_date', 'dag_end_date', 'schedule_interval', 'task_id',
                                'ingest_date', 'source_db', 'source_table', 'destination_db', 'destination_table',
                                'start_date', 'end_date', 'rows_affected', 'status', 'sql_query')
    target_fields_if_fail = ('dag_id', 'dag_start_date', 'dag_end_date', 'schedule_interval', 'task_id', 'ingest_date',
                             'source_db', 'source_table', 'destination_db', 'destination_table', 'start_date',
                             'end_date', 'status', 'sql_query', 'error_message')

    def __init__(self, conn_id: str, source_db: str, destination_db: str, source_table: str,
                 destination_table: str, ingest_date: str, sql: str, schedule_interval: Optional = None,
                 autocommit: bool = False, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.source_db = source_db
        self.destination_db = destination_db
        self.source_table = source_table
        self.destination_table = destination_table
        self.ingest_date = ingest_date
        self.sql = sql
        self.schedule_interval = schedule_interval
        self.autocommit = autocommit
        self.hook = MsSqlHook(mssql_conn_id=self.conn_id, schema=self.destination_db, log_sql=True)
        self.hook_etl_job = PostgresHook(postgres_conn_id=self.etl_job_conn, schema=self.etl_job_db)

    def _get_cursor_etl_job(self) -> object:
        conn = self.hook_etl_job.get_conn()
        cursor = conn.cursor()
        return cursor

    @staticmethod
    def change_row_to_str(row) -> str:
        row_str = str(row)
        cond_dict = {"(": "", ")": "", "'": "", " ": "", "\"": ""}
        for x, y in cond_dict.items():
            row_str = row_str.replace(x, y)
        return row_str

    def get_last_row_sql_sensor(self, cursor) -> str:
        sql = self.sql_support_class.get_last_row_sql_sensor_query()
        cursor.execute(sql)
        row = cursor.fetchone()
        row_str = self.change_row_to_str(row)
        return row_str

    def get_rows_count_sql_sensor(self, cursor) -> int:
        sql = self.sql_support_class.get_rows_count_sql_sensor_query()
        cursor.execute(sql)
        rows_count = cursor.fetchone()
        return rows_count[0]

    def execute(self, context: Any) -> None:
        # run main sql guery
        final_sql = []
        final_sql.append(SQL(self.sql))
        sql_str = final_sql[0].as_string(SQL)
        try:

            self.hook.run(final_sql, self.autocommit)

            # prepare etl summary row when -> Success
            row = ((self.dag_id, self.start_date, self.end_date, self.schedule_interval, self.task_id, self.ingest_date,
                    self.source_db, self.source_table, self.destination_db, self.destination_table,
                    self.hook.query_start_datetime, self.hook.query_end_datetime, self.hook.rows_affected, 'Success',
                    sql_str.replace("'", "''")),)

            # insert success row to etl job summary table
            self.hook_etl_job.insert_rows(table=self.etl_job_table, rows=row,
                                          target_fields=self.target_fields_if_success,
                                          commit_every=0)

            # push to xcom number of rows for sql sensor
            context['ti'].xcom_push(key=f"{self.destination_db}.{self.destination_table}_rows_affected",
                                    value=self.hook.rows_affected)

        except Exception as e:
            error_message = str(e).replace("'", "''")
            row = ((self.dag_id, self.start_date, self.end_date, self.schedule_interval, self.task_id,
                    self.ingest_date, self.source_db, self.source_table, self.destination_db, self.destination_table,
                    self.hook.query_start_datetime, self.hook.query_end_datetime, 'Fail',
                    sql_str.replace("'", "''"), error_message),)

            # insert fail row to etl job summary table
            self.hook_etl_job.insert_rows(table=self.etl_job_table, rows=row,
                                          target_fields=self.target_fields_if_fail,
                                          commit_every=0)
            raise e

        for output in self.hook.conn.notices:
            self.log.info(output)