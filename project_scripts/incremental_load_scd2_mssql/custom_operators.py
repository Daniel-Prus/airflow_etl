from airflow.models import BaseOperator
from project_scripts.incremental_load_scd2_mssql.custom_hooks import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import Any, Optional, Union, Iterable, Mapping


class MsSqlCustomOperator(BaseOperator):
    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    # class variables to keep track of etl proccess
    etl_job_conn = "postgres_etl_conn"
    etl_job_db = "airflow"
    etl_job_table = "etl_job_summary"
    target_fields_if_success = ('dag_id', 'dag_start_date', 'dag_end_date', 'schedule_interval', 'task_id',
                                'ingest_date', 'source_db', 'source_table', 'destination_db', 'destination_table',
                                'start_date', 'end_date', 'rows_affected', 'status', 'sql_query')
    target_fields_if_fail = ('dag_id', 'dag_start_date', 'dag_end_date', 'schedule_interval', 'task_id', 'ingest_date',
                             'source_db', 'source_table', 'destination_db', 'destination_table', 'start_date',
                             'end_date', 'status', 'sql_query', 'error_message')

    def __init__(self, conn_id: str,
                 sql: Union[str, Iterable[str]],
                 source_db: str,
                 destination_db: str,
                 source_table: str,
                 destination_table: str,
                 schedule_interval: Optional = None,
                 autocommit: bool = False,
                 parameters: Optional[Union[Iterable, Mapping]] = None,
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql = sql
        self.parameters = parameters
        self.source_db = source_db
        self.destination_db = destination_db
        self.source_table = source_table
        self.destination_table = destination_table
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

    def execute(self, context: Any) -> None:

        try:
            self.hook.run(sql=self.sql, autocommit=self.autocommit, parameters=self.parameters)

            # prepare etl summary row when -> Success
            row = ((self.dag_id, self.start_date, self.end_date, self.schedule_interval, self.task_id,
                    self.params["ingest_date"], self.source_db, self.source_table, self.destination_db,
                    self.destination_table, self.hook.query_start_datetime, self.hook.query_end_datetime,
                    self.hook.rows_affected, 'Success', self.sql.replace("'", "''")),)

            # insert 'success row' to etl job summary table
            self.hook_etl_job.insert_rows(table=self.etl_job_table, rows=row,
                                          target_fields=self.target_fields_if_success,
                                          commit_every=0)

            # push to xcom number of rows for sql sensor
            context['ti'].xcom_push(key=f"{self.destination_db}.{self.destination_table}_rows_affected",
                                    value=self.hook.rows_affected)

        except Exception as e:
            error_message = str(e).replace("(", "").replace(")", "")
            row = ((self.dag_id, self.start_date, self.end_date, self.schedule_interval, self.task_id,
                    self.params["ingest_date"], self.source_db, self.source_table, self.destination_db,
                    self.destination_table, self.hook.query_start_datetime, self.hook.query_end_datetime, 'Fail',
                    self.sql.replace("'", "''"), error_message),)

            # insert fail row to etl job summary table
            self.hook_etl_job.insert_rows(table=self.etl_job_table, rows=row,
                                          target_fields=self.target_fields_if_fail,
                                          commit_every=0)
            raise e
