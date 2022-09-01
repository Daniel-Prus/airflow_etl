from airflow.models import BaseOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from project_scripts.incremental_load_scd2_mssql.mssql_query import MsSqlQuerySupportSCD2
from airflow.models import XCom


class MsSqlCustomOperator(BaseOperator):

    def __init__(self, conn_id: str, source_db: str, destination_db: str, source_table: str, destination_table: str,
                 ingest_date: str, sql: str, autocommit: bool, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.source_db = source_db
        self.destination_db = destination_db
        self.source_table = source_table
        self.destination_table = destination_table
        self.ingest_date = ingest_date
        self.sql = sql
        self.autocommit = autocommit
        self.hook = MsSqlHook(mssql_conn_id=self.conn_id, schema=self.destination_db)
        self.__MsSqlQuerySupportSCD2 = MsSqlQuerySupportSCD2(ingest_date=ingest_date, source_db=source_db,
                                                             destination_db=destination_db,
                                                             source_table=source_table,
                                                             destination_table=destination_table)

    def _get_cursor_msql(self) -> object:
        conn = self.hook.get_conn()
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
        sql_query = self.__MsSqlQuerySupportSCD2.get_last_row_sql_sensor_query()
        cursor.execute(sql_query)
        row = cursor.fetchone()
        row_str = self.change_row_to_str(row)
        return row_str

    def get_rows_count_sql_sensor(self, cursor) -> int:
        sql_query = self.__MsSqlQuerySupportSCD2.get_rows_count_sql_sensor_query()
        cursor.execute(sql_query)
        rows_count = cursor.fetchone()
        return rows_count

    def get_and_push_xcom(key: str, value: object, **context):
        context['ti'].xcom_push(key=key, value=value)

    def execute(self, context) -> None:
        cursor = self._get_cursor_msql()

        try:
            # run sql statement
            self.hook.run(self.sql, self.autocommit)

            # push rows number to xcom
            rows_num = self.get_rows_count_sql_sensor(cursor)
            self.get_and_push_xcom(key=f"{self.task_id}_rows_num_{self.start_date}", value=rows_num)

        except Exception as e:
            print(str(e))


"""
for output in self.hook.conn.notices:
 self.log.info(output)
"""
