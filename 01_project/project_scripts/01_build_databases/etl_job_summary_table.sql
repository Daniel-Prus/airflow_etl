DROP TABLE IF EXISTS etl_job_summary;

CREATE TABLE etl_job_summary(
	id SERIAL,
	dag_id VARCHAR(100),
	dag_start_date TIMESTAMP,
	dag_end_date TIMESTAMP,
	schedule_interval VARCHAR(100),
	task_id VARCHAR(100),
	ingest_date DATE,
	source_db VARCHAR(100),
	source_table VARCHAR(100),
	destination_db VARCHAR(100),
	destination_table VARCHAR(100),
	start_date TIMESTAMP,
	end_date TIMESTAMP,
	execution_time double precision GENERATED ALWAYS AS (EXTRACT (EPOCH FROM end_date) - EXTRACT (EPOCH FROM start_date)) STORED,
	rows_affected INT,
    status CHAR(7),
	sql_query TEXT,
	error_message TEXT,
	created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
