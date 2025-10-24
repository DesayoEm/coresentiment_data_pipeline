from airflow.sdk import dag, task
from pendulum import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from coresentiment.include.python_tasks.extract import download_file_task
from coresentiment.include.python_tasks.notification import success_callback
from coresentiment.include.python_tasks.transform import process_page_views_count_task
from coresentiment.include.python_tasks.load_data import load_data_from_file


@dag(
    dag_id="process_page_views",
    start_date=datetime(2025, 10, 18),
    catchup=False,
    schedule=None,
    tags=["pageviews"]
)

def process_page_views():

    @task(on_success_callback=success_callback)
    def download_file():
        return download_file_task()

    @task(on_success_callback=success_callback)
    def process_page_views_count(raw_file):
        return process_page_views_count_task(
            file_location=raw_file['file_location'],
            dump_date=raw_file['dump_date'],
            dump_hour=raw_file['dump_hour']
        )

    create_schemas_and_tables = SQLExecuteQueryOperator(
        task_id="create_schemas_and_tables",
        sql="include/sql/create_tables.sql",
        conn_id="coresentiment_db",
    )

    truncate_staging = SQLExecuteQueryOperator(
        task_id="truncate_staging",
        sql="include/sql/clear_staging.sql",
        conn_id="coresentiment_db"
    )

    @task(on_success_callback=success_callback)
    def stage_data_in_warehouse(processed_file):
        return load_data_from_file(
            file_location=processed_file['file_location'],
            dump_date=processed_file['dump_date'],
            dump_hour=processed_file['dump_hour']
        )

    load_analytics_table = SQLExecuteQueryOperator(
        task_id="load_analytics_table",
        sql="include/sql/load_analytics_table.sql",
        conn_id="coresentiment_db"
    )

    analyse_page_views = SQLExecuteQueryOperator(
        task_id="run_analysis",
        sql="include/sql/analyse_views.sql",
        conn_id="coresentiment_db",
        do_xcom_push=True

    )

    @task
    def log_analysis_result(ti=None):
        result = ti.xcom_pull(task_ids="run_analysis")
        print("Top performing company:", result)

    raw_file = download_file()
    processed_file = process_page_views_count(raw_file)


    tables_created = create_schemas_and_tables
    staging_cleared = truncate_staging
    tables_created >> staging_cleared

    stage_data = stage_data_in_warehouse(processed_file)
    [staging_cleared, processed_file] >> stage_data


    stage_data >> load_analytics_table

    analysis = analyse_page_views
    load_analytics_table >> analysis

    analysis >> log_analysis_result()


process_page_views()