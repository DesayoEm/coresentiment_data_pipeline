from airflow.sdk import dag, task
from pendulum import datetime
from coresentiment.include.python_tasks.download_source import download_file_task
from coresentiment.include.python_tasks.callbacks import success_callback
from coresentiment.include.python_tasks.process_pages import process_page_views_count
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

    # @task
    # def process_page_views_task(**file_info):
    #     return process_page_views_count(
    #         file_location=file_info['file_location'],
    #         dump_date=file_info['dump_date'],
    #         dump_hour=file_info['dump_hour']
    #     )
    #
    # @task
    # def create_table_task():
    #     from airflow.providers.common.sql.hooks.sql import DbApiHook
    #     hook = SqlHook(conn_id="coresentiment_db")
    #     with open("include/sql/create_page_views_table.sql", 'r') as f:
    #         sql = f.read()
    #     hook.run(sql)
    #
    #
    # @task
    # def load_to_warehouse_task():
    #     return load_data_from_file()
    #
    # @task
    # def analyse_page_views_task():
    #     from airflow.providers.common.sql.hooks.sql import SqlHook
    #     hook = SqlHook(conn_id="coresentiment_db")
    #     with open("include/sql/analyse_views.sql", 'r') as f:
    #         sql = f.read()
    #     result = hook.get_first(sql)
    #     print(f"Analysis result: {result}")
    #     return result


    download_file()
    # processed = process_page_views_task(file_info)
    # table_created = create_table_task()
    # table_created.set_upstream(processed)
    # loaded = load_to_warehouse_task()
    # loaded.set_upstream(table_created)
    # analysis = analyse_page_views_task()
    # analysis.set_upstream(loaded)


process_page_views()