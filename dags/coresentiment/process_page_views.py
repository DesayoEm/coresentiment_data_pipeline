from airflow.sdk import dag, task
from pendulum import datetime
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from coresentiment.include.python_tasks.download_source import download_file
from coresentiment.include.python_tasks.process_pages import process_page_views_count


@dag(
    dag_id="process_page_views",
    start_date=datetime(2025, 10, 18),
    catchup=False,
    schedule=None,
    tags=["pageviews"]
)
def process_page_views():

    _download_file = PythonOperator(
        task_id="download_file",
        python_callable=download_file

    )

    _process_page_views_count = PythonOperator(
        task_id="process_page_views_count",
        python_callable=process_page_views_count,
        op_kwargs={
            'file_location': '{{ti.xcom_pull(task_ids="download_file", key="file_location")}}',
            'dump_date': '{{ti.xcom_pull(task_ids="download_file", key="dump_date")}}',
            'dump_hour': '{{ti.xcom_pull(task_ids="download_file", key="dump_hour")}}'
        }
    )

    _create_page_views_table = SQLExecuteQueryOperator(
        task_id="create_page_views_table",
        sql="include/sql/create_page_views_table.sql",
        conn_id="coresentiment_db"
    )

    # _load_to_warehouse = SQLExecuteQueryOperator(
    #     task_id="_load_to_warehouse",
    #     conn_id="postgres",
    #     sql="include/sql/load_to_dw.sql",
    #     parameters = {'input_file_path': '{{ti.xcom_pull(task_ids="process_page_views_count")}}'}
    # )

    # @task
    # def analyze_pageviews():
    #     return ""

    _download_file >> _process_page_views_count >> _create_page_views_table
    # _download_file >> _process_page_views_count >>  _load_to_warehouse


process_page_views()
