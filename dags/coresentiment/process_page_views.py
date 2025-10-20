from airflow.sdk import dag, task
from pendulum import datetime
from airflow.providers.standard.operators.python import PythonOperator
from coresentiment.include.download_source import download_file
from coresentiment.include.process_pages import process_page_views_count


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
        op_kwargs={'file_location': '{{ti.xcom_pull(task_ids="download_file")}}'}
    )


    # @task
    # def load_pages():
    #     return ""
    #
    # @task
    # def analyze_pageviews():
    #     return ""


    _download_file >> _process_page_views_count


process_page_views()