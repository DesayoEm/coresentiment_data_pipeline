from airflow.sdk import DAG, task
from pendulum import datetime
from coresentiment.include.extract import download_file


with DAG(
    dag_id="page_views",
    start_date=datetime(2025, 10, 18),
    catchup=False,
    schedule=None,
    tags={"pageviews"}
):


    @task
    def _download_file():
        return download_file()

    @task
    def extract_pages():
        return ""

    @task
    def transform_pages():
        return ""

    @task
    def load_pages():
        return ""

    @task
    def analyze_pageviews():
        return ""


    _download_file() >> extract_pages() >> transform_pages() >> load_pages() >> analyze_pageviews()
