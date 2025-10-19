from airflow.sdk import dag, task
from pendulum import datetime
from coresentiment.include.download_source import download_file
from coresentiment.include.extract_pages import extract_page_counts


@dag(
    dag_id="process_page_views",
    start_date=datetime(2025, 10, 18),
    catchup=False,
    schedule=None,
    tags=["pageviews"]
)
def process_page_views():


    @task
    def _download_file():
        return download_file()

    @task
    def extract_pages(file_location):
        return extract_page_counts(file_location)

    @task
    def load_pages():
        return ""

    @task
    def analyze_pageviews():
        return ""


    file_path = _download_file()
    page_counts = extract_pages(file_path)