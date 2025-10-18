from datetime import datetime as dt, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
import requests

from settings import config

log = LoggingMixin().log



def download_file():
    now = dt.now() - timedelta(hours=1)
    url = (
        f"https://dumps.wikimedia.org/other/pageviews/"
        f"{now:%Y}/{now:%Y-%m}/"
        f"pageviews-{now:%Y%m%d-%H}0000.gz"
    )
    response = requests.get(url)
    file_name = f"{now.strftime('%d %B %Y - %I')}00"

    file_location = f"{config.PAGE_VIEWS_DIR}/{file_name}.gz"

    with open(file_location, "wb") as txt:
        txt.write(response.content)

    log.info(f"Downloaded {file_name} successfully at {file_location} ")





