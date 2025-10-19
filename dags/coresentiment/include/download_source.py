from datetime import datetime as dt, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
import requests
from .config.settings import config

log = LoggingMixin().log

def download_file():
    now = dt.now() - timedelta(hours=4)

    try:
        url = (
            f"https://dumps.wikimedia.org/other/pageviews/"
            f"{now:%Y}/{now:%Y-%m}/"
            f"pageviews-{now:%Y%m%d-%H}0000.gz"
        )

        log.info(f"Downloading pageviews dump at {url}")
        response = requests.get(url, stream=True)
        response.raw.decode_content = False

        file_name = f"{now.strftime('%d %B %Y - %I %p')}"
        file_location = f"{config.PAGE_VIEWS_DIR}/{file_name}.gz"


        with open(file_location, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        log.info(f"Dumped page views for {"pass"} successfully at {file_location}")
        return file_location



    except Exception as e:
        log.error(f"Could not download file at {url}. Details: {str(e)}")
        raise e


