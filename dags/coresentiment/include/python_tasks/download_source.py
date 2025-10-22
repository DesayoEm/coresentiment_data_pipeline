from datetime import datetime as dt, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
import requests
from coresentiment.include.config.settings import config

log = LoggingMixin().log

def download_file(ti):
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

        file_name = f"{now.strftime('%d %B %Y - %H')}00"
        file_location = f"{config.PAGE_VIEWS_DIR}/{file_name}.gz"


        with open(file_location, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        ti.xcom_push(key='file_location', value=file_location)
        ti.xcom_push(key='dump_date', value=now.date())
        ti.xcom_push(key='dump_hour', value=now.hour)

        log.info(f"Dumped page views for {"pass"} successfully at {file_location}")



    except Exception as e:
        log.error(f"Could not download file at {url}. Details: {str(e)}")
        raise e


