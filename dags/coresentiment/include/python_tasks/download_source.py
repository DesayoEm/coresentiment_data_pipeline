from datetime import datetime as dt, timedelta
import requests
from airflow.utils.log.logging_mixin import LoggingMixin
from coresentiment.include.config.settings import config
from coresentiment.include.python_tasks.callbacks import deploy_failure
from airflow.sdk import task

log = LoggingMixin().log


def download_file_task(**context):
    now = dt.now() - timedelta(hours=45)

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

        log.info(f"Dumped page views successfully at {file_location}")


        return {
            'file_location': file_location,
            'dump_date': str(now.date()),
            'dump_hour': now.hour,
            'task_display': 'file download',
        }

    except Exception as e:
        deploy_failure(
            error = e,
            context = context,
            metadata = {
                'task_display': 'File download',
                'dump_date': str(now.date()),
                'dump_hour': now.hour
            }


        )


