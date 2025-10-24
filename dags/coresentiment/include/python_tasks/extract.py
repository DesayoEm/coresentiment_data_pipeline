from datetime import datetime as dt, timedelta
import os
import requests
import shutil
from airflow.utils.log.logging_mixin import LoggingMixin
from coresentiment.include.config.settings import config
from coresentiment.include.python_tasks.notification import deploy_failure



log = LoggingMixin().log


def download_file_task(**context):
    now = dt.now() - timedelta(hours=5)
    url = (
        f"https://dumps.wikimedia.org/other/pageviews/"
        f"{now:%Y}/{now:%Y-%m}/"
        f"pageviews-{now:%Y%m%d-%H}0000.gz"
    )

    log.info(f"Attempting download at {url}")
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raw.decode_content = False
        response.raise_for_status()


        first_chunk = next(response.iter_content(chunk_size=8192))
        if not is_valid_gzip(first_chunk):
            log.warning(f"Invalid content received (likely HTML 404). File not ready yet.")
            #deploywarning

        dest_folder = f"{config.RAW_PAGE_VIEWS_DIR}/{now.date()}"
        os.makedirs(dest_folder, exist_ok=True)

        file_name = f"{now.strftime('%d %B %Y - %H')}00"
        file_location = f"{dest_folder}/{file_name}.gz"

        with open(file_location, "wb") as f:
            f.write(first_chunk)
            shutil.copyfileobj(response.raw, f)

        log.info(f"Dumped page views successfully at {file_location}")

        return {
            'file_location': file_location,
            'dump_date': str(now.date()),
            'dump_hour': now.hour,
            'task_display': 'file download',
        }

    except requests.exceptions.RequestException as e:
        log.error(f"Network error: {str(e)}")
        deploy_failure(
            error=e,
            context=context,
            metadata={
                'task_display': 'File download',
                'dump_date': str(now.date()),
                'dump_hour': now.hour
            }
        )

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

def is_valid_gzip(content: bytes) -> bool:
    try:
        if len(content) < 2 or content[:2] != b'\x1f\x8b':
            log.debug(f"Failed magic byte check. First bytes: {content[:20]}")
            return False

        return True

    except Exception as e:
        log.debug(f"Failed content structure check: {str(e)}")
        return False
