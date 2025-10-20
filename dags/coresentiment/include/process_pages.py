import pandas as pd
import os
import hashlib
from airflow.utils.log.logging_mixin import LoggingMixin
from coresentiment.include.config.company_pages_config import company_pages
from .config.settings import config


log = LoggingMixin().log


def generate_key(*args) -> str:
    """Generate a deterministic surrogate key from input values."""
    combined = '|'.join(str(arg) for arg in args if arg is not None)
    return hashlib.md5(combined.encode()).hexdigest()[:16]


def process_page_views_count(file_location):
    log.info(f"Processing")

    df = pd.read_csv(
        file_location,
        compression="gzip",
        sep=" ",
        names=["domain", "title", "view_count", "response_size"]
    )

    df["view_count"] = pd.to_numeric(df["view_count"], errors="coerce").fillna(0)

    counts = {"Amazon": 0, "Apple": 0, "Facebook": 0, "Google": 0, "Microsoft": 0}


    for company in counts.keys():
        pages = company_pages.get(company, [])

        mask = df["title"].isin(pages)
        counts[company] = df.loc[mask, "view_count"].sum()

    log.info(f"Counted pages successfully {counts}")
    result_df = pd.DataFrame(list(counts.items()), columns=["company", "view_count"])
    return store_page_views_count(result_df)


def store_page_views_count(df: pd.DataFrame):
    output_dir = config.PAGE_VIEWS_DIR
    output_file = f"{output_dir}/processed_page_views.txt"

    log.info(f"Data for {''} saved at {output_file}")

    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(output_file, sep=" ", index=False, header= False,  encoding="utf-8")

    return output_file

