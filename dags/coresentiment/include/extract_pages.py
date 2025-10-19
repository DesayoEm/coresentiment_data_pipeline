import pandas as pd
import hashlib
from zipfile import ZipFile
from airflow.utils.log.logging_mixin import LoggingMixin
from dags.coresentiment.include.related_pages.compile_related_pages import company_pages


log = LoggingMixin().log


def generate_key(*args) -> str:
    """Generate a deterministic surrogate key from input values."""
    combined = '|'.join(str(arg) for arg in args if arg is not None)
    return hashlib.md5(combined.encode()).hexdigest()[:16]


def read_zipped_file(file_location):
    log.info(f"Processing")

    df = pd.read_csv(
        file_location,
        compression="zip",
        sep=" ",
        names=["domain", "title", "view_count", "response_size"]
    )

    df["view_count"] = pd.to_numeric(df["view_count"], errors="coerce").fillna(0)

    counts = {"Amazon": 0, "Apple": 0, "Facebook": 0, "Google": 0, "Microsoft": 0}

    for company in counts.keys():
        pages = company_pages.get(company, [])

    mask = df["title"].isin(pages)
    counts[company] = df.loc[mask, "view_count"].sum()

    return counts