from datetime import date
import pandas as pd
import os
import hashlib
from airflow.utils.log.logging_mixin import LoggingMixin
from coresentiment.include.config.company_pages_config import company_pages
from coresentiment.include.config.settings import config


log = LoggingMixin().log


def generate_key(*args) -> str:
    combined = '|'.join(str(arg) for arg in args if arg is not None)
    return hashlib.md5(combined.encode()).hexdigest()[:16]


def process_page_views_count(file_location, dump_date: date, dump_hour: int):
    log.info(f"Processing page views count for {file_location}")

    df = pd.read_csv(
        file_location,
        compression="gzip",
        sep=" ",
        names=["domain", "title", "view_count", "response_size"]
    )
    df = df.drop(columns=["response_size"])
    processed_rows = []

    for company, company_data in company_pages.items():
        pages = company_data["pages"]
        company_df = df[df["title"].isin(pages)].copy()

        if not company_df.empty:
            company_df["page_view_id"] = company_df.apply(
                lambda row: generate_key(dump_date, dump_hour, row["title"]),
                axis=1
            )
            company_df["page_id"] = company_df.apply(
                lambda row: generate_key(company, row["domain"], row["title"]),
                axis=1
            )
            company_df["company_id"] = generate_key(company)
            company_df["company_name"] = company
            company_df["page_title"] = company_df["title"]
            company_df["view_count"] = company_df["view_count"].astype(int)
            company_df["day"] = dump_date
            company_df["hour"] = dump_hour

            processed_rows.append(company_df)
            log.info(f"Successfully counted {len(company_df)} pages for {company}")

    if processed_rows:
        result_df = pd.concat(processed_rows, ignore_index=True)

        log.info(f"Processed {len(company_pages)} companies, found {len(result_df)} matching pages")
    else:
        result_df = pd.DataFrame()
        log.info("No matching pages found for any company")

    return store_page_views_count(result_df)


def store_page_views_count(df: pd.DataFrame):
    output_dir = config.PAGE_VIEWS_DIR
    output_file = f"{output_dir}/processed_page_views.csv"

    log.info(f"Data for {''} saved at {output_file}")

    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(output_file, sep=",", index=False, header= True,  encoding="utf-8")

    return output_file

