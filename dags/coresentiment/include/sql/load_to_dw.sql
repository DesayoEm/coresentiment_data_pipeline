COPY page_views
FROM '{{ ti.xcom_pull(task_ids="export_page_views", key="abs_csv_path") }}'
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '"');
