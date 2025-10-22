COPY page_views (
    view_domain,
    view_count,
    page_view_id,
    page_id,
    company_id,
    company_name,
    page_title,
    source_date,
    source_hour
)
FROM STDIN
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '"');