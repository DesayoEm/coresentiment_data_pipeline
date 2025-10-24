INSERT INTO analytics.page_views AS pv (
    page_view_id,
    company_id,
    company_name,
    page_id,
    page_title,
    view_domain,
    view_count,
    source_date,
    source_hour,
    etl_extracted_at
)
SELECT *
FROM staging.page_views
ON CONFLICT (page_view_id) DO NOTHING;
