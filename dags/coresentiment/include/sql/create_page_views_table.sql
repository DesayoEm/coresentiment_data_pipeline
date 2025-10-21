----- create page views table
CREATE TABLE IF NOT EXISTS page_views (
    page_view_id VARCHAR PRIMARY KEY,
    company_id VARCHAR NOT NULL,
    company_name VARCHAR NOT NULL,
    page_id VARCHAR,
    page_title VARCHAR,
    view_domain VARCHAR,
    view_count INTEGER NOT NULL,
    source_date DATE NOT NULL,
    source_hour INTEGER NOT NULL CHECK (source_hour >= 0 AND source_hour <= 23),
    etl_extracted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);