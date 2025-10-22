SELECT company_name, SUM(view_count) as total_views
FROM page_views
GROUP BY company_name
ORDER BY total_views DESC
LIMIT 1;