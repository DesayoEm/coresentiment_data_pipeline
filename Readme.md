# CoreSentiment: Wikipedia Pageview Analysis Pipeline

An Apache Airflow data pipeline that analyzes Wikipedia pageview data to predict stock market sentiment for major tech companies.

## Overview

This pipeline processes Wikipedia pageview data to generate stock sentiment signals for five major tech companies: Amazon, Apple, Facebook (Meta), Google, and Microsoft. The hypothesis is that increased interest in a company's Wikipedia pages (including products and services) indicates positive sentiment and potential stock price movement.

**Implementation Scope:** An idempotent pipeline with staging tables, deterministic keys, parallel execution, and observability. 
This pipeline processes one hour of Wikipedia pageview data for these companies and identify which has the highest aggregate pageviews.

---

## Architecture & Design Decisions

### 1. DAG Structure: Shortest Path Orchestration

**Decision:** Tasks are only chained when there's a true dependency. Independent tasks run in parallel.

**Rationale:** In a directed acyclic graph, unnecessary dependencies increase total execution time. By allowing independent tasks (like table creation and file download) to run concurrently, I took the shortest path possible through the pipeline.

**Example:**
```
download_file (21s)     create_schemas (1s)
      ↓                        ↓
process (8s)            truncate_staging (1s)
      ↓                        ↓
      └────────-> stage_data ←─┘
```

![Alt text](docs/graph1.png)

Total time: ~32 seconds instead of ~40 seconds if run serially.

---

### 2. Idempotency: Deterministic Surrogate Keys

**Decision:** All primary keys are generated using deterministic hash functions, not auto-increment IDs.

**Rationale:** Reprocessing the same source data must produce identical results. Hash-based keys ensure that the same page, viewed at the same hour, for the same company, always generates the same `page_view_id`.

**Implementation:**
```python
page_view_id = hash(company, date, hour, title, domain)
page_id = hash(company, domain, title)
company_id = hash(company)
```

**Impact:** The pipeline can be rerun without creating duplicates. Same input -> same keys -> `ON CONFLICT` skips duplicates.

---

### 3. Staging -> Analytics Pattern

**Decision:** Data loads in two stages: fast bulk load to staging, then conflict-managed transfer to analytics.

**Problem:** PostgreSQL's `COPY` command (fastest bulk load method) doesn't support `ON CONFLICT` for deduplication.

**Solution:**
1. **Staging table:** Truncated before each run, loaded via `COPY` from  a cleaned CSV file with deterministic keys(fast, no conflicts possible)
2. **Analytics table:** Persistent storage, loaded via `INSERT...ON CONFLICT DO NOTHING` (idempotent)

**Trade-offs:**
- Combines speed of `COPY` with correctness of conflict resolution
- Separates transient data (staging) from persistent data (analytics)
- Requires two tables and an extra transfer step

**Why this matters:** In production, speed matters, second to accuracy. Processing 24 hours of data means 24 files. COPY loads 50MB files in seconds; individual INSERTs take minutes.

---

### 4. Multi-Page Tracking Per Company

**Decision:** Track Wikipedia pages related to each company, not just the main company page.

**Rationale:** Stock sentiment isn't just about the company itself—it's about products, services, and ecosystem.

- Interest in "iPhone", "AirPods", "Apple Vision Pro" signals Apple sentiment
- Interest in "AWS Lambda", "Amazon Prime" signals Amazon sentiment
- Interest in "Instagram", "WhatsApp" signals Meta sentiment

**Methodology:** Used Wikipedia's Category API to discover all pages in company categories, then manually curated to remove noise (defunct products, legal cases, individual executives) and retain core products and active services.

**Impact:** Provides a more complete sentiment signal than single-page tracking.

---

### 5. Data Granularity Preservation

**Decision:** Preserve domain-level detail (en, en.m, es, ja, etc.) instead of aggregating to total pageviews.

**Rationale:** Downstream ML models may need this context for weighting or segmentation:
- Mobile (en.m) vs desktop (en) traffic might have different signals
- Global sentiment (es, ja, zh) matters for companies with international sales
- Different pages carry different sentiment weight (new product launch ≠ legal controversy)

**Philosophy:** Don't aggregate prematurely. Let the consumer (ML models) decide how to interpret granular data.

---

### 6. Database Choice: PostgreSQL

**Decision:** Use PostgreSQL as the target database.

**Rationale:**
- Native Airflow integration via built-in hooks and operators
- Transactional support (commits, rollbacks) for data integrity
- UPSERT capability via `ON CONFLICT` enables idempotent design

---

### 7. Centralized Failure Handling

**Decision:** Build a `deploy_failure()` function that sends Slack notifications with full context before re-raising exceptions.

**Problem:** Airflow only persists return values (XComs) when tasks succeed. If a task fails mid-execution, downstream callbacks can't access failure context.

**Solution:** Catch exceptions, send detailed Slack notification (task name, date, hour, error message), then re-raise the original exception.

**Implementation:**
```python
def deploy_failure(error: Exception,
                   context:dict,
                   metadata: dict
            )-> None:

    details = '\n'.join([f'{k}: {v}' for k, v in (metadata or {}).items() if k != 'task_display'])
    notifier = SlackNotifier(
        slack_conn_id='slack',
        text=f'{metadata.get('task_display', "Unknown task")} FAILED\n{details}\nError: {str(error)}',
        channel='page-views'
    )
    notifier.notify(context)
    raise error
```

**usage:**
```python
    except Exception as e:
        deploy_failure(
            error=e,
            context=context,
            metadata={
                'task_display': 'File processing',
                'dump_date': dump_date,
                'dump_hour': dump_hour
            }
        )
```


**Impact:**
- No silent failures
- Team notified immediately with actionable context
- Error still propagates through Airflow's retry logic

---

## Pipeline Flow

### Task Breakdown

```
START
  ↓
  ├─-> download_file                    (extracts Wikipedia pageview data)
  │     ↓
  │   process_page_views_count         (filters to company pages, enriches data)
  │     ↓
  └─-> create_schemas_and_tables        (creates staging + analytics tables)
        ↓
      truncate_staging                  (clears staging for fresh load)
        ↓
      stage_data_in_warehouse           (COPY bulk load to staging)
        ↓
      load_analytics_table              (INSERT with conflict resolution)
        ↓
      run_analysis                      (SQL: find company with most views)
        ↓
      log_analysis_result               (print result to logs)
```

### Task Details

#### 1. `download_file`
- Constructs Wikimedia URL dynamically based on the target date or hour
```python
now = dt.now() - timedelta(hours=5)
    url = (
        f"https://dumps.wikimedia.org/other/pageviews/"
        f"{now:%Y}/{now:%Y-%m}/"
        f"pageviews-{now:%Y%m%d-%H}0000.gz"
    )
```
- Downloads compressed pageview file (~50MB gzipped)
- Validates gzip format by checking magic bytes
- Saves to date-based folders with hour-based naming
- Returns: file location, dump date, dump hour

#### 2. `process_page_views_count`
- Reads compressed file using Pandas
- Filters to pages matching company config (100+ pages per company)
- Generates deterministic keys for page_view_id, page_id, company_id
- Enriches with company metadata and timestamp
- Outputs: processed CSV for database load

#### 3. `create_schemas_and_tables`
- Creates `staging` and `analytics` schemas if not exist
- Creates identical table structures in both schemas
- Idempotent (IF NOT EXISTS)

#### 4. `truncate_staging`
- Wipes staging table clean before each load
- Ensures no conflicts during COPY operation

#### 5. `stage_data_in_warehouse`
- Bulk loads processed CSV to staging table via COPY

#### 6. `load_analytics_table`
- Transfers data from staging to analytics
- Uses `INSERT...ON CONFLICT (page_view_id) DO NOTHING`
- Deduplicates based on deterministic keys

#### 7. `run_analysis`
- Returns top company by aggregate pageviews

#### 8. `log_analysis_result`
- Prints analysis result to Airflow logs
- Format: `Top performing company: [('Facebook', 13150)]`

---

## Key Features

### Idempotent
Rerunning the DAG for the same hour produces identical results. No duplicate data.

### Fast
- Parallel execution where possible
- COPY for bulk loading (10-100x faster than INSERT)
- Total pipeline time: ~40 seconds for one hour of data

### Observable
- Slack notifications on success
- Slack notifications include a summarised error message on failure
- Metadata tracked throughout pipeline (date, hour, task name)
- Analysis results logged and accessible

### Recoverable
- Staging truncate-and-reload handles partial failures
- Deterministic keys ensure consistency across reruns
- Transactional database operations

### Extensible
- Config-driven company/page mappings
- Schema separation (staging vs analytics)
- Multi-level keys support flexible querying


### 1. Clone the Repository
```
git clone https://github.com/DesayoEm/coresentiment_data_pipeline
cd coresentiment
```


Create a `.env` file in the root directory with the following variables:

```env
AIRFLOW_UID=50000
AIRFLOW_CONN_CORESENTIMENT_DB=postgresql://postgres:your_password@coresentiment_db:5432/coresentiment
RAW_PAGE_VIEWS_DIR=/opt/airflow/data/raw
PROCESSED_PAGE_VIEWS_DIR=/opt/airflow/data/processed
DB_NAME=coresentiment
DB_USER=postgres
DB_PASSWORD=your_password
```
##  Running the Pipeline

###  Build and run all services once
```
docker-compose up --build
```

### Cleaning Up
```
docker-compose down -v
```


