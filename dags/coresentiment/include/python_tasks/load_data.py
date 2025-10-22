from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_data_from_file(**context):
    hook = PostgresHook(postgres_conn_id='coresentiment_db')
    file_path = context["ti"].xcom_pull(task_ids='process_page_views_count')

    sql_file = "/opt/airflow/dags/coresentiment/include/sql/copy_page_views.sql"

    with open(sql_file) as f:
        sql = f.read()
    hook.copy_expert(sql, file_path)
