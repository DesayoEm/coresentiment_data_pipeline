from airflow.providers.postgres.hooks.postgres import PostgresHook
from coresentiment.include.python_tasks.notification import deploy_failure

def load_data_from_file(file_location, dump_date, dump_hour,**context):
    try:
        hook = PostgresHook(postgres_conn_id='coresentiment_db')
        sql_file = "/opt/airflow/dags/coresentiment/include/sql/stage_data.sql"

        with open(sql_file) as f:
            sql = f.read()
        hook.copy_expert(sql, file_location)

        return {
            'dump_date': dump_date,
            'dump_hour': dump_hour,
            'task_display': 'warehouse load',

        }

    except Exception as e:
        deploy_failure(
            error=e,
            context=context,
            metadata={
                'task_display': 'staging data load',
                'dump_date': dump_date,
                'dump_hour': dump_hour
            }
        )
