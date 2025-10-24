from airflow.providers.slack.notifications.slack import SlackNotifier
from datetime import date

def success_callback(context):
    try:
        ti = context['ti']
        task_info = ti.xcom_pull(task_ids=ti.task_id, key="return_value")

        if ti.task_id == "download_file":
            dump_hour = task_info.get('dump_hour')
            dump_date = task_info.get('dump_date')
            task_display = task_info.get('task_display')
            
        elif ti.task_id == "process_page_views_count":
            dump_hour = task_info.get('dump_hour')
            dump_date = task_info.get('dump_date')
            task_display = task_info.get('task_display')

        elif ti.task_id == "stage_data_in_warehouse":
            dump_hour = task_info.get('dump_hour')
            dump_date = task_info.get('dump_date')
            task_display = task_info.get('task_display')

        elif ti.task_id == "stage_data_in_warehouse":
            dump_hour = task_info.get('dump_hour')
            dump_date = task_info.get('dump_date')
            task_display = task_info.get('task_display')





        notifier = SlackNotifier(
            slack_conn_id='slack',
            text=f'CoreSentiment Page views DAG for {dump_date} hour {dump_hour}: {task_display} SUCCEEDED',
            channel='page-views'
        )
        notifier.notify(context)

    except Exception as e:
        print(f"Callback failed: {e}")


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
