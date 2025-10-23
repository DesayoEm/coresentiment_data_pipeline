from airflow.providers.slack.notifications.slack import SlackNotifier
from datetime import date

def success_callback(context):
    try:
        ti = context['ti']
        file_info = ti.xcom_pull(task_ids="download_file", key="return_value")

        dump_hour = file_info.get('dump_hour')
        dump_date = file_info.get('dump_date')
        task_display = file_info.get('task_display')


        notifier = SlackNotifier(
            slack_conn_id='slack',
            text=f'Page views {task_display} for hour {dump_hour} on {dump_date} DAG has SUCCEEDED',
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
