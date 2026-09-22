# колбэки для on_success_callback / on_failure_callback в DAG

import os

from airflow.providers.telegram.hooks.telegram import TelegramHook


def send_telegram_success_message(context):
    hook = TelegramHook(token=os.environ['TELEGRAM_TOKEN'], chat_id=os.environ['TELEGRAM_CHAT_ID'])
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    message = f'Исполнение DAG {dag_id} с id={run_id} прошло успешно!'
    hook.send_message({'chat_id': os.environ['TELEGRAM_CHAT_ID'], 'text': message})


def send_telegram_failure_message(context):
    hook = TelegramHook(token=os.environ['TELEGRAM_TOKEN'], chat_id=os.environ['TELEGRAM_CHAT_ID'])
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
    message = f'Исполнение DAG {dag_id} с id={run_id} прошло с ошибкой в задаче {task_instance_key_str}!'
    hook.send_message({'chat_id': os.environ['TELEGRAM_CHAT_ID'], 'text': message})
