import os
import json
import requests
from datetime import datetime

def send_slack_alert(context, status='failed'):
    """
    Envía una alerta a Slack usando un Incoming Webhook.
    Diseñado para usarse como callback en Airflow.
    """
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    
    if not webhook_url:
        print("⚠️ SLACK_WEBHOOK_URL no configurada. Saltando alerta.")
        return

    # Extraer info del contexto de Airflow
    task_instance = context.get('task_instance')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date').isoformat()
    log_url = task_instance.log_url
    
    # Definir colores y emojis según el estado
    if status == 'failed':
        color = "#FF0000"  # Rojo
        emoji = "🚨"
        title = "Task Failed"
    else:
        color = "#36a64f"  # Verde
        emoji = "✅"
        title = "Pipeline Success"

    exception = context.get('exception')
    error_msg = str(exception)
    if len(error_msg) > 1000:
        error_msg = error_msg[:1000] + "... (truncated)"

    # Construir el mensaje (Block Kit simplificado para Webhooks)
    payload = {
        "text": f"{emoji} {title}: {dag_id}.{task_id}",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} {title}: {task_id}"
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*DAG:*\n{dag_id}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Task:*\n{task_id}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Time:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    }
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error Log:* \n```{error_msg}```"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"<{log_url}|Ver Logs en Airflow>"
                }
            }
        ]
    }

    try:
        response = requests.post(
            webhook_url, 
            data=json.dumps(payload),
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code != 200:
            print(f"⚠️ Error enviando a Slack: {response.status_code} - {response.text}")
        else:
            print("🔔 Alerta enviada a Slack correctamente.")
    except Exception as e:
        print(f"⚠️ Excepción enviando a Slack: {e}")

def slack_failure_callback(context):
    send_slack_alert(context, status='failed')

def slack_success_callback(context):
    send_slack_alert(context, status='success')
