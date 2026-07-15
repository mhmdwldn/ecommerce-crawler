"""Alerting callbacks for Airflow DAGs.

ponytail: POST to a configurable webhook URL on DAG failure.
Set ALERT_WEBHOOK_URL env var to enable (Telegram, Discord, Slack, etc.).
Set ALERT_WEBHOOK_HEADER for custom auth headers.
"""

import json
import os
import urllib.request
from datetime import datetime


def webhook_failure(context: dict) -> None:
    """Airflow on_failure_callback — POST failure info to webhook.

    Compatible with:
    - Telegram: https://api.telegram.org/bot<TOKEN>/sendMessage
    - Discord: https://discord.com/api/webhooks/<ID>/<TOKEN>
    - Slack: https://hooks.slack.com/services/<ID>
    - ntfy.sh: https://ntfy.sh/<topic>
    """
    url = os.getenv("ALERT_WEBHOOK_URL")
    if not url:
        return  # alerting not configured — silent no-op

    dag_id = context.get("dag", {}).dag_id if isinstance(context.get("dag"), object) else "unknown"
    task_id = context.get("task_instance", {}).task_id if context.get("task_instance") else "unknown"
    exec_date = str(context.get("logical_date", datetime.now()))
    error = str(context.get("exception", "no exception details"))
    run_id = context.get("run_id", "unknown")

    message = (
        f"❌ Pipeline GAGAL\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Run: {run_id}\n"
        f"Time: {exec_date[:19]}\n"
        f"Error: {error[:200]}"
    )

    payload: dict = _build_payload(url, message)
    if payload is None:
        return

    headers = {"Content-Type": "application/json"}
    if os.getenv("ALERT_WEBHOOK_HEADER"):
        headers["Authorization"] = os.getenv("ALERT_WEBHOOK_HEADER")

    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode(),
            headers=headers,
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
        print("Alert sent to webhook")
    except Exception as e:
        print(f"Alert webhook failed: {e}")


def _build_payload(url: str, message: str) -> dict | None:
    """Build payload for common webhook formats."""
    if "api.telegram.org" in url:
        return {"chat_id": os.getenv("TELEGRAM_CHAT_ID", ""), "text": message, "parse_mode": "HTML"}
    if "discord.com" in url or "hooks.slack.com" in url:
        return {"content": message}
    if "ntfy.sh" in url:
        return {"topic": url.split("/")[-1], "message": message, "title": "Pipeline GAGAL"}
    return {"text": message}  # generic fallback
