"""
Reusable Slack và email notification helpers cho Airflow DAGs.
"""
from __future__ import annotations
import logging
import requests
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.models import Variable

logger = logging.getLogger(__name__)


def _post_slack(webhook_url: str, message: str) -> None:
    """Gửi Slack message qua Incoming Webhook."""
    try:
        resp = requests.post(
            webhook_url,
            json={"text": message},
            timeout=10,
        )
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Slack notification failed (non-critical): {e}")


def make_success_notifier(
    task_id: str,
    dag_display_name: str,
    slack_webhook_var: str,
    dag,
    extra_info_fn=None,
) -> PythonOperator:
    """
    Tạo task gửi Slack khi DAG thành công.

    Args:
        slack_webhook_var: tên Airflow Variable chứa Slack webhook URL
        extra_info_fn:    optional callable(**context) -> str để thêm thông tin
    """
    def _notify(**context):
        ds = context["ds"]
        dag_run = context["dag_run"]
        duration = (
            datetime.utcnow() - dag_run.start_date.replace(tzinfo=None)
        ).total_seconds() / 60

        extra = ""
        if extra_info_fn:
            try:
                extra = "\n" + extra_info_fn(**context)
            except Exception:
                pass

        webhook = Variable.get(slack_webhook_var, default_var="")
        if webhook:
            _post_slack(
                webhook,
                f":white_check_mark: *{dag_display_name}* SUCCESS\n"
                f"Date: `{ds}` | Duration: `{duration:.1f} min`{extra}"
            )

    return PythonOperator(
        task_id=task_id,
        python_callable=_notify,
        dag=dag,
    )


def make_failure_notifier(
    task_id: str,
    dag_display_name: str,
    slack_webhook_var: str,
    dag,
) -> PythonOperator:
    """Tạo task gửi Slack khi pipeline fail."""
    def _notify(**context):
        ds = context["ds"]
        errors = context["ti"].xcom_pull(key="quality_errors") or ["Unknown error"]
        webhook = Variable.get(slack_webhook_var, default_var="")
        if webhook:
            _post_slack(
                webhook,
                f":x: *{dag_display_name}* FAILED\n"
                f"Date: `{ds}`\nErrors:\n" +
                "\n".join(f"  - {e}" for e in errors)
            )

    return PythonOperator(
        task_id=task_id,
        python_callable=_notify,
        dag=dag,
    )