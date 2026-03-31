"""
Finance Monthly Reconciliation Pipeline.

Chay vao ngay 5 hang thang, so sanh du lieu thang truoc.
Spark job (monthly_reconcile.py) thuc hien toan bo logic reconcile.
DAG chi can: submit → watch → verify → notify.

Owner: team-finance
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
import requests
import logging

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "team-finance",
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=5),
}

ICEBERG_REST = "http://iceberg-rest.platform-storage:8181"

SPARK_APP_YAML = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "spark-apps",
    "monthly-reconcile.yaml",
)


def post_reconcile_check(**context):
    """Verify ket qua reconcile qua Iceberg REST API."""
    try:
        resp = requests.get(
            f"{ICEBERG_REST}/v1/namespaces/finance/tables/monthly_reconcile",
            timeout=30,
        )
        if resp.status_code == 404:
            logger.warning("Table finance.monthly_reconcile not found — "
                           "Spark job may not have created it yet")
            return

        resp.raise_for_status()
        metadata = resp.json()
        snapshots = metadata.get("metadata", {}).get("snapshots", [])

        if not snapshots:
            logger.warning("No snapshots in monthly_reconcile table")
            return

        latest = max(snapshots, key=lambda s: s.get("timestamp-ms", 0))
        summary = latest.get("summary", {})
        total_records = int(summary.get("total-records", "0"))
        added_records = int(summary.get("added-records", "0"))

        logger.info(
            f"Reconcile result: total_records={total_records}, "
            f"added_records={added_records}"
        )
        context["ti"].xcom_push(key="total_records", value=total_records)
        context["ti"].xcom_push(key="added_records", value=added_records)

    except Exception as e:
        logger.error(f"Post-reconcile check failed: {e}")
        raise


def notify_result(**context):
    """Gui Slack notification voi ket qua reconcile."""
    ds = context["ds"]
    total = context["ti"].xcom_pull(
        task_ids="post_reconcile_check", key="total_records"
    )

    webhook = Variable.get("slack_webhook_finance", default_var="")
    if webhook:
        try:
            requests.post(webhook, json={
                "text": (
                    f":ledger: *Finance Monthly Reconcile* completed\n"
                    f"Date: `{ds}` | Total records: `{total}`"
                ),
            }, timeout=10)
        except Exception as e:
            logger.warning(f"Slack notification failed: {e}")


with DAG(
    dag_id="finance_monthly_reconcile",
    default_args=DEFAULT_ARGS,
    description="Monthly reconciliation of transaction data",
    schedule_interval="0 6 5 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["finance", "monthly", "reconcile"],
) as dag:

    run_reconcile = SparkKubernetesOperator(
        task_id="run_reconcile",
        namespace="team-finance",
        application_file=SPARK_APP_YAML,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    watch_reconcile = SparkKubernetesSensor(
        task_id="watch_reconcile",
        namespace="team-finance",
        application_name="{{ task_instance.xcom_pull(task_ids='run_reconcile') }}",
        kubernetes_conn_id="kubernetes_default",
        timeout=14400,
        poke_interval=60,
    )

    check = PythonOperator(
        task_id="post_reconcile_check",
        python_callable=post_reconcile_check,
    )

    notify = PythonOperator(
        task_id="notify_result",
        python_callable=notify_result,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    run_reconcile >> watch_reconcile >> check >> notify
