"""
Finance Daily Revenue Pipeline DAG.

Pipeline:
  run_etl (Spark) → watch_etl → post_etl_check (Iceberg REST)
  → notify_success | notify_failure

Spark job (daily_revenue_etl.py) thuc hien toan bo:
  extract → transform → quality check → load → compact

DAG chi can: submit 1 Spark job, doi xong, verify, notify.

SLA: Complete by 08:00 every day.
Owner: team-finance
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
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
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "execution_timeout": timedelta(hours=4),
}

ICEBERG_REST = "http://iceberg-rest.platform-storage:8181"
K8S_NAMESPACE = "team-finance"
K8S_CONN_ID = "kubernetes_default"

# SparkApp YAML nam cung folder voi DAG, duoc git-sync vao Airflow pod.
# Jinja2 FileSystemLoader dung DAG's own directory lam searchpath (dag.folder),
# nen path phai la relative so voi thu muc chua DAG file nay.
SPARK_APP_YAML = "spark-apps/daily-revenue.yaml"


def _get_slack_webhook():
    return Variable.get("slack_webhook_finance", default_var="")


def post_etl_check(**context):
    """
    Verify sau khi Spark job hoan thanh.
    Check: table ton tai, snapshot moi co data.
    """
    ds = context["ds"]
    logger.info(f"Post-ETL check for {ds}")

    errors = []

    # Check 1: Table ton tai
    try:
        resp = requests.get(
            f"{ICEBERG_REST}/v1/namespaces/finance/tables/transactions_silver",
            timeout=30,
        )
        if resp.status_code == 404:
            errors.append("Table finance.transactions_silver does not exist")
        else:
            resp.raise_for_status()
            metadata = resp.json()
            snapshots = metadata.get("metadata", {}).get("snapshots", [])

            # Check 2: Co snapshots
            if not snapshots:
                errors.append("No snapshots found — table has no data")
            else:
                # Check 3: Snapshot moi nhat co records
                latest = max(snapshots, key=lambda s: s.get("timestamp-ms", 0))
                summary = latest.get("summary", {})
                total_records = int(summary.get("total-records", "0"))

                if total_records == 0:
                    errors.append("Latest snapshot has 0 records")
                else:
                    logger.info(
                        f"Verified: {total_records} total records, "
                        f"{len(snapshots)} snapshots"
                    )
    except requests.exceptions.ConnectionError as e:
        errors.append(f"Iceberg REST unreachable: {e}")
    except Exception as e:
        errors.append(f"Check failed: {e}")

    if errors:
        context["ti"].xcom_push(key="check_errors", value=errors)
        raise Exception(f"Post-ETL check failed: {errors}")

    context["ti"].xcom_push(key="check_errors", value=[])


def notify_success(**context):
    """Gui Slack notification khi pipeline thanh cong."""
    ds = context["ds"]
    dag_run = context["dag_run"]
    duration = (
        datetime.now() - dag_run.start_date.replace(tzinfo=None)
    ).total_seconds() / 60

    webhook = _get_slack_webhook()
    if webhook:
        try:
            requests.post(webhook, json={
                "text": (
                    f":white_check_mark: *Finance Daily Revenue* SUCCESS\n"
                    f"Date: `{ds}` | Duration: `{duration:.1f} min`"
                ),
            }, timeout=10)
        except Exception as e:
            logger.warning(f"Slack notification failed: {e}")


def notify_failure(**context):
    """Gui Slack alert khi pipeline fail."""
    ds = context["ds"]
    errors = context["ti"].xcom_pull(
        task_ids="post_etl_check", key="check_errors"
    ) or ["Spark job failed — check Spark UI / driver logs"]

    webhook = _get_slack_webhook()
    if webhook:
        try:
            requests.post(webhook, json={
                "text": (
                    f":x: *Finance Daily Revenue* FAILED\n"
                    f"Date: `{ds}`\n"
                    f"Errors:\n" + "\n".join(f"  - {e}" for e in errors)
                ),
            }, timeout=10)
        except Exception as e:
            logger.warning(f"Slack notification failed: {e}")


with DAG(
    dag_id="finance_daily_revenue",
    default_args=DEFAULT_ARGS,
    description="Daily revenue ETL: S3 bronze → Iceberg silver",
    schedule_interval="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["finance", "daily", "revenue", "silver"],
    doc_md="""
    # Finance Daily Revenue Pipeline

    ## Flow
    1. Spark job: extract → transform → quality check → load → compact
    2. Post-ETL: verify Iceberg table via REST API
    3. Notify: Slack success/failure

    ## Owner
    team-finance | #finance-data-alerts
    """,
) as dag:

    # 1. Submit Spark ETL job (does extract + transform + quality + load + compact)
    run_etl = SparkKubernetesOperator(
        task_id="run_etl",
        namespace=K8S_NAMESPACE,
        application_file=SPARK_APP_YAML,
        kubernetes_conn_id=K8S_CONN_ID,
        do_xcom_push=True,
    )

    # 2. Wait for Spark job to complete
    watch_etl = SparkKubernetesSensor(
        task_id="watch_etl",
        namespace=K8S_NAMESPACE,
        application_name="{{ task_instance.xcom_pull(task_ids='run_etl') }}",
        kubernetes_conn_id=K8S_CONN_ID,
        timeout=7200,
        poke_interval=30,
    )

    # 3. Verify result via Iceberg REST API
    check = PythonOperator(
        task_id="post_etl_check",
        python_callable=post_etl_check,
    )

    # 4a. Success notification
    success = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    # 4b. Failure notification (runs if any upstream failed)
    failure = PythonOperator(
        task_id="notify_failure",
        python_callable=notify_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Flow: run → watch → check → success → end
    #                                   ↘ failure → end
    run_etl >> watch_etl >> check >> success >> end
    [run_etl, watch_etl, check] >> failure >> end
