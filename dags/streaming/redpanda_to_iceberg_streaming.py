"""Airflow DAG for the Redpanda -> Iceberg streaming job.

This DAG is intentionally operationally friendly:
- easy manual trigger for testing
- pause/resume safe
- validates runtime config before submitting SparkApplication
- submits in the isolated streaming namespace
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

DAG_DIR = os.path.dirname(__file__)
SPARK_APP_YAML = os.path.join(DAG_DIR, "spark-apps", "redpanda-to-iceberg.yaml")


default_args = {
    "owner": "platform-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(hours=2),
}


with DAG(
    dag_id="streaming_redpanda_to_iceberg",
    default_args=default_args,
    description="Consume Redpanda events and load them into Iceberg",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "redpanda", "spark", "iceberg"],
) as dag:
    start = EmptyOperator(task_id="start")

    @task(task_id="prepare_streaming_environment")
    def prepare_streaming_environment() -> dict:
        return {
            "topic": os.environ.get("REDPANDA_TOPIC", "finance.transactions.v1"),
            "bootstrap": os.environ.get(
                "REDPANDA_BOOTSTRAP_SERVERS",
                "redpanda.platform-streaming.svc.cluster.local:9092",
            ),
            "checkpoint": os.environ.get(
                "CHECKPOINT_LOCATION",
                "s3a://team-finance/checkpoints/redpanda-to-iceberg/",
            ),
        }

    @task(task_id="validate_streaming_config")
    def validate_streaming_config(config: dict) -> dict:
        required_keys = ["topic", "bootstrap", "checkpoint"]
        missing = [key for key in required_keys if not config.get(key)]
        if missing:
            raise ValueError(f"Missing streaming config keys: {missing}")
        return config

    config = validate_streaming_config(prepare_streaming_environment())

    stream_to_iceberg = SparkKubernetesOperator(
        task_id="stream_to_iceberg",
        namespace="platform-streaming",
        application_file=SPARK_APP_YAML,
        do_xcom_push=False,
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,
        reattach_on_restart=True,
    )

    end = EmptyOperator(task_id="end")

    start >> config >> stream_to_iceberg >> end
