"""
Reusable wrapper cho SparkKubernetesOperator.
Giảm boilerplate trong mọi team DAG.
"""
from __future__ import annotations
from typing import Optional
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)

DEFAULT_K8S_CONN = "kubernetes_default"


def make_spark_task(
    task_id: str,
    application_file: str,
    namespace: str,
    dag,
    params: Optional[dict] = None,
    timeout_seconds: int = 7200,
    poke_interval: int = 30,
):
    """
    Tạo cặp (submit_task, watch_task) cho 1 Spark job.

    Args:
        task_id:          prefix cho task IDs, e.g. 'extract'
        application_file: đường dẫn đến SparkApplication yaml
        namespace:        K8s namespace của team
        dag:              Airflow DAG object
        params:           dict các params override cho yaml
        timeout_seconds:  timeout đợi Spark job xong
        poke_interval:    số giây giữa các lần check

    Returns:
        tuple(submit_task, watch_task)
    """
    submit = SparkKubernetesOperator(
        task_id=f"{task_id}_submit",
        namespace=namespace,
        application_file=application_file,
        kubernetes_conn_id=DEFAULT_K8S_CONN,
        do_xcom_push=True,
        params=params or {},
        dag=dag,
    )
    watch = SparkKubernetesSensor(
        task_id=f"{task_id}_watch",
        namespace=namespace,
        application_name=(
            "{{ task_instance.xcom_pull(task_ids='" + f"{task_id}_submit" + "') }}"
        ),
        kubernetes_conn_id=DEFAULT_K8S_CONN,
        timeout=timeout_seconds,
        poke_interval=poke_interval,
        dag=dag,
    )
    return submit, watch