"""
MinIO Health Check DAG.

Chay daily: kiem tra MinIO S3 API co hoat dong binh thuong khong.
- Check health endpoint
- Check buckets co accessible khong
- Check Iceberg REST Catalog

Cluster hien tai dung MinIO (khong phai Ceph) lam S3-compatible storage.
MinIO endpoint: minio.platform-storage:9000
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import logging

logger = logging.getLogger(__name__)

MINIO_ENDPOINT = "http://minio.platform-storage:9000"


def check_minio_health(**context):
    """Kiem tra MinIO co respond binh thuong khong."""
    try:
        resp = requests.get(f"{MINIO_ENDPOINT}/minio/health/live", timeout=10)
        if resp.status_code == 200:
            logger.info("MinIO health check: OK")
            return True
        else:
            logger.error(f"MinIO health check FAILED: status {resp.status_code}")
            raise Exception(f"MinIO unhealthy: {resp.status_code}")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"MinIO unreachable: {e}")
        raise


def check_minio_buckets(**context):
    """
    Kiem tra cac bucket quan trong co ton tai khong.
    HEAD bucket: 200/403 = exists, 404 = not found.
    """
    important_buckets = ["team-finance"]
    results = {}

    for bucket in important_buckets:
        try:
            resp = requests.head(
                f"{MINIO_ENDPOINT}/{bucket}",
                timeout=10,
            )
            if resp.status_code in (200, 403):
                results[bucket] = "exists"
                logger.info(f"Bucket '{bucket}': exists")
            elif resp.status_code == 404:
                results[bucket] = "NOT_FOUND"
                logger.warning(f"Bucket '{bucket}': NOT FOUND")
            else:
                results[bucket] = f"status_{resp.status_code}"
        except Exception as e:
            results[bucket] = f"error: {e}"
            logger.error(f"Bucket check failed for '{bucket}': {e}")

    context["ti"].xcom_push(key="bucket_check_results", value=results)

    missing = [b for b, s in results.items() if s == "NOT_FOUND"]
    if missing:
        logger.warning(f"Missing buckets: {missing}")


def check_iceberg_rest(**context):
    """Kiem tra Iceberg REST Catalog co hoat dong khong."""
    iceberg_rest = "http://iceberg-rest.platform-storage:8181"
    try:
        resp = requests.get(f"{iceberg_rest}/v1/config", timeout=10)
        if resp.status_code == 200:
            logger.info("Iceberg REST Catalog: OK")
        else:
            logger.warning(f"Iceberg REST Catalog: status {resp.status_code}")
    except Exception as e:
        logger.error(f"Iceberg REST Catalog unreachable: {e}")
        raise


with DAG(
    dag_id="platform_minio_health",
    default_args={
        "owner": "platform-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=10),
    },
    description="Daily MinIO + Iceberg REST health check",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["platform", "minio", "health", "storage"],
) as dag:

    minio_health = PythonOperator(
        task_id="check_minio_health",
        python_callable=check_minio_health,
    )

    bucket_check = PythonOperator(
        task_id="check_minio_buckets",
        python_callable=check_minio_buckets,
    )

    iceberg_check = PythonOperator(
        task_id="check_iceberg_rest",
        python_callable=check_iceberg_rest,
    )

    minio_health >> [bucket_check, iceberg_check]
