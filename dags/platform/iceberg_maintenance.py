"""
Iceberg Maintenance DAG.

Chay daily: compact small files + expire old snapshots cho tat ca tables.
Giu Iceberg performance on dinh theo thoi gian.

Cluster hien tai KHONG co Trino, nen dung Iceberg REST API de list tables
va submit SparkApplication de chay maintenance operations.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import requests
import logging

logger = logging.getLogger(__name__)

ICEBERG_REST = "http://iceberg-rest.platform-storage:8181"
S3_ENDPOINT = "http://minio.platform-storage:9000"


def list_all_tables() -> list[str]:
    """Lay danh sach tat ca Iceberg tables qua REST Catalog API."""
    tables = []
    try:
        ns_resp = requests.get(f"{ICEBERG_REST}/v1/namespaces", timeout=30)
        ns_resp.raise_for_status()
        namespaces = ns_resp.json().get("namespaces", [])
    except Exception as e:
        logger.warning(f"Cannot list namespaces: {e}")
        return tables

    for ns_parts in namespaces:
        ns_name = ns_parts[0] if isinstance(ns_parts, list) else ns_parts
        try:
            tbl_resp = requests.get(
                f"{ICEBERG_REST}/v1/namespaces/{ns_name}/tables",
                timeout=30,
            )
            if tbl_resp.status_code == 200:
                for t in tbl_resp.json().get("identifiers", []):
                    ns = t["namespace"][0] if isinstance(t["namespace"], list) else t["namespace"]
                    tables.append(f"{ns}.{t['name']}")
        except Exception as e:
            logger.warning(f"Cannot list tables in {ns_name}: {e}")
    return tables


def check_tables_health(**context):
    """
    Kiem tra metadata cua tat ca Iceberg tables.
    Log so luong tables, snapshots, va kich thuoc metadata.
    """
    tables = list_all_tables()
    logger.info(f"Found {len(tables)} Iceberg tables")

    report = {}
    for table in tables:
        parts = table.split(".", 1)
        if len(parts) != 2:
            continue
        ns, tbl = parts
        try:
            resp = requests.get(
                f"{ICEBERG_REST}/v1/namespaces/{ns}/tables/{tbl}",
                timeout=30,
            )
            if resp.status_code == 200:
                metadata = resp.json()
                snapshots = metadata.get("metadata", {}).get("snapshots", [])
                report[table] = {
                    "snapshot_count": len(snapshots),
                    "status": "ok",
                }
                logger.info(
                    f"Table {table}: {len(snapshots)} snapshots"
                )
            else:
                report[table] = {"status": f"error_{resp.status_code}"}
        except Exception as e:
            report[table] = {"status": f"error: {e}"}
            logger.warning(f"Health check failed for {table}: {e}")

    context["ti"].xcom_push(key="table_health_report", value=report)
    return report


# Spark maintenance script chay trong SparkApplication pod
# Compact files va expire snapshots cho tung table
SPARK_MAINTENANCE_SCRIPT = """
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("iceberg-maintenance") \\
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \\
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \\
    .config("spark.sql.catalog.iceberg.type", "rest") \\
    .config("spark.sql.catalog.iceberg.uri",
            "http://iceberg-rest.platform-storage:8181") \\
    .config("spark.sql.catalog.iceberg.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO") \\
    .config("spark.sql.catalog.iceberg.s3.endpoint",
            "http://minio.platform-storage:9000") \\
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \\
    .config("spark.hadoop.fs.s3a.endpoint",
            "http://minio.platform-storage:9000") \\
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \\
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \\
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \\
    .getOrCreate()

# List all namespaces and tables
namespaces = [row[0] for row in spark.sql("SHOW NAMESPACES IN iceberg").collect()]
tables = []
for ns in namespaces:
    for row in spark.sql(f"SHOW TABLES IN iceberg.{ns}").collect():
        tables.append(f"iceberg.{ns}.{row['tableName']}")

print(f"Found {len(tables)} tables for maintenance")

for table in tables:
    try:
        # Compact small files (binpack strategy)
        spark.sql(f\"\"\"
            CALL iceberg.system.rewrite_data_files(
                table => '{table}',
                strategy => 'binpack',
                options => map(
                    'target-file-size-bytes', '134217728',
                    'min-input-files', '5'
                )
            )
        \"\"\")
        print(f"Compacted: {table}")
    except Exception as e:
        print(f"Compaction skipped for {table}: {e}")

    try:
        # Expire old snapshots (giu lai 5 snapshots gan nhat)
        spark.sql(f\"\"\"
            CALL iceberg.system.expire_snapshots(
                table => '{table}',
                retain_last => 5
            )
        \"\"\")
        print(f"Expired snapshots: {table}")
    except Exception as e:
        print(f"Expire skipped for {table}: {e}")

spark.stop()
print("Maintenance completed")
sys.exit(0)
"""


with DAG(
    dag_id="platform_iceberg_maintenance",
    default_args={
        "owner": "platform-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=30),
        "execution_timeout": timedelta(hours=2),
    },
    description="Daily Iceberg maintenance: health check + compact + expire snapshots",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["platform", "iceberg", "maintenance"],
) as dag:

    # Task 1: Kiem tra health cua tat ca tables qua REST API (nhe, chay trong Airflow)
    health_check = PythonOperator(
        task_id="check_tables_health",
        python_callable=check_tables_health,
    )

    # Task 2: Chay Spark job de compact va expire snapshots
    # Dung KubernetesPodOperator vi SparkKubernetesOperator can provider rieng
    # va cluster nho nen chay local[*] mode (khong can executor rieng)
    run_maintenance = KubernetesPodOperator(
        task_id="run_spark_maintenance",
        namespace="platform-compute",
        image="apache/spark:3.5.0",
        cmds=["python3", "-c", SPARK_MAINTENANCE_SCRIPT],
        name="iceberg-maintenance",
        service_account_name="spark-operator-spark",
        is_delete_operator_pod=True,
        get_logs=True,
        startup_timeout_seconds=300,
        container_resources={
            "requests": {"cpu": "500m", "memory": "640m"},
            "limits": {"cpu": "1", "memory": "768m"},
        },
        env_vars={
            "SPARK_LOCAL_IP": "0.0.0.0",
        },
    )

    health_check >> run_maintenance
