"""
Reusable data quality gate task.
Query Iceberg REST API de kiem tra table health truoc khi tiep tuc pipeline.

Cluster hien tai KHONG co Trino, nen dung Iceberg REST Catalog API
de kiem tra metadata (snapshot count, schema, ...).
Cac check nang hon (null rate, row count) can chay qua Spark job.
"""
from __future__ import annotations
import logging
from typing import Callable, Optional
import requests
from airflow.operators.python import BranchPythonOperator

logger = logging.getLogger(__name__)
ICEBERG_REST = "http://iceberg-rest.platform-storage:8181"


def _get_table_metadata(namespace: str, table: str) -> dict:
    """Lay metadata cua 1 Iceberg table qua REST API."""
    resp = requests.get(
        f"{ICEBERG_REST}/v1/namespaces/{namespace}/tables/{table}",
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _get_latest_snapshot(metadata: dict) -> dict | None:
    """Lay snapshot moi nhat tu table metadata."""
    snapshots = metadata.get("metadata", {}).get("snapshots", [])
    if not snapshots:
        return None
    return max(snapshots, key=lambda s: s.get("timestamp-ms", 0))


def make_quality_gate(
    task_id: str,
    table: str,
    dag,
    pass_branch: str,
    fail_branch: str,
    min_snapshots: int = 1,
    max_snapshot_age_hours: int = 48,
    extra_checks: Optional[list[Callable]] = None,
) -> BranchPythonOperator:
    """
    Tao BranchPythonOperator kiem tra data quality qua Iceberg REST API.

    Cac check kha dung (khong can Trino):
    - Table ton tai va co the truy cap
    - Co it nhat min_snapshots snapshots (= da co data)
    - Snapshot moi nhat khong qua cu (< max_snapshot_age_hours)

    Args:
        table:       Iceberg table dang 'namespace.table_name'
        pass_branch: task_id tiep theo neu pass
        fail_branch: task_id tiep theo neu fail
        min_snapshots: so snapshot toi thieu
        max_snapshot_age_hours: snapshot moi nhat khong duoc cu hon X gio
        extra_checks: list of callable(**context) -> bool
    """
    def _check(**context):
        errors = []
        parts = table.split(".", 1)
        if len(parts) != 2:
            errors.append(f"Invalid table format: {table}, expected 'namespace.table'")
            context["ti"].xcom_push(key="quality_errors", value=errors)
            return fail_branch

        namespace, table_name = parts

        # Check 1: Table ton tai va accessible
        try:
            metadata = _get_table_metadata(namespace, table_name)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                errors.append(f"Table {table} does not exist")
            else:
                errors.append(f"Cannot access table {table}: {e}")
            context["ti"].xcom_push(key="quality_errors", value=errors)
            logger.error(f"Quality gate FAILED: {errors}")
            return fail_branch
        except Exception as e:
            errors.append(f"REST API error: {e}")
            context["ti"].xcom_push(key="quality_errors", value=errors)
            logger.error(f"Quality gate FAILED: {errors}")
            return fail_branch

        # Check 2: Co du snapshots (= da co data duoc ghi)
        snapshots = metadata.get("metadata", {}).get("snapshots", [])
        if len(snapshots) < min_snapshots:
            errors.append(
                f"Snapshot count {len(snapshots)} < minimum {min_snapshots}"
            )

        # Check 3: Snapshot moi nhat khong qua cu
        latest = _get_latest_snapshot(metadata)
        if latest:
            import time
            snapshot_age_hours = (
                time.time() * 1000 - latest.get("timestamp-ms", 0)
            ) / (1000 * 3600)
            if snapshot_age_hours > max_snapshot_age_hours:
                errors.append(
                    f"Latest snapshot is {snapshot_age_hours:.1f}h old "
                    f"> threshold {max_snapshot_age_hours}h"
                )
            logger.info(
                f"Latest snapshot age: {snapshot_age_hours:.1f}h, "
                f"records summary: {latest.get('summary', {})}"
            )

        # Check 4: Extra checks (user-defined)
        for fn in (extra_checks or []):
            try:
                ok = fn(**context)
                if not ok:
                    errors.append(f"Extra check {fn.__name__} failed")
            except Exception as e:
                errors.append(f"Extra check error: {e}")

        if errors:
            context["ti"].xcom_push(key="quality_errors", value=errors)
            logger.error(f"Quality gate FAILED: {errors}")
            return fail_branch

        context["ti"].xcom_push(key="quality_errors", value=[])
        logger.info("Quality gate PASSED")
        return pass_branch

    return BranchPythonOperator(
        task_id=task_id,
        python_callable=_check,
        dag=dag,
    )
