# Platform DAGs

Airflow DAGs cho Data Platform — dinh nghia cac workflow xu ly du lieu, duoc git-sync tu **GitHub** vao Airflow.

## Kien thuc nen biet

### Airflow DAG la gi?
- **DAG** (Directed Acyclic Graph) la mot workflow gom nhieu task duoc thuc thi theo thu tu.
- Moi file `.py` trong `dags/` dinh nghia 1 hoac nhieu DAG.
- Airflow doc thu muc `dags/` dinh ky va tu dong dang ky DAGs moi.

### Git-sync
- Airflow trong cluster duoc cau hinh **git-sync** tu repo `platform-dags` tren GitHub.
- Khi push code len GitHub → Airflow tu dong cap nhat DAGs (khong can redeploy).
- SparkApplication YAML templates nam trong `dags/<team>/spark-apps/` cung duoc sync vao Airflow pod.

## Cau truc thu muc

```
platform-dags/
+-- dags/
|   +-- platform/                           # DAGs cap platform (maintenance, monitoring)
|   |   +-- daily_minio_health.py           #   Kiem tra MinIO + Iceberg REST health
|   |   +-- iceberg_maintenance.py          #   Compact files + expire snapshots
|   +-- finance/                            # DAGs cua team Finance
|   |   +-- finance_daily_revenue_pipeline.py #  ETL daily: S3 bronze → Iceberg silver
|   |   +-- finance_monthly_reconcile.py    #   Doi soat hang thang
|   |   +-- spark-apps/                     #   SparkApplication YAML templates
|   |       +-- daily-revenue.yaml          #     Spark job config cho daily ETL
|   |       +-- monthly-reconcile.yaml      #     Spark job config cho reconcile
|   +-- _shared/                            # Modules dung chung cho nhieu DAGs
|       +-- spark_operator_task.py          #   Helper tao SparkApplication task
|       +-- quality_gate_task.py            #   Data quality check qua Iceberg REST API
|       +-- notification_task.py            #   Gui thong bao (Slack)
+-- plugins/
    +-- iceberg_lineage_plugin.py           # Airflow plugin theo doi data lineage (placeholder)
```

## Modules dung chung (`dags/_shared/`)

### spark_operator_task.py
Helper tao Airflow task chay **SparkApplication** tren Kubernetes thong qua Spark Operator.

### quality_gate_task.py
Kiem tra data quality qua **Iceberg REST API** (khong can Trino):
- Table ton tai va accessible
- Snapshot moi nhat khong qua cu
- Snapshot co du records

### notification_task.py
Gui Slack thong bao khi pipeline thanh cong hoac that bai.

## SparkApplication YAML templates

Moi team co thu muc `spark-apps/` chua YAML dinh nghia SparkApplication. Cac file nay:
- Duoc git-sync vao Airflow pod cung voi DAGs
- Duoc DAGs reference qua `os.path.join(os.path.dirname(__file__), "spark-apps", "xxx.yaml")`
- Image tag duoc CI tu dong cap nhat khi build image moi

## Cach viet DAG moi

1. Tao file `.py` trong `dags/<team>/` hoac `dags/platform/`.
2. Tao SparkApplication YAML trong `dags/<team>/spark-apps/` (neu can Spark job).
3. Import modules tu `dags/_shared/` neu can.
4. Push len GitHub repo `platform-dags` → Airflow tu dong nhan DAG moi.

## Lien ket voi cac project khac

| Project | Quan he |
|---------|---------|
| **platform-infra** | Airflow duoc deploy boi ArgoCD, cau hinh git-sync tro ve repo nay |
| **team-finance/finance-app** | Source code Spark jobs, build Docker image chay trong SparkApplication |
| **team-finance/finance-config** | Pipeline config, S3 credentials |

## Luu y

- Cluster hien tai dung **MinIO** (khong phai Ceph) va **Iceberg REST Catalog** (khong co Trino).
- Module `_shared/` la code Python thuan — khong phai DAG, Airflow se khong dang ky chung nhu DAG.
- SparkApp YAML dung **GitHub Container Registry** (`ghcr.io/KaitoKid-123/finance-app/etl`) de pull image.
