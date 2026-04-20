# Platform DAGs

Airflow DAGs cho Data Platform — định nghĩa các workflow xử lý dữ liệu, được git-sync từ **GitHub** vào Airflow.

## Kiến thức nền tảng

### Airflow DAG là gì?
- **DAG** (Directed Acyclic Graph) là một workflow gồm nhiều task được thực thi theo thứ tự.
- Mỗi file `.py` trong `dags/` định nghĩa 1 hoặc nhiều DAG.
- Airflow đọc thư mục `dags/` định kỳ và tự động đăng ký DAGs mới.

### Git-sync
- Airflow trong cluster được cấu hình **git-sync** từ repo `platform-dags` trên **GitHub**.
- Khi push code lên GitHub → Airflow tự động cập nhật DAGs (không cần redeploy).
- SparkApplication YAML templates nằm trong `dags/<team>/spark-apps/` cũng được sync cùng.

## Cấu trúc thư mục

```
platform-dags/
├── dags/
│   ├── _shared/                              # Modules dùng chung
│   │   ├── spark_operator_task.py             #   make_spark_task() helper
│   │   ├── quality_gate_task.py               #   Iceberg REST quality gate
│   │   └── notification_task.py               #   Slack notifications
│   ├── platform/                             # DAGs cấp platform (ops/maintenance)
│   │   ├── daily_minio_health.py              #   Health check: MinIO + Iceberg REST
│   │   └── iceberg_maintenance.py             #   Snapshot expire + file compaction
│   └── finance/                              # DAGs của team Finance
│       ├── finance_daily_revenue_pipeline.py #   ETL daily: S3 bronze → Iceberg silver
│       ├── finance_monthly_reconcile.py      #   Đối soát hàng tháng
│       └── spark-apps/                       #   SparkApplication YAML templates
│           ├── daily-revenue.yaml             #     daily_revenue_etl job
│           └── monthly-reconcile.yaml          #     monthly_reconcile job
├── plugins/
│   └── iceberg_lineage_plugin.py             # Airflow plugin: data lineage (placeholder)
└── README.md
```

## Modules dùng chung (`dags/_shared/`)

### `spark_operator_task.py`
Helper tạo Airflow task chạy **SparkApplication** trên Kubernetes qua Spark Operator.
Dùng `SparkKubernetesOperator` — task nhận YAML template và submit lên Spark Operator.

### `quality_gate_task.py`
Kiểm tra data quality qua **Iceberg REST API**:
- Table tồn tại và accessible
- Snapshot mới nhất không quá cũ
- Số records đạt ngưỡng tối thiểu

### `notification_task.py`
Gửi Slack notification khi pipeline thành công hoặc thất bại.

## SparkApplication YAML templates

Mỗi team có thư mục `spark-apps/` chứa YAML định nghĩa SparkApplication. Các file này:
- Được git-sync vào Airflow pod cùng với DAGs
- Được DAGs reference qua `os.path.join(os.path.dirname(__file__), "spark-apps", "xxx.yaml")`
- Image tag được CI tự động cập nhật khi build image mới (xem `cicd-pipeline-detail.drawio`)

## Viết DAG mới

1. Tạo file `.py` trong `dags/<team>/` hoặc `dags/platform/`.
2. Tạo SparkApplication YAML trong `dags/<team>/spark-apps/` (nếu cần Spark job).
3. Import modules từ `dags/_shared/` nếu cần.
4. Push lên GitHub → Airflow tự động nhận DAG mới (git-sync poll 60s).

## Liên kết với các project khác

| Project | Quan hệ |
|---------|---------|
| **platform-infra** | Airflow được deploy bởi ArgoCD; git-sync trỏ đến repo này |
| **team-finance/finance-app** | Source code Spark jobs, Docker image trên GHCR |
| **team-finance/finance-config** | Pipeline config, Spark tuning, S3 credentials |

## Lưu ý

- Cluster hiện tại dùng **MinIO** (S3-compatible) và **Iceberg REST Catalog** (không dùng Trino).
- Module `_shared/` là code Python thuần — không phải DAG, Airflow không đăng ký chúng.
- SparkApp YAML dùng **GitHub Container Registry** (`ghcr.io/KaitoKid-123/finance-app/etl`).
- Git-sync credentials nằm trong Secret `github-dags-token` ở namespace `platform-data`.
