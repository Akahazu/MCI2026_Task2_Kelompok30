"""
DAG Airflow untuk pipeline orders real-time.

Alurnya:
  http://96.9.212.102:8000/orders
      → fetch_orders.py (simpan parquet ke data_lake/orders/)
      → process_orders_spark.py (agregat top 30 produk → ClickHouse)
      → divisualisasikan di Metabase.

Isi DAG sengaja ringkas: cuma struktur dan jadwal.
Logika data tinggalnya di dags/scripts/.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "mmds_engineer",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "orders_realtime_pipeline",
    default_args=default_args,
    # Setiap 10 menit narik snapshot baru dari API
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    description="Micro-batching Orders API → Spark → ClickHouse",
) as dag:

    fetch_step = BashOperator(
        task_id="fetch_orders",
        bash_command="python /opt/airflow/dags/scripts/fetch_orders.py",
    )

    process_top_products_spark = BashOperator(
        task_id="process_top_products_spark",
        bash_command="python /opt/airflow/dags/scripts/process_orders_spark.py",
    )

    process_user_behavior = BashOperator(
        task_id="process_user_behavior",
        bash_command="python /opt/airflow/dags/scripts/process_user_behavior.py",
    )

    fetch_step >>process_top_products_spark >> process_user_behavior
