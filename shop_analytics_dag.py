"""ELT-пайплайн: генерация данных -> raw (как есть) -> stg (типизация, дедуп) -> mart (витрины).

Трансформация живёт в SQL внутри ClickHouse, Python только извлекает и грузит.
DAG перезапускается безопасно: сид генератора фиксированный, raw накапливает
дубликаты, stg дедуплицирует их через ReplacingMergeTree + FINAL.
"""

import os
import subprocess
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

from ch_utils import ch, run_sql_file

PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/airflow")


def run_extract(**_):
    subprocess.run(
        [sys.executable, "scripts/extract.py", "--out", "data", "--users", "30000", "--days", "120"],
        cwd=PROJECT_DIR, check=True,
    )


def run_load(**_):
    subprocess.run(
        [sys.executable, "scripts/load.py", "--dir", "data"],
        cwd=PROJECT_DIR, check=True,
    )


def run_sql(filename: str, **_):
    run_sql_file(os.path.join(PROJECT_DIR, "sql", filename))


def dq_checks(**_):
    """Простые проверки качества данных: пайплайн падает, если слои разъехались."""
    def scalar(sql: str) -> int:
        return int(ch(sql).decode().strip() or "0")

    checks = {
        "stg.events не пуст": scalar("SELECT count() FROM stg.events FINAL") > 0,
        "stg.users не пуст": scalar("SELECT count() FROM stg.users FINAL") > 0,
        "нет дублей событий (FINAL)": scalar(
            "SELECT count() FROM ("
            "SELECT event_id FROM stg.events FINAL GROUP BY event_id HAVING count() > 1 LIMIT 1)") == 0,
        "есть покупки": scalar("SELECT count() FROM stg.orders FINAL WHERE status = 'completed'") > 0,
        "mart.dau_daily заполнена": scalar("SELECT count() FROM mart.dau_daily") > 0,
        "mart.revenue_daily заполнена": scalar("SELECT count() FROM mart.revenue_daily") > 0,
    }
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise AirflowException("DQ-проверки не прошли: " + ", ".join(failed))
    print("Все DQ-проверки прошли:", ", ".join(checks))


with DAG(
    dag_id="shop_analytics_elt",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "data-team", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["clickhouse", "elt", "analytics"],
) as dag:
    ensure_schemas = PythonOperator(
        task_id="ensure_schemas", python_callable=run_sql, op_args=["01_raw.sql"])
    extract = PythonOperator(task_id="extract_generate_data", python_callable=run_extract)
    load = PythonOperator(task_id="load_to_raw", python_callable=run_load)
    staging = PythonOperator(
        task_id="build_staging", python_callable=run_sql, op_args=["02_staging.sql"])
    marts = PythonOperator(
        task_id="build_marts", python_callable=run_sql, op_args=["03_marts.sql"])
    dq = PythonOperator(task_id="data_quality_check", python_callable=dq_checks)

    ensure_schemas >> extract >> load >> staging >> marts >> dq
