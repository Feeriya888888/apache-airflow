"""Мини-клиент ClickHouse через HTTP-интерфейс (только стандартная библиотека).

Зависимостей не нужно: apache-airflow-providers-clickhouse здесь не требуется.
Креды берутся из переменных окружения (в docker-compose.yml), а в проде
их место — Airflow Connections и секрет-бэкенд.
"""

import os
import urllib.error
import urllib.parse
import urllib.request


def ch(sql: str, data: bytes | None = None, params: dict | None = None) -> bytes:
    """Выполняет запрос к ClickHouse, возвращает тело ответа."""
    host = os.environ.get("CLICKHOUSE_HOST", "http://localhost:8123")
    qs = urllib.parse.urlencode({
        "user": os.environ.get("CLICKHOUSE_USER", "analyst"),
        "password": os.environ.get("CLICKHOUSE_PASSWORD", "analyst_pass"),
        **(params or {}),
    })
    req = urllib.request.Request(f"{host}/?{qs}", data=data if data is not None else sql.encode())
    try:
        with urllib.request.urlopen(req, timeout=600) as resp:
            return resp.read()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"ClickHouse вернул ошибку: {e.read().decode('utf-8', 'replace')}") from e


def run_sql_file(path: str) -> None:
    """Исполняет SQL-файл по оператору: ClickHouse HTTP принимает один statement за запрос."""
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    # комментарии убираем, чтобы не мешали простому сплиту по точке с запятой
    lines = [ln for ln in sql.splitlines() if not ln.strip().startswith("--")]
    statements = [s.strip() for s in "\n".join(lines).split(";") if s.strip()]
    for stmt in statements:
        ch(stmt)
