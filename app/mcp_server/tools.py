import requests
import os
import time


DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("PAT")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID")

HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}


def _execute_statement(payload):
    try:
        response = requests.post(
            f"{DATABRICKS_HOST}/api/2.0/sql/statements",
            headers=HEADERS,
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return f"Databricks API error: {str(e)}"


def run_sql(query: str) -> str:
    """Execute SQL query in Databricks"""
    payload = {
        "statement": query,
        "warehouse_id": WAREHOUSE_ID
    }

    result = _execute_statement(payload)
    return str(result)


def best_month(stock: str, year: int) -> str:
    """Find best performing month for a stock"""
    query = f"""
    SELECT month, SUM(return) as total_return
    FROM gold.stock_returns
    WHERE ticker = '{stock}' AND year = {year}
    GROUP BY month
    ORDER BY total_return DESC
    LIMIT 1
    """
    return run_sql(query)


def list_tables() -> str:
    return run_sql("SHOW TABLES IN gold")


def describe_table(table: str) -> str:
    return run_sql(f"DESCRIBE {table}")