from __future__ import annotations

import pytest

from DBDuck.adapters.mssql_adapter import MSSQLAdapter
from DBDuck.core.exceptions import QueryError


def test_mssql_ensure_table_uses_sql_server_if_not_exists_pattern() -> None:
    adapter = MSSQLAdapter(url="sqlite:///:memory:")
    captured: dict[str, object] = {}

    def _capture_run_native(query, params=None):
        captured["query"] = query
        captured["params"] = params
        return {"rows_affected": 0}

    adapter.run_native = _capture_run_native  # type: ignore[method-assign]
    adapter._ensure_table("Orders", {"order_id": 101, "customer": "A", "paid": True})

    query = str(captured["query"])
    assert "CREATE TABLE IF NOT EXISTS" not in query
    assert "IF OBJECT_ID(" in query
    assert "CREATE TABLE [Orders]" in query
    assert captured.get("params") is None


def test_mssql_find_uses_top_not_limit() -> None:
    adapter = MSSQLAdapter(url="sqlite:///:memory:")
    captured: dict[str, object] = {}

    def _capture_run_native(query, params=None):
        captured["query"] = query
        captured["params"] = params
        return []

    adapter.run_native = _capture_run_native  # type: ignore[method-assign]
    adapter.find("Orders", where={"paid": True}, limit=10)

    query = str(captured["query"])
    assert "SELECT TOP 10 * FROM [Orders]" in query
    assert "LIMIT" not in query
    assert captured["params"] == {"w_0": True}


def test_mssql_convert_uql_find_uses_top_not_limit() -> None:
    adapter = MSSQLAdapter(url="sqlite:///:memory:")
    sql = adapter.convert_uql("FIND Orders WHERE paid = true ORDER BY order_id DESC LIMIT 10")
    assert "SELECT TOP 10 * FROM [Orders]" in sql
    assert "LIMIT" not in sql


def test_mssql_convert_uql_rejects_where_injection() -> None:
    adapter = MSSQLAdapter(url="sqlite:///:memory:")
    with pytest.raises(QueryError, match="Potential SQL injection"):
        adapter.convert_uql("FIND Orders WHERE paid = true; DROP TABLE Orders LIMIT 10")


def test_mssql_aggregate_uses_top_not_limit() -> None:
    adapter = MSSQLAdapter(url="sqlite:///:memory:")
    captured: dict[str, object] = {}

    def _capture_run_native(query, params=None):
        captured["query"] = query
        captured["params"] = params
        return []

    adapter.run_native = _capture_run_native  # type: ignore[method-assign]
    adapter.aggregate("Orders", metrics={"total": "count(*)"}, limit=5)

    query = str(captured["query"])
    assert "SELECT TOP 5 COUNT(*) AS [total] FROM [Orders]" in query
    assert "LIMIT" not in query
