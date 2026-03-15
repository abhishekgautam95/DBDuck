from __future__ import annotations

import pytest

from DBDuck import UDOM
from DBDuck.core.exceptions import ConnectionError


def test_udom_ping_sql(tmp_path) -> None:
    db_file = tmp_path / "ping_sql.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    result = db.ping()
    assert isinstance(result, list)
    assert len(result) == 1
    db.close()


def test_udom_ping_nosql_non_mongo() -> None:
    db = UDOM(db_type="nosql", db_instance="redis", url="redis://localhost:6379")
    result = db.ping()
    assert result["ok"] == 1
    db.close()


def test_udom_context_manager_sql(tmp_path) -> None:
    db_file = tmp_path / "ctx_sql.db"
    with UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}") as db:
        db.create("Orders", {"order_id": 501, "customer": "C", "paid": True})
        rows = db.find("Orders", where={"order_id": 501})
        assert len(rows) == 1


def test_udom_invalid_config_raises_connection_error() -> None:
    with pytest.raises(ConnectionError):
        UDOM(db_type="sql", db_instance="unknown-db")
