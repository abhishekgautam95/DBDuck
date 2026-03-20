from __future__ import annotations

from typing import Any

import bcrypt
import pytest

from DBDuck import UDOM
from DBDuck.adapters.mssql_adapter import MSSQLAdapter
from DBDuck.adapters.mysql_adapter import MySQLAdapter
from DBDuck.adapters.sqlite_adapter import SQLiteAdapter
from DBDuck.core.exceptions import QueryError
from DBDuck.core.security import SensitiveFieldProtector
from DBDuck.udom.adapters.graph_adapter import GraphAdapter
from DBDuck.udom.adapters.nosql_adapter import NoSQLAdapter
from DBDuck.udom.adapters.sql.base_sql_adapter import BaseSQLAdapter
from DBDuck.udom.adapters.sql_adapter import ParameterizedSQL, SQLAdapter
from DBDuck.udom.uql.uql_parser import UQLParser


def _sqlite_db(tmp_path, **options: Any) -> UDOM:
    db_file = tmp_path / "security.db"
    return UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}", **options)


class TestVULN1:
    def test_attack_payload_is_blocked(self, tmp_path) -> None:
        db = _sqlite_db(tmp_path)
        with pytest.raises(QueryError):
            db.uexecute("CREATE Orders {id: 1, customer: 'x') OR ('1'='1}")

    def test_legitimate_use_still_works(self, tmp_path) -> None:
        db = _sqlite_db(tmp_path)
        result = db.uexecute("CREATE Orders {id: 1, customer: 'alice'}")
        assert result["rows_affected"] == 1


class TestVULN2:
    def test_attack_payload_is_blocked(self, tmp_path) -> None:
        db = _sqlite_db(tmp_path)
        db.create("Orders", {"id": 1, "paid": 1})
        with pytest.raises(QueryError):
            db.uexecute("FIND Orders WHERE paid = 1 AND SLEEP(5)-- ")

    def test_legitimate_use_still_works(self, tmp_path) -> None:
        db = _sqlite_db(tmp_path)
        db.create("Orders", {"id": 1, "paid": 1})
        rows = db.uexecute("FIND Orders WHERE paid = 1")
        assert len(rows) == 1


class TestVULN3:
    def test_attack_payload_is_blocked(self, tmp_path) -> None:
        db = _sqlite_db(tmp_path, allow_unsafe_where_strings=True)
        db.create("Orders", {"id": 1, "paid": 1})
        with pytest.raises(QueryError):
            db.find("Orders", where="paid = 1 UNION SELECT password FROM users")

    def test_legitimate_use_still_works(self, tmp_path) -> None:
        db = _sqlite_db(tmp_path, allow_unsafe_where_strings=True)
        db.create("Orders", {"id": 1, "paid": 1})
        rows = db.find("Orders", where="paid = 1")
        assert len(rows) == 1


class TestVULN4:
    def test_attack_payload_is_blocked(self, tmp_path) -> None:
        db = _sqlite_db(tmp_path)
        with pytest.raises(QueryError):
            db.create_view("v_orders", "SELECT 1; DROP TABLE users--")

    def test_legitimate_use_still_works(self, tmp_path) -> None:
        db = _sqlite_db(tmp_path, admin_mode=True)
        db.create("Orders", {"id": 1, "paid": 1})
        db.create_view("v_orders", "SELECT id FROM Orders WHERE paid = 1", replace=True)
        assert db.find("v_orders")[0]["id"] == 1


class TestVULN5:
    def test_attack_payload_is_blocked(self) -> None:
        adapter = MySQLAdapter(url="sqlite:///:memory:", admin_mode=True)
        with pytest.raises(QueryError):
            adapter.create_procedure("sync_orders", "() BEGIN DROP TABLE users END", replace=True)
        with pytest.raises(QueryError):
            adapter.create_function("calc_tax", "(amount INT) RETURNS INT DROP TABLE users", replace=True)

    def test_legitimate_use_still_works(self) -> None:
        adapter = MySQLAdapter(url="sqlite:///:memory:", admin_mode=True)
        calls: list[str] = []
        adapter.run_native = lambda query, params=None: calls.append(str(query)) or {"rows_affected": 0}  # type: ignore[method-assign]
        adapter.create_procedure("sync_orders", "() BEGIN SELECT 1 END", replace=True)
        adapter.create_function("calc_tax", "(amount INT) RETURNS INT RETURN amount", replace=True)
        assert any("CREATE PROCEDURE" in query for query in calls)
        assert any("CREATE FUNCTION" in query for query in calls)


class TestVULN6:
    def test_attack_payload_is_blocked(self) -> None:
        adapter = MySQLAdapter(url="sqlite:///:memory:", admin_mode=True)
        with pytest.raises(QueryError):
            adapter.create_event("nightly_cleanup", "EVERY 1 SECOND", "DROP TABLE users")

    def test_legitimate_use_still_works(self) -> None:
        adapter = MySQLAdapter(url="sqlite:///:memory:", admin_mode=True)
        calls: list[str] = []
        adapter.run_native = lambda query, params=None: calls.append(str(query)) or {"rows_affected": 0}  # type: ignore[method-assign]
        adapter.create_event("nightly_cleanup", "EVERY 1 DAY", "INSERT INTO jobs(status) VALUES ('new')")
        assert any("CREATE EVENT" in query for query in calls)


class TestVULN7:
    def test_attack_payload_is_blocked(self) -> None:
        adapter = SQLAdapter(url="sqlite:///:memory:")
        with pytest.raises(ValueError):
            adapter._ensure_table("t', N'U') IS NULL; DROP TABLE users--", {"id": "1"})

    def test_legitimate_use_still_works(self) -> None:
        adapter = MSSQLAdapter(url="sqlite:///:memory:")
        calls: list[tuple[str, dict[str, Any] | None]] = []

        def _capture(query, params=None):
            calls.append((str(query), params))
            return [{"oid": None}] if "OBJECT_ID" in str(query) else {"rows_affected": 0}

        adapter.run_native = _capture  # type: ignore[method-assign]
        adapter._ensure_table("Orders", {"id": 1})
        assert calls[0][1] == {"tname": "Orders"}


class TestVULN8:
    def test_attack_payload_is_blocked(self) -> None:
        adapter = MSSQLAdapter(url="sqlite:///:memory:")
        with pytest.raises(QueryError):
            adapter.convert_uql("FIND Orders WHERE 1=1 UNION SELECT password FROM users--")

    def test_legitimate_use_still_works(self) -> None:
        adapter = MSSQLAdapter(url="sqlite:///:memory:")
        query, params = adapter.convert_uql("FIND Orders WHERE paid = true LIMIT 1")
        assert "SELECT TOP 1 * FROM [Orders]" in query
        assert params == {"ws_0": True}


class TestVULN9:
    def test_attack_payload_is_blocked(self) -> None:
        adapter = SQLAdapter(url="sqlite:///:memory:")
        with pytest.raises(ValueError):
            adapter.create("users;DROP", {"name": "x', 1); DROP TABLE users--"})

    def test_legitimate_use_still_works(self) -> None:
        adapter = SQLAdapter(url="sqlite:///:memory:")
        query = adapter.convert_uql("CREATE users {name: 'alice'}")
        assert isinstance(query, ParameterizedSQL)
        assert query.params == {"v_0": "alice"}


class TestVULN10:
    def test_attack_payload_is_blocked(self) -> None:
        adapter = SQLAdapter(url="sqlite:///:memory:")
        with pytest.raises(ValueError):
            adapter.find("users", where="1=1 UNION SELECT * FROM secrets--")

    def test_legitimate_use_still_works(self) -> None:
        adapter = SQLAdapter(url="sqlite:///:memory:")
        adapter.create("users", {"name": "alice"})
        rows = adapter.find("users", where="name = 'alice'")
        assert rows


class TestVULN11:
    def test_attack_payload_is_blocked(self) -> None:
        adapter = SQLAdapter(url="sqlite:///:memory:")
        with pytest.raises(ValueError):
            adapter._ensure_table("bad-name", {"id": "1"})

    def test_legitimate_use_still_works(self) -> None:
        adapter = SQLAdapter(url="sqlite:///:memory:")
        adapter._ensure_table("users", {"id": "1"})


class TestVULN12:
    def test_attack_payload_is_blocked(self) -> None:
        adapter = SQLAdapter(url="sqlite:///:memory:")
        with pytest.raises(QueryError):
            adapter.run_native("SELECT * FROM missing_table")

    def test_legitimate_use_still_works(self) -> None:
        adapter = SQLAdapter(url="sqlite:///:memory:")
        assert adapter.run_native("SELECT 1")[0][0] == 1


class TestVULN13:
    def test_attack_payload_is_blocked(self) -> None:
        class _Adapter(BaseSQLAdapter):
            def create(self, entity, data): raise NotImplementedError
            def create_many(self, entity, rows): raise NotImplementedError
            def find(self, entity, where=None, order_by=None, limit=None): raise NotImplementedError
            def delete(self, entity, where): raise NotImplementedError
            def update(self, entity, data, where): raise NotImplementedError
            def count(self, entity, where=None): raise NotImplementedError
            def _quote(self, name): return f'"{name}"'
            def _format_value(self, val): return val
            def _ensure_table(self, table_name, fields): return None

        adapter = _Adapter("sqlite:///:memory:")
        with pytest.raises(QueryError):
            adapter.run_native("SELECT * FROM missing_table")

    def test_legitimate_use_still_works(self) -> None:
        class _Adapter(BaseSQLAdapter):
            def create(self, entity, data): raise NotImplementedError
            def create_many(self, entity, rows): raise NotImplementedError
            def find(self, entity, where=None, order_by=None, limit=None): raise NotImplementedError
            def delete(self, entity, where): raise NotImplementedError
            def update(self, entity, data, where): raise NotImplementedError
            def count(self, entity, where=None): raise NotImplementedError
            def _quote(self, name): return f'"{name}"'
            def _format_value(self, val): return val
            def _ensure_table(self, table_name, fields): return None

        adapter = _Adapter("sqlite:///:memory:")
        assert adapter.run_native("SELECT 1")[0][0] == 1


class TestVULN14:
    def test_attack_payload_is_blocked(self) -> None:
        adapter = GraphAdapter()
        with pytest.raises(QueryError):
            adapter.find("User", where="1=1 WITH {} CALL db.labels() YIELD label RETURN label")

    def test_legitimate_use_still_works(self) -> None:
        adapter = GraphAdapter()
        result = adapter.find("User", where="name = 'alice'")
        assert result["params"] == {"w_0": "alice"}


class TestVULN15:
    def test_attack_payload_is_blocked(self) -> None:
        adapter = GraphAdapter()
        with pytest.raises(QueryError):
            adapter.update("User", {"RETURN n; MATCH(x)": "x"}, where="1=1 DETACH DELETE n//")

    def test_legitimate_use_still_works(self) -> None:
        adapter = GraphAdapter()
        result = adapter.update("User", {"name": "alice"}, where="age > 20")
        assert "SET n.name = $set_1" in result["query"]


class TestVULN16:
    def test_attack_payload_is_blocked(self) -> None:
        adapter = GraphAdapter()
        with pytest.raises(QueryError):
            adapter._convert_conditions("name = x} RETURN n; MATCH (m) DETACH DELETE m //")

    def test_legitimate_use_still_works(self) -> None:
        adapter = GraphAdapter()
        clause, params = adapter._convert_conditions("age > 18 AND active = true")
        assert clause == "n.age > $w_0 AND n.active = $w_1"
        assert params == {"w_0": 18, "w_1": True}


class TestVULN17:
    def test_attack_payload_is_blocked(self) -> None:
        adapter = GraphAdapter()
        with pytest.raises(QueryError):
            adapter.find("User}) RETURN n; MATCH (evil) DETACH DELETE evil //")

    def test_legitimate_use_still_works(self) -> None:
        adapter = GraphAdapter()
        result = adapter.find("User")
        assert "MATCH (n:User)" in result["query"]


class TestVULN18:
    def test_attack_payload_is_blocked(self) -> None:
        adapter = GraphAdapter()
        with pytest.raises(QueryError):
            adapter.convert_uql("CREATE User {na-me: 'alice'}")

    def test_legitimate_use_still_works(self) -> None:
        adapter = GraphAdapter()
        query, params = adapter.convert_uql("CREATE User {name: 'alice', age: 30}")
        assert "{name: $p_0, age: $p_1}" in query
        assert params == {"p_0": "alice", "p_1": 30}


class TestVULN19:
    def test_attack_payload_is_blocked(self) -> None:
        parser = UQLParser()
        with pytest.raises(ValueError):
            parser._parse_key_value_pairs("$where: 1")

    def test_legitimate_use_still_works(self) -> None:
        parser = UQLParser()
        parsed = parser._parse_key_value_pairs("url: http://example.com")
        assert parsed["url"] == "http://example.com"


class TestVULN20:
    def test_attack_payload_is_blocked(self) -> None:
        parser = UQLParser()
        with pytest.raises(ValueError):
            parser._cast_value("'$where'")

    def test_legitimate_use_still_works(self) -> None:
        parser = UQLParser()
        assert parser._cast_value("'hello'") == "hello"


class TestVULN21:
    def test_attack_payload_is_blocked(self) -> None:
        parser = UQLParser()
        with pytest.raises(ValueError):
            parser._parse_find("FIND")

    def test_legitimate_use_still_works(self) -> None:
        parser = UQLParser()
        parsed = parser._parse_delete("DELETE users WHERE id = 1")
        assert parsed["entity"] == "users"


class TestVULN22:
    def test_attack_payload_is_blocked(self) -> None:
        adapter = NoSQLAdapter(db_instance="redis")
        with pytest.raises(QueryError):
            adapter._convert_condition("type = $where")

    def test_legitimate_use_still_works(self) -> None:
        adapter = NoSQLAdapter(db_instance="redis")
        assert adapter._convert_condition("type = 'login'") == {"type": "login"}


class TestVULN23:
    def test_attack_payload_is_blocked(self) -> None:
        adapter = NoSQLAdapter(db_instance="redis")
        with pytest.raises(QueryError):
            adapter._cast_value("$gt")

    def test_legitimate_use_still_works(self) -> None:
        adapter = NoSQLAdapter(db_instance="redis")
        assert adapter._cast_value("'hello'") == "hello"


class TestVULN24:
    def test_attack_payload_is_blocked(self) -> None:
        assert SensitiveFieldProtector._looks_like_bcrypt_hash(b"$2b$12$fakebutlookslikeahash") is False

    def test_legitimate_use_still_works(self) -> None:
        hashed = bcrypt.hashpw(b"secret", bcrypt.gensalt()).decode("utf-8").encode("utf-8")
        assert SensitiveFieldProtector._looks_like_bcrypt_hash(hashed) is True


class TestVULN25:
    def test_attack_payload_is_blocked(self, tmp_path) -> None:
        db = _sqlite_db(tmp_path)
        with pytest.raises(QueryError):
            db.create("security_logs", {"event_type": "fake"})

    def test_legitimate_use_still_works(self, tmp_path) -> None:
        db = _sqlite_db(tmp_path)
        result = db.create("users", {"id": 1, "name": "alice"})
        assert result["rows_affected"] == 1


class TestVULN26:
    def test_attack_payload_is_blocked(self, tmp_path) -> None:
        db = _sqlite_db(
            tmp_path,
            rate_limit_enabled=True,
            rate_limit_max_requests=1,
            rate_limit_window_seconds=60,
            caller_id="user-a",
        )
        db._enforce_rate_limit("find", entity="users", caller_id="user-a")
        with pytest.raises(QueryError):
            db._enforce_rate_limit("find", entity="users", caller_id="user-a")

    def test_legitimate_use_still_works(self, tmp_path) -> None:
        db = _sqlite_db(
            tmp_path,
            rate_limit_enabled=True,
            rate_limit_max_requests=1,
            rate_limit_window_seconds=60,
            caller_id="user-a",
        )
        db._enforce_rate_limit("find", entity="users", caller_id="user-a")
        db._enforce_rate_limit("find", entity="users", caller_id="user-b")


class TestVULN27:
    def test_attack_payload_is_blocked(self, tmp_path) -> None:
        db = _sqlite_db(tmp_path)
        with pytest.raises(QueryError):
            db.find_page("users", page=2_147_483_647, page_size=1000)

    def test_legitimate_use_still_works(self, tmp_path) -> None:
        db = _sqlite_db(tmp_path)
        db.create("users", {"id": 1, "name": "alice"})
        page = db.find_page("users", page=1, page_size=10)
        assert page["total"] == 1


class TestVULN28:
    def test_attack_payload_is_blocked(self) -> None:
        value = UDOM._to_uql_value("a\x00b\nc'd")
        assert "\x00" not in value
        assert "\n" not in value
        assert "\\'" not in value

    def test_legitimate_use_still_works(self) -> None:
        assert UDOM._to_uql_value("O'Reilly") == "'O''Reilly'"
