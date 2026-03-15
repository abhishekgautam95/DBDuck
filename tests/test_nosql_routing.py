from __future__ import annotations

from DBDuck import UDOM


def test_nosql_crud_uses_direct_adapter_methods(monkeypatch) -> None:
    db = UDOM(db_type="nosql", db_instance="redis", url="redis://localhost:6379")

    def _no_convert(_uql):
        raise AssertionError("convert_uql must not be called for direct NoSQL CRUD path")

    monkeypatch.setattr(db.adapter, "convert_uql", _no_convert)

    created = db.create("events", {"ok": True, "type": "login"})
    assert created["insert"] == "events"
    assert created["document"]["ok"] is True

    found = db.find("events", where={"ok": True})
    assert found["find"] == "events"
    assert found["where"]["ok"] is True

    deleted = db.delete("events", {"ok": True})
    assert deleted["delete"] == "events"
    assert deleted["where"]["ok"] is True


def test_nosql_create_many_direct_path(monkeypatch) -> None:
    db = UDOM(db_type="nosql", db_instance="redis", url="redis://localhost:6379")

    def _no_convert(_uql):
        raise AssertionError("convert_uql must not be called for direct NoSQL CRUD path")

    monkeypatch.setattr(db.adapter, "convert_uql", _no_convert)
    result = db.create_many("events", [{"ok": True}, {"ok": False}])
    assert result["inserted_count"] == 2
