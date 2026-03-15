from __future__ import annotations

from DBDuck import UDOM
from DBDuck.udom.models.umodel import UModel


class _FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, order_by):
        field, direction = order_by[0]
        reverse = direction == -1
        self._docs.sort(key=lambda d: d.get(field), reverse=reverse)
        return self

    def limit(self, size):
        self._docs = self._docs[:size]
        return self

    def __iter__(self):
        return iter(self._docs)


class _FakeCollection:
    def __init__(self):
        self.docs = []

    def find(self, where, session=None):
        if not where:
            return _FakeCursor(self.docs)
        out = []
        for d in self.docs:
            ok = True
            for key, val in where.items():
                if isinstance(val, dict):
                    if "$gt" in val and not (d.get(key) > val["$gt"]):
                        ok = False
                    if "$lt" in val and not (d.get(key) < val["$lt"]):
                        ok = False
                    if "$gte" in val and not (d.get(key) >= val["$gte"]):
                        ok = False
                    if "$lte" in val and not (d.get(key) <= val["$lte"]):
                        ok = False
                    if "$ne" in val and not (d.get(key) != val["$ne"]):
                        ok = False
                else:
                    if d.get(key) != val:
                        ok = False
            if ok:
                out.append(d)
        return _FakeCursor(out)

    def insert_one(self, doc, session=None):
        self.docs.append(dict(doc))
        return type("_R", (), {"inserted_id": len(self.docs)})

    def insert_many(self, docs, ordered=True, session=None):
        for d in docs:
            self.docs.append(dict(d))
        return type("_R", (), {"inserted_ids": list(range(1, len(docs) + 1))})

    def delete_many(self, where, session=None):
        before = len(self.docs)
        self.docs = [d for d in self.docs if not all(d.get(k) == v for k, v in where.items())]
        return type("_R", (), {"deleted_count": before - len(self.docs)})

    def update_many(self, where, values, session=None):
        matched = 0
        modified = 0
        for d in self.docs:
            if all(d.get(k) == v for k, v in where.items()):
                matched += 1
                for key, value in values.get("$set", {}).items():
                    d[key] = value
                modified += 1
        return type("_R", (), {"matched_count": matched, "modified_count": modified})

    def count_documents(self, where, session=None):
        return len(list(self.find(where, session=session)))

    def create_index(self, keys, **options):
        return options.get("name", "idx")


class _FakeDB:
    def __init__(self):
        self.collections = {}

    def __getitem__(self, name):
        if name not in self.collections:
            self.collections[name] = _FakeCollection()
        return self.collections[name]

    def command(self, cmd):
        return {"ok": 1 if cmd == "ping" else 0}


class _FakeSession:
    def start_transaction(self):
        return None

    def commit_transaction(self):
        return None

    def abort_transaction(self):
        return None

    def end_session(self):
        return None


class _FakeClient:
    def __init__(self):
        self.db = _FakeDB()

    def __getitem__(self, _name):
        return self.db

    def start_session(self):
        return _FakeSession()

    def list_databases(self):
        return [{"name": "udom"}]


def _install_fake_mongo(db: UDOM):
    fake_client = _FakeClient()

    def _fake_ensure():
        db.adapter._client = fake_client
        db.adapter._db = fake_client.db

    db.adapter._ensure_mongo = _fake_ensure
    return fake_client


def test_sql_update_count_find_page(tmp_path) -> None:
    db_file = tmp_path / "real_sql.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    db.create_many(
        "Orders",
        [
            {"order_id": 1, "customer": "A", "paid": False},
            {"order_id": 2, "customer": "B", "paid": True},
            {"order_id": 3, "customer": "C", "paid": True},
        ],
    )
    update_result = db.update("Orders", {"paid": True}, where={"order_id": 1})
    assert update_result["rows_affected"] == 1
    assert db.count("Orders", where={"paid": True}) == 3
    page = db.find_page("Orders", page=2, page_size=2, order_by="order_id ASC")
    assert page["total"] == 3
    assert page["page"] == 2
    assert len(page["items"]) == 1


def test_nosql_update_count_find_page_mongo_fake() -> None:
    db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/udom")
    _install_fake_mongo(db)
    db.create_many(
        "events",
        [
            {"type": "a", "ok": False},
            {"type": "b", "ok": True},
            {"type": "c", "ok": True},
        ],
    )
    update_result = db.update("events", {"ok": True}, where={"type": "a"})
    assert update_result["modified_count"] == 1
    assert db.count("events", where={"ok": True}) == 3
    page = db.find_page("events", page=1, page_size=2, order_by="type ASC")
    assert page["total"] == 3
    assert len(page["items"]) == 2


class OrderModel(UModel):
    __entity__ = "Orders"
    order_id: int
    customer: str
    paid: bool


def test_umodel_update_count_find_page(tmp_path) -> None:
    db_file = tmp_path / "real_model.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    OrderModel.bind(db)
    OrderModel.bulk_create(
        [
            {"order_id": 11, "customer": "A", "paid": False},
            {"order_id": 12, "customer": "B", "paid": True},
            {"order_id": 13, "customer": "C", "paid": True},
        ]
    )
    obj = OrderModel(order_id=11, customer="A", paid=False)
    obj.update({"paid": True}, where={"order_id": 11})
    assert OrderModel.count(where={"paid": True}) == 3
    page = OrderModel.find_page(page=1, page_size=2, order_by="order_id ASC")
    assert page["total"] == 3
    assert len(page["items"]) == 2
