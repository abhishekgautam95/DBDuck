from __future__ import annotations

import os
from dataclasses import dataclass

from DBDuck import UDOM
from DBDuck.core.exceptions import DatabaseError
from DBDuck.udom.models.umodel import UModel


@dataclass(frozen=True)
class AppConfig:
    db_type: str
    db_instance: str
    db_url: str
    log_level: str

    @staticmethod
    def from_env() -> "AppConfig":
        db_type = os.getenv("APP_DB_TYPE", "sql").strip().lower()
        db_instance = os.getenv("APP_DB_INSTANCE", "sqlite").strip().lower()
        default_url = "sqlite:///dbduck_app.db"
        db_url = os.getenv("APP_DB_URL", default_url).strip()
        log_level = os.getenv("APP_LOG_LEVEL", "INFO").strip().upper()
        return AppConfig(db_type=db_type, db_instance=db_instance, db_url=db_url, log_level=log_level)


class Order(UModel):
    __entity__ = "Orders"
    __strict__ = True
    order_id: int
    customer: str
    paid: bool


class Event(UModel):
    __collection__ = "events"
    __strict__ = True
    __indexes__ = [
        {
            "fields": [{"name": "type", "order": "asc"}, {"name": "user", "order": "asc"}],
            "options": {"name": "idx_type_user"},
        }
    ]
    type: str
    user: str
    ok: bool


def _safe_seed_cleanup_sql() -> None:
    for order_id in (1001, 1002, 1003):
        try:
            existing = Order.find(where={"order_id": order_id}, limit=1)
            if existing:
                existing[0].delete(where={"order_id": order_id})
        except DatabaseError:
            # First run may not have the table yet.
            pass


def run_sql_workflow(db: UDOM) -> None:
    Order.bind(db)
    _safe_seed_cleanup_sql()

    print(
        "seed:",
        Order.bulk_create(
            [
                Order(order_id=1001, customer="A", paid=True),
                {"order_id": 1002, "customer": "B", "paid": False},
                {"order_id": 1003, "customer": "C", "paid": True},
            ]
        ),
    )

    with db.transaction():
        print("update_in_tx:", Order(order_id=1002, customer="B", paid=False).update(where={"order_id": 1002}, data={"paid": True}))

    print("find_paid:", [m.to_dict() for m in Order.find(where={"paid": True}, order_by="order_id ASC", limit=10)])
    page = Order.find_page(page=1, page_size=2, where={"paid": True}, order_by="order_id ASC")
    page["items"] = [m.to_dict() for m in page["items"]]
    print("page:", page)
    print("count:", Order.count(where={"paid": True}))
    print(
        "aggregate:",
        Order.aggregate(
            group_by="paid",
            metrics={"total_orders": "count(*)"},
            order_by="paid DESC",
        ),
    )

    victim = Order.find_one(where={"order_id": 1003})
    print("delete:", victim.delete(where={"order_id": 1003}) if victim else {"rows_affected": 0})
    print("uquery_sql:", db.uquery("FIND Orders WHERE paid = true ORDER BY order_id DESC LIMIT 2"))
    print("native:", db.execute('SELECT * FROM "Orders"'))


def _safe_seed_cleanup_mongo() -> None:
    for event_type, user in (("login", "alice"), ("purchase", "bob"), ("logout", "alice")):
        try:
            existing = Event.find(where={"type": event_type, "user": user}, limit=1)
            if existing:
                existing[0].delete(where={"type": event_type, "user": user})
        except DatabaseError:
            pass


def run_mongo_workflow(db: UDOM) -> None:
    Event.bind(db)
    _safe_seed_cleanup_mongo()
    print(
        "seed:",
        Event.bulk_create(
            [
                Event(type="login", user="alice", ok=True),
                {"type": "purchase", "user": "bob", "ok": False},
                {"type": "logout", "user": "alice", "ok": True},
            ]
        ),
    )
    print("indexes:", Event.ensure_indexes())

    with db.transaction():
        print("update_in_tx:", Event(type="purchase", user="bob", ok=False).update(where={"user": "bob"}, data={"ok": True}))

    print("find_ok:", [m.to_dict() for m in Event.find(where={"ok": True}, limit=10)])
    page = Event.find_page(page=1, page_size=2, where={"ok": True})
    page["items"] = [m.to_dict() for m in page["items"]]
    print("page:", page)
    print("count:", Event.count(where={"ok": True}))
    print(
        "aggregate:",
        Event.aggregate(
            group_by="type",
            metrics={"total": "count(*)"},
            order_by="total DESC",
        ),
    )
    victim = Event.find_one(where={"type": "logout", "user": "alice"})
    print("delete:", victim.delete(where={"type": "logout", "user": "alice"}) if victim else {"rows_affected": 0})
    print("native_ping:", db.execute("ping"))


def main() -> int:
    cfg = AppConfig.from_env()
    db: UDOM | None = None
    try:
        db = UDOM(
            db_type=cfg.db_type,
            db_instance=cfg.db_instance,
            url=cfg.db_url,
            log_level=cfg.log_level,
            allow_unsafe_where_strings=False,
        )
        print("config:", cfg)
        print("health:", db.ping())

        if cfg.db_type == "sql":
            run_sql_workflow(db)
        elif cfg.db_type == "nosql" and cfg.db_instance in {"mongodb", "mongo"}:
            run_mongo_workflow(db)
        else:
            raise ValueError(
                f"Unsupported app mode: db_type={cfg.db_type!r}, db_instance={cfg.db_instance!r}. "
                "Use SQL or MongoDB for this example."
            )

        print("status: success")
        return 0
    except (DatabaseError, ValueError) as exc:
        print(f"status: failed error={exc}")
        return 1
    finally:
        if db is not None:
            db.close()


if __name__ == "__main__":
    raise SystemExit(main())
