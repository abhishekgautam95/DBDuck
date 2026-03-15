from __future__ import annotations

from DBDuck import UDOM


def main() -> None:
    db = UDOM(db_type="sql", db_instance="sqlite", url="sqlite:///dbduck_sqlite_basic.db")
    print(db.create("Orders", {"order_id": 101, "customer": "A", "paid": True}))
    print(db.find("Orders", where={"paid": True}, limit=10))


if __name__ == "__main__":
    main()
