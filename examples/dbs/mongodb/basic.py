from __future__ import annotations

from DBDuck import UDOM


def main() -> None:
    # Update URL for your local/remote MongoDB.
    db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/dbduck")

    print(db.execute("ping"))
    print(db.create("events", {"type": "login", "user": "alice", "ok": True}))
    print(db.find("events", where={"ok": True}, limit=10))


if __name__ == "__main__":
    main()
