from __future__ import annotations

from DBDuck import UDOM


def main() -> None:
    # Update URL for your local/remote MongoDB.
    db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/dbduck")

    db.create_many(
        "events",
        [
            {"type": "login", "user": "alice", "ok": True},
            {"type": "purchase", "user": "bob", "ok": False},
            {"type": "logout", "user": "alice", "ok": True},
        ],
    )

    db.ensure_indexes(
        "events",
        [
            {
                "fields": [{"name": "type", "order": "asc"}, {"name": "user", "order": "asc"}],
                "options": {"name": "idx_type_user"},
            }
        ],
    )

    with db.transaction():
        db.update("events", data={"ok": True}, where={"user": "bob"})

    print("count_ok:", db.count("events", where={"ok": True}))
    print("page_1:", db.find_page("events", page=1, page_size=2, where={"ok": True}))
    print("delete_result:", db.delete("events", where={"type": "logout"}))
    print("final_rows:", db.find("events", order_by="type ASC"))


if __name__ == "__main__":
    main()
