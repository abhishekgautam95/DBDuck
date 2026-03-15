from __future__ import annotations

from DBDuck import UDOM


def main() -> None:
    db = UDOM(db_type="sql", db_instance="sqlite", url="sqlite:///dbduck_sqlite_advanced.db")

    db.create_many(
        "Orders",
        [
            {"order_id": 201, "customer": "A", "paid": True},
            {"order_id": 202, "customer": "B", "paid": False},
            {"order_id": 203, "customer": "C", "paid": True},
        ],
    )

    with db.transaction():
        db.update("Orders", data={"paid": True}, where={"order_id": 202})

    print("count_paid:", db.count("Orders", where={"paid": True}))
    print("page_1:", db.find_page("Orders", page=1, page_size=2, order_by="order_id ASC"))
    print("delete_result:", db.delete("Orders", where={"order_id": 203}))
    print("final_rows:", db.find("Orders", order_by="order_id ASC"))


if __name__ == "__main__":
    main()
