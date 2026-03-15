# SQL Migration Baseline (Alembic)

This folder provides the initial migration baseline for SQL backends.

## Setup

1. Install dev dependencies:

```bash
pip install .[dev]
```

2. Export database URL:

```bash
# PowerShell
$env:DATABASE_URL="sqlite:///test.db"
```

3. Create a revision:

```bash
alembic -c migrations/sql/alembic.ini revision -m "init"
```

4. Apply migrations:

```bash
alembic -c migrations/sql/alembic.ini upgrade head
```

## Notes

- `DATABASE_URL` is required for migration execution.
- Current UDOM SQL path supports dynamic table creation. This baseline exists to
  transition toward explicit schema-managed production deployments.
