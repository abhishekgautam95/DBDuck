# Changelog

## 0.1.0

### Added

- Adapter-driven architecture for SQL and NoSQL runtime paths.
- Thread-safe SQL connection manager and Mongo connection manager.
- SQL + Mongo transaction support with context manager patterns.
- Central schema validation for create/find/delete operations.
- Structured logging for query, transaction, and connection events.
- Custom exception hierarchy and adapter-level exception mapping.
- Batch insert support via `create_many` for SQL and Mongo.
- Health/lifecycle hooks: `ping()` and `close()`.
- GitHub Actions test workflow.
- Runtime settings module with env-driven secure defaults.
- SQL migration baseline scaffold via Alembic (`migrations/sql`).
- Security CI checks (`pip-audit`, `bandit`).
- `.env.example` for backend deployment configuration.
- Mongo retry wrapper for transient operation errors.
- Mongo index management APIs (`ensure_indexes`, model `__indexes__` support).
- Mongo integration test scaffold in `tests/integration/`.
- Production-capable backend support for `SQLite`, `MySQL`, `PostgreSQL`, `SQL Server`, and `MongoDB`.
- Automatic BCrypt hashing for sensitive fields on SQL and Mongo write paths.
- `verify_secret(...)` helper for validating plaintext secrets against stored BCrypt hashes.
- Model-level sensitive field hashing via `UModel.__sensitive_fields__`.
- Security audit log persistence to `security_logs`.
- In-memory rate limiting controls for UDOM operations.
- Native SQL and Mongo pagination support for `find_page()`.
- Real backend integration test scaffolding for `MySQL`, `PostgreSQL`, `SQL Server`, and `MongoDB`.
- Integration coverage design for:
  - CRUD roundtrip
  - transaction commit/rollback
  - native pagination
  - connection-failure mapping

### Changed

- `UDOM` now routes SQL and NoSQL CRUD directly to adapters.
- SQL where-clause handling now parameterizes parsed string expressions.
- Mongo where-clause parsing supports safe operators and order/limit handling.
- SQL and Mongo execution paths now mask raw driver/database errors from callers.
- Structured logs no longer expose raw SQL values or sensitive parameters in standard output.

### Tests

- Added/expanded pytest coverage for:
  - adapter routing
  - SQL and Mongo transactions
  - connection manager caching
  - query validation/injection protection
  - exception handler behavior
  - BCrypt hashing and secret verification
  - audit logs and rate limiting
  - model-level sensitive field behavior
  - opt-in real backend integration scaffolding
