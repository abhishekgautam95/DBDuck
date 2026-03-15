"""Core abstractions for DBDuck."""

from .adapter_router import AdapterRouter
from .exceptions import ConnectionError, DatabaseError, QueryError, TransactionError
from .mongo_connection_manager import MongoConnectionManager
from .security import SecurityAuditor, SecurityRateLimiter, SensitiveFieldProtector
from .schema import SchemaValidator
from .settings import RuntimeSettings, load_runtime_settings

__all__ = [
    "AdapterRouter",
    "MongoConnectionManager",
    "RuntimeSettings",
    "load_runtime_settings",
    "SchemaValidator",
    "SensitiveFieldProtector",
    "SecurityAuditor",
    "SecurityRateLimiter",
    "DatabaseError",
    "ConnectionError",
    "QueryError",
    "TransactionError",
]
