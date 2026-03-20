"""Runtime settings with secure defaults for backend execution."""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from typing import Any


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


@dataclass(frozen=True)
class RuntimeSettings:
    """Runtime settings consumed by SQL/NoSQL adapters."""

    log_level: str = "INFO"
    sql_pool_size: int = 5
    sql_max_overflow: int = 10
    sql_pool_timeout: int = 30
    sql_pool_recycle: int = 1800
    sql_pool_pre_ping: bool = True
    sql_echo: bool = False
    mongo_connect_timeout_ms: int = 3000
    mongo_max_pool_size: int = 100
    mongo_min_pool_size: int = 0
    mongo_retry_attempts: int = 3
    mongo_retry_backoff_ms: int = 100
    hash_sensitive_fields: bool = True
    bcrypt_rounds: int = 12
    security_audit_enabled: bool = True
    security_audit_entity: str = "security_logs"
    rate_limit_enabled: bool = False
    rate_limit_max_requests: int = 60
    rate_limit_window_seconds: int = 60

    def as_adapter_options(self) -> dict[str, Any]:
        """Convert settings to adapter-compatible option keys."""
        return {
            "log_level": self.log_level,
            "pool_size": self.sql_pool_size,
            "max_overflow": self.sql_max_overflow,
            "pool_timeout": self.sql_pool_timeout,
            "pool_recycle": self.sql_pool_recycle,
            "pool_pre_ping": self.sql_pool_pre_ping,
            "echo": self.sql_echo,
            "connect_timeout_ms": self.mongo_connect_timeout_ms,
            "max_pool_size": self.mongo_max_pool_size,
            "min_pool_size": self.mongo_min_pool_size,
            "retry_attempts": self.mongo_retry_attempts,
            "retry_backoff_ms": self.mongo_retry_backoff_ms,
            "hash_sensitive_fields": self.hash_sensitive_fields,
            "bcrypt_rounds": self.bcrypt_rounds,
            "security_audit_enabled": self.security_audit_enabled,
            "security_audit_entity": self.security_audit_entity,
            "rate_limit_enabled": self.rate_limit_enabled,
            "rate_limit_max_requests": self.rate_limit_max_requests,
            "rate_limit_window_seconds": self.rate_limit_window_seconds,
            "_runtime_settings": asdict(self),
        }


def load_runtime_settings(**overrides: Any) -> RuntimeSettings:
    """Load settings from env with explicit override precedence."""
    env = os.environ
    resolved = RuntimeSettings(
        log_level=str(overrides.get("log_level", env.get("DBDUCK_LOG_LEVEL", "INFO"))),
        sql_pool_size=_to_int(overrides.get("pool_size", env.get("DBDUCK_SQL_POOL_SIZE")), 5),
        sql_max_overflow=_to_int(overrides.get("max_overflow", env.get("DBDUCK_SQL_MAX_OVERFLOW")), 10),
        sql_pool_timeout=_to_int(overrides.get("pool_timeout", env.get("DBDUCK_SQL_POOL_TIMEOUT")), 30),
        sql_pool_recycle=_to_int(overrides.get("pool_recycle", env.get("DBDUCK_SQL_POOL_RECYCLE")), 1800),
        sql_pool_pre_ping=_to_bool(
            overrides.get("pool_pre_ping", env.get("DBDUCK_SQL_POOL_PRE_PING")),
            default=True,
        ),
        sql_echo=_to_bool(overrides.get("echo", env.get("DBDUCK_SQL_ECHO")), default=False),
        mongo_connect_timeout_ms=_to_int(
            overrides.get("connect_timeout_ms", env.get("DBDUCK_MONGO_CONNECT_TIMEOUT_MS")), 3000
        ),
        mongo_max_pool_size=_to_int(overrides.get("max_pool_size", env.get("DBDUCK_MONGO_MAX_POOL_SIZE")), 100),
        mongo_min_pool_size=_to_int(overrides.get("min_pool_size", env.get("DBDUCK_MONGO_MIN_POOL_SIZE")), 0),
        mongo_retry_attempts=_to_int(overrides.get("retry_attempts", env.get("DBDUCK_MONGO_RETRY_ATTEMPTS")), 3),
        mongo_retry_backoff_ms=_to_int(
            overrides.get("retry_backoff_ms", env.get("DBDUCK_MONGO_RETRY_BACKOFF_MS")), 100
        ),
        hash_sensitive_fields=_to_bool(
            overrides.get("hash_sensitive_fields", env.get("DBDUCK_HASH_SENSITIVE_FIELDS")),
            default=True,
        ),
        bcrypt_rounds=_to_int(overrides.get("bcrypt_rounds", env.get("DBDUCK_BCRYPT_ROUNDS")), 12),
        security_audit_enabled=_to_bool(
            overrides.get("security_audit_enabled", env.get("DBDUCK_SECURITY_AUDIT_ENABLED")),
            default=True,
        ),
        security_audit_entity=str(
            overrides.get("security_audit_entity", env.get("DBDUCK_SECURITY_AUDIT_ENTITY", "security_logs"))
        ),
        rate_limit_enabled=_to_bool(
            overrides.get("rate_limit_enabled", env.get("DBDUCK_RATE_LIMIT_ENABLED")),
            default=False,
        ),
        rate_limit_max_requests=_to_int(
            overrides.get("rate_limit_max_requests", env.get("DBDUCK_RATE_LIMIT_MAX_REQUESTS")),
            60,
        ),
        rate_limit_window_seconds=_to_int(
            overrides.get("rate_limit_window_seconds", env.get("DBDUCK_RATE_LIMIT_WINDOW_SECONDS")),
            60,
        ),
    )
    return resolved
