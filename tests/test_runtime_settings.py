from __future__ import annotations

from DBDuck.core.settings import load_runtime_settings


def test_runtime_settings_defaults() -> None:
    settings = load_runtime_settings()
    assert settings.sql_pool_size == 5
    assert settings.sql_pool_pre_ping is True
    assert settings.allow_unsafe_where_strings is False
    assert settings.hash_sensitive_fields is True
    assert settings.security_audit_enabled is True
    assert settings.rate_limit_enabled is False


def test_runtime_settings_override_values() -> None:
    settings = load_runtime_settings(
        pool_size=12,
        pool_pre_ping=False,
        allow_unsafe_where_strings=True,
        hash_sensitive_fields=False,
        security_audit_enabled=False,
        rate_limit_enabled=True,
        rate_limit_max_requests=5,
        rate_limit_window_seconds=30,
    )
    assert settings.sql_pool_size == 12
    assert settings.sql_pool_pre_ping is False
    assert settings.allow_unsafe_where_strings is True
    assert settings.hash_sensitive_fields is False
    assert settings.security_audit_enabled is False
    assert settings.rate_limit_enabled is True
    assert settings.rate_limit_max_requests == 5
    assert settings.rate_limit_window_seconds == 30
