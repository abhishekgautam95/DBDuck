"""Security helpers for payload protection, auditing, and rate limiting."""

from __future__ import annotations

import json
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Mapping

import bcrypt

from ..utils.logger import log_event, log_internal_debug


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    retry_after_seconds: float = 0.0


class SensitiveFieldProtector:
    """Apply BCrypt hashing to configured sensitive fields."""

    DEFAULT_FIELDS = {
        "password",
        "passwd",
        "pwd",
        "secret",
        "secret_key",
        "api_key",
        "access_token",
        "refresh_token",
    }

    @classmethod
    def protect_mapping(
        cls,
        payload: Mapping[str, Any],
        *,
        enabled: bool,
        rounds: int,
        field_names: set[str] | None = None,
    ) -> dict[str, Any]:
        if not enabled:
            return dict(payload)
        sensitive = {name.lower() for name in (field_names or cls.DEFAULT_FIELDS)}
        protected: dict[str, Any] = {}
        for key, value in payload.items():
            if str(key).lower() in sensitive:
                protected[key] = cls._hash_value(value, rounds=rounds)
            else:
                protected[key] = value
        return protected

    @staticmethod
    def _hash_value(value: Any, *, rounds: int) -> Any:
        if value is None:
            return None
        if isinstance(value, bytes):
            raw = value
        else:
            raw = str(value).encode("utf-8")
        if SensitiveFieldProtector._looks_like_bcrypt_hash(raw):
            return raw.decode("utf-8")
        hashed = bcrypt.hashpw(raw, bcrypt.gensalt(rounds=max(4, int(rounds))))
        return hashed.decode("utf-8")

    @staticmethod
    def _looks_like_bcrypt_hash(value: bytes) -> bool:
        return value.startswith((b"$2a$", b"$2b$", b"$2y$"))

    @classmethod
    def verify_secret(cls, plain_value: Any, stored_hash: Any) -> bool:
        """Validate a plaintext secret against a stored BCrypt hash."""
        if plain_value is None or stored_hash is None:
            return False
        plain_bytes = plain_value if isinstance(plain_value, bytes) else str(plain_value).encode("utf-8")
        hash_bytes = stored_hash if isinstance(stored_hash, bytes) else str(stored_hash).encode("utf-8")
        if not cls._looks_like_bcrypt_hash(hash_bytes):
            return False
        try:
            return bool(bcrypt.checkpw(plain_bytes, hash_bytes))
        except ValueError:
            return False


class SecurityRateLimiter:
    """Simple in-memory sliding-window limiter for UDOM operations."""

    def __init__(self, *, enabled: bool, max_requests: int, window_seconds: int) -> None:
        self.enabled = enabled and max_requests > 0 and window_seconds > 0
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._events: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def check(self, key: str) -> RateLimitDecision:
        if not self.enabled:
            return RateLimitDecision(True, 0.0)
        now = time.monotonic()
        with self._lock:
            bucket = self._events[key]
            cutoff = now - self.window_seconds
            while bucket and bucket[0] <= cutoff:
                bucket.popleft()
            if len(bucket) >= self.max_requests:
                retry_after = max(0.0, self.window_seconds - (now - bucket[0]))
                return RateLimitDecision(False, retry_after)
            bucket.append(now)
            return RateLimitDecision(True, 0.0)


class SecurityAuditor:
    """Persist security events to backend storage in a best-effort manner."""

    def __init__(
        self,
        *,
        enabled: bool,
        entity_name: str,
        redact_fields: set[str] | None = None,
    ) -> None:
        self.enabled = enabled
        self.entity_name = entity_name
        self.redact_fields = {name.lower() for name in (redact_fields or SensitiveFieldProtector.DEFAULT_FIELDS)}

    def should_skip(self, entity: str | None) -> bool:
        return not self.enabled or (entity or "").strip().lower() == self.entity_name.lower()

    def record(
        self,
        *,
        adapter: Any,
        logger: Any,
        db_type: str,
        db_instance: str,
        operation: str,
        entity: str | None,
        reason: str,
        input_data: Any,
    ) -> None:
        if self.should_skip(entity):
            return
        payload = {
            "event_type": "security.blocked",
            "operation": operation,
            "entity": entity or "-",
            "reason": reason,
            "db_type": db_type,
            "db_instance": db_instance,
            "input_snapshot": self._serialize_input(input_data),
            "created_at": int(time.time()),
        }
        try:
            adapter.create(self.entity_name, payload)
            log_event(logger, 30, "Security event recorded", event="security.audit", db=db_instance, entity=entity or "-")
        except Exception as exc:
            log_internal_debug(
                logger,
                "Failed to persist security audit log",
                event="security.audit.internal",
                db=db_instance,
                entity=entity or "-",
                exc=exc,
            )

    def _serialize_input(self, value: Any) -> str:
        try:
            redacted = self._redact(value)
            return json.dumps(redacted, separators=(",", ":"), default=str)[:4000]
        except Exception:
            return "<unserializable>"

    def _redact(self, value: Any) -> Any:
        if isinstance(value, Mapping):
            return {
                str(key): ("***REDACTED***" if str(key).lower() in self.redact_fields else self._redact(item))
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [self._redact(item) for item in value]
        if isinstance(value, tuple):
            return [self._redact(item) for item in value]
        return value
