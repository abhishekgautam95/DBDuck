from __future__ import annotations

import os
from uuid import uuid4

import pytest


def env_enabled(name: str) -> bool:
    return os.getenv(name, "0") == "1"


def env_value(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def unique_entity(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:10]}"


def require_env_flag(flag_name: str, *, reason: str):
    return pytest.mark.skipif(not env_enabled(flag_name), reason=reason)
