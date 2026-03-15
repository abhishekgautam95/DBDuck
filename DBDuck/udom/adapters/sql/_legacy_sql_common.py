"""Shared helpers for legacy SQL adapters."""

from __future__ import annotations

import re
from typing import Any, Callable


class ParameterizedSQL(str):
    """String query wrapper that carries bound parameters."""

    def __new__(cls, sql: str, params: dict[str, Any] | None = None):
        obj = super().__new__(cls, sql)
        obj.params = params or {}
        return obj


def parse_literal_value(raw: Any) -> Any:
    value = str(raw).strip()
    if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
        return value[1:-1]
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered == "null":
        return None
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if re.fullmatch(r"-?\d+(?:\.\d+)?", value):
        return float(value)
    return value


def literal_to_uql(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def parameterize_condition(
    condition: str | None,
    *,
    quote_identifier: Callable[[str], str],
    normalize_condition: Callable[[str | None], str | None] | None = None,
) -> tuple[str, dict[str, Any]]:
    normalized = normalize_condition(condition) if normalize_condition is not None else condition
    text_condition = (normalized or "").strip()
    if not text_condition:
        return "", {}
    tokens = re.split(r"\s+(AND|OR)\s+", text_condition, flags=re.IGNORECASE)
    clauses: list[str] = []
    params: dict[str, Any] = {}
    value_index = 0
    for token in tokens:
        piece = token.strip()
        if not piece:
            continue
        upper = piece.upper()
        if upper in {"AND", "OR"}:
            clauses.append(upper)
            continue
        match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\s*(=|!=|>=|<=|>|<)\s*(.+)", piece)
        if not match:
            raise ValueError("Unsupported legacy WHERE clause")
        field, op, raw = match.group(1), match.group(2), match.group(3)
        pname = f"w_{value_index}"
        value_index += 1
        clauses.append(f"{quote_identifier(field)} {op} :{pname}")
        params[pname] = parse_literal_value(raw)
    return " ".join(clauses), params
