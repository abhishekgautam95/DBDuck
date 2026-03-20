import re
from typing import Any, Mapping

from ...core.exceptions import QueryError
from .base_adapter import BaseAdapter


class GraphAdapter(BaseAdapter):
    """Adapter for Graph databases (Neo4j / Cypher Query Language)."""

    IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

    def __init__(self, db_instance="neo4j", url=None, **options):
        self.db_instance = db_instance
        self.url = url
        self.options = options

    def run_native(self, query: Any, params: Mapping[str, Any] | None = None):
        if not isinstance(query, str) or not query.strip():
            raise QueryError("Graph query must be a non-empty string")
        return {"query": query, "params": dict(params or {})}

    def convert_uql(self, uql_query: str):
        uql_query = uql_query.strip()

        if uql_query.upper().startswith("FIND"):
            label, condition = self._extract_label_and_condition(uql_query)
            where_clause, params = self._convert_conditions(condition)
            query = f"MATCH (n:{label})"
            if where_clause:
                query += f" WHERE {where_clause}"
            query += " RETURN n;"
            return query, params

        if uql_query.upper().startswith("CREATE"):
            label, properties = self._extract_label_and_body(uql_query)
            props, params = self._convert_create_properties(properties)
            return f"CREATE (n:{label} {props}) RETURN n;", params

        if uql_query.upper().startswith("DELETE"):
            label, condition = self._extract_label_and_condition(uql_query)
            where_clause, params = self._convert_conditions(condition)
            query = f"MATCH (n:{label})"
            if where_clause:
                query += f" WHERE {where_clause}"
            query += " DELETE n;"
            return query, params

        raise QueryError("Unsupported or invalid UQL syntax")

    def create(self, entity: str, data: Mapping[str, Any]) -> Any:
        label = self._validate_identifier(entity, kind="entity")
        props, params = self._build_property_map(data)
        return self.run_native(f"CREATE (n:{label} {props}) RETURN n;", params)

    def create_many(self, entity: str, rows: list[Mapping[str, Any]]) -> Any:
        return [self.create(entity, row) for row in rows]

    def find(
        self,
        entity: str,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> Any:
        if order_by is not None or limit is not None:
            raise QueryError("GraphAdapter.find currently supports only entity + where")
        label = self._validate_identifier(entity, kind="entity")
        where_clause, params = self._normalize_where(where)
        query = f"MATCH (n:{label})"
        if where_clause:
            query += f" WHERE {where_clause}"
        query += " RETURN n;"
        return self.run_native(query, params)

    def delete(self, entity: str, where: Mapping[str, Any] | str) -> Any:
        label = self._validate_identifier(entity, kind="entity")
        where_clause, params = self._normalize_where(where)
        if not where_clause:
            raise QueryError("delete requires a non-empty where condition")
        return self.run_native(f"MATCH (n:{label}) WHERE {where_clause} DELETE n;", params)

    def update(self, entity: str, data: Mapping[str, Any], where: Mapping[str, Any] | str) -> Any:
        label = self._validate_identifier(entity, kind="entity")
        if not isinstance(data, Mapping) or not data:
            raise QueryError("update data must be non-empty")
        where_clause, where_params = self._normalize_where(where)
        if not where_clause:
            raise QueryError("update requires where")
        assignments = []
        params = dict(where_params)
        offset = len(params)
        for index, (key, value) in enumerate(data.items()):
            field_name = self._validate_identifier(str(key), kind="field")
            pname = f"set_{offset + index}"
            assignments.append(f"n.{field_name} = ${pname}")
            params[pname] = value
        query = f"MATCH (n:{label}) WHERE {where_clause} SET {', '.join(assignments)} RETURN n;"
        return self.run_native(query, params)

    def count(self, entity: str, where: Mapping[str, Any] | str | None = None) -> int:
        label = self._validate_identifier(entity, kind="entity")
        where_clause, params = self._normalize_where(where)
        query = f"MATCH (n:{label})"
        if where_clause:
            query += f" WHERE {where_clause}"
        query += " RETURN count(n) AS total;"
        self.run_native(query, params)
        return 0

    @staticmethod
    def _to_uql_value(value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    def _normalize_where(self, where: Mapping[str, Any] | str | None) -> tuple[str, dict[str, Any]]:
        if where is None:
            return "", {}
        if isinstance(where, str):
            return self._convert_conditions(where.strip())
        if isinstance(where, Mapping):
            parts = [f"{self._validate_identifier(str(k), kind='field')} = {self._to_uql_value(v)}" for k, v in where.items()]
            return self._convert_conditions(" AND ".join(parts))
        raise QueryError("where must be mapping, string, or None")

    def _extract_label_and_condition(self, uql_query):
        match = re.match(r"(FIND|DELETE)\s+(\w+)\s*(?:WHERE\s+(.+))?", uql_query, re.IGNORECASE)
        if not match:
            raise QueryError("Invalid UQL query")
        label = self._validate_identifier(match.group(2), kind="entity")
        condition = match.group(3) if match.group(3) else ""
        return label, condition

    def _extract_label_and_body(self, uql_query):
        match = re.match(r"CREATE\s+(\w+)\s*\{(.+)\}", uql_query, re.IGNORECASE)
        if not match:
            raise QueryError("Invalid CREATE UQL")
        return self._validate_identifier(match.group(1), kind="entity"), match.group(2)

    def _convert_conditions(self, condition: str):
        if not condition:
            return "", {}
        tokens = re.split(r"\s+(AND|OR)\s+", condition, flags=re.IGNORECASE)
        cypher_conditions = []
        params: dict[str, Any] = {}
        index = 0
        for token in tokens:
            part = token.strip()
            if not part:
                continue
            connector = part.upper()
            if connector in {"AND", "OR"}:
                cypher_conditions.append(connector)
                continue
            match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\s*(=|!=|>=|<=|>|<)\s*(.+)", part)
            if not match:
                raise QueryError("Unsupported graph where expression")
            key = self._validate_identifier(match.group(1).strip(), kind="field")
            operator = match.group(2)
            pname = f"w_{index}"
            index += 1
            cypher_conditions.append(f"n.{key} {operator} ${pname}")
            params[pname] = self._parse_literal_value(match.group(3).strip())
        return " ".join(cypher_conditions), params

    def _convert_create_properties(self, fields):
        data: dict[str, Any] = {}
        for pair in fields.split(","):
            if ":" not in pair:
                raise QueryError("Invalid CREATE UQL payload")
            key, val = pair.split(":", 1)
            key = self._validate_identifier(key.strip(), kind="field")
            data[key] = self._parse_literal_value(val.strip())
        return self._build_property_map(data)

    def _build_property_map(self, data: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
        if not isinstance(data, Mapping) or not data:
            raise QueryError("properties must be a non-empty mapping")
        parts = []
        params: dict[str, Any] = {}
        for index, (key, value) in enumerate(data.items()):
            field_name = self._validate_identifier(str(key), kind="field")
            pname = f"p_{index}"
            parts.append(f"{field_name}: ${pname}")
            params[pname] = value
        return "{" + ", ".join(parts) + "}", params

    @classmethod
    def _validate_identifier(cls, value: str, *, kind: str) -> str:
        if not isinstance(value, str) or not cls.IDENTIFIER_RE.fullmatch(value):
            raise QueryError(f"Invalid graph {kind}: {value!r}")
        return value

    @staticmethod
    def _parse_literal_value(raw: str) -> Any:
        value = raw.strip()
        if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
            return value[1:-1]
        lower = value.lower()
        if lower == "true":
            return True
        if lower == "false":
            return False
        if re.fullmatch(r"-?\d+", value):
            return int(value)
        if re.fullmatch(r"-?\d+(?:\.\d+)?", value):
            return float(value)
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise QueryError("String values in graph conditions must be quoted")
        return value

    def ping(self) -> Any:
        return {"ok": 1, "db_type": "graph", "db_instance": self.db_instance}
