import re


class UQLParser:
    """Basic UQL Parser that converts UQL text into structured Python dict"""

    IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

    def parse(self, uql_query):
        uql_query = uql_query.strip()

        if uql_query.upper().startswith("FIND"):
            return self._parse_find(uql_query)

        elif uql_query.upper().startswith("CREATE"):
            return self._parse_create(uql_query)

        elif uql_query.upper().startswith("DELETE"):
            return self._parse_delete(uql_query)

        elif uql_query.upper().startswith("UPDATE"):
            return self._parse_update(uql_query)

        else:
            return {"error": "Invalid or unsupported UQL command"}

    def _parse_find(self, query):
        match = re.match(r"FIND\s+(\w+)(?:\s+WHERE\s+(.+))?", query, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid UQL syntax: {query!r}")
        return {
            "action": "FIND",
            "entity": match.group(1),
            "condition": match.group(2) if match.group(2) else None,
        }

    def _parse_create(self, query):
        match = re.match(r"CREATE\s+(\w+)\s*\{(.+)\}", query, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid UQL syntax: {query!r}")
        fields = match.group(2)
        field_data = self._parse_key_value_pairs(fields)

        return {
            "action": "CREATE",
            "entity": match.group(1),
            "fields": field_data,
        }

    def _parse_delete(self, query):
        match = re.match(r"DELETE\s+(\w+)(?:\s+WHERE\s+(.+))?", query, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid UQL syntax: {query!r}")
        return {
            "action": "DELETE",
            "entity": match.group(1),
            "condition": match.group(2) if match.group(2) else None,
        }

    def _parse_update(self, query):
        match = re.match(r"UPDATE\s+(\w+)\s+SET\s+(.+)\s+WHERE\s+(.+)", query, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid UQL syntax: {query!r}")
        fields = self._parse_key_value_pairs(match.group(2))
        return {
            "action": "UPDATE",
            "entity": match.group(1),
            "fields": fields,
            "condition": match.group(3),
        }

    def _parse_key_value_pairs(self, text):
        data = {}
        pairs = text.split(",")
        for pair in pairs:
            key, val = pair.split(":", 1)
            key = key.strip()
            if not self.IDENTIFIER_RE.fullmatch(key):
                raise ValueError(f"Invalid field name in UQL: {key!r}")
            data[key] = self._cast_value(val.strip())
        return data

    def _cast_value(self, val):
        if val.lower() == "true":
            return True
        if val.lower() == "false":
            return False
        if val.isdigit():
            return int(val)
        stripped = val.strip('"').strip("'")
        if stripped.startswith("$"):
            raise ValueError(f"Mongo operator expressions are not allowed: {stripped!r}")
        return stripped
