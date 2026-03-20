"""Adapter interface and shared SQL adapter utilities."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Mapping

from .exceptions import QueryError


class BaseAdapter(ABC):
    """Common adapter contract for UDOM routing."""

    @abstractmethod
    def run_native(self, query: str, params: Mapping[str, Any] | None = None) -> Any:
        """Execute a native backend query."""

    @abstractmethod
    def convert_uql(self, uql_query: str) -> Any:
        """Convert UQL to backend-native query language."""

    @abstractmethod
    def create(self, entity: str, data: Mapping[str, Any]) -> Any:
        """Create a new record/document for the given entity."""

    @abstractmethod
    def create_many(self, entity: str, rows: list[Mapping[str, Any]]) -> Any:
        """Batch-create records/documents."""

    @abstractmethod
    def find(
        self,
        entity: str,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> Any:
        """Find records/documents from an entity."""

    @abstractmethod
    def delete(self, entity: str, where: Mapping[str, Any] | str) -> Any:
        """Delete records/documents from an entity."""

    @abstractmethod
    def update(self, entity: str, data: Mapping[str, Any], where: Mapping[str, Any] | str) -> Any:
        """Update records/documents for an entity."""

    @abstractmethod
    def count(self, entity: str, where: Mapping[str, Any] | str | None = None) -> int:
        """Count records/documents for an entity."""

    def aggregate(
        self,
        entity: str,
        *,
        group_by: str | list[str] | tuple[str, ...] | None = None,
        metrics: Mapping[str, Any] | None = None,
        where: Mapping[str, Any] | str | None = None,
        having: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
        pipeline: list[Mapping[str, Any]] | None = None,
    ) -> Any:
        """Aggregate records/documents for an entity."""
        raise QueryError("aggregate is not supported by this adapter")

    def ping(self) -> Any:
        """Optional health check for adapter connection."""
        raise NotImplementedError

    def close(self) -> None:
        """Optional cleanup hook for adapter resources."""
        return None

    def ensure_indexes(self, entity: str, indexes: list[Mapping[str, Any]]) -> Any:
        """Optional index creation hook."""
        raise NotImplementedError

    def create_view(self, name: str, select_query: str, *, replace: bool = False) -> Any:
        raise QueryError("views are not supported by this adapter")

    def drop_view(self, name: str, *, if_exists: bool = True) -> Any:
        raise QueryError("views are not supported by this adapter")

    def create_procedure(self, name: str, definition: str, *, replace: bool = False) -> Any:
        raise QueryError("stored procedures are not supported by this adapter")

    def drop_procedure(self, name: str, *, if_exists: bool = True) -> Any:
        raise QueryError("stored procedures are not supported by this adapter")

    def call_procedure(self, name: str, params: list[Any] | tuple[Any, ...] | None = None) -> Any:
        raise QueryError("stored procedures are not supported by this adapter")

    def create_function(self, name: str, definition: str, *, replace: bool = False) -> Any:
        raise QueryError("functions are not supported by this adapter")

    def drop_function(self, name: str, *, if_exists: bool = True) -> Any:
        raise QueryError("functions are not supported by this adapter")

    def call_function(self, name: str, params: list[Any] | tuple[Any, ...] | None = None) -> Any:
        raise QueryError("functions are not supported by this adapter")

    def create_event(
        self,
        name: str,
        schedule: str,
        body: str,
        *,
        replace: bool = False,
        preserve: bool = True,
        enable: bool = True,
    ) -> Any:
        raise QueryError("events are not supported by this adapter")

    def drop_event(self, name: str, *, if_exists: bool = True) -> Any:
        raise QueryError("events are not supported by this adapter")
