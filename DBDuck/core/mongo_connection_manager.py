"""Thread-safe MongoDB client manager."""

from __future__ import annotations

import threading
from typing import Any

from .exceptions import ConnectionError
from ..utils.logger import get_logger, log_event


class MongoConnectionManager:
    """Lazily creates and reuses MongoClient per URL."""

    _instance: "MongoConnectionManager | None" = None
    _instance_lock = threading.Lock()

    def __new__(cls) -> "MongoConnectionManager":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._clients = {}
                    cls._instance._lock = threading.RLock()
                    cls._instance._logger = get_logger()
        return cls._instance

    def get_client(
        self,
        url: str,
        *,
        max_pool_size: int = 100,
        min_pool_size: int = 0,
        connect_timeout_ms: int = 3000,
        server_selection_timeout_ms: int = 3000,
    ):
        with self._lock:
            client = self._clients.get(url)
            if client is not None:
                return client
            try:
                from pymongo import MongoClient
            except Exception as exc:
                raise ConnectionError("pymongo is required for MongoDB support") from exc
            try:
                client = MongoClient(
                    url,
                    maxPoolSize=max_pool_size,
                    minPoolSize=min_pool_size,
                    connectTimeoutMS=connect_timeout_ms,
                    serverSelectionTimeoutMS=server_selection_timeout_ms,
                )
                self._clients[url] = client
                log_event(
                    self._logger,
                    20,
                    "Mongo client created",
                    event="connection.create",
                    db="mongodb",
                )
                return client
            except Exception as exc:
                raise ConnectionError(f"Failed to create Mongo client for URL {url!r}") from exc

    def close_client(self, url: str) -> None:
        """Close one cached Mongo client."""
        with self._lock:
            client = self._clients.pop(url, None)
            if client is not None:
                client.close()

    def close_all(self) -> None:
        """Close all cached Mongo clients."""
        with self._lock:
            for client in self._clients.values():
                client.close()
            self._clients.clear()
