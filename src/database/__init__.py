"""
Database package for SQLite integration.
Provides connection pooling, schema initialization, high-level CRUD operations,
analytical queries, and PySpark streaming sinks.
"""

from src.database.config import DatabaseConfig
from src.database.connection_pool import SQLiteConnectionPool
from src.database.operations import DatabaseOperations
from src.database.schema import create_schema

__all__ = [
    "DatabaseConfig",
    "SQLiteConnectionPool",
    "DatabaseOperations",
    "create_schema",
]
