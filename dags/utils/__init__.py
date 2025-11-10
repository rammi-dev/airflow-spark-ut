"""Utilities for Airflow DAGs."""

# Re-export commonly used functions and classes
from dags.utils.storage_config import (
    SimpleConnection,
    StorageConfig,
    create_spark_session_with_connection,
    create_spark_session_with_connection_dict,
    create_spark_session_with_storage,
    get_connection_as_dict,
)

__all__ = [
    "SimpleConnection",
    "StorageConfig",
    "create_spark_session_with_connection",
    "create_spark_session_with_connection_dict",
    "create_spark_session_with_storage",
    "get_connection_as_dict",
]
