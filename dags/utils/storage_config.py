"""Storage utilities for handling different storage backends (local, S3/MinIO)."""

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from pyspark.sql import SparkSession

# Only import Connection for type hints, not at runtime
if TYPE_CHECKING:
    from airflow.models import Connection


class SimpleConnection:
    """Lightweight connection object without SQLAlchemy dependencies.
    
    Used in virtualenv contexts where we can't use Airflow ORM models.
    """
    def __init__(self, conn_dict: dict[str, Any]):
        self.conn_id = conn_dict["conn_id"]  # Fail fast if missing
        self.conn_type = conn_dict["conn_type"]  # Fail fast if missing
        self.host = conn_dict.get("host")  # Optional
        self.schema = conn_dict.get("schema")  # Optional
        self.login = conn_dict.get("login")  # Optional
        self.password = conn_dict.get("password")  # Optional
        self.port = conn_dict.get("port")  # Optional
        self.extra = conn_dict.get("extra")  # Optional


class StorageConfig:
    """Configuration for storage backend based on Airflow Connection."""

    def __init__(self, connection: "Connection", project_root: Path | None = None):
        """Initialize storage configuration from Airflow connection.

        Args:
            connection: Airflow connection object
            project_root: Project root path (for local filesystem)
        """
        self.connection = connection
        self.conn_type = connection.conn_type
        self.project_root = project_root or Path.cwd()

        # Parse extra config
        self.extra = json.loads(connection.extra) if connection.extra else {}

    @classmethod
    def from_dict(cls, connection_dict: dict[str, Any], project_root: Path | None = None):
        """Create StorageConfig from a connection dictionary.

        This avoids importing Airflow ORM models which can fail in virtualenv.

        Args:
            connection_dict: Connection details as dictionary
            project_root: Project root path (for local filesystem)

        Returns:
            StorageConfig instance
        """
        # Create a simple connection object that mimics the Airflow Connection API
        simple_conn = SimpleConnection(connection_dict)
        
        # Create instance without calling __init__
        instance = cls.__new__(cls)
        instance.connection = simple_conn
        instance.conn_type = connection_dict["conn_type"]  # Fail fast if missing
        instance.project_root = project_root or Path.cwd()
        instance.extra = json.loads(connection_dict.get("extra", "{}")) if connection_dict.get("extra") else {}
        return instance

    def is_local_fs(self) -> bool:
        """Check if connection is for local filesystem."""
        return self.conn_type in ["fs", "file", "local"]

    def is_s3(self) -> bool:
        """Check if connection is for S3/MinIO."""
        return self.conn_type in ["s3", "aws"]

    def get_warehouse_path(self) -> str:
        """Get warehouse path based on connection type."""
        if self.is_local_fs():
            warehouse_rel = self.extra.get("warehouse_path", "warehouse")
            return str(self.project_root / warehouse_rel)
        elif self.is_s3():
            bucket = self.extra.get("bucket", "iceberg-warehouse")
            return f"s3a://{bucket}/"
        else:
            raise ValueError(f"Unsupported connection type: {self.conn_type}")

    def get_catalog_type(self) -> str:
        """Get Iceberg catalog type."""
        return self.extra.get("catalog_type", "hadoop")

    def get_spark_config(self, catalog_name: str = "local") -> dict[str, str]:
        """Get Spark configuration for the storage backend.

        Args:
            catalog_name: Name of the Iceberg catalog

        Returns:
            Dictionary of Spark configuration properties
        """
        warehouse_path = self.get_warehouse_path()
        catalog_type = self.get_catalog_type()

        config = {
            "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{catalog_name}.type": catalog_type,
            f"spark.sql.catalog.{catalog_name}.warehouse": warehouse_path,
        }

        # Add REST catalog configuration
        if catalog_type == "rest":
            rest_uri = self.extra.get("rest_uri")
            if not rest_uri:
                raise ValueError(
                    f"REST catalog requires 'rest_uri' in connection extra for {self.connection.conn_id}"
                )

            config[f"spark.sql.catalog.{catalog_name}.uri"] = rest_uri

            # Optional: Add authentication credential (API token)
            if "rest_credential" in self.extra:
                config[f"spark.sql.catalog.{catalog_name}.credential"] = self.extra[
                    "rest_credential"
                ]

            # Optional: OAuth 2.0 token
            if "rest_oauth2_token" in self.extra:
                config[f"spark.sql.catalog.{catalog_name}.token"] = self.extra["rest_oauth2_token"]

            # Optional: OAuth 2.0 server URI
            if "rest_oauth2_server_uri" in self.extra:
                config[f"spark.sql.catalog.{catalog_name}.oauth2-server-uri"] = self.extra[
                    "rest_oauth2_server_uri"
                ]

            # Optional: Additional REST headers
            if "rest_headers" in self.extra:
                headers = self.extra["rest_headers"]
                if isinstance(headers, dict):
                    for key, value in headers.items():
                        config[f"spark.sql.catalog.{catalog_name}.header.{key}"] = value

        # Add S3-specific configuration
        if self.is_s3():
            endpoint_url = self.extra.get(
                "endpoint_url", f"http://{self.connection.host}:{self.connection.port}"
            )
            access_key = self.extra.get("aws_access_key_id", self.connection.login)
            secret_key = self.extra.get("aws_secret_access_key", self.connection.password)

            # Get timeout configurations from connection
            connection_timeout = self.extra.get("s3_connection_establish_timeout", 10)
            socket_timeout = self.extra.get("s3_connection_timeout", 30)

            s3_config = {
                "spark.hadoop.fs.s3a.endpoint": endpoint_url,
                "spark.hadoop.fs.s3a.access.key": access_key,
                "spark.hadoop.fs.s3a.secret.key": secret_key,
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                "spark.hadoop.fs.s3a.connection.establish.timeout": str(
                    connection_timeout * 1000
                ),  # milliseconds
                "spark.hadoop.fs.s3a.connection.timeout": str(
                    socket_timeout * 1000
                ),  # milliseconds
            }

            # Add AWS SDK jars for S3 support
            s3_jars = (
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
            )
            config["spark.jars.packages"] += f",{s3_jars}"

            config.update(s3_config)

        return config

    def get_storage_info(self) -> dict[str, Any]:
        """Get storage information for logging/debugging."""
        info = {
            "connection_id": self.connection.conn_id,
            "connection_type": self.conn_type,
            "warehouse_path": self.get_warehouse_path(),
            "catalog_type": self.get_catalog_type(),
            "is_local": self.is_local_fs(),
            "is_s3": self.is_s3(),
        }

        # Add timeout information
        if self.extra:
            if "connection_timeout" in self.extra:
                info["connection_timeout"] = self.extra["connection_timeout"]
            if "socket_timeout" in self.extra:
                info["socket_timeout"] = self.extra["socket_timeout"]
            if self.is_s3():
                if "s3_connection_establish_timeout" in self.extra:
                    info["s3_connection_establish_timeout"] = self.extra[
                        "s3_connection_establish_timeout"
                    ]
                if "s3_connection_timeout" in self.extra:
                    info["s3_connection_timeout"] = self.extra["s3_connection_timeout"]

            # Add REST catalog information
            if self.get_catalog_type() == "rest":
                if "rest_uri" in self.extra:
                    info["rest_uri"] = self.extra["rest_uri"]
                if "rest_credential" in self.extra:
                    info["rest_credential"] = "***"  # Masked for security
                if "rest_oauth2_token" in self.extra:
                    info["rest_oauth2_token"] = "***"  # Masked for security
                if "rest_oauth2_server_uri" in self.extra:
                    info["rest_oauth2_server_uri"] = self.extra["rest_oauth2_server_uri"]

        return info


def create_spark_session_with_storage(
    connection_id: str,
    app_name: str = "IcebergApp",
    master: str = "local[*]",
    catalog_name: str = "local",
    project_root: Path | None = None,
) -> tuple[SparkSession, StorageConfig]:
    """Create a Spark session with storage configuration from Airflow connection.

    DEPRECATED: Use create_spark_session_with_connection_dict() in virtualenv tasks.
    This function requires access to Airflow database and won't work in isolated virtualenvs.

    Args:
        connection_id: Airflow connection ID for storage backend
        app_name: Name of the Spark application
        master: Spark master URL
        catalog_name: Name of the Iceberg catalog to configure
        project_root: Project root path (for resolving relative paths)

    Returns:
        Tuple of (SparkSession, StorageConfig)
    """
    from airflow.hooks.base import BaseHook

    # Get connection
    conn = BaseHook.get_connection(connection_id)
    storage = StorageConfig(conn, project_root)

    # Build Spark session with storage-specific config
    builder = SparkSession.builder.appName(app_name).master(master)

    for key, value in storage.get_spark_config(catalog_name).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()

    return spark, storage


def create_spark_session_with_connection_dict(
    connection_dict: dict[str, Any],
    app_name: str = "IcebergApp",
    master: str = "local[*]",
    catalog_name: str = "local",
    project_root: Path | None = None,
) -> tuple[SparkSession, StorageConfig]:
    """Create a Spark session with storage configuration from connection dict.

    This function works in virtualenv tasks since it doesn't access the Airflow database.
    Use get_connection_as_dict() in DAG context to fetch connection details before
    passing to virtualenv.

    Args:
        connection_dict: Connection details as dict (from get_connection_as_dict())
        app_name: Name of the Spark application
        master: Spark master URL
        catalog_name: Name of the Iceberg catalog to configure
        project_root: Project root path (for resolving relative paths)

    Returns:
        Tuple of (SparkSession, StorageConfig)
    """
    # Create StorageConfig directly from dict without using Connection ORM object
    storage = StorageConfig.from_dict(connection_dict, project_root)

    # Build Spark session with storage-specific config
    builder = SparkSession.builder.appName(app_name).master(master)

    for key, value in storage.get_spark_config(catalog_name).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()

    return spark, storage


def get_connection_as_dict(connection_id: str) -> dict[str, Any]:
    """Get Airflow connection as a serializable dictionary.

    Use this function in DAG context (outside virtualenv) to fetch connection details
    that can be passed to virtualenv tasks.

    Args:
        connection_id: Airflow connection ID

    Returns:
        Connection details as dictionary
    """
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(connection_id)
    
    return {
        "conn_id": conn.conn_id,
        "conn_type": conn.conn_type,
        "host": conn.host,
        "schema": conn.schema,
        "login": conn.login,
        "password": conn.password,
        "port": conn.port,
        "extra": conn.extra,
    }
