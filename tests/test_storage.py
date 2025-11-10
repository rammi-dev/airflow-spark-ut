"""Unit tests for storage utilities."""

import logging
from pathlib import Path

from airflow.hooks.base import BaseHook

from dags.utils.storage import StorageConfig, create_spark_session_with_storage

# Configure logger
logger = logging.getLogger(__name__)


class TestStorageConfig:
    """Test storage configuration utilities."""

    def test_local_fs_connection(self) -> None:
        """Test local filesystem connection detection and configuration."""
        conn = BaseHook.get_connection("iceberg_storage")
        storage = StorageConfig(conn, project_root=Path.cwd())

        # Verify connection type detection
        assert storage.is_local_fs()
        assert not storage.is_s3()

        # Verify catalog type
        assert storage.get_catalog_type() == "hadoop"

        # Verify warehouse path
        warehouse = storage.get_warehouse_path()
        assert "warehouse" in warehouse
        assert Path(warehouse).is_absolute()

        # Verify storage info
        info = storage.get_storage_info()
        assert info["connection_id"] == "iceberg_storage"
        assert info["connection_type"] == "fs"
        assert info["is_local"] is True
        assert info["is_s3"] is False

    def test_s3_connection(self) -> None:
        """Test S3/MinIO connection detection and configuration."""
        conn = BaseHook.get_connection("minio_s3")
        storage = StorageConfig(conn, project_root=Path.cwd())

        # Verify connection type detection
        assert not storage.is_local_fs()
        assert storage.is_s3()

        # Verify warehouse path
        warehouse = storage.get_warehouse_path()
        assert warehouse.startswith("s3a://")
        assert "iceberg-warehouse" in warehouse

        # Verify storage info
        info = storage.get_storage_info()
        assert info["connection_id"] == "minio_s3"
        assert info["connection_type"] == "s3"
        assert info["is_local"] is False
        assert info["is_s3"] is True

    def test_spark_config_local_fs(self) -> None:
        """Test Spark configuration for local filesystem."""
        conn = BaseHook.get_connection("iceberg_storage")
        storage = StorageConfig(conn, project_root=Path.cwd())

        config = storage.get_spark_config(catalog_name="test_catalog")

        # Verify Iceberg packages
        assert "iceberg-spark-runtime" in config["spark.jars.packages"]

        # Verify catalog configuration
        assert config["spark.sql.catalog.test_catalog"] == "org.apache.iceberg.spark.SparkCatalog"
        assert config["spark.sql.catalog.test_catalog.type"] == "hadoop"
        assert "warehouse" in config["spark.sql.catalog.test_catalog.warehouse"]

        # Verify no S3 config for local
        assert "spark.hadoop.fs.s3a.endpoint" not in config

    def test_spark_config_s3(self) -> None:
        """Test Spark configuration for S3/MinIO with timeout settings."""
        conn = BaseHook.get_connection("minio_s3")
        storage = StorageConfig(conn, project_root=Path.cwd())

        config = storage.get_spark_config(catalog_name="test_catalog")

        # Verify Iceberg packages
        assert "iceberg-spark-runtime" in config["spark.jars.packages"]

        # Verify S3 packages added
        assert "hadoop-aws" in config["spark.jars.packages"]
        assert "aws-java-sdk-bundle" in config["spark.jars.packages"]

        # Verify S3 configuration
        assert "spark.hadoop.fs.s3a.endpoint" in config
        assert "localhost:9000" in config["spark.hadoop.fs.s3a.endpoint"]
        assert config["spark.hadoop.fs.s3a.access.key"] == "minioadmin"
        assert config["spark.hadoop.fs.s3a.secret.key"] == "minioadmin"
        assert config["spark.hadoop.fs.s3a.path.style.access"] == "true"

        # Verify timeout configurations from config
        assert "spark.hadoop.fs.s3a.connection.establish.timeout" in config
        assert (
            config["spark.hadoop.fs.s3a.connection.establish.timeout"] == "10000"
        )  # 10 seconds in ms
        assert "spark.hadoop.fs.s3a.connection.timeout" in config
        assert config["spark.hadoop.fs.s3a.connection.timeout"] == "30000"  # 30 seconds in ms

        # Verify warehouse uses s3a://
        assert config["spark.sql.catalog.test_catalog.warehouse"].startswith("s3a://")

        # Verify storage info includes timeout values
        info = storage.get_storage_info()
        assert info["s3_connection_establish_timeout"] == 10
        assert info["s3_connection_timeout"] == 30

    def test_rest_catalog_config(self) -> None:
        """Test REST catalog configuration with S3 storage."""
        # Create a mock connection for REST catalog
        from airflow.models import Connection

        rest_conn = Connection(
            conn_id="test_rest",
            conn_type="s3",
            host="s3.amazonaws.com",
            login="test-key",
            password="test-secret",
            extra='{"bucket": "test-warehouse", "catalog_type": "rest", "rest_uri": "https://catalog.example.com/v1", "rest_credential": "test-token"}',
        )

        storage = StorageConfig(rest_conn, project_root=Path.cwd())

        # Verify catalog type
        assert storage.get_catalog_type() == "rest"

        config = storage.get_spark_config(catalog_name="prod")

        # Verify REST catalog configuration
        assert config["spark.sql.catalog.prod.type"] == "rest"
        assert config["spark.sql.catalog.prod.uri"] == "https://catalog.example.com/v1"
        assert config["spark.sql.catalog.prod.credential"] == "test-token"

        # Verify storage info masks credentials
        info = storage.get_storage_info()
        assert info["rest_uri"] == "https://catalog.example.com/v1"
        assert info["rest_credential"] == "***"

    def test_rest_catalog_without_uri_raises_error(self) -> None:
        """Test that REST catalog without rest_uri raises an error."""
        from airflow.models import Connection

        rest_conn = Connection(
            conn_id="test_rest_invalid",
            conn_type="s3",
            extra='{"bucket": "test-warehouse", "catalog_type": "rest"}',
        )

        storage = StorageConfig(rest_conn, project_root=Path.cwd())

        # Should raise ValueError when rest_uri is missing
        try:
            storage.get_spark_config(catalog_name="prod")
            raise AssertionError("Expected ValueError for missing rest_uri")
        except ValueError as e:
            assert "rest_uri" in str(e)


class TestSparkSessionCreation:
    """Test Spark session creation with different storage backends."""

    def test_create_spark_with_local_storage(self) -> None:
        """Test creating Spark session with local filesystem storage."""
        spark, storage = create_spark_session_with_storage(
            app_name="test_local",
            connection_id="iceberg_storage",
            catalog_name="local",
            master="local[1]",
            project_root=Path.cwd(),
        )

        try:
            # Verify Spark session created
            assert spark is not None
            assert spark.sparkContext.appName == "test_local"

            # Verify storage config
            assert storage.is_local_fs()

            # Verify Iceberg extensions loaded
            spark_config = spark.sparkContext.getConf().getAll()
            config_dict = dict(spark_config)

            # Check Iceberg catalog is configured
            assert "spark.sql.catalog.local" in config_dict
            assert config_dict["spark.sql.catalog.local"] == "org.apache.iceberg.spark.SparkCatalog"

            # Verify warehouse path is set
            assert "spark.sql.catalog.local.warehouse" in config_dict
            assert "warehouse" in config_dict["spark.sql.catalog.local.warehouse"]

        finally:
            spark.stop()
