"""Unit tests for Airflow DAGs."""

import logging

import pytest
from airflow.hooks.base import BaseHook
from airflow.models import DagBag, Variable

# Configure logger
logger = logging.getLogger(__name__)


class TestDAGStructure:
    """Test DAG structure and configuration."""

    @pytest.fixture(scope="class")
    def dag_bag(self) -> DagBag:
        """Load all DAGs from the dags folder."""
        return DagBag(dag_folder="dags/", include_examples=False)

    def test_no_import_errors(self, dag_bag: DagBag) -> None:
        """Test that all DAGs load without import errors."""
        assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"

    def test_iceberg_create_products_dag_exists(self, dag_bag: DagBag) -> None:
        """Test that iceberg_create_products_dag exists."""
        assert "iceberg_create_products_dag" in dag_bag.dags
        dag = dag_bag.dags["iceberg_create_products_dag"]
        assert dag is not None
        assert len(dag.tasks) > 0

        # Verify tasks exist
        task_ids = [task.task_id for task in dag.tasks]
        assert "create_iceberg_table_from_csv" in task_ids
        assert "log_results" in task_ids

    def test_spark_iceberg_dag_exists(self, dag_bag: DagBag) -> None:
        """Test that spark_iceberg_dag exists."""
        assert "spark_iceberg_dag" in dag_bag.dags
        dag = dag_bag.dags["spark_iceberg_dag"]
        assert dag is not None
        assert len(dag.tasks) > 0

        # Verify tasks exist
        task_ids = [task.task_id for task in dag.tasks]
        assert "create_users_table_from_csv" in task_ids
        assert "log_table_stats" in task_ids

    def test_dag_tags(self, dag_bag: DagBag) -> None:
        """Test that DAGs have appropriate tags."""
        iceberg_create_dag = dag_bag.dags["iceberg_create_products_dag"]
        assert "iceberg" in iceberg_create_dag.tags
        assert "spark" in iceberg_create_dag.tags
        assert "etl" in iceberg_create_dag.tags
        assert "products" in iceberg_create_dag.tags

        spark_dag = dag_bag.dags["spark_iceberg_dag"]
        assert "spark" in spark_dag.tags
        assert "iceberg" in spark_dag.tags
        assert "users" in spark_dag.tags

    def test_dag_retries_set(self, dag_bag: DagBag) -> None:
        """Test that DAGs have retry configuration."""
        for dag_id, dag in dag_bag.dags.items():
            assert dag.default_args.get("retries") is not None, f"{dag_id} missing retries"

    def test_dag_schedule(self, dag_bag: DagBag) -> None:
        """Test that DAGs have proper schedule configuration."""
        iceberg_create_dag = dag_bag.dags["iceberg_create_products_dag"]
        # Airflow 3 converts @daily to cron format
        assert iceberg_create_dag.timetable.summary == "0 0 * * *"  # @daily in cron format

        spark_dag = dag_bag.dags["spark_iceberg_dag"]
        assert spark_dag.timetable.summary == "0 0 * * *"  # @daily in cron format

    def test_dag_catchup_disabled(self, dag_bag: DagBag) -> None:
        """Test that catchup is disabled for all DAGs."""
        for dag_id, dag in dag_bag.dags.items():
            assert dag.catchup is False, f"{dag_id} has catchup enabled"

    def test_daily_csv_to_iceberg_dag_exists(self, dag_bag: DagBag) -> None:
        """Test that daily_csv_to_iceberg DAG exists and is properly configured."""
        assert "daily_csv_to_iceberg" in dag_bag.dags
        dag = dag_bag.dags["daily_csv_to_iceberg"]

        # Verify DAG properties
        assert dag is not None
        assert len(dag.tasks) > 0

        # Verify it's scheduled daily
        assert dag.timetable.summary == "0 0 * * *"  # @daily

        # Verify tags
        assert "spark" in dag.tags
        assert "iceberg" in dag.tags
        assert "daily" in dag.tags
        assert "etl" in dag.tags

        # Verify task exists
        task_ids = [task.task_id for task in dag.tasks]
        assert "process_csv_to_iceberg" in task_ids


class TestAirflowConfig:
    """Test Airflow configuration and variables."""

    def test_airflow_variables_loaded(self) -> None:
        """Test that Airflow variables from config are loaded."""
        # Test that variables from test_variables.json are available
        csv_path = Variable.get("csv_input_path")
        assert csv_path == "tests/resources/sample_employees.csv"

        warehouse_path = Variable.get("warehouse_path")
        assert warehouse_path == "warehouse"

        catalog = Variable.get("iceberg_catalog")
        assert catalog == "local"

        namespace = Variable.get("iceberg_namespace")
        assert namespace == "default"

        batch_size = Variable.get("processing_batch_size")
        assert int(batch_size) == 1000

        spark_master = Variable.get("spark_master")
        assert spark_master == "local[*]"

        storage_conn_id = Variable.get("storage_connection_id")
        assert storage_conn_id == "iceberg_storage"

    def test_airflow_connections_loaded(self) -> None:
        """Test that Airflow connections from config are loaded."""
        # Test that local filesystem connection exists
        conn = BaseHook.get_connection("iceberg_storage")
        assert conn is not None
        assert conn.conn_type == "fs"
        assert conn.conn_id == "iceberg_storage"

        # Test MinIO connection exists (for reference)
        minio_conn = BaseHook.get_connection("minio_s3")
        assert minio_conn is not None
        assert minio_conn.conn_type == "s3"
        assert minio_conn.host == "localhost"
        assert minio_conn.port == 9000
