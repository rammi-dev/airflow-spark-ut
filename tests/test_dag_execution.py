"""Tests for DAG task execution.

These tests execute individual DAG tasks by calling task.execute() directly,
validating the task logic including Spark transformations, Iceberg table creation,
and data validation.

NOTE: These are NOT full Airflow DAG execution tests. They bypass the Airflow
scheduler/executor and do not support XCom, task state management, retries, or
other Airflow runtime features. They test the task code itself, not the orchestration.
"""

import logging
from datetime import datetime

import pytest
from airflow.models import DagBag

# Configure logger
logger = logging.getLogger(__name__)


class TestDAGExecution:
    """Test actual execution of DAG tasks."""

    @pytest.fixture(scope="class")
    def dag_bag(self) -> DagBag:
        """Load all DAGs from the dags folder."""
        return DagBag(dag_folder="dags/", include_examples=False)

    def test_execute_iceberg_products_dag_task(self, dag_bag: DagBag) -> None:
        """Execute the create_iceberg_table_from_csv task and verify it works."""
        dag = dag_bag.dags["iceberg_create_products_dag"]
        task = dag.get_task("create_iceberg_table_from_csv")

        # Create execution context
        execution_date = datetime(2024, 11, 10)
        context = {
            "execution_date": execution_date,
            "logical_date": execution_date,
            "dag": dag,
            "task": task,
            "task_instance": None,
            "run_id": f"test_{execution_date.isoformat()}",
        }

        # Execute the task
        result = task.execute(context=context)

        # Verify the result
        assert isinstance(result, dict)
        assert "table_name" in result
        assert "total_products" in result
        assert "unique_categories" in result
        assert "storage_type" in result
        assert "warehouse_path" in result

        assert result["total_products"] == 15
        assert result["unique_categories"] >= 4

        logger.info("Task executed successfully!")
        logger.info("Table: %s", result["table_name"])
        logger.info("Products: %d", result["total_products"])
        logger.info("Categories: %d", result["unique_categories"])

    def test_execute_spark_users_dag_task(self, dag_bag: DagBag) -> None:
        """Execute the create_users_table_from_csv task and verify it works."""
        dag = dag_bag.dags["spark_iceberg_dag"]
        task = dag.get_task("create_users_table_from_csv")

        # Create execution context
        execution_date = datetime(2024, 11, 10)
        context = {
            "execution_date": execution_date,
            "logical_date": execution_date,
            "dag": dag,
            "task": task,
            "task_instance": None,
            "run_id": f"test_{execution_date.isoformat()}",
        }

        # Execute the task
        result = task.execute(context=context)

        # Verify the result
        assert isinstance(result, dict)
        assert "table_name" in result
        assert "total_users" in result
        assert "unique_departments" in result

        assert result["total_users"] == 8
        assert result["unique_departments"] >= 4

        logger.info("Task executed successfully!")
        logger.info("Table: %s", result["table_name"])
        logger.info("Users: %d", result["total_users"])
        logger.info("Departments: %d", result["unique_departments"])

    def test_execute_daily_csv_to_iceberg_task(self, dag_bag: DagBag) -> None:
        """Execute the process_csv_to_iceberg task and verify it works."""
        dag = dag_bag.dags["daily_csv_to_iceberg"]
        task = dag.get_task("process_csv_to_iceberg")

        # Create execution context - logical_date passed via task's op_args
        execution_date = datetime(2024, 11, 10)
        context = {
            "execution_date": execution_date,
            "dag": dag,
            "task": task,
            "task_instance": None,
            "run_id": f"test_{execution_date.isoformat()}",
        }

        # Execute the task
        result = task.execute(context=context)

        # Verify the result
        assert isinstance(result, dict)
        assert "table_name" in result
        assert "records_processed" in result
        assert "logical_date" in result

        assert result["records_processed"] == 10
        # logical_date is returned as ISO string - template may not render in tests
        # so we just verify it's a valid datetime string
        from dateutil.parser import parse

        parsed_date = parse(result["logical_date"])
        assert isinstance(parsed_date, datetime)

        logger.info("Task executed successfully!")
        logger.info("Table: %s", result["table_name"])
        logger.info("Records: %d", result["records_processed"])
        logger.info("Logical date: %s", result["logical_date"])

    def test_dag_task_dependencies_products(self, dag_bag: DagBag) -> None:
        """Test that task dependencies are correctly set up in iceberg_create_products_dag."""
        dag = dag_bag.dags["iceberg_create_products_dag"]

        create_task = dag.get_task("create_iceberg_table_from_csv")
        log_task = dag.get_task("log_results")

        # Verify dependencies
        assert log_task in create_task.downstream_list
        assert create_task in log_task.upstream_list

    def test_dag_task_dependencies_users(self, dag_bag: DagBag) -> None:
        """Test that task dependencies are correctly set up in spark_iceberg_dag."""
        dag = dag_bag.dags["spark_iceberg_dag"]

        create_task = dag.get_task("create_users_table_from_csv")
        log_task = dag.get_task("log_table_stats")

        # Verify dependencies
        assert log_task in create_task.downstream_list
        assert create_task in log_task.upstream_list

    def test_dag_has_correct_number_of_tasks(self, dag_bag: DagBag) -> None:
        """Test that DAGs have the expected number of tasks."""
        products_dag = dag_bag.dags["iceberg_create_products_dag"]
        assert len(products_dag.tasks) == 2  # create + log

        users_dag = dag_bag.dags["spark_iceberg_dag"]
        assert len(users_dag.tasks) == 2  # create + log

        daily_dag = dag_bag.dags["daily_csv_to_iceberg"]
        assert len(daily_dag.tasks) == 1  # process only

    def test_all_dags_have_owners(self, dag_bag: DagBag) -> None:
        """Test that all DAGs have owners set."""
        for dag_id, dag in dag_bag.dags.items():
            assert dag.default_args.get("owner") is not None, f"{dag_id} missing owner"
            assert dag.default_args["owner"] != "", f"{dag_id} has empty owner"

    def test_all_tasks_have_task_ids(self, dag_bag: DagBag) -> None:
        """Test that all tasks have valid task IDs."""
        for dag_id, dag in dag_bag.dags.items():
            for task in dag.tasks:
                assert task.task_id is not None, f"Task in {dag_id} missing task_id"
                assert task.task_id != "", f"Task in {dag_id} has empty task_id"
                assert " " not in task.task_id, f"Task {task.task_id} in {dag_id} contains spaces"
