"""Spark and Iceberg integration DAG - Creates users table from CSV."""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task
from airflow.models import Variable

# Configure logger
logger = logging.getLogger(__name__)


default_args: dict[str, Any] = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="spark_iceberg_dag",
    default_args=default_args,
    description="Spark and Iceberg - Creates users table from CSV",
    schedule="@daily",  # Changed from schedule_interval to schedule in Airflow 3
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "iceberg", "users"],
    dag_display_name="👥 Iceberg Users ETL",
    doc_md="""
# Iceberg Users ETL

**Last Updated:** 2025-11-10

## Description
Creates an Iceberg table from users CSV file using Apache Spark.

## Dependencies
See `dags/spark_iceberg_dag/requirements.txt` for full dependency list:
- PySpark 3.5.3
- PyIceberg 0.10.0
- PyArrow 17.0.0+

## Schedule
Daily at midnight UTC

## Tasks
1. **create_users_table_from_csv**: Reads CSV, creates Iceberg table
2. **log_table_stats**: Logs table statistics

## Outputs
- Table: `local.default.users`
- Statistics: total users, departments, avg age, avg salary

## Version Tracking
Airflow automatically tracks DAG versions based on code hash.
Check the DAG Details page in the UI for version history.
""",
)
def spark_iceberg_dag() -> None:
    """DAG for creating Iceberg users table from CSV using Spark."""

    # Read requirements from file
    requirements_file = Path(__file__).parent / "spark_iceberg_dag" / "requirements.txt"
    requirements = requirements_file.read_text().strip().split('\n')
    
    # Get venv cache path from Airflow Variables with DAG-specific subdirectory
    venv_cache_base = Variable.get("venv_cache_path", default_var=".venv_cache")
    venv_cache = f"{venv_cache_base}/spark_iceberg_dag"
    
    # Get workspace root to add to PYTHONPATH so virtualenv can import dags module
    workspace_root = str(Path(__file__).parent.parent)
    
    # Build env vars - include AIRFLOW_HOME and AIRFLOW_CONFIG if set (for testing)
    import os
    env_vars = {"PYTHONPATH": workspace_root}
    if "AIRFLOW_HOME" in os.environ:
        env_vars["AIRFLOW_HOME"] = os.environ["AIRFLOW_HOME"]
    if "AIRFLOW_CONFIG" in os.environ:
        env_vars["AIRFLOW_CONFIG"] = os.environ["AIRFLOW_CONFIG"]

    @task.virtualenv(
        requirements=requirements,  # Pass list of requirements
        venv_cache_path=venv_cache,  # Cache venv on persistent storage
        python_version="3.11",  # Specify Python version explicitly for uv
        env_vars=env_vars,  # Pass environment variables including AIRFLOW_HOME
    )
    def create_users_table_from_csv(
        storage_conn_dict: dict[str, Any],
        iceberg_catalog: str,
        iceberg_namespace: str,
        spark_master: str,
    ) -> dict[str, Any]:
        """Read CSV file and create Iceberg users table.
        
        Args:
            storage_conn_dict: Storage connection details as dictionary
            iceberg_catalog: Iceberg catalog name
            iceberg_namespace: Iceberg namespace
            spark_master: Spark master URL
            
        Returns:
            Dictionary with table statistics
        """
        import logging
        from pathlib import Path
        from typing import Any

        from pyspark.sql import functions as F

        from dags.utils import create_spark_session_with_connection_dict

        logger = logging.getLogger(__name__)

        # Setup paths
        project_root = Path(__file__).parent.parent
        csv_path = project_root / "tests" / "resources" / "sample_users.csv"

        # Create Spark session with storage configuration
        spark, storage = create_spark_session_with_connection_dict(
            connection_dict=storage_conn_dict,
            app_name="spark_iceberg_users",
            catalog_name=iceberg_catalog,
            master=spark_master,
            project_root=project_root,
        )

        try:
            logger.info("Starting users table creation from CSV")
            logger.info(f"Storage configuration: {storage.get_storage_info()}")

            # Read CSV file
            logger.info(f"Reading CSV from: {csv_path}")
            df = spark.read.csv(
                str(csv_path),
                header=True,
                inferSchema=True,
            )
            logger.info(f"CSV loaded successfully with {df.count()} records")

            # Add processing timestamp
            df_with_timestamp = df.withColumn("processed_at", F.current_timestamp())

            # Create Iceberg table
            table_name = f"{iceberg_catalog}.{iceberg_namespace}.users"
            logger.info(f"Creating Iceberg table: {table_name}")

            # Drop table if exists (for daily refresh)
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                logger.info(f"Dropped existing table: {table_name}")
            except Exception as e:
                logger.debug(f"Table doesn't exist or couldn't be dropped: {e}")

            # Create table
            df_with_timestamp.writeTo(table_name).create()
            logger.info(f"Table created successfully: {table_name}")

            # Get statistics
            users_df = spark.table(table_name)
            users_df.count()

            stats = users_df.agg(
                F.count("*").alias("total_users"),
                F.countDistinct("department").alias("unique_departments"),
                F.avg("age").alias("avg_age"),
                F.avg("salary").alias("avg_salary"),
            ).collect()[0]

            logger.info(
                f"Table statistics - Total users: {stats['total_users']}, "
                f"Departments: {stats['unique_departments']}, "
                f"Avg age: {stats['avg_age']:.1f}, "
                f"Avg salary: ${stats['avg_salary']:,.2f}"
            )

            return {
                "table_name": table_name,
                "total_users": stats["total_users"],
                "unique_departments": stats["unique_departments"],
                "avg_age": float(stats["avg_age"]),
                "avg_salary": float(stats["avg_salary"]),
                "storage_type": storage.conn_type,
            }

        finally:
            spark.stop()
            logger.info("Spark session stopped")

    @task
    def log_table_stats(stats: dict[str, Any]) -> None:
        """Log the users table statistics."""
        logger.info("=" * 60)
        logger.info("Iceberg Users Table Created Successfully!")
        logger.info("=" * 60)
        logger.info(f"Table: {stats['table_name']}")
        logger.info(f"Total Users: {stats['total_users']}")
        logger.info(f"Unique Departments: {stats['unique_departments']}")
        logger.info(f"Average Age: {stats['avg_age']:.1f}")
        logger.info(f"Average Salary: ${stats['avg_salary']:,.2f}")
        logger.info(f"Storage Type: {stats['storage_type']}")
        logger.info("=" * 60)

    # Get configuration from Airflow variables (fetched in DAG context, not in virtualenv)
    from dags.utils import get_connection_as_dict

    storage_conn_id = Variable.get("storage_connection_id", default_var="iceberg_storage")
    iceberg_catalog = Variable.get("iceberg_catalog", default_var="local")
    iceberg_namespace = Variable.get("iceberg_namespace", default_var="default")
    spark_master = Variable.get("spark_master", default_var="local[*]")

    # Fetch connection as dict (can be serialized and passed to virtualenv)
    storage_conn_dict = get_connection_as_dict(storage_conn_id)

    # Define task dependencies - pass config as parameters
    table_stats = create_users_table_from_csv(
        storage_conn_dict=storage_conn_dict,
        iceberg_catalog=iceberg_catalog,
        iceberg_namespace=iceberg_namespace,
        spark_master=spark_master,
    )
    log_table_stats(table_stats)


# Instantiate the DAG
spark_iceberg_dag_instance = spark_iceberg_dag()
