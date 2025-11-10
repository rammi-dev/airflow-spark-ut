"""Daily CSV to Iceberg DAG with logical date tracking."""

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
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="daily_csv_to_iceberg",
    default_args=default_args,
    description="Daily job to load CSV data to Iceberg with logical date",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "iceberg", "daily", "etl"],
    dag_display_name="📅 Daily CSV to Iceberg",
    doc_md="""
# Daily CSV to Iceberg

**Last Updated:** 2025-11-10

## Description
Daily job that loads CSV employee data to Iceberg table with logical date tracking.

## Dependencies
See `dags/daily_csv_to_iceberg/requirements.txt` for full dependency list:
- PySpark 3.5.3
- PyIceberg 0.10.0
- PyArrow 17.0.0+
- python-dateutil 2.8.2+

## Schedule
Daily at midnight UTC

## Tasks
1. **process_csv_to_iceberg**: Reads CSV, adds logical_date, writes to Iceberg

## Features
- Tracks data by execution date
- Supports append or overwrite operations
- Automatic schema evolution

## Outputs
- Table: `local.default.daily_employees`
- Records processed per day with logical_date column

## Version Tracking
Airflow automatically tracks DAG versions based on code hash.
Check the DAG Details page in the UI for version history.
""",
)
def daily_csv_to_iceberg() -> None:
    """DAG that loads CSV data to Iceberg table with Airflow logical date."""

    # Read requirements from file in same directory as DAG
    requirements_file = Path(__file__).parent / "requirements.txt"
    requirements = requirements_file.read_text().strip().split('\n')
    
    # Get venv cache path from Airflow Variables with DAG-specific subdirectory
    venv_cache_base = Variable.get("venv_cache_path", default_var=".venv_cache")
    venv_cache = f"{venv_cache_base}/daily_csv_to_iceberg"
    
    # Get workspace root to add to PYTHONPATH so virtualenv can import dags module
    # Can be overridden via PROJECT_ROOT env var for deployment flexibility
    import os
    workspace_root = os.environ.get(
        "PROJECT_ROOT",
        str(Path(__file__).parent.parent.parent)  # Default: DAG is in dags/daily_employees/
    )
    
    # Build env vars - include AIRFLOW_HOME and AIRFLOW_CONFIG if set (for testing)
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
        system_site_packages=True,  # Allow access to parent environment for dags.utils imports
    )
    def process_csv_to_iceberg(
        csv_input_path: str,
        iceberg_catalog: str,
        iceberg_namespace: str,
        spark_master: str,
        storage_conn_dict: dict[str, Any],
        project_root: str,
        logical_date: datetime | str | None = None,  # Context param must have default
    ) -> dict[str, Any]:
        """Process CSV file and write to Iceberg table with logical date column.

        Args:
            csv_input_path: Path to input CSV file (relative to project root)
            iceberg_catalog: Iceberg catalog name
            iceberg_namespace: Iceberg namespace
            spark_master: Spark master URL
            storage_conn_dict: Storage connection details as dictionary
            logical_date: Airflow logical date for the DAG run (datetime or ISO string)

        Returns:
            Dictionary with processing statistics
        """
        import logging
        from pathlib import Path
        from datetime import datetime
        from typing import Any

        from pyspark.sql.functions import lit
        from dags.utils import create_spark_session_with_connection_dict

        logger = logging.getLogger(__name__)

        # Convert string to datetime if needed (for template rendering)
        if logical_date is None:
            logical_date = datetime.now()
        elif isinstance(logical_date, str):
            # Handle Jinja template that wasn't rendered
            if logical_date == "{{ logical_date }}":
                logical_date = datetime.now()
            else:
                from dateutil.parser import parse

                logical_date = parse(logical_date)

        # Setup paths - project_root passed as parameter from DAG context
        # Handle both absolute and relative paths
        if Path(csv_input_path).is_absolute():
            csv_path = Path(csv_input_path)
        else:
            csv_path = Path(project_root) / csv_input_path

        # Create Spark session with storage configuration
        spark, storage = create_spark_session_with_connection_dict(
            connection_dict=storage_conn_dict,
            app_name="daily_csv_to_iceberg",
            catalog_name=iceberg_catalog,
            master=spark_master,
            project_root=Path(project_root),
        )

        # Log storage info
        storage_info = storage.get_storage_info()
        logger.info("Starting daily CSV to Iceberg processing")
        logger.info(f"Logical date: {logical_date}")
        logger.info(f"Storage configuration: {storage_info}")

        try:
            # Read CSV file
            logger.info(f"Reading CSV from: {csv_path}")
            df = spark.read.csv(
                str(csv_path),
                header=True,
                inferSchema=True,
            )
            logger.info(f"CSV loaded successfully with {df.count()} records")

            # Add logical_date column from Airflow
            df_with_date = df.withColumn("logical_date", lit(logical_date))

            # Write to Iceberg table (append mode to track multiple runs)
            table_name = f"{iceberg_catalog}.{iceberg_namespace}.daily_employees"
            logger.info(f"Writing to Iceberg table: {table_name}")

            # Check if table exists and append, otherwise create
            try:
                spark.sql(f"DESCRIBE TABLE {table_name}")
                # Table exists, append data
                df_with_date.writeTo(table_name).append()
                operation = "appended"
                logger.info(f"Data appended to existing table: {table_name}")
            except Exception:
                # Table doesn't exist, create it
                df_with_date.writeTo(table_name).create()
                operation = "created"
                logger.info(f"New table created: {table_name}")

            record_count = df_with_date.count()
            logger.info(f"Operation: {operation}, Records processed: {record_count}")

            return {
                "table_name": table_name,
                "logical_date": logical_date.isoformat(),
                "records_processed": record_count,
                "operation": operation,
                "storage_info": storage_info,
            }

        finally:
            spark.stop()
            logger.info("Spark session stopped")

    # Get configuration from Airflow variables (fetched in DAG context, not in virtualenv)
    from dags.utils import get_connection_as_dict

    csv_input_path = Variable.get(
        "csv_input_path", default_var="tests/resources/sample_employees.csv"
    )
    iceberg_catalog = Variable.get("iceberg_catalog", default_var="local")
    iceberg_namespace = Variable.get("iceberg_namespace", default_var="default")
    spark_master = Variable.get("spark_master", default_var="local[*]")
    storage_conn_id = Variable.get("storage_connection_id", default_var="iceberg_storage")

    # Fetch connection as dict (can be serialized and passed to virtualenv)
    storage_conn_dict = get_connection_as_dict(storage_conn_id)

    # Execute the task with logical_date from context and config as parameters
    # Task invocation with parameter passing
    process_csv_to_iceberg(
        csv_input_path=csv_input_path,
        iceberg_catalog=iceberg_catalog,
        iceberg_namespace=iceberg_namespace,
        spark_master=spark_master,
        storage_conn_dict=storage_conn_dict,
        project_root=workspace_root,  # Pass workspace root for file path resolution
        logical_date="{{ logical_date }}",  # Context param last
    )

# Instantiate the DAG
daily_csv_to_iceberg_dag = daily_csv_to_iceberg()
