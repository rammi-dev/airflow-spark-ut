"""Iceberg Create DAG - Creates Iceberg table from CSV using Spark."""

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
    dag_id="iceberg_create_products_dag",
    default_args=default_args,
    description="Create Iceberg table from products CSV using Spark",
    schedule="@daily",  # Changed from schedule_interval to schedule in Airflow 3
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["iceberg", "spark", "etl", "products"],
    dag_display_name="📊 Iceberg Products ETL",
    doc_md="""
# Iceberg Products ETL

**Last Updated:** 2025-11-10

## Description
Creates an Iceberg table from products CSV file using Apache Spark.

## Dependencies
See `dags/iceberg_create_products_dag/requirements.txt` for full dependency list:
- PySpark 3.5.3
- PyIceberg 0.10.0
- PyArrow 17.0.0+

## Schedule
Daily at midnight UTC

## Tasks
1. **create_iceberg_table_from_csv**: Reads CSV, creates Iceberg table
2. **log_results**: Logs table statistics

## Outputs
- Table: `local.default.products`
- Statistics: total products, categories, avg price, total stock

## Version Tracking
Airflow automatically tracks DAG versions based on code hash.
Check the DAG Details page in the UI for version history.
""",
)
def iceberg_create_products_dag() -> None:
    """DAG that creates an Iceberg table from products CSV."""

    # Read requirements from file
    requirements_file = Path(__file__).parent / "iceberg_create_products_dag" / "requirements.txt"
    requirements = requirements_file.read_text().strip().split('\n')
    
    # Get venv cache path from Airflow Variables with DAG-specific subdirectory
    venv_cache_base = Variable.get("venv_cache_path", default_var=".venv_cache")
    venv_cache = f"{venv_cache_base}/iceberg_create_products_dag"
    
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
    def create_iceberg_table_from_csv(
        storage_conn_dict: dict[str, Any],
        iceberg_catalog: str,
        iceberg_namespace: str,
        spark_master: str,
    ) -> dict[str, Any]:
        """Create Iceberg table from products CSV file using Spark.

        Args:
            storage_conn_dict: Storage connection details as dictionary
            iceberg_catalog: Iceberg catalog name
            iceberg_namespace: Iceberg namespace
            spark_master: Spark master URL

        Returns:
            Dictionary with table creation statistics
        """
        import logging
        from pathlib import Path
        from typing import Any

        from pyspark.sql import functions as F

        from dags.utils import create_spark_session_with_connection_dict

        logger = logging.getLogger(__name__)

        # Setup paths
        project_root = Path(__file__).parent.parent
        csv_path = project_root / "tests" / "resources" / "sample_products.csv"

        # Create Spark session with storage configuration
        spark, storage = create_spark_session_with_connection_dict(
            connection_dict=storage_conn_dict,
            app_name="iceberg_create_products",
            catalog_name=iceberg_catalog,
            master=spark_master,
            project_root=project_root,
        )

        try:
            logger.info("Starting products table creation from CSV")
            logger.info(f"Storage configuration: {storage.get_storage_info()}")

            # Read CSV file
            logger.info(f"Reading CSV from: {csv_path}")
            df = spark.read.csv(
                str(csv_path),
                header=True,
                inferSchema=True,
            )
            logger.info(f"CSV loaded successfully with {df.count()} records")

            # Add metadata columns
            df_with_metadata = df.withColumn("created_at", F.current_timestamp()).withColumn(
                "updated_at", F.current_timestamp()
            )

            # Create Iceberg table
            table_name = f"{iceberg_catalog}.{iceberg_namespace}.products"
            logger.info(f"Creating Iceberg table: {table_name}")

            # Drop table if exists (for testing)
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                logger.info(f"Dropped existing table: {table_name}")
            except Exception as e:
                logger.debug(f"Table doesn't exist or couldn't be dropped: {e}")

            # Create table
            df_with_metadata.writeTo(table_name).create()
            logger.info(f"Table created successfully: {table_name}")

            # Read back and get statistics
            products_df = spark.table(table_name)
            products_df.count()

            # Calculate some statistics
            stats = products_df.agg(
                F.count("*").alias("total_products"),
                F.countDistinct("category").alias("unique_categories"),
                F.avg("price").alias("avg_price"),
                F.sum("stock_quantity").alias("total_stock"),
            ).collect()[0]

            logger.info(
                f"Table statistics - Total products: {stats['total_products']}, "
                f"Categories: {stats['unique_categories']}, "
                f"Avg price: ${stats['avg_price']:.2f}, "
                f"Total stock: {stats['total_stock']}"
            )

            return {
                "table_name": table_name,
                "total_products": stats["total_products"],
                "unique_categories": stats["unique_categories"],
                "avg_price": float(stats["avg_price"]),
                "total_stock": stats["total_stock"],
                "storage_type": storage.conn_type,
                "warehouse_path": storage.get_warehouse_path(),
            }

        finally:
            spark.stop()
            logger.info("Spark session stopped")

    @task
    def log_results(stats: dict[str, Any]) -> None:
        """Log the table creation results.

        Args:
            stats: Dictionary with table statistics
        """
        logger.info("=" * 60)
        logger.info("Iceberg Products Table Created Successfully!")
        logger.info("=" * 60)
        logger.info(f"Table Name: {stats['table_name']}")
        logger.info(f"Total Products: {stats['total_products']}")
        logger.info(f"Unique Categories: {stats['unique_categories']}")
        logger.info(f"Average Price: ${stats['avg_price']:.2f}")
        logger.info(f"Total Stock: {stats['total_stock']}")
        logger.info(f"Storage Type: {stats['storage_info']['connection_type']}")
        logger.info(f"Warehouse Path: {stats['storage_info']['warehouse_path']}")
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
    table_stats = create_iceberg_table_from_csv(
        storage_conn_dict=storage_conn_dict,
        iceberg_catalog=iceberg_catalog,
        iceberg_namespace=iceberg_namespace,
        spark_master=spark_master,
    )
    log_results(table_stats)


# Instantiate the DAG
iceberg_create_products_dag_instance = iceberg_create_products_dag()
