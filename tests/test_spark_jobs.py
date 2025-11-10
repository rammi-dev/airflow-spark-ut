"""Unit tests for Spark job logic."""

import logging
from collections.abc import Generator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Configure logger
logger = logging.getLogger(__name__)


class TestSparkJobs:
    """Test Spark job processing logic."""

    @pytest.fixture(scope="class")
    def spark_session(self) -> Generator[SparkSession, None, None]:
        """Create a Spark session for testing with Iceberg support."""
        # Use project-local warehouse directory
        project_root = Path(__file__).parent.parent
        warehouse_path = project_root / "warehouse"

        spark = (
            SparkSession.builder.master("local[1]")
            .appName("test")
            .config(
                "spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0",
            )
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", str(warehouse_path))
            .getOrCreate()
        )
        yield spark
        spark.stop()

    def test_spark_session_creation(self, spark_session: SparkSession) -> None:
        """Test that Spark session can be created with Iceberg extensions."""
        assert spark_session is not None
        assert spark_session.version.startswith("3.5")
        # Verify Iceberg extension is loaded
        extensions = spark_session.conf.get("spark.sql.extensions")
        assert "IcebergSparkSessionExtensions" in extensions

    def test_iceberg_table_creation(self, spark_session: SparkSession) -> None:
        """Test Iceberg table creation and basic operations."""
        # Create test data
        test_data = [("test1", 100), ("test2", 200)]
        df = spark_session.createDataFrame(test_data, ["name", "value"])

        # Write to Iceberg table
        table_name = "local.default.test_table"
        df.writeTo(table_name).createOrReplace()

        # Read back and validate
        result_df = spark_session.table(table_name)
        assert result_df.count() == 2
        assert "name" in result_df.columns
        assert "value" in result_df.columns

        # Clean up
        spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_data_transformation(self) -> None:
        """Test data transformation logic."""
        # Example test for business logic
        # Separate from Spark/Airflow for easier testing
        sample_data = {"value": 100}
        result = sample_data["value"] * 2
        assert result == 200

    def test_csv_to_iceberg_table(self, spark_session: SparkSession) -> None:
        """Test creating a CSV file and converting it to an Iceberg table."""
        # Get path to sample CSV from resources
        resources_dir = Path(__file__).parent / "resources"
        csv_path = resources_dir / "sample_employees.csv"

        # Verify the sample CSV exists
        assert csv_path.exists(), f"Sample CSV not found at {csv_path}"

        # Read CSV into Spark DataFrame
        df = spark_session.read.csv(
            str(csv_path),
            header=True,
            inferSchema=True,
        )

        # Verify DataFrame was read correctly
        assert df.count() == 10  # We have 10 records in sample
        assert set(df.columns) == {"id", "name", "age", "city", "salary"}

        # Validate data types
        schema_dict = {field.name: str(field.dataType) for field in df.schema.fields}
        assert "IntegerType" in schema_dict["id"]
        assert "StringType" in schema_dict["name"]
        assert "IntegerType" in schema_dict["age"]
        assert "DoubleType" in schema_dict["salary"]

        # Convert to Iceberg table
        table_name = "local.default.employees"
        df.writeTo(table_name).createOrReplace()

        # Read from Iceberg table and validate
        iceberg_df = spark_session.table(table_name)
        assert iceberg_df.count() == 10

        # Verify data integrity - check first record (Alice)
        alice = iceberg_df.filter(iceberg_df.name == "Alice").collect()[0]
        assert alice["age"] == 30
        assert alice["city"] == "New York"
        assert alice["salary"] == 75000.50

        # Test filtering on Iceberg table (salaries > 80000: Bob, Charlie, Iris)
        high_earners = iceberg_df.filter(iceberg_df.salary > 80000).count()
        assert high_earners == 3

        # Test aggregation on Iceberg table
        avg_salary = iceberg_df.agg({"salary": "avg"}).collect()[0][0]
        # Average of all 10 salaries: (75000.50 + 85000.75 + 95000.00 + 70000.25 + 80000.00 + 72000.00 + 78000.50 + 69000.75 + 88000.00 + 71000.25) / 10 = 78300.3
        expected_avg = 78300.3
        assert abs(avg_salary - expected_avg) < 10  # Within $10 tolerance

        # Clean up Iceberg table
        spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_csv_from_resources_to_iceberg(self, spark_session: SparkSession) -> None:
        """Test loading sample CSV from resources and converting to Iceberg table."""
        # Get path to sample CSV in resources
        resources_dir = Path(__file__).parent / "resources"
        csv_path = resources_dir / "sample_employees.csv"

        # Verify the sample CSV exists
        assert csv_path.exists(), f"Sample CSV not found at {csv_path}"

        # Read CSV into Spark DataFrame
        df = spark_session.read.csv(
            str(csv_path),
            header=True,
            inferSchema=True,
        )

        # Verify DataFrame was read correctly
        assert df.count() == 10  # Updated sample has 10 records
        assert set(df.columns) == {"id", "name", "age", "city", "salary"}

        # Convert to Iceberg table
        table_name = "local.default.sample_employees"
        df.writeTo(table_name).createOrReplace()

        # Read from Iceberg table and validate
        iceberg_df = spark_session.table(table_name)
        assert iceberg_df.count() == 10

        # Test some queries on the table
        # Find employees in specific cities
        ny_employees = iceberg_df.filter(iceberg_df.city == "New York").count()
        assert ny_employees == 1

        # Find employees over 30
        senior_employees = iceberg_df.filter(iceberg_df.age > 30).count()
        assert senior_employees == 4  # Charlie, Eve, Grace, Iris

        # Calculate statistics
        from pyspark.sql.functions import avg, count, max, min

        stats = iceberg_df.agg(
            count("id").alias("total_employees"),
            avg("salary").alias("avg_salary"),
            max("salary").alias("max_salary"),
            min("salary").alias("min_salary"),
        ).collect()[0]

        assert stats["total_employees"] == 10
        assert 74000 < stats["avg_salary"] < 79000  # Average around 76-78k
        assert stats["max_salary"] == 95000.00  # Charlie
        assert stats["min_salary"] == 69000.75  # Henry

        # Don't clean up the table - keep it for inspection
        logger.info(
            "Iceberg table created at: %s",
            spark_session.conf.get("spark.sql.catalog.local.warehouse"),
        )
        logger.info("Table name: %s", table_name)
        logger.info("Total records: %d", iceberg_df.count())

    def test_daily_dag_with_logical_date(self, spark_session: SparkSession) -> None:
        """Test that CSV to Iceberg DAG properly adds logical_date column."""
        from datetime import datetime

        from pyspark.sql.functions import lit

        # Get path to sample CSV
        resources_dir = Path(__file__).parent / "resources"
        csv_path = resources_dir / "sample_employees.csv"

        # Simulate what the DAG does
        logical_date = datetime(2024, 11, 10, 0, 0, 0)

        # Read CSV
        df = spark_session.read.csv(
            str(csv_path),
            header=True,
            inferSchema=True,
        )

        # Add logical_date column (as the DAG does)
        df_with_date = df.withColumn("logical_date", lit(logical_date))

        # Verify the column was added
        assert "logical_date" in df_with_date.columns
        assert df_with_date.count() == 10

        # Write to Iceberg table
        table_name = "local.default.daily_employees_test"
        df_with_date.writeTo(table_name).createOrReplace()

        # Read back and validate
        iceberg_df = spark_session.table(table_name)

        # Verify all records have the logical_date
        assert iceberg_df.count() == 10
        assert "logical_date" in iceberg_df.columns

        # Check that all records have the correct logical_date
        dates = iceberg_df.select("logical_date").distinct().collect()
        assert len(dates) == 1
        assert dates[0]["logical_date"] == logical_date

        # Test appending with a different logical_date (simulating next day's run)
        logical_date_2 = datetime(2024, 11, 11, 0, 0, 0)
        df_with_date_2 = df.withColumn("logical_date", lit(logical_date_2))
        df_with_date_2.writeTo(table_name).append()

        # Verify we now have data for 2 days
        iceberg_df_updated = spark_session.table(table_name)
        assert iceberg_df_updated.count() == 20  # 10 records x 2 days

        # Verify we have 2 distinct logical_dates
        distinct_dates = iceberg_df_updated.select("logical_date").distinct().count()
        assert distinct_dates == 2

        # Query data for specific date
        day_1_data = iceberg_df_updated.filter(iceberg_df_updated.logical_date == logical_date)
        assert day_1_data.count() == 10

        day_2_data = iceberg_df_updated.filter(iceberg_df_updated.logical_date == logical_date_2)
        assert day_2_data.count() == 10

        logger.info("Daily DAG simulation successful!")
        logger.info("Table: %s", table_name)
        logger.info("Total records: %d", iceberg_df_updated.count())
        logger.info("Distinct dates: %s", distinct_dates)

        # Clean up test table
        spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_iceberg_create_products_dag(self, spark_session: SparkSession) -> None:
        """Test the Iceberg create products DAG workflow."""
        from pathlib import Path

        from pyspark.sql import functions as F

        # Setup paths
        project_root = Path.cwd()
        csv_path = project_root / "tests" / "resources" / "sample_products.csv"

        assert csv_path.exists(), f"Products CSV not found at {csv_path}"

        # Read CSV
        df = spark_session.read.csv(
            str(csv_path),
            header=True,
            inferSchema=True,
        )

        # Add metadata columns (simulating DAG behavior)
        df_with_metadata = df.withColumn("created_at", F.current_timestamp()).withColumn(
            "updated_at", F.current_timestamp()
        )

        # Create Iceberg table
        table_name = "local.default.test_products"

        # Drop if exists
        try:
            spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass

        # Create table
        df_with_metadata.writeTo(table_name).create()

        # Verify table was created
        products_df = spark_session.table(table_name)
        assert products_df.count() > 0

        # Verify columns
        columns = products_df.columns
        assert "product_id" in columns
        assert "product_name" in columns
        assert "category" in columns
        assert "price" in columns
        assert "stock_quantity" in columns
        assert "supplier" in columns
        assert "created_at" in columns
        assert "updated_at" in columns

        # Calculate statistics (as the DAG does)
        stats = products_df.agg(
            F.count("*").alias("total_products"),
            F.countDistinct("category").alias("unique_categories"),
            F.avg("price").alias("avg_price"),
            F.sum("stock_quantity").alias("total_stock"),
        ).collect()[0]

        # Verify statistics
        assert stats["total_products"] == 15
        assert (
            stats["unique_categories"] >= 4
        )  # Electronics, Furniture, Stationery, Office, Kitchen
        assert stats["avg_price"] > 0
        assert stats["total_stock"] > 0

        logger.info("Products Table Statistics:")
        logger.info("Total Products: %d", stats["total_products"])
        logger.info("Unique Categories: %d", stats["unique_categories"])
        logger.info("Average Price: $%.2f", stats["avg_price"])
        logger.info("Total Stock: %d", stats["total_stock"])

        # Test querying by category
        electronics = products_df.filter(F.col("category") == "Electronics").count()
        assert electronics > 0
        logger.info("Electronics Products: %d", electronics)

    def test_spark_iceberg_users_dag(self, spark_session: SparkSession) -> None:
        """Test the Spark Iceberg users DAG workflow."""
        from pathlib import Path

        from pyspark.sql import functions as F

        # Setup paths
        project_root = Path.cwd()
        csv_path = project_root / "tests" / "resources" / "sample_users.csv"

        assert csv_path.exists(), f"Users CSV not found at {csv_path}"

        # Read CSV
        df = spark_session.read.csv(
            str(csv_path),
            header=True,
            inferSchema=True,
        )

        # Add processing timestamp (simulating DAG behavior)
        df_with_timestamp = df.withColumn("processed_at", F.current_timestamp())

        # Create Iceberg table
        table_name = "local.default.test_users"

        # Drop if exists
        try:
            spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass

        # Create table
        df_with_timestamp.writeTo(table_name).create()

        # Verify table was created
        users_df = spark_session.table(table_name)
        assert users_df.count() > 0

        # Verify columns
        columns = users_df.columns
        assert "user_id" in columns
        assert "name" in columns
        assert "age" in columns
        assert "department" in columns
        assert "salary" in columns
        assert "join_date" in columns
        assert "processed_at" in columns

        # Calculate statistics (as the DAG does)
        stats = users_df.agg(
            F.count("*").alias("total_users"),
            F.countDistinct("department").alias("unique_departments"),
            F.avg("age").alias("avg_age"),
            F.avg("salary").alias("avg_salary"),
        ).collect()[0]

        # Verify statistics
        assert stats["total_users"] == 8
        assert stats["unique_departments"] >= 4  # Engineering, Marketing, HR, Sales, Management
        assert stats["avg_age"] > 0
        assert stats["avg_salary"] > 0

        logger.info("Users Table Statistics:")
        logger.info("Total Users: %d", stats["total_users"])
        logger.info("Unique Departments: %d", stats["unique_departments"])
        logger.info("Average Age: %.1f", stats["avg_age"])
        logger.info("Average Salary: $%.2f", stats["avg_salary"])

        # Test querying by department
        engineering = users_df.filter(F.col("department") == "Engineering").count()
        assert engineering > 0
        logger.info("Engineering Employees: %d", engineering)
