# Configuration Directory

This directory contains Airflow configuration files used during testing.

## Files

### `airflow_test.cfg`
Test-specific Airflow configuration that:
- Uses SQLite database for testing (lightweight, no external dependencies)
- Points to the `dags/` folder in the project
- Disables example DAGs
- Enables unit test mode
- Configures minimal logging

This configuration is automatically copied to a temporary `AIRFLOW_HOME` directory during test execution by `tests/conftest.py`.

### `test_variables.json`
Airflow Variables that are automatically loaded during test setup:

```json
{
  "csv_input_path": "tests/resources/sample_employees.csv",
  "warehouse_path": "warehouse",
  "iceberg_catalog": "local",
  "iceberg_namespace": "default",
  "processing_batch_size": 1000,
  "spark_master": "local[*]",
  "storage_connection_id": "iceberg_storage",
  "spark_session_timeout": 60,
  "task_execution_timeout": 300,
  "s3_connection_timeout": 30
}
```

These variables are used by DAGs (especially `daily_csv_to_iceberg`) to make them configurable without hardcoding paths and settings.

**Timeout Variables:**
- `spark_session_timeout`: Spark session initialization timeout (seconds)
- `task_execution_timeout`: Task execution timeout (seconds)
- `s3_connection_timeout`: S3 connection timeout (seconds)

### `test_connections.json`
Airflow Connections that are automatically loaded during test setup:

#### Local Filesystem Connection (`iceberg_storage`)
```json
{
  "conn_id": "iceberg_storage",
  "conn_type": "fs",
  "extra": {
    "warehouse_path": "warehouse",
    "catalog_type": "hadoop",
    "connection_timeout": 60,
    "socket_timeout": 60
  }
}
```
- **Catalog**: Hadoop (file-based metadata)
- **Storage**: Local filesystem
- **Best For**: Development, testing

#### MinIO S3 Connection with Hadoop Catalog (`minio_s3`)
```json
{
  "conn_id": "minio_s3",
  "conn_type": "s3",
  "host": "localhost",
  "port": 9000,
  "login": "minioadmin",
  "password": "minioadmin",
  "extra": {
    "endpoint_url": "http://localhost:9000",
    "bucket": "iceberg-warehouse",
    "catalog_type": "hadoop",
    "connection_timeout": 30,
    "socket_timeout": 30,
    "s3_connection_establish_timeout": 10,
    "s3_connection_timeout": 30
  }
}
```
- **Catalog**: Hadoop (metadata stored in S3)
- **Storage**: MinIO S3 (local S3-compatible storage)
- **Best For**: Testing S3 integration locally

#### Production REST Catalog Connection (`iceberg_rest_s3`)
```json
{
  "conn_id": "iceberg_rest_s3",
  "conn_type": "s3",
  "extra": {
    "bucket": "prod-iceberg-warehouse",
    "catalog_type": "rest",
    "rest_uri": "https://catalog.example.com/v1",
    "rest_credential": "your-catalog-api-token",
    "aws_access_key_id": "your-access-key",
    "aws_secret_access_key": "your-secret-key",
    "region": "us-east-1"
  }
}
```
- **Catalog**: REST (centralized catalog server)
- **Storage**: AWS S3
- **Best For**: Production deployments
- **Features**: ACID catalog operations, multi-user concurrency

#### Polaris REST Catalog Connection (`iceberg_rest_polaris`)
```json
{
  "conn_id": "iceberg_rest_polaris",
  "conn_type": "s3",
  "extra": {
    "bucket": "iceberg-data",
    "catalog_type": "rest",
    "rest_uri": "https://polaris.example.com:8181/api/catalog",
    "rest_credential": "polaris-principal:polaris-credentials",
    "rest_headers": {
      "X-Iceberg-Access-Delegation": "vended-credentials"
    }
  }
}
```
- **Catalog**: Polaris (Apache open-source REST catalog)
- **Storage**: AWS S3
- **Best For**: Production with open-source catalog
- **Features**: Multi-catalog support, access delegation

**Timeout Settings in Connections:**
- `connection_timeout`: General connection timeout (seconds)
- `socket_timeout`: Socket read/write timeout (seconds)
- `s3_connection_establish_timeout`: S3-specific connection establishment timeout (seconds)
- `s3_connection_timeout`: S3-specific data transfer timeout (seconds)

**REST Catalog Settings:**
- `rest_uri`: REST catalog endpoint URL (required for catalog_type=rest)
- `rest_credential`: API token or credentials for authentication
- `rest_oauth2_token`: OAuth 2.0 bearer token (alternative to rest_credential)
- `rest_oauth2_server_uri`: OAuth 2.0 server URI for token refresh
- `rest_headers`: Additional HTTP headers (dict) for REST requests

## Iceberg Catalog Types

The project supports multiple Iceberg catalog implementations:

| Catalog Type | Use Case | Metadata Storage | Concurrency | Setup |
|-------------|----------|------------------|-------------|-------|
| **hadoop** | Dev/Test | Filesystem (local/S3) | Limited | Simple |
| **rest** | Production | REST API Server | Full | Requires catalog service |
| **hive** | Hive Integration | Hive Metastore | Good | Requires Hive |
| **glue** | AWS Native | AWS Glue Catalog | Full | AWS only |
| **jdbc** | Custom | SQL Database | Full | Requires DB |

**Recommendation**: Use `hadoop` for development/testing, `rest` for production.

### `airflow.cfg.example`
Example production Airflow configuration (not used in tests).

## How It Works

1. **Test Setup** (`tests/conftest.py`):
   - Creates temporary `AIRFLOW_HOME` directory
   - Copies `airflow_test.cfg` to temp directory
   - Initializes Airflow DB
   - Loads variables from `test_variables.json`
   - Loads connections from `test_connections.json`

2. **Storage Backend Selection** (`dags/utils`):
   - DAGs use `create_spark_session_with_storage()` utility
   - Automatically detects connection type (local fs vs S3)
   - Configures Spark session with appropriate settings
   - Applies timeout configurations from connection
   - Example:
     ```python
     from dags.utils.storage import create_spark_session_with_storage
     
     spark, storage = create_spark_session_with_storage(
         app_name="my_job",
         connection_id="iceberg_storage",  # or "minio_s3"
         catalog_name="local",
         master="local[*]",
     )
     ```

3. **DAG Usage**:
   - DAGs use `Variable.get()` to retrieve configuration
   - Fallback values provided for robustness
   - Example: `Variable.get("warehouse_path", default_var="warehouse")`

4. **Cleanup**:
   - Temporary directory is removed after tests complete
   - No persistent state between test runs

## Benefits

✅ **Consistent Test Environment**: All tests use same configuration  
✅ **Easy Configuration**: Change paths/settings in one place  
✅ **Production-Ready Pattern**: Same pattern used in production Airflow  
✅ **Isolation**: Each test session gets fresh configuration  
✅ **No Hardcoded Values**: DAGs read from Variables and Connections  
✅ **Multi-Storage Support**: Switch between local filesystem and S3/MinIO  
✅ **Configurable Timeouts**: All timeout values centralized in configuration
