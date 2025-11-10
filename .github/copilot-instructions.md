# GitHub Copilot Instructions

## Project Overview

Airflow DAG development project with Spark and Iceberg integration. Python 3.11 boilerplate with modern tooling (uv, ruff, pytest).

## Development Environment

- **Platform**: Linux (WSL Ubuntu 24.04)
- **Python**: 3.11+
- **Package Manager**: uv (fast Python package installer)
- **Orchestration**: Apache Airflow 3.0.1
- **Data Processing**: Apache Spark 3.5.3 + Apache Iceberg 1.7.0
- **Python Iceberg**: PyIceberg 0.10.0

## Key Commands

```bash
# Setup environment
uv venv --python 3.11
source .venv/bin/activate  # or: . .venv/bin/activate

# Install dependencies
uv sync

# Run tests
uv run pytest tests/ -v

# Code quality checks
uv run ruff check dags/ tests/
uv run ruff format dags/ tests/
uv run mypy dags/ tests/

# Run all quality checks
uv run ruff check . && uv run ruff format --check . && uv run mypy .
```

## Project Structure

```
.
├── dags/                 # Airflow DAG definitions
│   ├── example_dag.py
│   └── spark_iceberg_dag.py
├── tests/                # Unit tests (pytest)
│   ├── test_dags.py
│   └── test_spark_jobs.py
├── config/               # Configuration files
├── .venv/                # Virtual environment (gitignored)
├── pyproject.toml        # Project config & dependencies
├── uv.lock               # Locked dependencies
└── .github/
    └── copilot-instructions.md
```

## DAG Development Patterns

- Place all DAGs in `dags/` directory
- Use `@dag` decorator for TaskFlow API (preferred over classic operators)
- **Airflow 3.0**: Use `schedule` parameter instead of `schedule_interval`
- Keep DAG logic separate from business logic - DAGs orchestrate, don't process
- Extract Spark jobs into separate modules for testability
- Use `default_args` for common task parameters

### Airflow 3.0 Breaking Changes

1. **Schedule Parameter**: `schedule="@daily"` (not `schedule_interval`)
2. **Timetable Access**: Use `dag.timetable.summary` instead of `dag.schedule_interval`
3. **Python 3.8+ Required**: Airflow 3.0 drops support for Python 3.7

## Spark and Iceberg Configuration

### Spark Session Setup

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "/tmp/warehouse")
    .master("local[*]")
    .getOrCreate()
)
```

### Iceberg Table Operations

```python
# Write to Iceberg
df.writeTo("local.default.my_table").createOrReplace()

# Read from Iceberg
df = spark.table("local.default.my_table")

# Use PyIceberg for Python-native operations
from pyiceberg.catalog import load_catalog
catalog = load_catalog("local", **{"type": "hadoop", "warehouse": "/tmp/warehouse"})
```

## Testing Conventions

- Test DAG structure with `DagBag` to catch import/syntax errors
- Mock Airflow context for task testing
- Use pytest fixtures for Spark session setup
- Test Iceberg table operations with local catalog
- Run: `uv run pytest tests/ -v --cov=dags`

## Dependencies Management

```bash
# Add Airflow 3.0 dependency
uv add "apache-airflow==3.0.1"

# Add Spark/Iceberg with specific versions
uv add "pyspark==3.5.3" "pyiceberg==0.10.0" "pyarrow>=17.0.0"

# Add dev dependencies
uv add --dev pytest pytest-cov mypy ruff
```

## Important Version Compatibility

- **Spark 3.5.3** requires Scala 2.12
- **Iceberg 1.7.0** runtime: `iceberg-spark-runtime-3.5_2.12:1.7.0`
- **PyIceberg 0.10.0** (latest) requires PyArrow 17.0.0+
- **Airflow 3.0.1** requires Python 3.8+ (tested with 3.11)
- **Build System**: Setuptools (compatible with uv)

## Code Quality Standards

- **Ruff**: Linting + formatting (replaces black, isort, flake8)
- **Mypy**: Type checking (use type hints in all functions)
- **Pytest**: Unit testing with coverage reporting
- Configure all tools in `pyproject.toml` under `[tool.*]` sections

## Important Notes

- Run commands via `wsl` from PowerShell: `wsl uv run pytest`
- Airflow needs `AIRFLOW_HOME` env var (defaults to `~/airflow`)
- Spark/Iceberg configs go in DAG `default_args` or separate config files
- Use uv's lockfile (`uv.lock`) for reproducible environments across team
