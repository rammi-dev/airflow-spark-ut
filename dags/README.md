# Airflow DAGs

This directory contains Apache Airflow DAGs for Iceberg table ETL workflows.

## Airflow 3.0 Structure

This project uses Airflow 3.0 features:

- ✅ `schedule` parameter instead of `schedule_interval`
- ✅ `dag_display_name` for human-readable names in UI
- ✅ Separate `requirements.txt` for DAG dependencies
- ✅ TaskFlow API with `@task` decorators

## DAG Dependencies

DAG-level dependencies are managed in `requirements.txt` in this directory. This approach is better for:

- **Version Control**: Dependencies are tracked separately from code
- **Reusability**: Multiple DAGs can share the same requirements
- **Maintenance**: Easier to update versions across all DAGs
- **CI/CD**: Can validate dependencies independently

### Airflow 3 Dependency Loading

Airflow 3 supports multiple ways to specify DAG dependencies:

1. **Folder-level** (recommended): `dags/requirements.txt`
   - Applies to all DAGs in the folder
   - Better for shared dependencies

2. **DAG-level**: `dags/my_dag/requirements.txt`
   - Specific to one DAG
   - Better for isolated dependencies

3. **Inline**: `requirements=["pkg==1.0"]` in `@dag` decorator
   - Embedded in code
   - Harder to version control

**We use approach #1** for this project since all DAGs share the same Spark/Iceberg stack.

## DAG List

| DAG ID | Display Name | Description | Schedule |
|--------|--------------|-------------|----------|
| `iceberg_create_products_dag` | 📊 Iceberg Products ETL | Create products table from CSV | @daily |
| `spark_iceberg_dag` | 👥 Iceberg Users ETL | Create users table from CSV | @daily |
| `daily_csv_to_iceberg` | 📅 Daily CSV to Iceberg | Daily employees data with logical_date | @daily |

## Directory Structure

```
dags/
├── README.md                      # This file
├── requirements.txt               # Shared DAG dependencies
├── iceberg_dag.py                # Products ETL DAG
├── spark_iceberg_dag.py          # Users ETL DAG
├── daily_csv_to_iceberg_dag.py   # Daily ETL DAG
└── utils/                         # Shared utilities
    ├── __init__.py
    └── storage.py                # Storage configuration helpers
```

## Development

When adding new DAGs:

1. Add Python dependencies to `requirements.txt`
2. Use `dag_display_name` for readable UI labels
3. Add appropriate `tags` for filtering
4. Use `schedule` parameter (not `schedule_interval`)
5. Follow TaskFlow API patterns with `@task` decorators

## Testing

All DAGs are tested in `tests/` directory:

```bash
# Test DAG structure
uv run pytest tests/test_dags.py -v

# Test task execution
uv run pytest tests/test_dag_execution.py -v

# Test Spark logic
uv run pytest tests/test_spark_jobs.py -v
```

## Versioning Dependencies

The `requirements.txt` file should be:

- ✅ Committed to version control
- ✅ Updated when upgrading dependencies
- ✅ Tested in CI/CD before deployment
- ✅ Documented with version rationale

### Version Pinning Strategy

- **Exact versions** for critical dependencies (pyspark, pyiceberg)
- **Minimum versions** for flexible dependencies (pyarrow>=17.0.0)
- **Comments** explaining version constraints

Example:
```txt
# Pin Spark version for stability
pyspark==3.5.3

# Require PyArrow 17+ for Iceberg compatibility
pyarrow>=17.0.0
```
