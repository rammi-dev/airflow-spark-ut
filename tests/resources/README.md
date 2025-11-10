# Test Resources

This directory contains sample data files used for testing.

## Files

### sample_employees.csv

Sample employee dataset with 10 records containing:
- **id**: Employee ID (integer)
- **name**: Employee name (string)
- **age**: Employee age (integer)
- **city**: Employee city (string)
- **salary**: Employee salary (decimal)

Used in:
- `test_csv_from_resources_to_iceberg()` - Demonstrates loading CSV data and converting to Iceberg tables

## Adding New Test Data

To add new test data files:

1. Place the file in this directory
2. Update this README with file description
3. Reference the file in tests using:
   ```python
   resources_dir = Path(__file__).parent / "resources"
   csv_path = resources_dir / "your_file.csv"
   ```

## Format Support

Currently supported formats:
- CSV files with headers
- Schema inference supported by Spark

Future formats can include:
- JSON
- Parquet
- Avro

To refresh my Spark knowledge, I experimented with unit testing. Today, setting up an end-to-end Spark + Iceberg stack in Kubernetes—or using Testcontainers as a lightweight alternative—is straightforward and can be used for unit tests. Sometimes, the old-school approach of mocking with a Hadoop catalog remains the simplest way to quickly validate Spark ingestion or SQL logic in pipelines. I’m curious whether mocking will eventually become obsolete as these environments become easier to set up.