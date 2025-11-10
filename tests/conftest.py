"""Pytest configuration and fixtures for Airflow tests."""

import logging
import os
import shutil
import sys
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest


# Configure logging BEFORE any Airflow imports to prevent circular import issues
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
    force=True  # Override any existing configuration
)


@pytest.fixture(scope="session", autouse=True)
def setup_test_resources() -> Generator[None, None, None]:
    """Copy test CSV files to /tmp/tests/resources/ for virtualenv tasks.
    
    Virtualenv tasks can't access the project directory, so we copy test files
    to a location that matches the paths hardcoded in the DAG definitions.
    """
    test_resources_dir = Path("/tmp/tests/resources")
    test_resources_dir.mkdir(parents=True, exist_ok=True)
    
    # Copy CSV files
    source_dir = Path.cwd() / "tests" / "resources"
    for csv_file in source_dir.glob("*.csv"):
        shutil.copy(csv_file, test_resources_dir / csv_file.name)
    
    yield
    
    # Cleanup
    try:
        shutil.rmtree("/tmp/tests", ignore_errors=True)
    except Exception:
        pass  # Best effort cleanup


@pytest.fixture(scope="session", autouse=True)
def setup_airflow_db() -> Generator[None, None, None]:
    """Set up a temporary Airflow database for testing.

    This fixture:
    1. Creates a temporary directory for Airflow home
    2. Copies test config to the temp directory
    3. Initializes the Airflow database with test variables
    4. Cleans up everything after tests complete
    """
    # Create temporary directory for Airflow
    temp_dir = tempfile.mkdtemp(prefix="airflow_test_")
    temp_airflow_home = Path(temp_dir)

    # Copy test config to temp Airflow home
    config_source = Path.cwd() / "config" / "airflow_test.cfg"
    config_dest = temp_airflow_home / "airflow.cfg"
    shutil.copy(config_source, config_dest)

    # Set AIRFLOW_HOME environment variable BEFORE importing airflow
    original_airflow_home = os.environ.get("AIRFLOW_HOME")
    os.environ["AIRFLOW_HOME"] = str(temp_airflow_home)
    os.environ["AIRFLOW_CONFIG"] = str(config_dest)

    # Remove cached airflow modules to force reload with new settings
    modules_to_remove = [key for key in sys.modules.keys() if key.startswith("airflow")]
    for module in modules_to_remove:
        del sys.modules[module]

    try:
        # Import airflow AFTER setting env variables
        import json

        from airflow import settings
        from airflow.models import Connection, Variable
        from airflow.utils import db

        # Force settings to reload
        settings.configure_orm()

        # Create the database schema
        db.initdb()

        # Load test variables from config
        variables_file = Path.cwd() / "config" / "test_variables.json"
        if variables_file.exists():
            with open(variables_file) as f:
                test_vars = json.load(f)
                for key, value in test_vars.items():
                    Variable.set(key, value)

        # Load test connections from config
        connections_file = Path.cwd() / "config" / "test_connections.json"
        if connections_file.exists():
            with open(connections_file) as f:
                test_conns = json.load(f)
                for conn_data in test_conns:
                    conn = Connection(
                        conn_id=conn_data["conn_id"],
                        conn_type=conn_data["conn_type"],
                        description=conn_data.get("description", ""),
                        host=conn_data.get("host"),
                        login=conn_data.get("login"),
                        password=conn_data.get("password"),
                        schema=conn_data.get("schema"),
                        port=conn_data.get("port"),
                        extra=json.dumps(conn_data.get("extra", {})),
                    )
                    # Use session to add connection
                    session = settings.Session()
                    session.add(conn)
                    session.commit()
                    session.close()

        yield

    finally:
        # Clean up: remove temporary directory
        if original_airflow_home:
            os.environ["AIRFLOW_HOME"] = original_airflow_home
        else:
            os.environ.pop("AIRFLOW_HOME", None)

        # Clean up environment variables
        os.environ.pop("AIRFLOW_CONFIG", None)

        # Remove temporary directory
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception:
            pass  # Best effort cleanup
