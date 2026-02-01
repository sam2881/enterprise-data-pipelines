"""
Setup script for APEX dag_utilities package.

This package is installed into the Airflow container so that
generated DAGs can import dag_utilities at runtime.

Only includes runtime dependencies needed by DAGs — NOT
LangGraph/LLM dependencies (those belong to the data-agent).
"""

from setuptools import setup, find_packages

setup(
    name="apex-dag-utilities",
    version="1.0.0",
    description="APEX Data Agent - Reusable DAG Utilities for Airflow",
    author="Enterprise Data Team",
    python_requires=">=3.10",
    packages=find_packages(where="."),
    package_dir={"": "."},
    install_requires=[
        # Logging
        "structlog>=23.1.0",
        # Database (metadata client)
        "psycopg2-binary>=2.9.0",
        "sqlalchemy>=1.4.0,<2.0.0",
        # Google Cloud (storage, BigQuery)
        "google-cloud-storage>=2.10.0",
        "google-cloud-bigquery>=3.11.0",
        # HTTP (API source, notifications)
        "requests>=2.31.0",
        "tenacity>=8.2.0",
    ],
    extras_require={
        "ge": ["great_expectations>=0.18.0,<1.0.0"],
    },
)
