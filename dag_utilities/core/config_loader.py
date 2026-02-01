"""
APEX Data Agent - Configuration Loader

Loads configurations from metadata and environment.
Provides a centralized way to access all configuration values.
"""

import os
from typing import Any, Dict, Optional
from dataclasses import dataclass

from dag_utilities.core.metadata_client import MetadataClient


@dataclass
class APEXConfig:
    """APEX platform configuration."""

    # Database
    metadata_db: str
    metadata_db_pool_size: int = 5

    # Spark
    spark_master: str = "yarn"
    spark_home: str = "/opt/spark"
    spark_jobs_path: str = "/opt/spark/jobs"

    # Storage
    raw_bucket: str = "gs://datalake/raw"
    bronze_bucket: str = "gs://datalake/bronze"
    silver_bucket: str = "gs://datalake/silver"
    gold_bucket: str = "gs://datalake/gold"

    # BigQuery
    bq_project: str = "enterprise-data-lake"
    bq_dataset_bronze: str = "bronze"
    bq_dataset_silver: str = "silver"
    bq_dataset_gold: str = "gold"

    # Airflow
    airflow_home: str = "/opt/airflow"

    # Notifications
    smtp_host: Optional[str] = None
    slack_webhook_url: Optional[str] = None
    pagerduty_api_key: Optional[str] = None

    # Metadata DB URL (convenience property)
    metadata_db_url: str = ""

    def __post_init__(self):
        if not self.metadata_db_url:
            self.metadata_db_url = self.metadata_db

    @classmethod
    def from_environment(cls) -> "APEXConfig":
        """Load APEXConfig from environment variables."""
        return cls(
            metadata_db=os.getenv("APEX_METADATA_DB", "postgresql://apex:apex@localhost:5432/apex_metadata"),
            metadata_db_pool_size=int(os.getenv("APEX_DB_POOL_SIZE", "5")),
            spark_master=os.getenv("SPARK_MASTER", "yarn"),
            spark_home=os.getenv("SPARK_HOME", "/opt/spark"),
            spark_jobs_path=os.getenv("APEX_SPARK_JOBS_PATH", "/opt/spark/jobs"),
            raw_bucket=os.getenv("APEX_RAW_BUCKET", "gs://datalake/raw"),
            bronze_bucket=os.getenv("APEX_BRONZE_BUCKET", "gs://datalake/bronze"),
            silver_bucket=os.getenv("APEX_SILVER_BUCKET", "gs://datalake/silver"),
            gold_bucket=os.getenv("APEX_GOLD_BUCKET", "gs://datalake/gold"),
            bq_project=os.getenv("GCP_PROJECT", "enterprise-data-lake"),
            bq_dataset_bronze=os.getenv("BQ_DATASET_BRONZE", "bronze"),
            bq_dataset_silver=os.getenv("BQ_DATASET_SILVER", "silver"),
            bq_dataset_gold=os.getenv("BQ_DATASET_GOLD", "gold"),
            airflow_home=os.getenv("AIRFLOW_HOME", "/opt/airflow"),
            smtp_host=os.getenv("SMTP_HOST"),
            slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
            pagerduty_api_key=os.getenv("PAGERDUTY_API_KEY"),
        )


class ConfigLoader:
    """
    Loads and manages APEX configurations.

    Combines environment variables, metadata, and defaults
    to provide a unified configuration interface.
    """

    _instance: Optional["ConfigLoader"] = None
    _config: Optional[APEXConfig] = None

    def __new__(cls):
        """Singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize config loader."""
        if self._config is None:
            self._load_config()

    def _load_config(self) -> None:
        """Load configuration from environment and defaults."""
        self._config = APEXConfig(
            # Database
            metadata_db=os.getenv(
                "APEX_METADATA_DB",
                "postgresql://apex:apex@localhost:5432/apex_metadata"
            ),
            metadata_db_pool_size=int(os.getenv("APEX_DB_POOL_SIZE", "5")),

            # Spark
            spark_master=os.getenv("SPARK_MASTER", "yarn"),
            spark_home=os.getenv("SPARK_HOME", "/opt/spark"),
            spark_jobs_path=os.getenv("APEX_SPARK_JOBS_PATH", "/opt/spark/jobs"),

            # Storage
            raw_bucket=os.getenv("APEX_RAW_BUCKET", "gs://datalake/raw"),
            bronze_bucket=os.getenv("APEX_BRONZE_BUCKET", "gs://datalake/bronze"),
            silver_bucket=os.getenv("APEX_SILVER_BUCKET", "gs://datalake/silver"),
            gold_bucket=os.getenv("APEX_GOLD_BUCKET", "gs://datalake/gold"),

            # BigQuery
            bq_project=os.getenv("GCP_PROJECT", "enterprise-data-lake"),
            bq_dataset_bronze=os.getenv("BQ_DATASET_BRONZE", "bronze"),
            bq_dataset_silver=os.getenv("BQ_DATASET_SILVER", "silver"),
            bq_dataset_gold=os.getenv("BQ_DATASET_GOLD", "gold"),

            # Airflow
            airflow_home=os.getenv("AIRFLOW_HOME", "/opt/airflow"),

            # Notifications
            smtp_host=os.getenv("SMTP_HOST"),
            slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
            pagerduty_api_key=os.getenv("PAGERDUTY_API_KEY"),
        )

    @property
    def config(self) -> APEXConfig:
        """Get current configuration."""
        if self._config is None:
            self._load_config()
        return self._config

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key."""
        return getattr(self._config, key, default)

    def get_storage_path(self, zone: str, *path_parts: str) -> str:
        """Get full storage path for a zone."""
        zone_mapping = {
            "RAW": self._config.raw_bucket,
            "BRONZE": self._config.bronze_bucket,
            "SILVER": self._config.silver_bucket,
            "GOLD": self._config.gold_bucket,
        }
        base = zone_mapping.get(zone.upper(), self._config.raw_bucket)
        return "/".join([base.rstrip("/")] + list(path_parts))

    def get_bq_table(self, zone: str, table_name: str) -> str:
        """Get full BigQuery table name for a zone."""
        dataset_mapping = {
            "BRONZE": self._config.bq_dataset_bronze,
            "SILVER": self._config.bq_dataset_silver,
            "GOLD": self._config.bq_dataset_gold,
        }
        dataset = dataset_mapping.get(zone.upper(), self._config.bq_dataset_bronze)
        return f"{self._config.bq_project}.{dataset}.{table_name}"

    def get_spark_job_path(self, job_name: str) -> str:
        """Get path to a Spark job file."""
        if not job_name.endswith(".py"):
            job_name = f"{job_name}.py"
        return os.path.join(self._config.spark_jobs_path, job_name)

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "metadata_db": self._config.metadata_db,
            "spark_master": self._config.spark_master,
            "spark_home": self._config.spark_home,
            "spark_jobs_path": self._config.spark_jobs_path,
            "raw_bucket": self._config.raw_bucket,
            "bronze_bucket": self._config.bronze_bucket,
            "silver_bucket": self._config.silver_bucket,
            "gold_bucket": self._config.gold_bucket,
            "bq_project": self._config.bq_project,
            "bq_dataset_bronze": self._config.bq_dataset_bronze,
            "bq_dataset_silver": self._config.bq_dataset_silver,
            "bq_dataset_gold": self._config.bq_dataset_gold,
        }


    # ═══════════════════════════════════════════════════════════════════════
    # MULTI-ENVIRONMENT SUPPORT
    # ═══════════════════════════════════════════════════════════════════════

    # Per-environment defaults (overridden by env vars when present)
    ENV_DEFAULTS = {
        "dev": {
            "gcs_prefix": "gs://dev-datalake",
            "bq_project": "enterprise-data-lake-dev",
            "metadata_db": "postgresql://apex:apex@localhost:5432/apex_metadata_dev",
            "dags_folder": "/opt/airflow/dags/dev",
        },
        "staging": {
            "gcs_prefix": "gs://staging-datalake",
            "bq_project": "enterprise-data-lake-staging",
            "metadata_db": "postgresql://apex:apex@localhost:5432/apex_metadata_staging",
            "dags_folder": "/opt/airflow/dags/staging",
        },
        "prod": {
            "gcs_prefix": "gs://prod-datalake",
            "bq_project": "enterprise-data-lake",
            "metadata_db": "postgresql://apex:apex@localhost:5432/apex_metadata",
            "dags_folder": "/opt/airflow/dags",
        },
    }

    @property
    def environment(self) -> str:
        """Get current environment (dev, staging, prod)."""
        return os.getenv("APEX_ENVIRONMENT", "dev")

    def get_environment_config(self, env: Optional[str] = None) -> Dict[str, Any]:
        """
        Get environment-specific configuration.

        Args:
            env: Environment name (dev/staging/prod). Defaults to APEX_ENVIRONMENT.

        Returns:
            Dict with env-specific paths, DB URLs, GCS buckets.
        """
        env = env or self.environment
        defaults = self.ENV_DEFAULTS.get(env, self.ENV_DEFAULTS["dev"])

        return {
            "environment": env,
            "gcs_prefix": os.getenv("APEX_GCS_PREFIX", defaults["gcs_prefix"]),
            "bq_project": os.getenv("GCP_PROJECT", defaults["bq_project"]),
            "metadata_db": os.getenv("APEX_METADATA_DB", defaults["metadata_db"]),
            "dags_folder": os.getenv("APEX_DAGS_FOLDER", defaults["dags_folder"]),
            "raw_bucket": os.getenv("APEX_RAW_BUCKET", f"{defaults['gcs_prefix']}/raw"),
            "bronze_bucket": os.getenv("APEX_BRONZE_BUCKET", f"{defaults['gcs_prefix']}/bronze"),
            "silver_bucket": os.getenv("APEX_SILVER_BUCKET", f"{defaults['gcs_prefix']}/silver"),
            "gold_bucket": os.getenv("APEX_GOLD_BUCKET", f"{defaults['gcs_prefix']}/gold"),
        }


def get_config() -> APEXConfig:
    """Get APEX configuration (convenience function)."""
    return ConfigLoader().config
