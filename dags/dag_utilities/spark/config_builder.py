"""
APEX Data Agent - Spark Configuration Builder

Builds Spark configurations from metadata and runtime context.
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field


@dataclass
class AutoscalingPolicy:
    """Dataproc autoscaling policy configuration."""

    min_workers: int = 2
    max_workers: int = 20
    scale_up_factor: float = 1.0
    scale_down_factor: float = 1.0
    cooldown_period_seconds: int = 120


@dataclass
class NetworkConfig:
    """Network configuration for Spark clusters."""

    subnet: Optional[str] = None
    no_public_ip: bool = True
    service_account: Optional[str] = None


@dataclass
class SparkJobConfig:
    """Configuration for a Spark job."""

    job_name: str
    executor_instances: int = 4
    executor_memory: str = "4g"
    executor_cores: int = 2
    driver_memory: str = "2g"
    shuffle_partitions: int = 200
    adaptive_enabled: bool = True
    extra_conf: Dict[str, str] = None

    # Cost optimization
    cost_labels: Dict[str, str] = None
    preemptible_ratio: float = 0.0
    autoscaling_policy: Optional[AutoscalingPolicy] = None

    # Security
    encryption_key_name: Optional[str] = None  # CMEK key for GCS/BQ
    network_config: Optional[NetworkConfig] = None

    # Table format
    table_format: str = "parquet"  # parquet, delta, iceberg

    def __post_init__(self):
        if self.extra_conf is None:
            self.extra_conf = {}
        if self.cost_labels is None:
            self.cost_labels = {}


class SparkConfigBuilder:
    """
    Builds Spark configurations from metadata and runtime context.

    Provides methods to:
    - Build configurations for different job types
    - Optimize configurations based on data volume
    - Apply job-specific overrides
    """

    # Default configurations for each zone
    ZONE_DEFAULTS = {
        "BRONZE": {
            "executor_instances": 4,
            "executor_memory": "4g",
            "shuffle_partitions": 200,
        },
        "SILVER": {
            "executor_instances": 6,
            "executor_memory": "8g",
            "shuffle_partitions": 400,
        },
        "GOLD": {
            "executor_instances": 8,
            "executor_memory": "8g",
            "shuffle_partitions": 600,
        },
    }

    # Configurations for large data volumes (>1TB)
    LARGE_DATA_OVERRIDES = {
        "executor_instances": 20,
        "executor_memory": "16g",
        "executor_cores": 4,
        "shuffle_partitions": 2000,
        "extra_conf": {
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "256m",
            "spark.sql.adaptive.coalescePartitions.parallelismFirst": "false",
        },
    }

    def __init__(self):
        """Initialize config builder."""
        pass

    def build_config(
        self,
        job_name: str,
        zone: str,
        metadata_config: Optional[Dict[str, Any]] = None,
        estimated_data_size_gb: Optional[float] = None,
    ) -> SparkJobConfig:
        """
        Build Spark configuration for a job.

        Args:
            job_name: Name of the Spark job
            zone: Target zone (BRONZE, SILVER, GOLD)
            metadata_config: Configuration from metadata store
            estimated_data_size_gb: Estimated data size in GB

        Returns:
            SparkJobConfig with optimized settings
        """
        # Start with zone defaults
        config_dict = self.ZONE_DEFAULTS.get(zone.upper(), self.ZONE_DEFAULTS["BRONZE"]).copy()

        # Apply metadata overrides
        if metadata_config:
            for key in ["executor_instances", "executor_memory", "executor_cores",
                        "driver_memory", "shuffle_partitions", "adaptive_enabled"]:
                if key in metadata_config:
                    config_dict[key] = metadata_config[key]
            if "extra_conf" in metadata_config:
                config_dict["extra_conf"] = metadata_config["extra_conf"]

        # Apply large data overrides if needed
        if estimated_data_size_gb and estimated_data_size_gb > 1000:
            for key, value in self.LARGE_DATA_OVERRIDES.items():
                if key == "extra_conf":
                    existing = config_dict.get("extra_conf", {})
                    existing.update(value)
                    config_dict["extra_conf"] = existing
                else:
                    config_dict[key] = value

        # Build cost labels from metadata
        cost_labels = config_dict.get("cost_labels", {})
        if metadata_config:
            cost_labels.update({
                k: str(v) for k, v in metadata_config.items()
                if k in ("feed_id", "domain", "environment", "owner")
            })

        # Build autoscaling policy
        autoscaling = None
        if metadata_config and metadata_config.get("autoscaling_enabled"):
            autoscaling = AutoscalingPolicy(
                min_workers=metadata_config.get("min_workers", 2),
                max_workers=metadata_config.get("max_workers", 20),
                scale_up_factor=metadata_config.get("scale_up_factor", 1.0),
                cooldown_period_seconds=metadata_config.get("cooldown_period_seconds", 120),
            )

        # Preemptible ratio
        preemptible_ratio = 0.0
        if metadata_config:
            preemptible_ratio = metadata_config.get("preemptible_ratio", 0.6)

        # CMEK encryption key
        encryption_key = None
        if metadata_config:
            encryption_key = metadata_config.get("encryption_key_name")
            if encryption_key:
                config_dict.setdefault("extra_conf", {})
                config_dict["extra_conf"]["spark.hadoop.fs.gs.encryption.key"] = encryption_key

        # Network config
        network = None
        if metadata_config and metadata_config.get("subnet"):
            network = NetworkConfig(
                subnet=metadata_config.get("subnet"),
                no_public_ip=metadata_config.get("no_public_ip", True),
                service_account=metadata_config.get("service_account"),
            )

        # Build final config
        return SparkJobConfig(
            job_name=job_name,
            executor_instances=config_dict.get("executor_instances", 4),
            executor_memory=config_dict.get("executor_memory", "4g"),
            executor_cores=config_dict.get("executor_cores", 2),
            driver_memory=config_dict.get("driver_memory", "2g"),
            shuffle_partitions=config_dict.get("shuffle_partitions", 200),
            adaptive_enabled=config_dict.get("adaptive_enabled", True),
            extra_conf=config_dict.get("extra_conf", {}),
            cost_labels=cost_labels,
            preemptible_ratio=preemptible_ratio,
            autoscaling_policy=autoscaling,
            encryption_key_name=encryption_key,
            network_config=network,
        )

    def estimate_config_from_file_size(
        self,
        job_name: str,
        zone: str,
        file_size_bytes: int,
    ) -> SparkJobConfig:
        """
        Estimate configuration based on file size.

        Uses heuristics to determine optimal configuration.
        """
        file_size_gb = file_size_bytes / (1024 ** 3)

        # Calculate optimal executors based on file size
        if file_size_gb < 1:
            executor_instances = 2
            executor_memory = "2g"
            shuffle_partitions = 50
        elif file_size_gb < 10:
            executor_instances = 4
            executor_memory = "4g"
            shuffle_partitions = 200
        elif file_size_gb < 100:
            executor_instances = 8
            executor_memory = "8g"
            shuffle_partitions = 400
        else:
            executor_instances = 20
            executor_memory = "16g"
            shuffle_partitions = 2000

        return SparkJobConfig(
            job_name=job_name,
            executor_instances=executor_instances,
            executor_memory=executor_memory,
            shuffle_partitions=shuffle_partitions,
        )

    def get_ebcdic_config(
        self,
        job_name: str,
        record_length: int,
    ) -> SparkJobConfig:
        """Get optimized config for EBCDIC processing with Cobrix."""
        return SparkJobConfig(
            job_name=job_name,
            executor_instances=8,
            executor_memory="16g",
            executor_cores=4,
            shuffle_partitions=200,
            extra_conf={
                "spark.cobrix.allow_empty_files": "true",
                "spark.cobrix.record_format": "F",
                "spark.cobrix.record_length": str(record_length),
            },
        )
