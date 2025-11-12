"""MinIO and catalog configuration"""

from dataclasses import dataclass
from typing import Dict


@dataclass
class MinIOConfig:
    """MinIO connection configuration"""
    endpoint: str = "http://localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    bucket: str = "warehouse"

    def to_s3_config(self) -> Dict[str, str]:
        """Convert to s3_config dict for SparkConnect"""
        return {
            "endpoint": self.endpoint,
            "bucket": self.bucket,
            "access_key": self.access_key,
            "secret_key": self.secret_key
        }


@dataclass
class CatalogConfig:
    """Catalog configuration"""
    source_catalog: str = "local_catalog"
    target_catalog: str = "minio_catalog"
    source_db: str = "iceberg_db"
    target_db: str = "minio_iceberg_db"
    source_warehouse: str = "C:/Users/Public/Frequently_Case/warehouse"


@dataclass
class SparkJarsConfig:
    """Spark JAR dependencies"""

    @staticmethod
    def get_iceberg_jars() -> list:
        return [
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
            "org.apache.iceberg:iceberg-aws:1.5.2",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "software.amazon.awssdk:bundle:2.20.162",
            "software.amazon.awssdk:url-connection-client:2.20.162",
        ]
