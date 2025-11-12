"""Catalog setup utilities"""

from pyspark.sql import SparkSession
from pathlib import Path


class CatalogSetup:
    """Setup Iceberg catalogs for local and MinIO"""

    @staticmethod
    def setup_local_catalog(spark: SparkSession, catalog_name: str, warehouse_path: str):
        """Configure local filesystem catalog"""
        warehouse_posix = Path(warehouse_path).as_posix()

        spark.conf.set(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        spark.conf.set(f"spark.sql.catalog.{catalog_name}.type", "hadoop")
        spark.conf.set(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_posix)
        spark.conf.set(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")

        print(f"✓ Local catalog '{catalog_name}' configured")
        print(f"  Warehouse: {warehouse_posix}")

    @staticmethod
    def setup_minio_catalog(spark: SparkSession, catalog_name: str,
                            endpoint: str, bucket: str,
                            access_key: str, secret_key: str,
                            region: str = "us-east-1"):
        """
        Configure MinIO S3 catalog using S3FileIO

        Args:
            spark: SparkSession instance
            catalog_name: Name for the catalog (e.g., 'minio_catalog')
            endpoint: MinIO endpoint (e.g., 'http://localhost:9000')
            bucket: S3 bucket name
            access_key: MinIO access key
            secret_key: MinIO secret key
            region: AWS region (default: 'us-east-1' for MinIO compatibility)
        """

        # Base catalog configuration
        spark.conf.set(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        spark.conf.set(f"spark.sql.catalog.{catalog_name}.type", "hadoop")
        spark.conf.set(f"spark.sql.catalog.{catalog_name}.warehouse", f"s3a://{bucket}/iceberg")

        # S3FileIO implementation (required for MinIO)
        spark.conf.set(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

        # S3 connection settings
        spark.conf.set(f"spark.sql.catalog.{catalog_name}.s3.endpoint", endpoint)
        spark.conf.set(f"spark.sql.catalog.{catalog_name}.s3.access-key-id", access_key)
        spark.conf.set(f"spark.sql.catalog.{catalog_name}.s3.secret-access-key", secret_key)
        spark.conf.set(f"spark.sql.catalog.{catalog_name}.s3.path-style-access", "true")
        spark.conf.set(f"spark.sql.catalog.{catalog_name}.s3.connection-ssl.enabled", "false")

        # Region settings (required for S3FileIO)
        spark.conf.set(f"spark.sql.catalog.{catalog_name}.s3.region", region)
        spark.conf.set(f"spark.sql.catalog.{catalog_name}.client.region", region)

        print(f"✓ MinIO catalog '{catalog_name}' configured")
        print(f"  Endpoint: {endpoint}")
        print(f"  Warehouse: s3a://{bucket}/iceberg")
        print(f"  Region: {region}")