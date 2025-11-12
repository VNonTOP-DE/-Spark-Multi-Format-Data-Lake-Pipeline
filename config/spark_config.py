# config/spark_config.py
from pyspark.sql import SparkSession
from typing import Optional, Dict, List
from pathlib import Path

class SparkConnect:
    def __init__(self,
                 app_name: str,
                 master_url: str = "local[*]",
                 executor_memory: Optional[str] = "2g",
                 executor_cores: Optional[int] = 2,
                 driver_memory: Optional[str] = "4g",
                 num_executors: Optional[int] = 2,
                 jar_packages: Optional[List[str]] = None,
                 spark_conf: Optional[Dict[str, str]] = None,
                 iceberg_warehouse: Optional[str] = None,
                 s3_config: Optional[Dict[str, str]] = None,
                 log_level: str = "WARN"):

        full_conf = self._build_iceberg_conf(
            spark_conf or {},
            iceberg_warehouse,
            s3_config
        )

        self.app_name = app_name
        self.spark = self.create_spark_session(
            master_url, executor_memory, executor_cores,
            driver_memory, num_executors, jar_packages,
            full_conf, log_level
        )

    @staticmethod
    def _build_iceberg_conf(
            spark_conf: Dict[str, str],
            warehouse_path: Optional[str],
            s3_config: Optional[Dict[str, str]] = None
    ) -> Dict[str, str]:

        if not warehouse_path:
            return spark_conf

        # Local mode: use absolute path
        warehouse = str(Path(warehouse_path).resolve()) if s3_config is None else warehouse_path

        conf = {
            "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.local.type": "hadoop",
            "spark.sql.catalog.local.warehouse": warehouse,
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.local.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
            "spark.sql.catalog.local.format-version": "2",
        }

        # MINIO / S3 MODE
        if s3_config:
            bucket = s3_config.get("bucket", "warehouse")
            endpoint = s3_config.get("endpoint", "http://localhost:9000")
            access_key = s3_config.get("access_key", "minioadmin")
            secret_key = s3_config.get("secret_key", "minioadmin")

            conf.update({
                "spark.sql.catalog.local.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
                "spark.sql.catalog.local.warehouse": f"s3a://{bucket}/{warehouse_path}",

                "spark.hadoop.fs.s3a.access.key": access_key,
                "spark.hadoop.fs.s3a.secret.key": secret_key,
                "spark.hadoop.fs.s3a.endpoint": endpoint,
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
                "spark.hadoop.fs.s3a.aws.credentials.provider":
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",

                "spark.hadoop.fs.s3a.connection.maximum": "100",
                "spark.hadoop.fs.s3a.fast.upload": "true",
                "spark.hadoop.fs.s3a.multipart.size": "104857600",
                "spark.hadoop.fs.s3a.multipart.threshold": "104857600",
                "spark.hadoop.fs.s3a.attempts.maximum": "3",
                "spark.hadoop.fs.s3a.connection.timeout": "200000",
            })

        conf.update(spark_conf or {})
        return conf

    def create_spark_session(self,
                             master_url: str,
                             executor_memory: Optional[str],
                             executor_cores: Optional[int],
                             driver_memory: Optional[str],
                             num_executors: Optional[int],
                             jar_packages: Optional[List[str]],
                             spark_conf: Dict[str, str],
                             log_level: str) -> SparkSession:

        # REMOVE THE HARDCODED CONFIGS - Use parameters instead
        builder = SparkSession.builder \
            .appName(self.app_name) \
            .master(master_url)

        if executor_memory:
            builder = builder.config("spark.executor.memory", executor_memory)
        if executor_cores:
            builder = builder.config("spark.executor.cores", str(executor_cores))
        if driver_memory:
            builder = builder.config("spark.driver.memory", driver_memory)
        if num_executors:
            builder = builder.config("spark.executor.instances", str(num_executors))
        if jar_packages:
            builder = builder.config("spark.jars.packages", ",".join(jar_packages))

        # Apply all spark configurations
        for k, v in spark_conf.items():
            builder = builder.config(k, v)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(log_level)

        # Print configuration summary
        is_minio = any("s3a" in k for k in spark_conf.keys())
        if is_minio:
            warehouse = spark_conf.get("spark.sql.catalog.local.warehouse", "unknown")
            endpoint = spark_conf.get("spark.hadoop.fs.s3a.endpoint", "unknown")
            print(f"\n✅ Spark Session Created (MinIO Mode)")
            print(f"   App: {self.app_name}")
            print(f"   Warehouse: {warehouse}")
            print(f"   Endpoint: {endpoint}")
        else:
            warehouse = spark_conf.get("spark.sql.catalog.local.warehouse", "unknown")
            print(f"\n✅ Spark Session Created (Local Mode)")
            print(f"   App: {self.app_name}")
            print(f"   Warehouse: {warehouse}")

        return spark

    def stop(self):
        if hasattr(self, 'spark') and self.spark:
            self.spark.stop()
            print("---------------Stopped Spark Session---------------")