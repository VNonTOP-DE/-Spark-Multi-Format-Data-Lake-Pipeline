# main.py
from config.spark_config import SparkConnect
from schema_manager import load_all_sources
from utils.spark_write_iceberg import write_to_iceberg, verify_iceberg
import time

def main():
    jars = [
        "mysql:mysql-connector-java:8.0.33",
        "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        "org.apache.iceberg:iceberg-aws:1.5.2",  # Iceberg S3 support
        "org.apache.hadoop:hadoop-aws:3.3.4",  # ✓ CRITICAL: S3A FileSystem
        "software.amazon.awssdk:bundle:2.20.162",
        "software.amazon.awssdk:url-connection-client:2.20.162"
    ]

    # Toggle MinIO
    USE_MINIO = True

    if USE_MINIO:
        WAREHOUSE = "warehouse"
        s3_config = {
            "endpoint": "http://localhost:9000",
            "bucket": "warehouse",
            "access_key": "minioadmin",
            "secret_key": "minioadmin"
        }
    else:
        WAREHOUSE = "C:/Users/Public/Frequently_Case/warehouse"
        s3_config = None

    # ===============================================================
    # CLEANUP: Delete warehouse and drop database BEFORE initializing Spark
    # ===============================================================
    print("\n" + "=" * 60)
    print("=== CLEANUP: Removing previous data ===")
    print("=" * 60)

    import shutil
    import os

    if USE_MINIO:
        # For MinIO: Delete bucket contents using boto3
        try:
            import boto3
            from botocore.client import Config

            s3_client = boto3.client(
                's3',
                endpoint_url=s3_config["endpoint"],
                aws_access_key_id=s3_config["access_key"],
                aws_secret_access_key=s3_config["secret_key"],
                config=Config(signature_version='s3v4')
            )

            # List and delete all objects in the bucket
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=s3_config["bucket"])

            delete_count = 0
            for page in pages:
                if 'Contents' in page:
                    objects = [{'Key': obj['Key']} for obj in page['Contents']]
                    s3_client.delete_objects(
                        Bucket=s3_config["bucket"],
                        Delete={'Objects': objects}
                    )
                    delete_count += len(objects)

            print(f"✓ Deleted {delete_count} objects from MinIO bucket: {s3_config['bucket']}")
        except Exception as e:
            print(f"⚠ Warning: Could not clean MinIO bucket: {e}")
    else:
        # For local filesystem: Delete warehouse directory
        if os.path.exists(WAREHOUSE):
            try:
                shutil.rmtree(WAREHOUSE)
                print(f"✓ Deleted local warehouse: {WAREHOUSE}")
            except Exception as e:
                print(f"⚠ Warning: Could not delete warehouse: {e}")
        else:
            print(f"ℹ Warehouse directory does not exist: {WAREHOUSE}")

    sparkconnect = SparkConnect(
        app_name="vnontop",
        master_url="local[*]",
        executor_cores=1,
        driver_memory="4g",
        num_executors=1,
        jar_packages=jars,
        log_level="WARN",
        iceberg_warehouse=WAREHOUSE,
        s3_config=s3_config  # ← giờ đã luôn luôn tồn tại
    )
    spark = sparkconnect.spark

    # Drop existing database
    try:
        spark.sql("DROP DATABASE IF EXISTS local.iceberg_db CASCADE")
        print("✓ Dropped existing database: local.iceberg_db")
    except Exception as e:
        print(f"⚠ Warning: Could not drop database: {e}")

    print("=" * 60 + "\n")

    # ===============================================================
    # 1. LOAD ALL SOURCES (via schema_manager)
    # ===============================================================
    print("\n" + "="*60)
    print("=== STARTING INGESTION PIPELINE ===")
    print("="*60)

    all_dataframes = load_all_sources(spark)

    if not all_dataframes:
        print("\nError: No files loaded!")
        sparkconnect.stop()
        return

    # ===============================================================
    # 2. SUMMARY
    # ===============================================================
    print("\n" + "="*60)
    print("=== LOADING SUMMARY ===")
    print("="*60)
    print(f"Total tables: {len(all_dataframes)}")
    for _, name, typ, rows, cols in all_dataframes:
        print(f" • {name:40} [{typ:7}] {rows:8,} rows, {cols:5} cols")

    # ===============================================================
    # 3. SAMPLE SCHEMAS (first 3)
    # ===============================================================
    print("\n" + "="*60)
    print("=== SAMPLE SCHEMAS (first 3) ===")
    print("="*60)
    for i, (df, name, typ, _, _) in enumerate(all_dataframes[:3]):
        print(f"\n{i+1}. {name} ({typ})")
        print("-"*50)
        if len(df.columns) > 20:
            df.select(df.columns[:20]).printSchema()
            print(f"... +{len(df.columns)-20} cols")
        else:
            df.printSchema()
        df.show(3, truncate=100)

    # ===============================================================
    # 4. WRITE TO ICEBERG
    # ===============================================================
    print("\n" + "="*60)
    print("=== WRITING TO ICEBERG ===")
    print("="*60)

    tables_to_write = [(df, name) for df, name, _, _, _ in all_dataframes]

    write_to_iceberg(
        spark=spark,
        df_table_pairs=tables_to_write,
        database="local.iceberg_db",
        mode="overwrite",
        extra_options={"format-version": "2"}
    )

    verify_iceberg(spark, database="local.iceberg_db", sample_rows=3)

    # ===============================================================
    # 5. CLEANUP
    # ===============================================================
    print("\n" + "="*60)
    print("Success: PIPELINE COMPLETE!")
    print("="*60)
    for df, _, _, _, _ in all_dataframes:
        df.unpersist()

    input("\nPress Enter to exit...")
    time.sleep(1)
    sparkconnect.stop()


if __name__ == "__main__":
    main()