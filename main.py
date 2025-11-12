# main.py
from config.spark_config import SparkConnect
from schema_manager import load_all_sources
from utils.spark_write_iceberg import write_to_iceberg, verify_iceberg
from utils.arrow_utils import optimize_dataframe_for_arrow
import time
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

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

    # Enable PyArrow for faster processing
    ENABLE_ARROW = True
    ARROW_BATCH_SIZE = 10000

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
        s3_config=s3_config,
        enable_arrow=ENABLE_ARROW,
        arrow_batch_size=ARROW_BATCH_SIZE
    )
    spark = sparkconnect.spark

    # Verify Arrow is enabled
    arrow_enabled = spark.conf.get("spark.sql.execution.arrow.pyspark.enabled", "false")
    print(f"{'✓' if arrow_enabled == 'true' else '✗'} Arrow optimization: {arrow_enabled}")

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

    start_time = time.time()
    all_dataframes = load_all_sources(spark)

    if not all_dataframes:
        print("\nError: No files loaded!")
        sparkconnect.stop()
        return
    load_time = time.time() - start_time
    print(f"\n⏱️ Loading completed in {load_time:.2f} seconds")

    # ===============================================================
    # 2. OPTIMIZE DATAFRAMES FOR ARROW (if enabled)
    # ===============================================================
    if ENABLE_ARROW and arrow_enabled == "true":
        print("\n" + "=" * 60)
        print("=== OPTIMIZING DATAFRAMES FOR ARROW ===")
        print("=" * 60)

        optimized_dataframes = []
        for df, name, typ, rows, cols in all_dataframes:
            print(f"  Optimizing: {name}...")

            # Optimize partition size for Arrow
            optimized_df = optimize_dataframe_for_arrow(df)

            # Re-cache with optimized partitions
            optimized_df = optimized_df.persist()

            optimized_dataframes.append((optimized_df, name, typ, rows, cols))

        all_dataframes = optimized_dataframes
        print("✓ All DataFrames optimized for Arrow processing")

    # ===============================================================
    # 3. SUMMARY
    # ===============================================================
    print("\n" + "="*60)
    print("=== LOADING SUMMARY ===")
    print("="*60)
    print(f"Total tables: {len(all_dataframes)}")

    total_rows = sum(rows for _, _, _, rows, _ in all_dataframes)
    total_cols = sum(cols for _, _, _, _, cols in all_dataframes)

    print(f"Total rows: {total_rows:,}")
    print(f"Total columns: {total_cols:,}")
    print(f"\nTable details:")

    for _, name, typ, rows, cols in all_dataframes:
        print(f" • {name:40} [{typ:7}] {rows:8,} rows, {cols:5} cols")

    # ===============================================================
    # 4. SAMPLE SCHEMAS (first 3)
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
    # 5. WRITE TO ICEBERG
    # ===============================================================
    print("\n" + "="*60)
    print("=== WRITING TO ICEBERG ===")
    print("="*60)

    tables_to_write = [(df, name) for df, name, _, _, _ in all_dataframes]
    write_start = time.time()

    write_to_iceberg(
        spark=spark,
        df_table_pairs=tables_to_write,
        database="local.iceberg_db",
        mode="overwrite",
        extra_options={"format-version": "2"}
    )
    write_time = time.time() - write_start
    print(f"\n⏱️ Write completed in {write_time:.2f} seconds")

    # ===============================================================
    # 6. VERIFY
    # ===============================================================
    print("\n" + "=" * 60)
    print("=== VERIFYING ICEBERG TABLES ===")
    print("=" * 60)
    verify_iceberg(spark, database="local.iceberg_db", sample_rows=3)

    # ===============================================================
    # 7. PERFORMANCE SUMMARY
    # ===============================================================
    total_time = time.time() - start_time

    print("\n" + "=" * 60)
    print("=== PERFORMANCE SUMMARY ===")
    print("=" * 60)
    print(f"Total execution time: {total_time:.2f} seconds")
    print(f"  - Data loading: {load_time:.2f}s")
    print(f"  - Iceberg write: {write_time:.2f}s")
    print(f"  - Other operations: {(total_time - load_time - write_time):.2f}s")

    if ENABLE_ARROW and arrow_enabled == "true":
        print(f"\n✓ PyArrow optimization: ENABLED")
        print(f"  - Batch size: {ARROW_BATCH_SIZE:,}")
        print(f"  - This significantly speeds up pandas conversions")
    else:
        print(f"\n⚠️ PyArrow optimization: DISABLED")
        print(f"  - Install PyArrow for better performance:")
        print(f"    pip install pyarrow>=14.0.0")

    rows_per_second = total_rows / total_time if total_time > 0 else 0
    print(f"\nThroughput: {rows_per_second:,.0f} rows/second")

    # ===============================================================
    # 8. CLEANUP
    # ===============================================================
    print("\n" + "=" * 60)
    print("✅ SUCCESS: PIPELINE COMPLETE!")
    print("=" * 60)

    # Unpersist all DataFrames to free memory
    for df, _, _, _, _ in all_dataframes:
        df.unpersist()

    input("\nPress Enter to exit...")
    time.sleep(1)
    sparkconnect.stop()


if __name__ == "__main__":
    main()