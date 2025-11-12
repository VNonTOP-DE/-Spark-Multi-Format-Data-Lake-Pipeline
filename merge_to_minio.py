"""Merge Iceberg tables from local warehouse to MinIO"""

import sys
import atexit
import signal
import time

from config.spark_config import SparkConnect
from config.minio_config import MinIOConfig, CatalogConfig, SparkJarsConfig
from config.catalog_config import CatalogSetup
from utils.catalog_manager import CatalogManager
from utils.merge_utils import TableMerger

# ===============================================================
# CONFIGURATION
# ===============================================================
minio_cfg = MinIOConfig()
catalog_cfg = CatalogConfig()
jars = SparkJarsConfig.get_iceberg_jars()

# ===============================================================
# GLOBAL SPARK SESSION & CLEANUP
# ===============================================================
sparkconnect = None


def cleanup():
    """Graceful shutdown"""
    global sparkconnect
    if sparkconnect:
        print("\n🛑 Shutting down Spark...")
        try:
            sparkconnect.spark.catalog.clearCache()
            time.sleep(1)  # Give time for cleanup
            sparkconnect.stop()
        except Exception as e:
            print(f"⚠️  Warning during cleanup: {e}")


def signal_handler(sig, frame):
    """Handle Ctrl+C"""
    print("\n\n⚠️  Interrupted! Cleaning up...")
    cleanup()
    sys.exit(0)


# Register cleanup handlers
atexit.register(cleanup)
signal.signal(signal.SIGINT, signal_handler)


# ===============================================================
# MAIN EXECUTION
# ===============================================================
def main():
    global sparkconnect

    try:
        # ===============================================================
        # 1. CREATE SPARK SESSION
        # ===============================================================
        print("\n" + "=" * 70)
        print("🚀 INITIALIZING SPARK SESSION")
        print("=" * 70)

        # Spark configurations optimized to prevent JVM crashes
        spark_conf = {
            "spark.network.timeout": "1200s",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.shuffle.partitions": "50",
            # Memory settings to prevent crashes
            "spark.driver.maxResultSize": "2g",
            "spark.sql.files.maxPartitionBytes": "67108864",  # 64MB chunks
            "spark.memory.fraction": "0.8",
            "spark.memory.storageFraction": "0.3",
        }

        sparkconnect = SparkConnect(
            app_name="Merge-to-MinIO",
            master_url="local[2]",
            executor_cores=2,
            driver_memory="3g",
            num_executors=1,
            jar_packages=jars,
            log_level="WARN",
            iceberg_warehouse="warehouse",
            s3_config=minio_cfg.to_s3_config(),
            spark_conf=spark_conf  # Add custom Spark configurations
        )
        spark = sparkconnect.spark

        # ===============================================================
        # 2. SETUP CATALOGS
        # ===============================================================
        print("\n" + "=" * 70)
        print("⚙️  CONFIGURING CATALOGS")
        print("=" * 70)

        CatalogSetup.setup_local_catalog(
            spark,
            catalog_cfg.source_catalog,
            catalog_cfg.source_warehouse
        )

        # FIXED: Use S3FileIO (not HadoopFileIO) with region parameter
        CatalogSetup.setup_minio_catalog(
            spark,
            catalog_cfg.target_catalog,
            minio_cfg.endpoint,
            minio_cfg.bucket,
            minio_cfg.access_key,
            minio_cfg.secret_key,
            region="us-east-1"  # Required for S3FileIO
        )

        # ===============================================================
        # 3. LIST SOURCE TABLES
        # ===============================================================
        print("\n" + "=" * 70)
        print("📂 READING SOURCE TABLES")
        print("=" * 70)

        catalog_mgr = CatalogManager(spark)
        tables = catalog_mgr.list_tables(
            catalog_cfg.source_catalog,
            catalog_cfg.source_db
        )

        if not tables:
            print("❌ No tables found to merge!")
            sys.exit(0)

        print(f"\n✅ Found {len(tables)} table(s):")
        for ns, tbl in tables:
            print(f"   • {catalog_cfg.source_catalog}.{ns}.{tbl}")

        # ===============================================================
        # 4. PREPARE TARGET DATABASE
        # ===============================================================
        print("\n" + "=" * 70)
        print(f"🎯 PREPARING TARGET: {catalog_cfg.target_catalog}.{catalog_cfg.target_db}")
        print("=" * 70)

        catalog_mgr.create_namespace(
            catalog_cfg.target_catalog,
            catalog_cfg.target_db
        )

        # ===============================================================
        # 5. MERGE TABLES
        # ===============================================================
        print("\n" + "=" * 70)
        print("🔄 MERGING TABLES")
        print("=" * 70)

        merger = TableMerger(spark)
        success_count = 0

        for namespace, table_name in tables:
            source_table = f"{catalog_cfg.source_catalog}.{namespace}.{table_name}"
            target_table = f"{catalog_cfg.target_catalog}.{catalog_cfg.target_db}.{table_name}"

            print(f"\n{'─' * 60}")
            print(f"📋 Table: {table_name}")
            print(f"{'─' * 60}")
            print(f"   Source: {source_table}")
            print(f"   Target: {target_table}")

            try:
                # Read source
                df = spark.table(source_table)

                # Check if target exists
                target_exists = catalog_mgr.table_exists(target_table)

                # Execute merge
                result = merger.merge_table(df, source_table, target_table, target_exists)

                print(f"   Strategy: {result['strategy']}")
                print(f"   Source rows: {result['source_rows']:,}")
                print(f"   Final rows: {result['final_rows']:,}")
                print(f"   ✅ Success!")

                success_count += 1

            except Exception as e:
                print(f"   ❌ Failed: {e}")
                print(f"   Skipping to next table...")
                continue

        # ===============================================================
        # 6. VERIFICATION
        # ===============================================================
        print("\n" + "=" * 70)
        print("✅ MERGE COMPLETE - VERIFYING RESULTS")
        print("=" * 70)

        print(f"\n📊 Summary:")
        print(f"   Total tables: {len(tables)}")
        print(f"   Successfully merged: {success_count}")
        print(f"   Failed: {len(tables) - success_count}")

        print(f"\n📦 MinIO Details:")
        print(f"   Endpoint: {minio_cfg.endpoint}")
        print(f"   Bucket: {minio_cfg.bucket}")
        print(f"   Database: {catalog_cfg.target_db}")

        print(f"\n📋 Tables in MinIO:")
        target_tables = catalog_mgr.list_tables(
            catalog_cfg.target_catalog,
            catalog_cfg.target_db
        )

        for ns, tbl in target_tables:
            full_name = f"{catalog_cfg.target_catalog}.{catalog_cfg.target_db}.{tbl}"
            try:
                count = catalog_mgr.get_table_count(full_name)
                print(f"   ✓ {tbl}: {count:,} rows")
            except Exception as e:
                print(f"   ✗ {tbl}: Error reading ({e})")

        print("\n" + "=" * 70)
        print("🎉 ALL DONE!")
        print("=" * 70)

        input("\nPress Enter to exit...")

    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n{'=' * 70}")
        print("❌ ERROR OCCURRED")
        print(f"{'=' * 70}")
        print(f"{e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Cleanup will be called automatically via atexit
        pass


if __name__ == "__main__":
    main()