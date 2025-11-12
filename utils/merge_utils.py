"""Merge operations for Iceberg tables"""

from pyspark.sql import SparkSession, DataFrame
from typing import Optional
import time


class TableMerger:
    """Handle table merge operations"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.key_columns = ["id", "product_id", "sku", "code", "_id"]

    def find_key_column(self, df: DataFrame) -> Optional[str]:
        """Auto-detect primary key column"""
        for col in self.key_columns:
            if col in df.columns:
                return col
        return None

    def merge_table(self, df: DataFrame, source_table: str, target_table: str,
                    target_exists: bool) -> dict:
        """
        Merge source table into target using appropriate strategy

        CRITICAL: Uses SQL-based operations to prevent JVM crashes

        Returns:
            dict with keys: strategy, source_rows, final_rows
        """
        row_count = df.count()
        key_col = self.find_key_column(df)

        # ============================================
        # STRATEGY SELECTION (matching working code)
        # ============================================
        if key_col and target_exists:
            # MERGE INTO (upsert)
            strategy = f"MERGE (upsert) using key `{key_col}`"
            self._execute_merge(df, target_table, key_col)

        elif target_exists:
            # INSERT OVERWRITE (no key, but table exists)
            strategy = "INSERT OVERWRITE (replacing all data)"
            self._execute_insert_overwrite(df, target_table)

        else:
            # CREATE TABLE AS SELECT (new table)
            strategy = "CREATE TABLE AS SELECT"
            if key_col:
                strategy += f" (Key `{key_col}` available for future merges)"
            self._execute_create_table(df, target_table)

        # Clear cache after operation
        self.spark.catalog.clearCache()

        # Small delay to allow resources to be released
        time.sleep(0.5)

        # Get final count
        final_count = self.spark.table(target_table).count()

        return {
            "strategy": strategy,
            "source_rows": row_count,
            "final_rows": final_count
        }

    def _execute_merge(self, df: DataFrame, target_table: str, key_col: str):
        """Execute MERGE INTO operation (upsert)"""
        df.createOrReplaceTempView("source_changes")

        self.spark.sql(f"""
            MERGE INTO {target_table} t
            USING source_changes s
            ON t.`{key_col}` = s.`{key_col}`
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        self.spark.catalog.dropTempView("source_changes")

    def _execute_insert_overwrite(self, df: DataFrame, target_table: str):
        """Execute INSERT OVERWRITE (replace all data)"""
        df.createOrReplaceTempView("source_data")

        self.spark.sql(f"""
            INSERT OVERWRITE TABLE {target_table}
            SELECT * FROM source_data
        """)

        self.spark.catalog.dropTempView("source_data")

    def _execute_create_table(self, df: DataFrame, target_table: str):
        """Execute CREATE TABLE AS SELECT (new table)"""
        df.createOrReplaceTempView("source_data")

        self.spark.sql(f"""
            CREATE TABLE {target_table}
            USING iceberg
            AS SELECT * FROM source_data
        """)

        self.spark.catalog.dropTempView("source_data")