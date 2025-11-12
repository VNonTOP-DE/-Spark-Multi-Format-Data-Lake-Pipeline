"""Catalog management utilities"""

from pyspark.sql import SparkSession
from typing import List, Tuple


class CatalogManager:
    """Manage Iceberg catalog operations"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def list_tables(self, catalog: str, database: str) -> List[Tuple[str, str]]:
        """List all tables in a catalog.database"""
        try:
            tables_df = self.spark.sql(f"SHOW TABLES IN {catalog}.{database}")
            tables = [(row.namespace, row.tableName) for row in tables_df.collect()]
            return tables
        except Exception as e:
            print(f"Error listing tables in {catalog}.{database}: {e}")
            self._try_list_namespaces(catalog)
            return []

    def _try_list_namespaces(self, catalog: str):
        """Helper to list available namespaces"""
        try:
            namespaces = self.spark.sql(f"SHOW NAMESPACES IN {catalog}").collect()
            print(f"Available namespaces in '{catalog}': {[row.namespace for row in namespaces]}")
        except:
            print(f"Cannot list namespaces in '{catalog}'")

    def create_namespace(self, catalog: str, database: str):
        """Create namespace if not exists"""
        try:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{database}")
            print(f"✓ Namespace {catalog}.{database} ready")
        except Exception as e:
            print(f"Note: {e}")

    def table_exists(self, full_table_name: str) -> bool:
        """Check if table exists"""
        try:
            self.spark.table(full_table_name)
            return True
        except:
            return False

    def get_table_count(self, full_table_name: str) -> int:
        """Get row count for a table"""
        return self.spark.table(full_table_name).count()