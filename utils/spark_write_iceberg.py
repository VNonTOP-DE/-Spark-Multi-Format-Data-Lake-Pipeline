# utils/spark_write_iceberg.py
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from typing import List, Dict, Tuple, Optional
import logging

log = logging.getLogger(__name__)


def write_to_iceberg(
    spark: SparkSession,
    df_table_pairs: List[Tuple[DataFrame, str]],
    database: str = "local_iceberg_db",
    mode: str = "overwrite",
    partition_by: Optional[Dict[str, List[str]]] = None,
    extra_options: Optional[Dict[str, str]] = None,
) -> None:
    """
    Write a list of (DataFrame, table_name) pairs to Iceberg.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session (Iceberg catalog must be configured).
    df_table_pairs : list of (DataFrame, str)
        e.g. [(df_json, "json_products"), (df_csv, "csv_data"), ...]
    database : str, default "local.iceberg_db"
        Target Iceberg database.
    mode : str, default "overwrite"
        Spark write mode ("overwrite", "append", "ignore", ...).
    partition_by : dict, optional
        Mapping table_name → list of columns to partition by.
        Example: {"csv_data": ["year", "month"]}
    extra_options : dict, optional
        Additional Iceberg write options, e.g. {"format-version": "2"}.
    """

    #0. Drop Database
    # ------------------------------------------------------------------
    try:
        spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
        log.info("Existing database %s dropped (CASCADE)", database)
    except Exception as e:
        log.warning("Failed to drop database %s (might not exist): %s", database, e)


    # 1. Ensure database exists
    # ------------------------------------------------------------------

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    log.info("Database %s ready", database)

    # ------------------------------------------------------------------
    # 2. Default options (you can override via extra_options)
    # ------------------------------------------------------------------
    default_opts = {
        "format-version": "2",
        # you can add more defaults here
    }
    if extra_options:
        default_opts.update(extra_options)

    # ------------------------------------------------------------------
    # 3. Write each DataFrame
    # ------------------------------------------------------------------
    for df, tbl in df_table_pairs:
        full_name = f"{database}.{tbl}"
        writer = df.write.mode(mode)

        # ---- partitioning (optional) ----
        if partition_by and tbl in partition_by:
            cols = partition_by[tbl]
            writer = writer.partitionBy(*cols)
            log.info("Partitioning %s by %s", full_name, cols)

        # ---- Iceberg options ----
        for k, v in default_opts.items():
            writer = writer.option(k, v)

        log.info("Writing %s rows → %s (mode=%s)", full_name, mode)
        writer.saveAsTable(full_name)

    log.info("All %d tables written to Iceberg!", len(df_table_pairs))


# ----------------------------------------------------------------------
def verify_iceberg(
    spark: SparkSession,
    database: str = "local_iceberg_db",
    table_names: Optional[List[str]] = None,
    sample_rows: int = 3,
) -> None:
    """
    Quick verification: list tables + show a few rows + row-count.

    Parameters
    ----------
    spark : SparkSession
    database : str
    table_names : list, optional
        If None → auto-detect all tables in the DB.
    sample_rows : int, default 3
    """
    # ------------------------------------------------------------------
    # 1. Get table list
    # ------------------------------------------------------------------
    tables_df = spark.sql(f"SHOW TABLES IN {database}")
    if table_names is None:
        table_names = [row.tableName for row in tables_df.collect()]
    else:
        # filter only existing ones
        existing = {row.tableName for row in tables_df.collect()}
        table_names = [t for t in table_names if t in existing]

    if not table_names:
        log.warning("No tables found in %s", database)
        return

    tables_df.select("tableName").show(truncate=False)
    log.info("Found %d table(s) in %s", len(table_names), database)

    # ------------------------------------------------------------------
    # 2. Sample + count each table
    # ------------------------------------------------------------------
    for tbl in table_names:
        full = f"{database}.{tbl}"
        print(f"\n--- {tbl} (sample {sample_rows} rows) ---")
        spark.sql(f"SELECT * FROM {full} LIMIT {sample_rows}").show(truncate=False)

        cnt = spark.table(full).count()
        print(f"Rows in {tbl}: {cnt:,}")


# ----------------------------------------------------------------------
if __name__ == "__main__":
    # Tiny sanity-check when the file is executed directly
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    print("Spark version:", spark.version)
    verify_iceberg(spark)