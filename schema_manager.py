# schema_manager.py
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from typing import Optional, Tuple, List
import traceback

# ------------------------------------------------------------------
# PATHS
# ------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
CSV_FILE_PATH     = BASE_DIR / "data" / "csv"
DOCX_FILE_PATH    = BASE_DIR / "data" / "docx"
JSON_FILE_PATH    = BASE_DIR / "data" / "json"
PARQUET_FILE_PATH = BASE_DIR / "data" / "parquet"

# ------------------------------------------------------------------
# SAFE READERS
# ------------------------------------------------------------------
def _read_json(spark: SparkSession, filepath: Path) -> Tuple:
    try:
        print(f"  Reading JSON: {filepath.name}")
        df = spark.read \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .option("multiLine", "true") \
            .json(str(filepath))
        df = df.persist()
        rows = df.count()
        cols = len(df.columns)
        print(f"    Success: {rows:,} rows, {cols} cols")

        if "_corrupt_record" in df.columns:
            corrupt = df.filter(col("_corrupt_record").isNotNull())
            corrupt_count = corrupt.count()
            if corrupt_count > 0:
                print(f"    Warning: {corrupt_count} corrupt records")
                print("    Sample:")
                corrupt.select("_corrupt_record").show(1, truncate=False)
                df = df.filter(col("_corrupt_record").isNull())
            df = df.drop("_corrupt_record")

        return (df, f"json_{filepath.stem}", "JSON", rows, cols) if rows > 0 else None
    except Exception as e:
        print(f"    Error: {e}")
        return None


def _read_csv(spark: SparkSession, filepath: Path, max_cols=50000) -> Tuple:
    try:
        print(f"  Reading CSV: {filepath.name}")
        df = spark.read \
            .option("header", "true") \
            .option("maxColumns", max_cols) \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .option("inferSchema", "false") \
            .option("multiLine", "true") \
            .option("escape", '"') \
            .option("encoding", "UTF-8") \
            .csv(str(filepath))
        df = df.persist()
        rows = df.count()
        cols = len(df.columns)
        print(f"    Success: {rows:,} rows, {cols} cols")

        if "_corrupt_record" in df.columns:
            corrupt_count = df.filter(col("_corrupt_record").isNotNull()).count()
            if corrupt_count > 0:
                print(f"    Warning: {corrupt_count} corrupt records")
                df = df.filter(col("_corrupt_record").isNull())
            df = df.drop("_corrupt_record")

        return (df, f"csv_{filepath.stem}", "CSV", rows, cols) if rows > 0 else None
    except Exception as e:
        print(f"    Error: {e}")
        return None


def _read_parquet(spark: SparkSession, filepath: Path) -> Tuple:
    try:
        print(f"  Reading Parquet: {filepath.name}")
        df = spark.read.parquet(str(filepath)).persist()
        rows = df.count()
        cols = len(df.columns)
        print(f"    Success: {rows:,} rows, {cols} cols")
        return (df, f"parquet_{filepath.stem}", "PARQUET", rows, cols) if rows > 0 else None
    except Exception as e:
        print(f"    Error: {e}")
        return None


def _read_text(spark: SparkSession, filepath: Path) -> Tuple:
    try:
        print(f"  Reading Text: {filepath.name}")
        df = spark.read.text(str(filepath)).persist()
        rows = df.count()
        print(f"    Success: {rows:,} lines")
        return (df, f"text_{filepath.stem}", "TEXT", rows, 1) if rows > 0 else None
    except Exception as e:
        print(f"    Error: {e}")
        return None


# ------------------------------------------------------------------
# PUBLIC: Load all files
# ------------------------------------------------------------------
def load_all_sources(spark: SparkSession) -> List[Tuple]:
    """
    Returns list of:
        (DataFrame, table_name, source_type, row_count, col_count)
    """
    results = []

    # JSON
    print("\n" + "="*60 + "\n=== JSON ===\n" + "="*60)
    for f in JSON_FILE_PATH.glob("*.json"):
        item = _read_json(spark, f)
        if item: results.append(item)

    # CSV
    print("\n" + "="*60 + "\n=== CSV ===\n" + "="*60)
    for f in CSV_FILE_PATH.glob("*.csv"):
        item = _read_csv(spark, f)
        if item: results.append(item)

    # Parquet
    print("\n" + "="*60 + "\n=== PARQUET ===\n" + "="*60)
    for f in PARQUET_FILE_PATH.glob("*.parquet"):
        item = _read_parquet(spark, f)
        if item: results.append(item)

    # DOCX + TXT
    print("\n" + "="*60 + "\n=== TEXT (DOCX/TXT) ===\n" + "="*60)
    for f in list(DOCX_FILE_PATH.glob("*.docx")) + list(DOCX_FILE_PATH.glob("*.txt")):
        item = _read_text(spark, f)
        if item: results.append(item)

    return results



