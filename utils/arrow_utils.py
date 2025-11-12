# ===============================================================
# UTILITY FUNCTIONS FOR PYARROW
# ===============================================================
import logging

log = logging.getLogger(__name__)

def spark_to_pandas_arrow(df, batch_size: int = 10000):
    """
    Convert Spark DataFrame to Pandas with Arrow optimization

    This is significantly faster than df.toPandas() for large datasets

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame
    batch_size : int
        Number of rows per batch (not used but kept for API compatibility)

    Returns
    -------
    pandas.DataFrame
    """
    try:
        import pyarrow
        # Use Arrow for fast conversion
        return df.toPandas()
    except ImportError:
        log.warning("PyArrow not available, using standard conversion")
        return df.toPandas()


def pandas_to_spark_arrow(spark, pdf, schema=None):
    """
    Convert Pandas DataFrame to Spark with Arrow optimization

    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    pdf : pandas.DataFrame
        Pandas DataFrame
    schema : StructType, optional
        Spark schema

    Returns
    -------
    pyspark.sql.DataFrame
    """
    try:
        import pyarrow
        if schema:
            return spark.createDataFrame(pdf, schema=schema)
        else:
            return spark.createDataFrame(pdf)
    except ImportError:
        log.warning("PyArrow not available, using standard conversion")
        return spark.createDataFrame(pdf, schema=schema)


def optimize_dataframe_for_arrow(df):
    """
    Optimize DataFrame for Arrow operations

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame

    Returns
    -------
    pyspark.sql.DataFrame
        Optimized DataFrame
    """
    # Coalesce small partitions
    num_partitions = df.rdd.getNumPartitions()

    if num_partitions > 200:
        # Too many small partitions - coalesce
        optimal_partitions = min(200, max(1, df.count() // 10000))
        df = df.coalesce(optimal_partitions)

    return df

