import os
import logging
from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "spark-warehouse") -> SparkSession:

    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return SparkSession.builder.getOrCreate()
    
    logging.warning("[LOCAL] Running in local mode.")
    
    return (SparkSession.builder
            .master("local[2]")
            .appName(app_name)
            .config("spark.sql.warehouse.dir", os.path.abspath("spark-warehouse"))
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") 
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate())