from pyspark.sql import SparkSession, DataFrame
from typing import Dict
import os

class DataFrameReader:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.is_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ
    
    def read_file(self, file_path: str, file_format: str, options: Dict[str, str]) -> DataFrame:

        if self.is_databricks:
            return (
                self.spark.readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", file_format)
                    .options(**options)
                    .load(file_path)
            )
        else:
            return (
                self.spark.read
                    .format(file_format)
                    .options(**options)
                    .load(file_path)
            )
    
    def read_table(self, catalog: str, schema: str, table: str) -> DataFrame:
        if self.is_databricks:
            path_table = f"{catalog}.{schema}.{table}"
        else:
            path_table = f"{schema}.{table}"
        
        return self.spark.read.table(path_table)