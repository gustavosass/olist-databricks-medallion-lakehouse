from pyspark.sql import SparkSession, DataFrame
from typing import Dict
import os

class DataFrameReader:
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def read_file_stream(self, file_path: str, file_format: str, options: Dict[str, str], schema_location: str) -> DataFrame:
        return (
            self.spark.readStream
                        .format("cloudFiles")
                        .option("cloudFiles.format", file_format)
                        .option("cloudFiles.schemaLocation", schema_location)
                        .options(**options)
                        .load(file_path)
        )
    
    def read_table_stream(self, catalog: str, schema: str, table: str) -> DataFrame:
        return self.spark.readStream.table(f"{catalog}.{schema}.{table}")

    def read_table(self, catalog: str, schema: str, table: str) -> DataFrame:        
        return self.spark.read.table(f"{catalog}.{schema}.{table}")