from dataclasses import dataclass
from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, input_file_name
from src.config.dataset_config import DatasetConfig


@dataclass
class BronzeConfig(DatasetConfig):

    def _validate(self) -> None:
        if "file" not in self.raw_config or "format" not in self.raw_config["file"]:
            raise ValueError("Block 'file' with 'format' is required in the configuration.")
            
    @property
    def file_config(self) -> Dict[str, Any]:
        return self.raw_config.get("file", {})

    @property
    def file_format(self) -> str:
        file_cfg = self.file_config
        if "format" not in file_cfg:
            raise ValueError("File format not specified in configuration.")
        
        return file_cfg.get("format").lower()

    @property
    def spark_options(self) -> Dict[str, str]:
        file_cfg = self.file_config
        options = {}
        
        for key, value in file_cfg.items():
            if key == "format":
                continue
            
            if isinstance(value, bool):
                options[key] = str(value).lower()
            else:
                options[key] = str(value)
        
        return options
    
    def add_metadata(self, df: DataFrame, ingest_ts) -> DataFrame:
        df_ingest = (df
            .withColumn("_ingest_ts", lit(ingest_ts))
            .withColumn("_source_file_path", col("_metadata.file_path"))
        )
        return df_ingest


