from dataclasses import dataclass
from typing import Dict, Any, List, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, row_number, lit
from pyspark.sql.window import Window
from .dataset_config import DatasetConfig
from lakehouse_engine.schema import build_struct_schema

@dataclass
class SilverConfig(DatasetConfig):

    def _validate(self) -> None:
        if "schema" not in self.raw_config:
            raise ValueError(f"Block 'schema' is required for layer '{self.layer}' in the configuration.")
        self._validate_schema(self.raw_config["schema"])

    def _validate_schema(self, schema: List) -> None:
        if not isinstance(schema, list):
            raise ValueError("Schema must be a list of column definitions.")
        
        for col_config in schema:
            if not isinstance(col_config, dict):
                raise ValueError("Each schema element must be a dictionary.")
            
            if "column" not in col_config or "type" not in col_config or "nullable" not in col_config:
                raise ValueError("Each column must have 'column' and 'type' fields.")
            
            if not isinstance(col_config["column"], str) or not isinstance(col_config["type"], str):
                raise ValueError("Column name and type must be strings.")

            if not isinstance(col_config["nullable"], bool):
                raise ValueError(f"Column Nullable field must be a boolean. Found: {col_config['nullable']}")
    
    def apply_schema(self, df: DataFrame) -> DataFrame:
        struct = build_struct_schema(self.table_schema)
        
        schema_columns = [col_config["column"] for col_config in self.table_schema]
        metadata_columns = [c for c in df.columns if c.startswith("_")]

        df_selected = df.select(*schema_columns, *metadata_columns)
        
        for field in struct.fields:
            df_selected = df_selected.withColumn(field.name, df_selected[field.name].cast(field.dataType))
        
        for col_config in self.table_schema:
            col_name = col_config["column"]
            if not col_config["nullable"]:
                df_selected = df_selected.filter(col(col_name).isNotNull())
        
        return df_selected
    
    def deduplicate(self, df: DataFrame) -> DataFrame:
        pk_cols = [col(pk) for pk in self.primary_keys]
        version_col = self.version_column
        
        window_spec = Window.partitionBy(*pk_cols).orderBy(col(version_col).desc())
        df_dedup = (df
            .withColumn("_rn", row_number().over(window_spec))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )
        
        return df_dedup
    
    def add_metadata(self, df: DataFrame, run_id: str) -> DataFrame:
        df_metadata = (df
            .withColumn("_processed_ts", current_timestamp())
            .withColumn("_run_id", lit(run_id))
        )
        
        return df_metadata
    
    @property
    def source_config(self) -> Dict[str, Any]:
        return self.raw_config.get("source", {})

    @property
    def source_table(self) -> str:
        return self.source_config.get("table")

    @property
    def source_schema(self) -> str:
        return self.source_config.get("schema")

    @property
    def source_catalog(self) -> str:
        return self.source_config.get("catalog")
    
    @property
    def table_schema(self) -> List[Dict[str, Any]]:
        return self.raw_config.get("schema", [])

    @property
    def primary_keys(self) -> List[str]:
        keys = self.raw_config.get("keys", {}).get("primary_keys", "")
        return [k.strip() for k in keys.split(",")] if isinstance(keys, str) else keys

    @property
    def version_column(self) -> str:
        return self.raw_config.get("keys", {}).get("version_column", "_ingest_ts")