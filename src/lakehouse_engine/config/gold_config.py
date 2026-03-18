from dataclasses import dataclass
from typing import Dict, Any
from .dataset_config import DatasetConfig

@dataclass
class GoldConfig(DatasetConfig):

    def _validate(self) -> None:
        if self.source_config is None:
            raise ValueError(f"Block 'source' is required for layer '{self.layer}' in the configuration.")

        if self.target_config is None:
            raise ValueError(f"Block 'target' is required for layer '{self.layer}' in the configuration.")
    
    @property
    def source_config(self) -> Dict[str, Any]:
        return self.raw_config.get("source", None)

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
    def select_columns(self) -> Dict[str, str]:
        return self.raw_config.get("columns", None)