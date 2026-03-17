from dataclasses import dataclass, field
from typing import Dict, Any
from pathlib import Path
import yaml
import os

@dataclass
class DatasetConfig:

    dataset_name: str
    layer: str
    raw_config: Dict[str, Any] = field(default_factory=dict)
    project_root = Path(__file__).resolve().parents[3]

    def load_yaml(self):
        
        if self.layer not in {"bronze", "silver", "gold"}:
            raise ValueError(f"Invalid layer: {self.layer}. Expected: bronze, silver or gold.")

        config_path = self.project_root / "conf" / self.layer / f"{self.dataset_name}.yaml"        

        if not config_path.exists():
            raise FileNotFoundError(f"File not found: {config_path}")
            
        with open(config_path, "r", encoding="utf-8") as f:
            self.raw_config = yaml.safe_load(f)
            self._validate()

    @property
    def target_config(self) -> Dict[str, Any]:
        return self.raw_config.get("target", {})

    @property
    def target_table(self) -> str:
        return self.target_config.get("table")
        
    @property
    def target_schema(self) -> str:
        return self.target_config.get("schema")

    @property
    def target_catalog(self) -> str:
        return self.target_config.get("catalog")