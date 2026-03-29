import pytest
from src.config.dataset_config import DatasetConfig


class TestDatasetConfig:

    def test_load_yaml_populates_raw_config(self):
        config = DatasetConfig(dataset_name="orders", layer="silver")
        config.load_yaml()
        assert config.raw_config != {}

    def test_target_table_returns_correct_value(self):
        config = DatasetConfig(dataset_name="orders", layer="silver")
        config.load_yaml()
        assert config.target_table == "orders"

    def test_raises_for_invalid_layer(self):
        config = DatasetConfig(dataset_name="orders", layer="platinum")
        with pytest.raises(ValueError, match="Invalid layer"):
            config.load_yaml()

    def test_raises_for_nonexistent_dataset(self):
        config = DatasetConfig(dataset_name="nonexistent_dataset", layer="silver")
        with pytest.raises(FileNotFoundError):
            config.load_yaml()
