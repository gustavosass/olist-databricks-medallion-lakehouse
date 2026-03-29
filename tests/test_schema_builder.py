import pytest
from pyspark.sql.types import (
    StructType, StringType, IntegerType, LongType, DoubleType, TimestampType
)
from src.schema.schema_builder import resolve_column_type, build_struct_schema


class TestResolveColumnType:

    def test_returns_correct_type_for_known_types(self):
        assert isinstance(resolve_column_type("string"), StringType)
        assert isinstance(resolve_column_type("int"), IntegerType)
        assert isinstance(resolve_column_type("long"), LongType)
        assert isinstance(resolve_column_type("double"), DoubleType)
        assert isinstance(resolve_column_type("timestamp"), TimestampType)

    def test_returns_none_for_unknown_type(self):
        assert resolve_column_type("invalid_type") is None
        assert resolve_column_type("") is None


class TestBuildStructSchema:

    def test_returns_struct_type(self):
        schema_config = [{"column": "order_id", "type": "string", "nullable": False}]
        result = build_struct_schema(schema_config)
        assert isinstance(result, StructType)

    def test_field_name_and_type_are_mapped_correctly(self):
        schema_config = [
            {"column": "order_id", "type": "string", "nullable": False},
            {"column": "customer_id", "type": "string", "nullable": True},
            {"column": "freight_value", "type": "double", "nullable": True},
        ]
        result = build_struct_schema(schema_config)

        assert len(result.fields) == 3
        assert result["order_id"].dataType == StringType()
        assert result["order_id"].nullable is False
        assert result["freight_value"].dataType == DoubleType()

    def test_raises_value_error_for_missing_type(self):
        schema_config = [{"column": "order_id", "nullable": True}]
        with pytest.raises(ValueError, match="missing 'type'"):
            build_struct_schema(schema_config)

    def test_raises_value_error_for_unsupported_type(self):
        schema_config = [{"column": "order_id", "type": "unsupported", "nullable": True}]
        with pytest.raises(ValueError, match="not suport"):
            build_struct_schema(schema_config)
