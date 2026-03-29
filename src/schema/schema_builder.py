from typing import Dict, Any, Optional
from pyspark.sql.types import (
    DateType, StructType, StructField, DataType, 
    StringType, IntegerType, LongType, DoubleType, TimestampType
)

def resolve_column_type(type_name: str) -> Optional[DataType]:

    type_mapping: Dict[str, DataType] = {
        "string": StringType(),
        "int": IntegerType(),
        "long": LongType(),
        "double": DoubleType(),
        "timestamp": TimestampType()
    }
    
    return type_mapping.get(type_name)


def build_struct_schema(schema: Dict[str, Any]) -> StructType:
  
    struct_type = StructType()

    for column_config in schema:
        column_name = column_config.get("column")
        type_name = column_config.get("type")
        is_nullable = column_config.get("nullable", True)
        
        if not type_name:
            raise ValueError(f"Column '{column_name}' missing 'type' definition.")
        
        pyspark_type = resolve_column_type(type_name)
        
        if pyspark_type is None:
            raise ValueError(f"Type '{type_name}' not suport. Column: '{column_name}'.")

        field = StructField(
            name=column_name,
            dataType=pyspark_type,
            nullable=is_nullable
        )
        struct_type.add(field)
    
    return struct_type