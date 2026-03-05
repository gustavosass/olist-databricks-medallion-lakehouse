from pyspark.sql. types import *

def get_type(type):

    types = {
        "string" : StringType(),
        "int" : IntegerType()
    }
    
    return types.get(type)

def build_schema(schema):

    structType = StructType()

    for column in schema.get("columns"):

        field = column.get("name")
        data_type = get_type(column.get("type"))
        nullable = column.get("nullable", True)

        if (data_type is None):
            raise ValueError(f"Type is not configured: '{column.get("type")}' Column: '{field}'")

        structType.add(
            field=field,
            data_type=data_type,
            nullable=nullable
        )

    return structType