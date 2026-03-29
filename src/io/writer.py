from pyspark.sql import DataFrame

class DataFrameWriter:

    def __init__(self, spark):
        self.spark = spark

    def write_delta_stream(self, df: DataFrame, catalog: str, schema: str, table: str, checkpoint_location: str, mode: str = "append") -> None:
        query = (
            df.writeStream
                .format("delta")
                .outputMode(mode)
                .option("checkpointLocation", checkpoint_location)
                .trigger(availableNow=True)
                .toTable(f"{catalog}.{schema}.{table}")
        )
        query.awaitTermination()

    def write_stream_with_batch(self, df: DataFrame, checkpoint_location: str, foreach_batch_fn) -> None:
        query = (
            df.writeStream
                .foreachBatch(foreach_batch_fn)
                .option("checkpointLocation", checkpoint_location)
                .trigger(availableNow=True)
                .start()
        )
        query.awaitTermination()

    def write_delta_batch(self, df: DataFrame, catalog: str, schema: str, table: str, mode: str = "append") -> None:
        (
            df
            .write
            .format("delta")
            .mode(mode)
            .saveAsTable(f"{catalog}.{schema}.{table}")
        )

    def upsert_table(self, df: DataFrame, target_table: str, pk_columns: list) -> None:
        table_exists = self.spark.catalog.tableExists(target_table)

        if not table_exists:
            df.write.format("delta").mode("overwrite").saveAsTable(target_table)
            return

        df.createOrReplaceTempView("source")

        pk_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_columns])

        all_columns = df.columns
        update_columns = [col for col in all_columns if col not in pk_columns]

        vals_update = ", ".join([f"{col} = source.{col}" for col in update_columns])
        cols_insert = ", ".join(all_columns)
        vals_insert = ", ".join([f"source.{col}" for col in all_columns])

        self.spark.sql(f"""
            MERGE INTO {target_table} target
            USING source
            ON {pk_condition}
            WHEN MATCHED THEN UPDATE SET {vals_update}
            WHEN NOT MATCHED THEN INSERT ({cols_insert}) VALUES ({vals_insert})
        """)