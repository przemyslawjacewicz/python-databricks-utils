def get_df_or_empty(path: str, schema: StructType) -> DataFrame:
    try:
        return spark.read.format("delta").load(path)
    except Exception as e:
        print(f"{e=}")
        return spark.createDataFrame([], schema)
