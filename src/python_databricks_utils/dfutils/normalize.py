import uuid

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StructType


def normalize_col(col: Column | str, schema: StructType) -> Column:
    return F.from_json(F.to_json(col), schema)


# spark.createDataFrame(arg.withColumn(c, normalize_column(F.struct("*"), schema)).select(f"{c}.*").rdd, schema)
def normalize_dataframe(df: DataFrame, schema: StructType) -> DataFrame:
    c = str(uuid.uuid4())
    return df.withColumn(c, normalize_col(F.struct("*"), schema)).select(f"{c}.*")



def normalize_string_col(arg: Column | str) -> Column:
    if isinstance(arg, Column):
        def normalize_accents(text: str) -> str:
            match text:
                case str(t) if t is not None:
                    nfkd_form = unicodedata.normalize('NFKD', t)
                    return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])
                case _:
                    return text

        normalize_accents_udf = F.udf(normalize_accents, StringType())
        return normalize_accents_udf(F.trim(F.upper(arg.cast(StringType()))))
    else:
        return normalize_string_col(F.col(arg))
