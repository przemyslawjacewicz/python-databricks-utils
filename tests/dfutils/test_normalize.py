from pyspark.sql.types import StructType, StructField, LongType, Row

from python_spark_utils.dfutils.normalize import normalize_dataframe, normalize_col
from ..tests_utils import assert_schema_equal


def test_normalize_col__null(spark):
    df = spark.createDataFrame(
        [(None,)],
        StructType([StructField("_1", StructType([StructField("_1", LongType())]))])
    )

    schema = StructType([StructField("_1", LongType()), StructField("_2", LongType())])
    r = df.withColumn("_2", normalize_col("_1", schema))

    assert_schema_equal(
        r.schema,
        StructType(
            [
                StructField("_1", StructType(
                    [
                        StructField("_1", LongType())
                    ]
                )),
                StructField("_2", StructType(
                    [
                        StructField("_1", LongType()),
                        StructField("_2", LongType())
                    ]
                )),
            ]
        )
    )
    assert r.collect() == [Row(_1=None, _2=None)]


def test_normalize_col__no_change(spark):
    df = spark.createDataFrame(
        [((1,),)],
        StructType([StructField("_1", StructType([StructField("_1", LongType())]))])
    )

    schema = StructType([StructField("_1", LongType())])
    r = df.withColumn("_2", normalize_col("_1", schema))

    assert_schema_equal(
        r.schema,
        StructType(
            [
                StructField("_1", StructType(
                    [
                        StructField("_1", LongType())
                    ]
                )),
                StructField("_2", StructType(
                    [
                        StructField("_1", LongType())
                    ]
                )),
            ]
        )
    )
    assert r.collect() == [Row(_1=(1,), _2=(1,))]


def test_normalize_col__new_column(spark):
    df = spark.createDataFrame(
        [((1,),)],
        StructType([StructField("_1", StructType([StructField("_1", LongType())]))])
    )

    schema = StructType([StructField("_1", LongType()), StructField("_2", LongType())])
    r = df.withColumn("_2", normalize_col("_1", schema))

    assert_schema_equal(
        r.schema,
        StructType(
            [
                StructField("_1", StructType(
                    [
                        StructField("_1", LongType())
                    ]
                )),
                StructField("_2", StructType(
                    [
                        StructField("_1", LongType()),
                        StructField("_2", LongType())
                    ]
                )),
            ]
        )
    )
    assert r.collect() == [Row(_1=(1,), _2=(1, None))]


def test_normalize_dataframe__empty(spark):
    df = spark.createDataFrame([], StructType([]))

    schema = StructType(
        [
            StructField("_1", StructType(
                [
                    StructField("_1", LongType()),
                    StructField("_2", LongType())
                ]
            ))
        ]
    )

    r = normalize_dataframe(df, schema)

    r.show()
    assert r.schema == schema
    assert r.collect() == []


def test_normalize_dataframe__null(spark):
    df = spark.createDataFrame(
        [(None,)],
        StructType(
            [
                StructField("_1", StructType(
                    [
                        StructField("_1", LongType())
                    ]
                ))
            ]
        )
    )

    schema = StructType(
        [
            StructField("_1", StructType(
                [
                    StructField("_1", LongType()),
                    StructField("_2", LongType())
                ]
            ))
        ]
    )

    r = normalize_dataframe(df, schema)

    assert r.schema == schema
    assert r.collect() == [Row(_1=None)]


def test_normalize_dataframe__no_change(spark):
    df = spark.createDataFrame(
        [((1,),)],
        StructType(
            [
                StructField("_1", StructType(
                    [
                        StructField("_1", LongType())
                    ]
                ))
            ]
        )
    )

    schema = StructType(
        [
            StructField("_1", StructType(
                [
                    StructField("_1", LongType())
                ]
            ))
        ]
    )

    r = normalize_dataframe(df, schema)

    assert r.schema == schema
    assert r.collect() == [Row(_1=(1,))]


def test_normalize_dataframe__new_column(spark):
    df = spark.createDataFrame(
        [((1,),)],
        StructType(
            [
                StructField("_1", StructType(
                    [
                        StructField("_1", LongType())
                    ]
                ))
            ]
        )
    )

    schema = StructType(
        [
            StructField("_1", StructType(
                [
                    StructField("_1", LongType()),
                    StructField("_2", LongType())
                ]
            ))
        ]
    )

    r = normalize_dataframe(df, schema)

    assert r.schema == schema
    assert r.collect() == [Row(_1=(1, None))]
