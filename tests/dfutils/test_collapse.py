from pyspark.sql.types import StructType, StructField, LongType, ArrayType

from python_spark_utils.dfutils.collapse import collapse_dataframe
from ..tests_utils import assert_dataframe_equal


def test_collapse__StructField(spark):
    df = spark.createDataFrame(
        [(1,)],
        StructType(
            [
                StructField("_1", LongType())
            ]
        )
    )

    assert_dataframe_equal(
        collapse_dataframe(df, "@"),
        spark.createDataFrame(
            [(1,)],
            StructType(
                [
                    StructField("_1", LongType())
                ]
            )
        )
    )


def test_collapse__StructType_StructField(spark):
    df = spark.createDataFrame(
        [(1,)],
        StructType(
            [
                StructField("_1@_2", LongType())
            ]
        )
    )

    assert_dataframe_equal(
        collapse_dataframe(df, "@"),
        spark.createDataFrame(
            [((1,),)],
            StructType(
                [
                    StructField("_1", StructType(
                        [
                            StructField("_2", LongType())
                        ]
                    ), nullable=False)
                ]
            )
        )
    )


def test_collapse__StructType_StructType_StructField(spark):
    df = spark.createDataFrame(
        [(1,)],
        StructType(
            [
                StructField("_1@_2@_3", LongType())
            ]
        )
    )

    assert_dataframe_equal(
        collapse_dataframe(df, "@"),
        spark.createDataFrame(
            [(((1,),),)],
            StructType(
                [
                    StructField("_1", StructType(
                        [
                            StructField("_2", StructType(
                                [
                                    StructField("_3", LongType())
                                ]
                            ), nullable=False)
                        ]
                    ), nullable=False)
                ]
            )
        )
    )


def test_collapse__StructType_StructType_StructType_StructField(spark):
    df = spark.createDataFrame(
        [(1,)],
        StructType(
            [
                StructField("_1@_2@_3@_4", LongType())
            ]
        )
    )

    assert_dataframe_equal(
        collapse_dataframe(df, "@"),
        spark.createDataFrame(
            [((((1,),),),)],
            StructType(
                [
                    StructField("_1", StructType(
                        [
                            StructField("_2", StructType(
                                [
                                    StructField("_3", StructType(
                                        [
                                            StructField("_4", LongType())
                                        ]
                                    ), nullable=False),
                                ]
                            ), nullable=False)
                        ]
                    ), nullable=False)
                ]
            )
        )
    )


# arraytype with datatype

def test_collapse__ArrayType_with_StructType_StructType_StructField(spark):
    df = spark.createDataFrame(
        [([(1,)],)],
        StructType(
            [
                StructField("_1", ArrayType(
                    StructType(
                        [
                            StructField("_1@_2", LongType())
                        ]
                    ))
                            )
            ]
        )
    )

    assert_dataframe_equal(
        collapse_dataframe(df, "@"),
        spark.createDataFrame(
            [((1,),)],
            StructType(
                [
                    StructField("_1", ArrayType(StructType(
                        [
                            StructField("_1", StructType(
                                [
                                    StructField("_2", LongType())
                                ]
                            ))
                        ]
                    )))
                ]
            )
        )
    )
