import pyspark.sql.functions as F
from pyspark.sql.types import StructType, LongType, StructField, IntegerType

from python_spark_utils.dfutils.with_column import with_column
from ..tests_utils import assert_dataframe_equal


def test_with_column__new_StructField(spark):
    df = spark.createDataFrame([(1,)])

    assert_dataframe_equal(
        with_column(df, ["_2"], F.lit(2)),
        spark.createDataFrame(
            [(1, 2,)],
            StructType(
                [
                    StructField("_1", LongType()),
                    StructField("_2", IntegerType(), nullable=False)
                ]
            )
        )
    )


def test_with_column__new_StructType_StructField(spark):
    df = spark.createDataFrame([(1,)])

    assert_dataframe_equal(
        with_column(df, ["_2", "_3"], F.lit(2)),
        spark.createDataFrame(
            [(1, (2,),)],
            StructType(
                [
                    StructField("_1", LongType()),
                    StructField("_2", StructType(
                        [
                            StructField("_3", IntegerType(), nullable=False)
                        ]
                    ), nullable=False),
                ]
            )
        )
    )


def test_with_column__new_StructType_StructType_StructField(spark):
    df = spark.createDataFrame([(1,)])

    assert_dataframe_equal(
        with_column(df, ["_2", "_3", "_4"], F.lit(2)),
        spark.createDataFrame(
            [(1, ((2,),),)],
            StructType(
                [
                    StructField("_1", LongType()),
                    StructField("_2", StructType(
                        [
                            StructField("_3", StructType(
                                [
                                    StructField("_4", IntegerType(), nullable=False)
                                ]
                            ), nullable=False)
                        ]
                    ), nullable=False),
                ]
            )
        )
    )


def test_with_column__add_StructType_StructField(spark):
    df = spark.createDataFrame(
        [((1,),)],
        StructType(
            [
                StructField("_1", StructType(
                    [
                        StructField("_2", LongType()),
                    ]
                ))
            ]
        )
    )

    assert_dataframe_equal(
        with_column(df, ["x"], F.lit(2)),
        spark.createDataFrame(
            [((1,), 2,)],
            StructType(
                [
                    StructField("_1", StructType(
                        [
                            StructField("_2", LongType())
                        ]
                    )),
                    StructField("x", IntegerType(), nullable=False)
                ]
            )
        )
    )
    assert_dataframe_equal(
        with_column(df, ["_1", "x"], F.lit(2)),
        spark.createDataFrame(
            [((1, 2,),)],
            StructType(
                [
                    StructField("_1", StructType(
                        [
                            StructField("_2", LongType()),
                            StructField("x", IntegerType(), nullable=False)
                        ]
                    )),
                ]
            )
        )
    )


def test_with_column__add_StructType_StructType_StructField(spark):
    df = spark.createDataFrame(
        [(((1,),),)],
        StructType(
            [
                StructField("_1", StructType(
                    [
                        StructField("_2", StructType(
                            [
                                StructField("_3", LongType())
                            ]
                        ))
                    ]
                ))
            ]
        )
    )

    assert_dataframe_equal(
        with_column(df, ["x"], F.lit(2)),
        spark.createDataFrame(
            [(((1,),), 2,)],
            StructType(
                [
                    StructField("_1", StructType(
                        [
                            StructField("_2", StructType(
                                [
                                    StructField("_3", LongType())
                                ]
                            ))
                        ]
                    )),
                    StructField("x", IntegerType(), nullable=False)
                ]
            )
        )
    )
    assert_dataframe_equal(
        with_column(df, ["_1", "x"], F.lit(2)),
        spark.createDataFrame(
            [(((1,), 2,),)],
            StructType(
                [
                    StructField("_1", StructType(
                        [
                            StructField("_2", StructType(
                                [
                                    StructField("_3", LongType())
                                ]
                            )),
                            StructField("x", IntegerType(), nullable=False)
                        ]
                    ))
                ]
            )
        )
    )
    assert_dataframe_equal(
        with_column(df, ["_1", "_2", "x"], F.lit(2)),
        spark.createDataFrame(
            [(((1, 2,),),)],
            StructType(
                [
                    StructField("_1", StructType(
                        [
                            StructField("_2", StructType(
                                [
                                    StructField("_3", LongType()),
                                    StructField("x", IntegerType(), nullable=False)
                                ]
                            ))
                        ]
                    ))
                ]
            )
        )
    )


def test_with_column__add_StructType_StructType_StructType_StructType_StructField(spark):
    df = spark.createDataFrame(
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
                                ))
                            ]
                        ))
                    ]
                ))
            ]
        ))

    assert_dataframe_equal(
        with_column(df, ["x"], F.lit(2)),
        spark.createDataFrame(
            [((((1,),),), 2,)],
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
                                    ))
                                ]
                            ))
                        ]
                    )),
                    StructField("x", IntegerType(), nullable=False)
                ]
            )

        )
    )
    assert_dataframe_equal(
        with_column(df, ["_1", "x"], F.lit(2)),
        spark.createDataFrame(
            [((((1,),), 2,),)],
            StructType(
                [
                    StructField("_1", StructType(
                        [
                            StructField("_2", StructType(
                                [
                                    StructField("_3", StructType(
                                        [
                                            StructField("_4", LongType()),
                                        ]
                                    ))
                                ]
                            )),
                            StructField("x", IntegerType(), nullable=False)
                        ]
                    ))
                ]
            )

        )
    )
    assert_dataframe_equal(
        with_column(df, ["_1", "_2", "x"], F.lit(2)),
        spark.createDataFrame(
            [((((1,), 2,),),)],
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
                                    )),
                                    StructField("x", IntegerType(), nullable=False)
                                ]
                            ))
                        ]
                    ))
                ]
            )

        )
    )
    assert_dataframe_equal(
        with_column(df, ["_1", "_2", "_3", "x"], F.lit(2)),
        spark.createDataFrame(
            [((((1, 2,),),),)],
            StructType(
                [
                    StructField("_1", StructType(
                        [
                            StructField("_2", StructType(
                                [
                                    StructField("_3", StructType(
                                        [
                                            StructField("_4", LongType()),
                                            StructField("x", IntegerType(), nullable=False)
                                        ]
                                    ))
                                ]
                            ))
                        ]
                    ))
                ]
            )

        )
    )


def test_with_column__overwrite_StructField(spark):
    df = spark.createDataFrame([(1,)])

    assert_dataframe_equal(
        with_column(df, ["_1"], F.lit(2)),
        spark.createDataFrame(
            [(2,)],
            StructType(
                [
                    StructField("_1", IntegerType(), nullable=False)
                ]
            )

        )
    )


def test_with_column__overwrite_StructType_StructField(spark):
    df = spark.createDataFrame([(1,)])

    assert_dataframe_equal(
        with_column(df, ["_1", "_2"], F.lit(2)),
        spark.createDataFrame(
            [((2,),)],
            StructType(
                [
                    StructField("_1", StructType(
                        [
                            StructField("_2", IntegerType(), nullable=False)
                        ]
                    ), nullable=False)
                ]
            )

        )
    )


def test_with_column__overwrite_StructType_StructType_StructField(spark):
    df = spark.createDataFrame([(1,)])

    assert_dataframe_equal(
        with_column(spark.createDataFrame([(1,)]), ["_1", "_2", "_3"], F.lit(2)),
        spark.createDataFrame(
            [(((2,),),)],
            StructType(
                [
                    StructField("_1", StructType(
                        [
                            StructField("_2", StructType(
                                [
                                    StructField("_3", IntegerType(), nullable=False)
                                ]
                            ), nullable=False)
                        ]
                    ), nullable=False)
                ]
            )
        )
    )
