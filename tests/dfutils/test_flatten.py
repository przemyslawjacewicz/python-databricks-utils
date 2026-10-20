from pyspark.sql.types import StructType, StructField, LongType, ArrayType, MapType, StringType

from python_spark_utils.dfutils.flatten import flatten_dataframe
from ..tests_utils import assert_dataframe_equal


def test_flatten_dataframe__StructField(spark):
    assert_dataframe_equal(
        flatten_dataframe(spark.createDataFrame([(1,)]), "@"),
        spark.createDataFrame(
            [(1,)],
            StructType([StructField("_1", LongType())])
        )
    )


def test_flatten_dataframe__StructType_StructField(spark):
    assert_dataframe_equal(
        flatten_dataframe(spark.createDataFrame([((1,),)]), "@"),
        spark.createDataFrame(
            [(1,)],
            StructType([StructField("_1@_1", LongType())])
        )
    )


def test_flatten_dataframe__StructType_StructType_StructField(spark):
    assert_dataframe_equal(
        flatten_dataframe(spark.createDataFrame([(((1,),),)]), "@"),
        spark.createDataFrame(
            [(1,)],
            StructType([StructField("_1@_1@_1", LongType())])
        )
    )


def test_flatten_dataframe__ArrayType_with_DataType(spark):
    assert_dataframe_equal(
        flatten_dataframe(spark.createDataFrame([([1],)]), "@"),
        spark.createDataFrame(
            [([1],)],
            StructType([StructField("_1", ArrayType(LongType()))])
        )
    )


def test_flatten_dataframe__ArrayType_with_StructType_StructField(spark):
    assert_dataframe_equal(
        flatten_dataframe(spark.createDataFrame([([(1,)],)]), "@"),
        spark.createDataFrame(
            [([(1,)],)],
            StructType([StructField("_1", ArrayType(StructType([StructField("_1", LongType())])))])
        ),
        ignore_nullable=True
    )


def test_flatten_dataframe__ArrayType_with_StructType_StructType_StructField(spark):
    assert_dataframe_equal(
        flatten_dataframe(spark.createDataFrame([([((1,),)],)]), "@"),
        spark.createDataFrame(
            [([(1,)],)],
            StructType([StructField("_1", ArrayType(StructType([StructField("_1@_1", LongType())])))])
        )
    )


def test_flatten_dataframe__MapType_with_DataType(spark):
    assert_dataframe_equal(
        flatten_dataframe(spark.createDataFrame([({1: "a"},)]), "@"),
        spark.createDataFrame(
            [({1: "a"},)],
            StructType([StructField("_1", MapType(LongType(), StringType()))])
        )
    )


def test_flatten_dataframe__MapType_with_StructType_StructField(spark):
    assert_dataframe_equal(
        flatten_dataframe(spark.createDataFrame([({(1,): ("a",)},)]), "@"),
        spark.createDataFrame(
            [({(1,): ("a",)},)],
            StructType(
                [
                    StructField("_1", MapType(
                        StructType([StructField("_1", LongType())]),
                        StructType([StructField("_1", StringType())])
                    ))
                ]
            )
        )
    )


def test_flatten_dataframe__MapType_with_StructType_StructType_StructField(spark):
    assert_dataframe_equal(
        flatten_dataframe(spark.createDataFrame([({((1,),): (("a",),)},)]), "@"),
        spark.createDataFrame(
            [({(1,): ("a",)},)],
            StructType(
                [
                    StructField("_1", MapType(
                        StructType([StructField("_1@_1", LongType())]),
                        StructType([StructField("_1@_1", StringType())])
                    ))
                ]
            )
        )
    )
