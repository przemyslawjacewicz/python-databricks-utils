from pyspark.sql.types import StructType, StructField, NullType, ArrayType, MapType, StringType

from python_spark_utils.dfutils.fields import map_field, foreach_field, exists_field
from ..tests_utils import assert_schema_equal

struct_field = StructField("_1", NullType())
struct_type = StructField("_2", StructType(
    [
        StructField("_1", NullType())
    ]
))
array_type_data_type = StructField("_3", ArrayType(NullType()))
array_type_struct_type = StructField("_4", ArrayType(
    StructType(
        [
            StructField("_1", NullType())
        ]
    )
))
map_type_data_type = StructField("_5", MapType(NullType(), NullType()))
map_type_struct_type = StructField("_6", MapType(
    StructType(
        [
            StructField("_1", NullType())
        ]
    ),
    StructType(
        [
            StructField("_1", NullType())
        ]
    )
))
schema = StructType(
    [
        struct_field,
        struct_type,
        array_type_data_type,
        array_type_struct_type,
        map_type_data_type,
        map_type_struct_type,
    ]
)


def test_foreach_field():
    actual = []

    foreach_field(schema, lambda f, refs: actual.append((f, refs)))

    expected = [
        # struct_field
        (struct_field, ["_1"]),

        # struct_type
        (struct_type, ["_2"]),
        (struct_field, ["_2", "_1"]),

        # array_type_data_type
        (array_type_data_type, ["_3"]),
        (StructField("[element]", NullType()), ["_3", "[element]"]),

        # array_type_struct_type
        (array_type_struct_type, ["_4"]),
        (struct_field, ["_4", "[element]", "_1"]),

        # map_type_data_type
        (map_type_data_type, ["_5"]),
        (StructField("[key]", NullType()), ["_5", "[key]"]),
        (StructField("[value]", NullType()), ["_5", "[value]"]),

        # map_type_struct_type
        (map_type_struct_type, ["_6"]),
        (struct_field, ["_6", "[key]", "_1"]),
        (struct_field, ["_6", "[value]", "_1"]),
    ]

    assert actual == expected


def test_map_field__no_change():
    assert_schema_equal(
        map_field(
            schema,
            lambda f, _: f
        ),
        schema
    )


def test_map_field__StructField():
    assert_schema_equal(
        map_field(
            schema,
            lambda f, refs: f_new(f) if refs == ["_1"] else f
        ),
        StructType(
            [
                StructField("_1_new", NullType(), False, {"is_test": False}),
                struct_type,
                array_type_data_type,
                array_type_struct_type,
                map_type_data_type,
                map_type_struct_type
            ]
        )
    )


def test_map_field__StructType():
    assert_schema_equal(
        map_field(
            schema,
            lambda f, refs: f_new(f) if refs == ["_2"] else f
        ),
        StructType(
            [
                struct_field,
                StructField("_2_new", StructType(
                    [
                        StructField("_1", NullType())
                    ]
                ), False, {"is_test": False}),
                array_type_data_type,
                array_type_struct_type,
                map_type_data_type,
                map_type_struct_type
            ]
        )
    )
    assert_schema_equal(
        map_field(
            schema,
            lambda f, refs: f_new(f) if refs == ["_2", "_1"] else f
        ),
        StructType(
            [
                struct_field,
                StructField("_2", StructType(
                    [
                        StructField("_1_new", NullType(), False, {"is_test": False})
                    ]
                )),
                array_type_data_type,
                array_type_struct_type,
                map_type_data_type,
                map_type_struct_type
            ]
        )
    )


def test_map_field__ArrayType_with_DataType():
    assert_schema_equal(
        map_field(
            schema,
            lambda f, refs: f_new(f) if refs == ["_3"] else f
        ),
        StructType(
            [
                struct_field,
                struct_type,
                StructField("_3_new", ArrayType(NullType(), False), False, {"is_test": False}),
                array_type_struct_type,
                map_type_data_type,
                map_type_struct_type
            ]
        )
    )


def test_map_field__ArrayType_with_StructType():
    assert_schema_equal(
        map_field(
            schema,
            lambda f, refs: f_new(f) if refs == ["_4"] else f
        ),
        StructType(
            [
                struct_field,
                struct_type,
                array_type_data_type,
                StructField("_4_new", ArrayType(
                    StructType(
                        [
                            StructField("_1", NullType())
                        ]
                    ),
                    False
                ), False, {"is_test": False}),
                map_type_data_type,
                map_type_struct_type
            ]
        )
    )
    assert_schema_equal(
        map_field(
            schema,
            lambda f, refs: f_new(f) if refs == ["_4", "[element]", "_1"] else f
        ),
        StructType(
            [
                struct_field,
                struct_type,
                array_type_data_type,
                StructField("_4", ArrayType(
                    StructType(
                        [
                            StructField("_1_new", NullType(), False, {"is_test": False})
                        ]
                    )
                )),
                map_type_data_type,
                map_type_struct_type
            ]
        )
    )


def test_map_field__MapType_with_DataType():
    assert_schema_equal(
        map_field(
            schema,
            lambda f, refs: f_new(f) if refs == ["_5"] else f
        ),
        StructType(
            [
                struct_field,
                struct_type,
                array_type_data_type,
                array_type_struct_type,
                StructField(
                    "_5_new",
                    MapType(NullType(), NullType(), False),
                    False,
                    {"is_test": False}
                ),
                map_type_struct_type
            ]
        )
    )


def test_map_field__MapType_with_StructType():
    assert_schema_equal(
        map_field(
            schema,
            lambda f, refs: f_new(f) if refs == ["_6"] else f
        ),
        StructType(
            [
                struct_field,
                struct_type,
                array_type_data_type,
                array_type_struct_type,
                map_type_data_type,
                StructField("_6_new", MapType(
                    StructType(
                        [
                            StructField("_1", NullType())
                        ]
                    ),
                    StructType(
                        [
                            StructField("_1", NullType())
                        ]
                    ),
                    False
                ), False, {"is_test": False})
            ]
        )
    )
    assert_schema_equal(
        map_field(
            schema,
            lambda f, refs: f_new(f) if refs == ["_6", "[key]", "_1"] else f
        ),
        StructType(
            [
                struct_field,
                struct_type,
                array_type_data_type,
                array_type_struct_type,
                map_type_data_type,
                StructField("_6", MapType(
                    StructType(
                        [
                            StructField("_1_new", NullType(), False, {"is_test": False})
                        ]
                    ),
                    StructType(
                        [
                            StructField("_1", NullType())
                        ]
                    )
                ))
            ]
        )
    )
    assert_schema_equal(
        map_field(
            schema,
            lambda f, refs: f_new(f) if refs == ["_6", "[value]", "_1"] else f
        ),
        StructType(
            [
                struct_field,
                struct_type,
                array_type_data_type,
                array_type_struct_type,
                map_type_data_type,
                StructField("_6", MapType(
                    StructType(
                        [
                            StructField("_1", NullType())
                        ]
                    ),
                    StructType(
                        [
                            StructField("_1_new", NullType(), False, {"is_test": False})
                        ]
                    )
                ))
            ]
        )
    )


def test_map_field__schema():
    assert_schema_equal(
        map_field(
            schema,
            lambda f, refs: f_new(f)
        ),
        StructType(
            [
                # StructField
                StructField("_1_new", NullType(), False, {"is_test": False}),

                # StructType
                StructField("_2_new", StructType(
                    [
                        StructField("_1_new", NullType(), False, {"is_test": False})
                    ]
                ), False, {"is_test": False}),

                # ArrayType with DataType
                StructField("_3_new", ArrayType(NullType(), False), False, {"is_test": False}),

                # ArrayType with StructType
                StructField("_4_new", ArrayType(
                    StructType(
                        [
                            StructField("_1_new", NullType(), False, {"is_test": False})
                        ]
                    ),
                    False
                ), False, {"is_test": False}),

                # MapType with DataType
                StructField("_5_new", MapType(NullType(), NullType(), False), False, {"is_test": False}),

                # MapType with StructType
                StructField("_6_new", MapType(
                    StructType(
                        [
                            StructField("_1_new", NullType(), False, {"is_test": False})
                        ]
                    ),
                    StructType(
                        [
                            StructField("_1_new", NullType(), False, {"is_test": False})
                        ]
                    ),
                    False
                ), False, {"is_test": False})

            ]
        )
    )


f_new = lambda f: StructField(f"{f.name}_new", f.dataType, False, {"is_test": False})


def test_exists_field__StructField():
    schema = StructType(
        [
            StructField("aA", NullType())
        ]
    )

    assert_exists_field(schema, "aA")
    assert_exists_field(schema, "aA", NullType)
    assert_exists_field(schema, "aA", NullType, True)
    assert_exists_field(schema, "aA", NullType, True, {})

    assert_not_exists_field(schema, "bB")
    assert_not_exists_field(schema, "aA", StringType)
    assert_not_exists_field(schema, "aA", NullType, False)
    assert_not_exists_field(schema, "aA", NullType, True, {1: "value"})


def test_exists_field__StructType():
    schema = StructType(
        [
            StructField("aA", StructType(
                [
                    StructField("aA", NullType())
                ]
            ))
        ]
    )

    assert_exists_field(schema, "aA")
    assert_exists_field(schema, "aA", StructType)
    assert_exists_field(schema, "aA", StructType, True)
    assert_exists_field(schema, "aA", StructType, True, {})

    assert_not_exists_field(schema, "bB")
    assert_not_exists_field(schema, "aA", StringType)
    assert_not_exists_field(schema, "aA", StructType, False)
    assert_not_exists_field(schema, "aA", StructType, True, {1: "value"})

    assert_exists_field(schema, "aA.aA")
    assert_exists_field(schema, "aA.aA", NullType)
    assert_exists_field(schema, "aA.aA", NullType, True)
    assert_exists_field(schema, "aA.aA", NullType, True, {})

    assert_not_exists_field(schema, "aA.bB")
    assert_not_exists_field(schema, "aA.aA", StringType)
    assert_not_exists_field(schema, "aA.aA", NullType, False)
    assert_not_exists_field(schema, "aA.aA", NullType, True, {1: "value"})


def test_exists_field__ArrayType_with_DataType():
    schema = StructType(
        [
            StructField("aA", ArrayType(NullType()))
        ]
    )

    assert_exists_field(schema, "aA")
    assert_exists_field(schema, "aA", ArrayType)
    assert_exists_field(schema, "aA", ArrayType, True)
    assert_exists_field(schema, "aA", ArrayType, True, {})

    assert_not_exists_field(schema, "bB")
    assert_not_exists_field(schema, "aA", StringType)
    assert_not_exists_field(schema, "aA", ArrayType, False)
    assert_not_exists_field(schema, "aA", ArrayType, True, {1: "value"})

    assert_exists_field(schema, "aA.[element]")
    assert_exists_field(schema, "aA.[element]", NullType)
    assert_exists_field(schema, "aA.[element]", NullType, True)
    assert_exists_field(schema, "aA.[element]", NullType, True, {})

    assert_not_exists_field(schema, "aA.[not_element]")
    assert_not_exists_field(schema, "aA.[element]", StringType)
    assert_not_exists_field(schema, "aA.[element]", NullType, False)
    assert_not_exists_field(schema, "aA.[element]", NullType, True, {1: "value"})


def test_exists_field__ArrayType_with_StructType():
    schema = StructType(
        [
            StructField("aA", ArrayType(
                StructType(
                    [
                        StructField("aA", NullType())
                    ]
                )
            ))
        ]
    )

    assert_exists_field(schema, "aA")
    assert_exists_field(schema, "aA", ArrayType)
    assert_exists_field(schema, "aA", ArrayType, True)
    assert_exists_field(schema, "aA", ArrayType, True, {})

    assert_not_exists_field(schema, "bB")
    assert_not_exists_field(schema, "aA", StringType)
    assert_not_exists_field(schema, "aA", ArrayType, False)
    assert_not_exists_field(schema, "aA", ArrayType, True, {1: "value"})

    assert_exists_field(schema, "aA.[element].aA")
    assert_exists_field(schema, "aA.[element].aA", NullType)
    assert_exists_field(schema, "aA.[element].aA", NullType, True)
    assert_exists_field(schema, "aA.[element].aA", NullType, True, {})

    assert_not_exists_field(schema, "aA.[element].bB")
    assert_not_exists_field(schema, "aA.[element].aA", StringType)
    assert_not_exists_field(schema, "aA.[element].aA", NullType, False)
    assert_not_exists_field(schema, "aA.[element].aA", NullType, True, {1: "value"})


def test_exists_field__MapType_with_DataType():
    schema = StructType(
        [
            StructField("aA", MapType(NullType(), NullType()))
        ]
    )

    assert_exists_field(schema, "aA")
    assert_exists_field(schema, "aA", MapType)
    assert_exists_field(schema, "aA", MapType, True)
    assert_exists_field(schema, "aA", MapType, True, {})

    assert_not_exists_field(schema, "bB")
    assert_not_exists_field(schema, "aA", StringType)
    assert_not_exists_field(schema, "aA", MapType, False)
    assert_not_exists_field(schema, "aA", MapType, True, {1: "value"})

    assert_exists_field(schema, "aA.[key]")
    assert_exists_field(schema, "aA.[key]", NullType)
    assert_exists_field(schema, "aA.[key]", NullType, True)
    assert_exists_field(schema, "aA.[key]", NullType, True, {})

    assert_not_exists_field(schema, "aA.[not_key]")
    assert_not_exists_field(schema, "aA.[key]", StringType)
    assert_not_exists_field(schema, "aA.[key]", NullType, False)
    assert_not_exists_field(schema, "aA.[key]", NullType, True, {1: "value"})

    assert_exists_field(schema, "aA.[value]")
    assert_exists_field(schema, "aA.[value]", NullType)
    assert_exists_field(schema, "aA.[value]", NullType, True)
    assert_exists_field(schema, "aA.[value]", NullType, True, {})

    assert_not_exists_field(schema, "aA.[not_value]")
    assert_not_exists_field(schema, "aA.[value]", StringType)
    assert_not_exists_field(schema, "aA.[value]", NullType, False)
    assert_not_exists_field(schema, "aA.[value]", NullType, True, {1: "value"})


def test_exists_field__MapType_with_StructType():
    schema = StructType(
        [
            StructField("aA", MapType(
                StructType(
                    [
                        StructField("aA", NullType())
                    ]
                ),
                StructType(
                    [
                        StructField("aA", NullType())
                    ]
                )
            ))
        ]
    )

    assert_exists_field(schema, "aA")
    assert_exists_field(schema, "aA", MapType)
    assert_exists_field(schema, "aA", MapType, True)
    assert_exists_field(schema, "aA", MapType, True, {})

    assert_not_exists_field(schema, "bB")
    assert_not_exists_field(schema, "aA", StringType)
    assert_not_exists_field(schema, "aA", MapType, False)
    assert_not_exists_field(schema, "aA", MapType, True, {1: "value"})

    assert_exists_field(schema, "aA.[key].aA")
    assert_exists_field(schema, "aA.[key].aA", NullType)
    assert_exists_field(schema, "aA.[key].aA", NullType, True)
    assert_exists_field(schema, "aA.[key].aA", NullType, True, {})

    assert_not_exists_field(schema, "aA.[key].bB")
    assert_not_exists_field(schema, "aA.[key].aA", StringType)
    assert_not_exists_field(schema, "aA.[key].aA", NullType, False)
    assert_not_exists_field(schema, "aA.[key].aA", NullType, True, {1: "value"})

    assert_exists_field(schema, "aA.[value].aA")
    assert_exists_field(schema, "aA.[value].aA", NullType)
    assert_exists_field(schema, "aA.[value].aA", NullType, True)
    assert_exists_field(schema, "aA.[value].aA", NullType, True, {})

    assert_not_exists_field(schema, "aA.[value].bB")
    assert_not_exists_field(schema, "aA.[value].aA", StringType)
    assert_not_exists_field(schema, "aA.[value].aA", NullType, False)
    assert_not_exists_field(schema, "aA.[value].aA", NullType, True, {1: "value"})


def assert_exists_field(schema: StructType,
                        refs: str,
                        data_type=None,
                        nullable: bool = None,
                        metadata: dict = None):
    assert exists_field(schema, refs, data_type, nullable, metadata)

    assert exists_field(schema, refs.lower(), data_type, nullable, metadata)
    assert not exists_field(schema, refs.lower(), data_type, nullable, metadata, case_sensitive=True)

    assert exists_field(schema, refs.upper(), data_type, nullable, metadata)
    assert not exists_field(schema, refs.upper(), data_type, nullable, metadata, case_sensitive=True)


def assert_not_exists_field(schema: StructType,
                            refs: str,
                            data_type=None,
                            nullable: bool = None,
                            metadata: dict = None):
    assert not exists_field(schema, refs, data_type, nullable, metadata)

    assert not exists_field(schema, refs.lower(), data_type, nullable, metadata)
    assert not exists_field(schema, refs.lower(), data_type, nullable, metadata, case_sensitive=True)

    assert not exists_field(schema, refs.upper(), data_type, nullable, metadata)
    assert not exists_field(schema, refs.upper(), data_type, nullable, metadata, case_sensitive=True)
