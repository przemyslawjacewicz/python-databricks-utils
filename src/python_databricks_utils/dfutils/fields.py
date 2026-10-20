from typing import Callable, Tuple, Optional, Dict, Any

from pyspark.sql.types import StructField, StructType, ArrayType, MapType

from python_spark_utils.utils.utils import flatten_list


def foreach_field(schema: StructType, f: Callable[[StructField, list[str]], None]) -> None:
    def apply(field: StructField, refs: list[str]) -> None:
        if isinstance(field.dataType, MapType):
            f(field, refs)

            key_type = field.dataType.keyType
            if isinstance(key_type, StructType):
                for _f in key_type.fields:
                    apply(_f, refs + ["[key]", _f.name])
            else:
                f(StructField("[key]", key_type), refs + ["[key]"])

            value_type = field.dataType.valueType
            if isinstance(value_type, StructType):
                for _f in value_type.fields:
                    apply(_f, refs + ["[value]", _f.name])
            else:
                f(StructField("[value]", value_type), refs + ["[value]"])
        elif isinstance(field.dataType, ArrayType):
            f(field, refs)

            element_type = field.dataType.elementType
            if isinstance(element_type, StructType):
                for _f in element_type.fields:
                    apply(_f, refs + ["[element]", _f.name])
            else:
                f(StructField("[element]", element_type), refs + ["[element]"])
        elif isinstance(field.dataType, StructType):
            f(field, refs)
            for _f in field.dataType.fields:
                apply(_f, refs + [_f.name])
        else:
            f(field, refs)

    for _f in schema.fields:
        apply(_f, [_f.name])


def map_field(schema: StructType, f: Callable[[StructField, list[str]], StructField]) -> StructType:
    def apply(field: StructField, refs: list[str]) -> list[Tuple[StructField, list[str]]]:
        if isinstance(field.dataType, MapType):
            field_new = f(field, refs)

            key_type = field.dataType.keyType
            if isinstance(key_type, StructType):
                key_fields_new = flatten_list(
                    [apply(_f, refs + ["[key]", _f.name]) for _f in key_type.fields]
                )
                key_type_new = StructType([_f for (_f, _) in key_fields_new])
            else:
                key_type_new = key_type

            value_type = field.dataType.valueType
            if isinstance(value_type, StructType):
                value_fields_new = flatten_list(
                    [apply(_f, refs + ["[value]", _f.name]) for _f in value_type.fields]
                )
                value_type_new = StructType([_f for (_f, _) in value_fields_new])
            else:
                value_type_new = value_type

            return [
                (
                    StructField(
                        field_new.name,
                        MapType(key_type_new, value_type_new, field_new.nullable),
                        field_new.nullable,
                        field_new.metadata
                    ),
                    refs
                )
            ]
        elif isinstance(field.dataType, ArrayType):
            field_new = f(field, refs)

            element_type = field.dataType.elementType
            if isinstance(element_type, StructType):
                element_fields_new = flatten_list(
                    [apply(_f, refs + ["[element]", _f.name]) for _f in element_type.fields]
                )
                element_type_new = StructType([_f for (_f, _) in element_fields_new])
            else:
                element_type_new = element_type

            return [
                (
                    StructField(
                        field_new.name,
                        ArrayType(element_type_new, field_new.nullable),
                        field_new.nullable,
                        field_new.metadata
                    ),
                    refs
                )
            ]
        elif isinstance(field.dataType, StructType):
            field_new = f(field, refs)
            fields_new = flatten_list([apply(_f, refs + [_f.name]) for _f in field.dataType.fields])

            return [
                (
                    StructField(
                        field_new.name,
                        StructType([_f for (_f, _) in fields_new]),
                        field_new.nullable,
                        field_new.metadata
                    ),
                    refs
                )
            ]
        else:
            field_new = f(field, refs)
            return [
                (
                    field_new,
                    refs
                )
            ]

    fields = flatten_list([apply(_f, [_f.name]) for _f in schema.fields])
    result = StructType([_f for (_f, _) in fields])

    return result


def exists_field(
        schema: StructType,
        refs: list[str] | str,
        data_type=None,  # todo: add type ?
        nullable: bool = None,
        metadata: Optional[Dict[str, Any]] = None,
        case_sensitive: bool = False
) -> bool:
    if isinstance(refs, str):
        return exists_field(schema, refs.split("."), data_type, nullable, metadata, case_sensitive)
    else:
        collected = []

        foreach_field(schema, lambda _field, _refs: collected.append((_field, _refs)))

        norm = lambda l: l if case_sensitive else [_e.lower() for _e in l]

        filtered = [_field for (_field, _refs) in collected if (norm(refs) == norm(_refs))]

        match filtered:
            case [single]:
                by_data_type = isinstance(single.dataType, data_type) if data_type is not None else True
                by_nullable = single.nullable == nullable if nullable is not None else True
                by_metadata = single.metadata == metadata if metadata is not None else True
                return by_data_type and by_nullable and by_metadata
            case _:
                return False
