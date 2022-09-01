from services.datasources.redshift.redshift_table import (
    RedshiftColumnType,
    RedshiftColumn,
    RedshiftTable
)

from services.flat_file.flat_file_descriptor import ColumnDataType


class FlatFileToRedshiftConverter:

    MAX_VARCHAR_SIZE = 65535
    DEFAULT_VARCHAR_SIZE = 512

    FIELD_TYPE_MAP = {
        ColumnDataType.STRING: RedshiftColumnType("VARCHAR", has_precision=True, has_scale=False),
        ColumnDataType.DATE: RedshiftColumnType("DATE", has_precision=False, has_scale=False),
        ColumnDataType.DATETIME: RedshiftColumnType("TIMESTAMP", has_precision=False, has_scale=False),
        ColumnDataType.INTEGER: RedshiftColumnType("INTEGER", has_precision=False, has_scale=False),
        ColumnDataType.NUMERIC: RedshiftColumnType("NUMERIC", has_precision=True, has_scale=True),
        ColumnDataType.BOOLEAN: RedshiftColumnType("BOOLEAN", has_precision=False, has_scale=False),
        ColumnDataType.UNKNOWN: RedshiftColumnType("VARCHAR", has_precision=True, has_scale=False)
    }

    @staticmethod
    def redshift_table_from_flatfile(
        schema_name, table_name, field_list
    ):
        table = RedshiftTable(schema_name, table_name)

        for field in field_list:
            type_details = field.original_type if field.potential_type is None else field.potential_type
            precision = type_details.precision
            scale = type_details.scale
            column = FlatFileToRedshiftConverter.convert_flatfile_column(
                field.column_name,
                type_details.data_type,
                field.ordinal_position,
                precision=precision,
                scale=scale
            )
            
            table.add_column(
                column.column_name,
                column.column_type,
                precision=column.column_precision,
                scale=column.column_scale,
                is_primary_key=column.is_primary_key,
            )

        return table

    @staticmethod
    def convert_flatfile_column(
        column_name, column_type, position, precision=None, scale=None
    ):
        """Accepts a MySQL/Aurora column type, precision and scale and returns a column definiton
        RedshiftColumn ...
        """
        column_type = column_type.upper()
        if column_type not in FlatFileToRedshiftConverter.FIELD_TYPE_MAP:
            raise ValueError(column_type + " is an unsupported Redshift type")

        redshift_type = FlatFileToRedshiftConverter.FIELD_TYPE_MAP[column_type]

        # Set a default precision on string fields if none is provided
        if redshift_type.type_name == "VARCHAR" and precision is None:
            precision = FlatFileToRedshiftConverter.DEFAULT_VARCHAR_SIZE

        # Precision will exist on strings and numerics
        if redshift_type.has_precision and redshift_type.has_scale:
            if precision is None or scale is None:
                raise ValueError(column_type + " requires a precision and scale")
        elif redshift_type.has_precision:
            if (precision is None) or (
                precision > FlatFileToRedshiftConverter.MAX_VARCHAR_SIZE
            ):
                # Unsupported Field if no precision or precision > max redshift precision
                return None

        is_primary_key = True if column_name.upper() == "ID" else False
        redshift_column = RedshiftColumn(
            column_name,
            redshift_type.type_name,
            position,
            is_primary_key,
            precision=precision,
            scale=scale,
        )
        return redshift_column
