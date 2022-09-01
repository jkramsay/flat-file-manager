from enum import Enum


class RedshiftTableMergeStrategy(Enum):
    PRIMARY_KEY = 1
    UNIQUE_COLUMN_CONSTRAINT = 2
    APPEND_ONLY = 3
    FULL_RELOAD = 4
    REPLACE = 5


class RedshiftColumnType(object):
    def __init__(self, type_name, has_precision=False, has_scale=False):
        self.type_name = type_name
        self.has_precision = has_precision
        self.has_scale = has_scale


class RedshiftColumn(object):
    def __init__(
        self,
        column_name,
        column_type,
        column_position,
        is_primary_key=False,
        scale=None,
        precision=None,
        is_nullable=True,
    ):
        self.is_primary_key = is_primary_key
        self.column_name = column_name
        self.column_type = column_type
        self.column_position = column_position
        self.column_scale = scale
        self.column_precision = precision
        self.is_nullable = is_nullable
        self.is_sort_key = False
        self.is_dist_key = False

    def ddl(self):
        col_type = self.column_type
        not_null_stmt = "" if self.is_nullable is True else "NOT NULL"
        precision_text = ""
        precision = None
        scale = None

        if hasattr(col_type, "type_name"):
            col_type_name = self.column_type.type_name
        else:
            col_type_name = self.column_type

        # information schema can return precision for numerics but we do not use these in column create DDL
        if self.column_precision is not None and col_type_name not in [
            "FLOAT8",
            "INTEGER",
            "BIGINT",
        ]:
            precision = self.column_precision
            vals = [str(precision)]
            if self.column_scale is not None:
                scale = self.column_scale
                vals.append(str(scale))
            precision_text = "(" + ",".join(vals) + ")"

        return '"{0}" {1}{2} {3}'.format(
            self.column_name.lower(), col_type_name, precision_text, not_null_stmt
        )


class RedshiftTable(object):
    MAX_VARCHAR_SIZE = 65535
    SUPPORTED_COLUMNS = {
        "SMALLINT": RedshiftColumnType("SMALLINT"),
        "INTEGER": RedshiftColumnType("INTEGER"),
        "INT4": RedshiftColumnType("INTEGER"),
        "INT8": RedshiftColumnType("BIGINT"),
        "BIGINT": RedshiftColumnType("BIGINT"),
        "FLOAT8": RedshiftColumnType("FLOAT8"),
        "NUMERIC": RedshiftColumnType("NUMERIC", has_precision=True, has_scale=True),
        "VARCHAR": RedshiftColumnType("VARCHAR", has_precision=True, has_scale=False),
        "BPCHAR": RedshiftColumnType("VARCHAR", has_precision=True, has_scale=False),
        "BOOLEAN": RedshiftColumnType("BOOLEAN"),
        "BOOL": RedshiftColumnType("BOOLEAN"),
        "DATE": RedshiftColumnType("DATE"),
        "TIMESTAMP": RedshiftColumnType("TIMESTAMP WITHOUT TIME ZONE"),
    }

    def __init__(self, schema_name, table_name, column_definitions=None):
        self.schema_name = schema_name
        self.table_name = table_name
        self.columns = {}
        self.merge_strategy = None
        # Only supports a single set of columns
        self.unique_constraint = []
        self.table_dist_key = None
        self.table_sort_keys = []
        # Set the field that denotes the last modified date column in a table
        self.last_modified_column = None

        if column_definitions is not None and len(column_definitions) > 0:
            self.add_columns_from_definiion(column_definitions)

    def add_columns_from_definiion(self, column_definitions):
        for col in column_definitions:
            if "column_name" in col and "column_type" in col:
                # Check for optional args
                is_primary_key = (
                    False if "is_primary_key" not in col else col["is_primary_key"]
                )

                is_nullable = True if "is_nullable" not in col else col["is_nullable"]

                precision = None if "precision" not in col else col["precision"]
                scale = None if "scale" not in col else col["scale"]

                self.add_column(
                    col["column_name"],
                    col["column_type"],
                    precision=precision,
                    scale=scale,
                    is_primary_key=is_primary_key,
                    is_nullable=is_nullable,
                )
            else:
                raise AssertionError(
                    "Column Definition requires column_name and column_type"
                )

    def get_table_name(self):
        return '"{0}"."{1}"'.format(self.schema_name, self.table_name)

    def set_merge_strategy(self, strategy, unique_columns=[]):
        assert isinstance(strategy, RedshiftTableMergeStrategy)
        if (strategy == RedshiftTableMergeStrategy.UNIQUE_COLUMN_CONSTRAINT) or (
            strategy == RedshiftTableMergeStrategy.REPLACE
        ):
            assert len(unique_columns) > 0
            for column_name in unique_columns:
                self.is_valid_column(column_name)
        self.merge_strategy = strategy
        self.unique_constraint = unique_columns

    def set_distribution_key(self, column_name):
        column = self.get_column_by_name(column_name)
        self.table_dist_key = column

    def set_sort_keys(self, column_names=[]):
        self.table_sort_keys = []
        for column_name in column_names:
            column = self.get_column_by_name(column_name)
            self.table_sort_keys.append(column)

    def set_last_modified_column(self, column_name):
        self.is_valid_column(column_name)
        self.last_modified_column = column_name

    def get_unique_column_constraint(self):
        return self.unique_constraint

    def get_primary_key(self):
        for column in self.get_column_list():
            if column.is_primary_key:
                return column
        return None

    def set_primary_key(self, column_name):
        column = self.get_column_by_name(column_name)
        column.is_primary_key = True

    def add_column(
        self,
        column_name,
        column_type,
        precision=None,
        scale=None,
        is_primary_key=False,
        is_nullable=True,
    ):
        assert (
            column_type.upper() in self.SUPPORTED_COLUMNS
        ), "{0} is not a supported column type. Valid types are {1}".format(
            column_type.upper(), str(list(self.SUPPORTED_COLUMNS.keys()))
        )
        assert (
            column_name.upper() not in self.columns
        ), "{0} not in self.columns".format(column_name.upper())
        column_type = self.SUPPORTED_COLUMNS[column_type.upper()]
        if column_type.has_scale and scale is None:
            raise AssertionError("Column Type requires a scale")
        if column_type.has_precision and precision is None:
            raise AssertionError("Column Type requires a precision")

        next_column_position = self.column_count() + 1
        self.columns[column_name.upper()] = RedshiftColumn(
            column_name,
            column_type,
            column_position=next_column_position,
            scale=scale,
            precision=precision,
            is_primary_key=is_primary_key,
            is_nullable=is_nullable,
        )

    def is_valid_column(self, column_name):
        assert column_name.upper() in self.columns

    def get_column_by_name(self, column_name):
        assert column_name.upper() in self.columns
        return self.columns[column_name.upper()]

    def get_column_list(self):
        column_list = []
        for col_name, column in self.columns.items():
            column_list.append(column)

        column_list.sort(key=lambda x: x.column_position)
        return column_list

    def get_quoted_column_name_list(self):
        """Return a "column_name" list for use with SELECTS"""
        quoted_list = []
        column_list = self.get_column_list()
        for column in column_list:
            quoted_list.append('"{0}"'.format(column.column_name))
        return quoted_list

    def get_unquoted_column_name_list(self):
        """Return a "column_name" list for use with SELECTS"""
        quoted_list = []
        column_list = self.get_column_list()
        for column in column_list:
            quoted_list.append(column.column_name)
        return quoted_list

    def column_count(self):
        return len(self.columns.keys())

    ## Common SQL Statement Creation
    def get_last_modified_date(self):
        if self.last_modified_column is None:
            return None

        sql = 'SELECT MAX("{0}") AS last_modified_at FROM "{1}"."{2}"'.format(
            self.last_modified_column, self.schema_name, self.table_name
        )
        return sql

    ## DDL Statement Creation
    def drop_table_ddl(self):
        """Return the DDL to drop this table in RedShift"""
        sql = 'DROP TABLE IF EXISTS "{0}"."{1}"'.format(
            self.schema_name, self.table_name
        )
        return sql

    def create_table_ddl(self, if_not_exists=True):
        """Return the DDL to execute on RedShift to create a table"""
        target_table_name = '"' + self.schema_name + '"."' + self.table_name + '"'
        stmt = []
        primary_key = None
        stmt.append("CREATE TABLE")
        if if_not_exists:
            stmt.append("IF NOT EXISTS")
        stmt.append(target_table_name)
        stmt.append("\n(")
        column_ddl = []
        column_list = self.get_column_list()
        for column in column_list:
            if column.is_primary_key:
                primary_key = column.column_name
            is_nullable = "NOT NULL" if column.is_nullable is False else ""
            column_ddl.append(column.ddl() + " " + is_nullable + "\n")
        if primary_key:
            column_ddl.append("PRIMARY KEY (" + primary_key + ")")
        stmt.append(",".join(column_ddl))
        stmt.append("\n)")

        if self.table_dist_key is not None:
            stmt.append('DISTKEY("{0}")'.format(self.table_dist_key.column_name))
        else:
            stmt.append("DISTSTYLE EVEN")
        stmt.append("\n")

        if len(self.table_sort_keys) > 0:
            sort_key_columns = ", ".join(
                '"' + col.column_name + '"' for col in self.table_sort_keys
            )
            stmt.append("SORTKEY({0})".format(sort_key_columns))
            stmt.append("\n")

        stmt.append(";")
        return " ".join(stmt)

    def dbt_table_schema(self):
        dbt_schema = "- name: {0}:\n".format(self.table_name.lower())
        dbt_schema += "  description:\n"
        dbt_schema += "    columns:\n"

        column_list = self.get_column_list()
        for column in column_list:
            dbt_schema += "      - name: {0}\n".format(column.column_name)
            dbt_schema += '        description:""\n'.format(column.column_name)
        dbt_schema += "\n"

        return dbt_schema

    def copy_table_from_s3(self, s3_path, iam_role, region=None):
        """Return COPY statement that will load a file from S3 into Redshift"""
        region_string = "region '{0}'".format(region) if region is not None else ""
        load = """
            COPY {0}
            from '{1}'
            iam_role '{2}'
            format AS CSV
            delimiter '|'
            emptyasnull
            quote AS '"'
            trimblanks
            roundec
            truncatecolumns
            compupdate off
            acceptinvchars
            timeformat 'auto'
            dateformat 'auto'
            {3}
            """.format(
            self.get_table_name(), s3_path, iam_role, region_string
        )

        return load
