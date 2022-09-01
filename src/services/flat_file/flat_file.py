
import os
import re 
from decimal import Decimal
import numpy as np

import pandas as pd
from dateutil.parser import parse as duparse

from services.flat_file.flat_file_descriptor import FlatFileDescriptor, ColumnDataType
from services.datasources.redshift.redshift_column_converter import FlatFileToRedshiftConverter

PANDAS_TYPE_MAP = {
    'string': ColumnDataType.STRING, 
    'boolean': ColumnDataType.BOOLEAN,
    'Int64': ColumnDataType.INTEGER,
    'Float64': ColumnDataType.NUMERIC
}

RE_TRUE_STRING = re.compile(r'^(t(rue)?|yes)$', re.I)
RE_FALSE_STRING = re.compile(r'^(f(alse)?|no)$', re.I)
RE_DATETIME_INVALID_STRING = re.compile(r'[^0123456789ZT\:\/\-\s]', re.I)
MIN_DATETIME_STRING_LENGTH = 6 # We set this to 6 to ignore 5 digit dates : Days since Jan 1 1970

RECORD_INDEX_COL_NAME = '_record_index'

### Main DataProfiler class / entry point

class FlatFile(object):

    def __init__(self, file_path, original_file_name=None):
        self.data_frame = None 
        self.file_descriptor = self._get_descriptor_for_file(file_path, original_file_name=original_file_name)

    def _get_descriptor_for_file(self, file_path, original_file_name=None):

        # Enforce any file size checks here
        file_size = self._get_file_size(file_path)

        self.data_frame = pd.read_csv(file_path)
        total_records = len(self.data_frame.index)
        
        file_descriptor = FlatFileDescriptor(
            file_path,
            file_size=file_size,
            total_records=total_records,
            original_file_name=original_file_name
        )

        self.file_descriptor = FlatFile._get_column_list(
            file_descriptor,        
            self.data_frame,
            total_records
        )

        # Calculate DDL 
        ddl = self._get_ddl(self.file_descriptor)
        self.file_descriptor.ddl = ddl

        # Add a custome index field to the result set
        self.data_frame[RECORD_INDEX_COL_NAME] = self.data_frame.index + 1
        return self.file_descriptor

    def get_file_descriptor(self):
        return self.file_descriptor

    def get_records(self):
        df = self.data_frame.replace({np.nan: None})
        return df.to_dict('records')

    @staticmethod
    def _get_ddl(file_descriptor):
        table = FlatFileToRedshiftConverter.redshift_table_from_flatfile(
            'test_schema',
            file_descriptor.file_display_name,
            file_descriptor.columns
        )
        return table.create_table_ddl()

    @staticmethod
    def _get_file_size(file_path):
        if os.path.isfile(file_path):
            return os.path.getsize(file_path)
        raise IOError('File {0} does not exist'.format(file_path))


    @staticmethod
    def _get_column_list(file_descriptor, df, total_records):
        if (df.columns is None or len(df.columns) == 0):
            raise AssertionError('Dataframe requires column names')

        pandas_converted_types = dict(df.convert_dtypes().dtypes)

        value_counts = dict(df.notnull().sum())
        distinct_counts = dict(df.nunique())

        column_list = list(df.columns)
        for col_name in column_list:
            col_desc = file_descriptor.add_column(col_name)
            col_desc.total_records = total_records
            col_desc.non_null_values = value_counts[col_name] if col_name in value_counts else 0

            # Distinct Value Counts
            if col_name in distinct_counts:
                distinct_count = distinct_counts[col_name]                
                distinct_ratio = (distinct_count / total_records) if total_records > 0 else 0.00
                col_desc.distinct_values = distinct_count
                col_desc.distinct_ratio = distinct_ratio

            # Drop Null and Duplicate Values
            col_values_df = df[col_name].dropna().drop_duplicates()
            row_count = len(col_values_df.index)

            # Generic Value Types : String, Integer, Decimal, Boolean
            if col_name in pandas_converted_types:
                pandas_type = str(pandas_converted_types[col_name])
                local_field_type = ColumnDataType.STRING if pandas_type not in PANDAS_TYPE_MAP else PANDAS_TYPE_MAP[pandas_type]
                column_data_type = ColumnDataType.UNKNOWN if row_count == 0 else local_field_type
                col_desc.add_original_type(column_data_type)
            else:
                raise AssertionError("Data Type {0} not found in Map".format(pandas_type))

            # Find Max Values
            col_desc = FlatFile._get_max_column_values(col_desc, col_values_df)

            # Fetch Sample Records
            sample_size = 5 if row_count > 5 else row_count
            if sample_size > 0:
                sample_values_df = col_values_df.sample(n=sample_size)
                sample_values = sample_values_df.tolist()
                col_desc.sample_values = sample_values
                
            # Infer Data Types 
            col_desc = FlatFile._infer_datatype(col_desc, col_values_df)

        return file_descriptor

    @staticmethod
    def _get_max_column_values(col_desc, df_col):
        if col_desc.original_type.data_type == ColumnDataType.INTEGER:
            # Fetch Max Value of Integer field
            max_value = df_col.max()
            max_value = int(max_value)
            col_desc.original_type.max_value = max_value
            
        elif col_desc.original_type.data_type == ColumnDataType.STRING:
            # Fetch MAX length of String Column Fields
            max_length = df_col.map(len).max()
            col_desc.original_type.max_length = int(max_length)
            col_desc.original_type.precision = int(max_length)

        return col_desc

    @staticmethod
    def _infer_datatype(column_description, df_col):

        if column_description.original_type.data_type == ColumnDataType.STRING:

            # Check for potential Boolean Values 
            if FlatFile._is_string_boolean(column_description):
                column_description.potential_type = column_description.add_potential_type(ColumnDataType.BOOLEAN)
            else:
                # Check for potential Date Formats
                dt_type, parse_failures = FlatFile._string_datetime_type(df_col)
                if dt_type is not None:
                    if dt_type == 'DATETIME':
                        column_description.potential_type = column_description.add_potential_type(ColumnDataType.DATETIME)
                    else:
                        column_description.potential_type = column_description.add_potential_type(ColumnDataType.DATE)
                    column_description.potential_type.invalid_record_index = parse_failures

        elif column_description.original_type.data_type == ColumnDataType.INTEGER:
            if FlatFile._is_integer_boolean(column_description):
                column_description.potential_type = column_description.add_potential_type(ColumnDataType.BOOLEAN)
            
        elif column_description.original_type.data_type == ColumnDataType.NUMERIC:
            precision, scale = FlatFile._get_precision_and_scale(df_col)
            column_description.original_type.precision = precision
            column_description.original_type.scale = scale

        return column_description


    @staticmethod
    def _is_integer_boolean(column_description):
        """ If an integer field has no null values and """
        if column_description.total_records == column_description.non_null_values:
            sample_values = column_description.sample_values
            if(len(sample_values) == 2 and sum(sample_values) == 1 and min(sample_values) == 0):
                return True
        return False

    @staticmethod            
    def _is_string_boolean(column_description):
        if column_description.total_records == column_description.non_null_values:
            sample_values = column_description.sample_values
            if len(sample_values) != 2:
                return False
            sample_values = map(str.lower, sample_values)
            # Sort value so FALSE is the first value for the two sets we check
            # F/No | T/Yes
            sample_values = sorted(sample_values) 
            f = sample_values[0]
            t = sample_values[1]

            if(RE_TRUE_STRING.match(t) and RE_FALSE_STRING.match(f)):
                return True
        return False
            

    # Numeric helper methods
    @staticmethod
    def _get_precision_and_scale(df_col, sample_size=None):
        """
        Given a numeric column convert to a naive Decimal
        and calculate the maximum precision and scale of the 
        dataset. 
        Precision:  Total Digits
        Scale :     Total number of digits after the decimal
        """
        df = df_col.copy()
        decimal_tuple = df.map(FlatFile._convert_float_to_decimal_tuple)

        max_precision = decimal_tuple.map(lambda x: len(x.digits)).max()
        max_scale = decimal_tuple.map(lambda x: abs(x.exponent)).max()

        return max_precision, max_scale

    @staticmethod
    def _convert_float_to_decimal_tuple(val):
        val_str = str(val)
        val_d = Decimal(val_str)
        return val_d.as_tuple()

    # Datetime parsing helper methods 
        
    @staticmethod
    def _string_datetime_type(df_col, sample_size=None):
        val_count = len(df_col.index)
        if val_count == 0:
            return False

        if sample_size is not None:
            sample_size = val_count if val_count < sample_size else sample_size
            df_col = df_col.sample(n=sample_size)
        
        # For each Value in the Column : Attempt to parse it as a datetime
        potential_types = set([])
        parse_failures = []
        for i, v in df_col.items():
            if FlatFile._is_potential_datetime(v):
                dt_val, dt_type = FlatFile._try_parse_datetime(v)
                if dt_val is not None:
                    potential_types.add(dt_type)
                else:
                    # Not a valid Date
                    parse_failures.append(i)
            else:
                # If this record is now a potential datetime but 
                # we find additional records, treat it as a parse failure
                parse_failures.append(i)
    
        if len(potential_types) > 0:
            # If more than 1 date format is returned default to DATETIME 
            potential_type = 'DATETIME' if len(potential_types) > 1 else list(potential_types).pop()
            return potential_type, parse_failures
        return None, None
                   
    @staticmethod
    def _try_parse_datetime(val):
        try:
            has_time = FlatFile._date_has_time_component(val)
            dt = duparse(val)
            if (dt.time() and has_time == True): 
                return dt, 'DATETIME'
            else: # Return Date Type
                return dt, 'DATE'
        except TypeError: 
            return None, None
        except ValueError: 
            return None, None
    
    @staticmethod
    def _date_has_time_component(val):
        val = val.replace(" ", "T")
        val_list = val.split("T")
        if len(val_list) > 1:
            time_component = val_list[1]
            if ":" in time_component:
                return True 
        return False 

    @staticmethod
    def _is_potential_datetime(val):
        if len(val) < MIN_DATETIME_STRING_LENGTH:
            return False 
        match_val = RE_DATETIME_INVALID_STRING.search(val)
        if match_val is None:
            return True 
        return False
