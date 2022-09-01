import uuid
import dataclasses
import pathlib

from enum import Enum


class ColumnDataType(str, Enum): # Declaring as a subsclass of string so we json json serialize this
    STRING = "STRING",
    DATE = "DATE", 
    DATETIME = "DATETIME", 
    INTEGER = "INTEGER", 
    NUMERIC = "NUMERIC"
    BOOLEAN = "BOOLEAN"
    UNKNOWN = "UNKNOWN"

# Column Definitions
@dataclasses.dataclass 
class ColumnFieldDetails:
    data_type: ColumnDataType
    precision: int = None
    scale: int = None
    is_nullable: bool = True 
    max_length: int = 0 
    max_value: int = 0
    string_format: str = None # String representation of the field format (ie. YYYY-MM-DD, ##.00)
    invalid_record_index: list[any] = dataclasses.field(default_factory=list) # Store index reference to any rows that would fail parsing to the data_type

@dataclasses.dataclass
class ColumnDescriptor:
    """Description of a Column"""
    column_name: str
    ordinal_position: int = None
    potential_type: ColumnFieldDetails = None
    sample_values: list[any] = dataclasses.field(default_factory=list)
    original_type: ColumnFieldDetails = None
    total_records: int = 0
    non_null_values: int = 0
    distinct_values: int = 0
    distinct_ratio: float = 0.00


    def add_original_type(self, column_data_type):
        self.original_type = ColumnFieldDetails(column_data_type)
        return self.original_type

    def add_potential_type(self, column_data_type):
        self.potential_type = ColumnFieldDetails(column_data_type)
        return self.potential_type

    @property
    def column_type_display(self) -> str:
        """Return a text friendly description of the data type"""
        if self.original_type is None:
            return None

        t = self.original_type if self.potential_type is None else self.potential_type

        suffix = ""
        if t.precision is not None:
            suffix = " ({0}".format(t.precision)
            if t.scale is not None:
                suffix += ", {0}".format(t.scale)
            suffix += ")"
        
        return "{0}{1}".format(t.data_type.value, suffix)
 

@dataclasses.dataclass
class FlatFileDescriptor:
    local_file_path: str    
    original_file_name: str = None    
    file_name: str = None
    file_extension: str = None
    file_display_name: str = None
    unique_id: str = None
    version: int = 1
    file_size: int = None
    total_records: int = 0
    columns: list[ColumnDescriptor] = dataclasses.field(default_factory=list)
    ddl: str = None

    def __post_init__(self):
        self.unique_id = str(uuid.uuid4())
        file_path = self.local_file_path if self.original_file_name is None else self.original_file_name
        file_name_details = self.parse_filename(file_path)
        self.file_name = file_name_details[0]
        self.file_extension = file_name_details[1]
        self.file_display_name = file_name_details[2]

    def add_column(self, column_name: str):
        column = ColumnDescriptor(column_name)
        next_position = len(self.columns)
        column.ordinal_position = next_position
        self.columns.append(column)
        return column

    def parse_filename(self, file_path):
        path_details = pathlib.Path(file_path)        
        return [
            path_details.name, 
            path_details.suffix,
            path_details.stem
        ]
