import uuid
import os
import csv
import pandas as pd
from datetime import datetime


class LocalFileService:
    def __init__(self, local_directory):
        self.local_directory = local_directory

    def load_csv_to_dataframe(self, local_file_name):
        local_file_path = "{0}/{1}".format(self.local_directory, local_file_name)
        df = pd.read_csv(local_file_path, encoding="utf-8", index_col=False)
        df.columns = df.columns.str.replace(" ", "_")
        df.reset_index(drop=True, inplace=True)
        df.columns = map(str.lower, df.columns)
        return df

    def copy_results_to_csv(self, local_file_name, dataframe):
        dataframe.to_csv(local_file_name, encoding="utf-8", index=False)

    def copy_json_results_to_csv(self, file_name, json_data):
        # now we will open a file for writing
        data_file = open(file_name, "w")

        # create the csv writer object
        csv_writer = csv.writer(data_file)

        # Counter variable used for writing
        # headers to the CSV file
        count = 0

        for data_row in json_data:
            if count == 0:

                # Writing headers of CSV file
                header = data_row.keys()
                csv_writer.writerow(header)
                count += 1

            # Writing data of CSV file
            csv_writer.writerow(data_row.values())

        data_file.close()


    def get_csv_file_path(self, file_prefix=None, extension="csv", file_suffix=None):
        """file name should be object_name-yyyy-mm-dd-mm-hh-ss"""
        file_prefix = str(uuid.uuid4()) if file_prefix is None else file_prefix
        file_suffix = str(datetime.now()) if file_suffix is None else file_suffix
        file_name = (
            (file_prefix + "-" + file_suffix).replace(" ", "-").replace(":", "")
            + "."
            + extension
        )
        if self.local_directory is not None:
            file_name = os.path.join(self.local_directory, file_name)

        return os.path.abspath(file_name)   
