import os
from util import *

class Database:
    """ class that manages data using a directory """
    def __init__(self, data_dir):
        self.dir = data_dir
        makedir(self.dir)
        self._read_dir()

    def import_data(self, name, csv_path):
        config_file = f'{name}.config'
        data_file = f'{name}.data'
        self._read_dir()


    def find(self, primary_key):
        return

    def find_first_n_records(self, n):
        pass

    def update(self, record, field, new_value):
        """ each record is a list of values with equal length to fields """
        pass

    def delete(self, record):
        pass

    def insert(self, values):
        pass

    @property
    def fields(self):
        return list(self.field_to_length.keys())

    def _read_dir(self):
        """ read directory and set self.field_to_length, self.name, etc. if data is present """
        for path in os.listdir(self.dir):
            pass # TODO
        
        self.field_to_length = {} # TODO dict of {field:length} read from config file if present
