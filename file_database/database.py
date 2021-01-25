import os
from .util import *
import shutil
from .data_file import DataFile

""" TODO
    this does not handle files with commas in values

    illegal characters: , :

    make sure record size does not overflow

"""

class Database:
    """ class that manages data using a directory """
    def __init__(self, data_dir):
        self.dir = data_dir
        makedir(self.dir)
        self._init_datafile()

    def _init_datafile(self):
        try:
            data_path, config_path = self._find_data_files()
        except NoFilesFoundError:
            self.data_file = None
        else:
            self.data_file = DataFile(data_path, config_path)

    def import_data(self, name, csv_path):
        assert self.data_file == None
        config_path = os.path.join(self.dir, f'{name}.config')
        data_path = os.path.join(self.dir, f'{name}.data')
        self.data_file = DataFile(data_path, config_path)
        self.data_file.import_data(name, csv_path)

    @property
    def name(self):
        try:
            return self.data_file.name
        except AttributeError:
            return os.path.basename(os.path.splitext(self.dir)[0])

    @property
    def fields(self):
        return self.data_file.fields

    @property
    def num_fields(self):
        return self.data_file.num_fields
    
    def open(self):
        assert not self.is_open()
        self.data_file.open()

    def is_open(self):
        return self.data_file.is_open()

    def close(self):
        assert self.is_open()
        self.data_file.close()

    # TODO clean up 

    def find(self, primary_key):
        """ returns index, record of record with primary_key
            raises RecordNotFoundError if record not found
        """
        assert self.is_open()

        # check end values
        endpoints = [0, len(self.data_file)-1]
        for point in endpoints:
            try:
                index, key, record = self.get_nonblank_record_and_key(point)
            except RecordNotFoundError:
                pass
            else:
                if key == primary_key:
                    return index, record

        return self._binary_search(primary_key, endpoints[0], endpoints[1])

    def _binary_search(self, key, start_index, end_index):
        """ start and end indices not inclusive """

        mid = (end_index + start_index) // 2

        if mid == start_index: # endpoints already checked, so key does not exist
            raise RecordNotFoundError()
        
        index, record_key, record = self.get_nonblank_record_and_key(mid)

        if record_key == key:
            return index, record
        elif record_key > key:
            return self._binary_search(key, start_index, mid)
        else:
            return self._binary_search(key, mid, end_index)

    def find_first_n_records(self, n):
        assert self.is_open()
        records = []
        for i in range(len(self.data_file)):
            record = self.data_file[i]

            if record is not None:
                records.append(record)

            if len(records) == n:
                return records

    def update(self, index, record, field, new_value):
        """ each record is a list of values with equal length to fields """
        assert self.is_open()
        
        new_record = record.copy()
        field_index = self.data_file.fields.index(field)
        new_record[field_index] = new_value
        self.data_file[index] = new_record

    def delete(self, index):
        """  """
        assert self.is_open()
        self.data_file[index] = self.data_file.BLANK_RECORD # write blank line

    def insert(self, record):
        """ each record is a list of values with equal length to fields """
        assert self.is_open()

        key = get_key(record)

        # if record should go at start of file
        first_index, first_record = self.get_nonblank_record(self.data_file.MIN_INDEX)
        if get_key(first_record) >= key:
            if self.data_file[self.data_file.MIN_INDEX] is None:
                self.data_file[self.data_file.MIN_INDEX] = record
                return
            else:
                return self.data_file.insert_and_rewrite(record)
        
        # if record should go at end of file
        last_index, last_record = self.get_last_nonblank_record()
        if get_key(last_record) <= key:
            if self.data_file[self.data_file.MAX_INDEX] is None:
                self.data_file[self.data_file.MAX_INDEX] = record
                return
            else:
                return self.data_file.insert_and_rewrite(record)

        try:
            index = self.insert_binary_search(key, first_index, last_index)
        except NoSpaceToInsertError:
            self.data_file.insert_and_rewrite(record)
        else:
            self.data_file[index] = record


    def all_records_empty(self, start_idx, end_idx):
        """ returns True if all of the records between start and end idx are blank """
        return all(self.data_file[i] == None for i in range(start_idx+1, end_idx))


    def insert_binary_search(self, key, start_index, end_index):
        """ return index for which to store key in database.
            start and end indices are guaranteed to contain records with keys unequal to given key
        """

        if end_index == start_index + 1:
            # endpoints already checked, so no room to insert
            raise NoSpaceToInsertError()

        mid = (end_index + start_index) // 2
        index, record_key, record = self.get_nonblank_record_and_key(mid)
        # print(start_index, mid, index, end_index, key, record_key)

        if index == end_index: # no records between mid and end_index
            record = None

            # check all records between start_index and mid in reverse order
            for i in range(mid-1, start_index, -1):
                record = self.data_file[i]
                if record is not None:
                    record_key = get_key(record)
                    index = i
            
            if record is None:
                return mid # no records between start and end indices

        if record_key == key:
            # try to find empty space before or after
            for i in [index-1, index+1]:
                if self.data_file[i] is None:
                    return i
            raise NoSpaceToInsertError()
        elif record_key > key:
            return self.insert_binary_search(key, start_index, index)
        else:
            return self.insert_binary_search(key, index, end_index)








    def _find_data_files(self):
        """ attempts to find data and config files in self.dir """
        data_path = None
        config_path = None
        
        paths = list(os.listdir(self.dir))
        
        for path in paths:
            extension = os.path.splitext(path)[-1]
            if extension == '.data':
                data_path = path
            elif extension == '.config':
                config_path = path

        if data_path is None or config_path is None:
            raise NoFilesFoundError()

        data_path = os.path.join(self.dir, data_path)
        config_path = os.path.join(self.dir, config_path)
        return data_path, config_path
    


    def get_nonblank_record(self, i):
        """ returns index and record of first nonblank entry at or after ith record in database. 0 indexed """
        
        for index in range(i, len(self.data_file)):
            record = self.data_file[index]
            if record is not None:
                return index, record
            
        raise RecordNotFoundError()

    def get_last_nonblank_record(self):
        """ returns index and record of first nonblank entry at or after ith record in database. 0 indexed """
        for i in range(len(self.data_file) - 1, -1, -1):
            record = self.data_file[i]
            if record is not None:
                return i, record

    def get_nonblank_record_and_key(self, i):
        """ returns index, key, and record of first nonblank record at or after index i """
        index, record = self.get_nonblank_record(i) 
        return index, get_key(record), record

