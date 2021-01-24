import os
from .util import *


""" TODO
    this does not handle files with commas in values

    illegal characters: , :

"""

class Database:
    """ class that manages data using a directory """
    def __init__(self, data_dir):
        self.dir = data_dir
        makedir(self.dir)
        self.file = None # file to read / write from
        # self.field_to_length = None # dict of {field:length}
        # self.name = None
        self.initialized = False
        
        try:
            self._init_database() # this sets self.initialized
        except NoFilesFoundError:
            pass

    def import_data(self, name, csv_path):
        config_path = os.path.join(self.dir, f'{name}.config')
        data_path = os.path.join(self.dir, f'{name}.data')

        fields = None
        num_records = 0
        max_column_widths = None
        with open(csv_path, 'r') as csv:
            # find field names and max column widths
            for i, line in enumerate(csv):
                values = parse_csv_line(line)
                if i == 0:
                    fields = values
                    max_column_widths = [0]*len(values)
                else:
                    widths = [len(x) for x in values]
                    max_column_widths = [max(x,y) for x,y in zip(max_column_widths, widths)]
                    num_records += 1


            csv.seek(0)
            # write data from csv file to data file
            with open(data_path, 'w') as data:
                for i, line in enumerate(csv):
                    if i == 0:
                        continue
                    
                    line = parse_csv_line(line)
                    data.write(pad_values(line, max_column_widths))
                    data.write("\n")

        field_to_length = {f: w for f, w in zip(fields, max_column_widths)}

        self._write_config(name, num_records, field_to_length, config_path)
        self._init_database()

    def open(self):
        assert not self.is_open()
        self.file = open(self.data_path, 'r+')

    def is_open(self):
        return self.initialized and self.file != None

    def close(self):
        assert self.is_open()
        self.file.close()
        self.file = None

    # TODO clean up 

    def find(self, primary_key):
        """ returns record with primary_key
            raises RecordNotFoundError if record not found
        """
        assert self.is_open()

        # check end values
        endpoints = [0, self.num_records-1]
        for point in endpoints:
            _, key, record = self.get_record_and_key(point)
            if key == primary_key:
                return record

        return self._binary_search(primary_key, endpoints[0], endpoints[1])

    def _binary_search(self, key, start_index, end_index):
        """ start and end indices not inclusive """

        mid = (end_index + start_index + 1) // 2

        if mid in [start_index, end_index]:
            raise RecordNotFoundError()
        
        index, record_key, record = self.get_record_and_key(mid)
        
        if record_key == key:
            return record
        elif record_key > key:
            return self._binary_search(key, start_index, index)
        else:
            return self._binary_search(key, index, end_index)

    def get_record(self, i):
        """ returns index and record of first nonblank entry at or after ith record in database. 0 indexed """
        if i >= self.num_records:
            raise InvalidRecordIndexError()
        
        self.file.seek(i * self.line_size)
        record = None
        index = i
        while record == None:
            record = self.parse_line(self.file.readline())
            index += 1

        return index - 1, record
    

    def find_first_n_records(self, n):
        assert self.is_open()
        records = []
        index = 0
        for i in range(n):
            index, record = self.get_record(index)
            index += 1
            records.append(record)

        return records

    def update(self, record, field, new_value):
        """ each record is a list of values with equal length to fields """
        assert self.is_open()
        pass

    def delete(self, record):
        """ each record is a list of values with equal length to fields """
        assert self.is_open()
        pass

    def insert(self, record):
        """ each record is a list of values with equal length to fields """
        assert self.is_open()
        pass




    @property
    def fields(self):
        return list(self.field_to_length.keys())

    @property
    def num_fields(self):
        return len(self.fields)

    def _init_database(self):
        """ tries to initialize database from files found in self.dir """
        assert not self.initialized
        self.data_path, self.config_path = self._find_data_files()
        self.name, self.num_records, self.field_to_length = self._read_config(self.config_path)
        self.line_size = sum(x for x in self.field_to_length.values()) + 1 # newline
        self.initialized = True
        
    def _read_config(self, path):
        """ reads name and field_to_length from config file """
        with open(path, 'r') as f:
            name = f.readline().strip()
            num_records = int(f.readline().strip())
            field_lengths = f.readline().strip().split(",")

            field_to_length = {}
            for field_length in field_lengths:
                field, length = field_length.split(':')
                field_to_length[field] = int(length)

        return name, num_records, field_to_length

    def _write_config(self, name, num_records, field_to_length, path):
        """ stores database name and field_to_length dictionary in config file at path """
        with open(path, 'w') as config:
            config.write(name)
            config.write('\n')
            config.write(str(num_records))
            config.write('\n')
            config.write(','.join([
                f'{f}:{w}' for f, w in field_to_length.items()
            ]))

    def _find_data_files(self):
        """ attempts to find data and config files in self.dir """
        data_path = None
        config_path = None
        
        paths = list(os.listdir(self.dir))
        
        if len(paths) != 2:
            raise NoFilesFoundError()
        
        for path in paths:
            extension = os.path.splitext(path)[-1]
            if extension == '.data':
                data_path = path
            elif extension == '.config':
                config_path = path

        if data_path is None or config_path is None:
            raise NoFilesFoundError()

        return os.path.join(self.dir, data_path), os.path.join(self.dir, config_path)
    
    def get_record_and_key(self, i):
        """ returns index, key, and record of first nonblank record at or after index i """
        index, record = self.get_record(i) 
        return index, int(record[0]), record # TODO fix this not having to be an int

    def parse_line(self, line):
        """ returns list of fields from line in database data file. removes newline at end """
        if len(line) > 0 and line[-1] == '\n':
            line = line[:-1]

        if len(line) == 0:
            return None

        values = []
        i = 0
        for length in self.field_to_length.values():
            values.append(line[i:i+length].strip())
            i+=length
            
        return values

def parse_csv_line(line):
    """ from a CSV line, returns list of fields. removes newline at end """
    if line[-1] == '\n':
        line = line[:-1]
    return line.split(',')

def pad_values(values, widths):
    """ values is a list of strings to store
        widths is a list of int length of each field
        returns a string that is padded with the correct field widths
    """

    # TODO time zip and maybe remove
    return ''.join([fixed_len(v, w) for v, w in zip(values, widths)])