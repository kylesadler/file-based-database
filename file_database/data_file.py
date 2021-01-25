import os
from .util import *
import shutil
from traceback import print_exc


""" TODO
    this does not handle files with commas in values

    illegal characters: , :

    make sure record size does not overflow

"""

class DataFile:
    """ class that manages a data file. this provides a python list-like interface """
    def __init__(self, data_path, config_path):
        self.data_path = data_path
        self.config_path = config_path
        
        # file to read / write from
        self.file = None
        
        try:
            self._load_config()
            # self.field_to_length # dict of {field:length}
            # self.name
            # self.num_records
            # self.line_size = sum(x for x in self.field_to_length.values()) + 1 # newline
            # self.BLANK_RECORD = self.format(['']*self.num_fields)
            self.initialized = True
        except FileNotFoundError:
            self.initialized = False


    def __getitem__(self, index):
        self._seek_to(index)
        return self.parse(self.file.readline())

    def __len__(self):
        return self.num_records

    def __setitem__(self, index, record):
        """ writes a record in the data file at specified location (or current location if index is None) """
        self._seek_to(index)
        # make sure field sizes are legal
        if not self._fields_correct_length(record):
            raise InvalidRecordSizeError()

        self.file.write(self.format(record))

    def _fields_correct_length(self, record):
        return all(l >= len(v) for l, v in zip(self.field_to_length.values(), record))


    def _seek_to(self, line_num):
        if not self._is_valid_index(line_num):
            raise InvalidRecordIndexError()
        print('seeking to ', line_num)
        self.file.seek(line_num * self.line_size)

    def _is_valid_index(self, index):
        return index >= 0 and index < len(self)

    def close(self):
        self.file.close()
        self.file = None

    def open(self):
        self.file = open(self.data_path, 'r+')
        self._seek_to(0)

    def is_open(self):
        return self.file is not None


    def insert_and_rewrite(self, record_to_insert):
        """ rewrite the entire file, inserting the record, and leaving blank lines between entries """
        key = get_key(record_to_insert)
        inserted = False
        num_records = 0

        self._seek_to(0)
        print('rewriting file...')
        tmp_path = self.data_path + '.tmp'
        with open(tmp_path, 'w') as f:
            f.write(self.BLANK_RECORD)
            num_records += 1
            for i in range(self.num_records):
                record = self[i]
                if record is not None:
                    if get_key(record) > key and not inserted:
                        f.write(self.format(record_to_insert))
                        f.write(self.BLANK_RECORD)
                        inserted = True
                        num_records += 2

                    f.write(self.format(record))
                    f.write(self.BLANK_RECORD)
                    num_records += 2
            
            # insert at end
            if not inserted:
                f.write(self.format(record_to_insert))
                f.write(self.BLANK_RECORD)
                num_records += 2

        shutil.move(tmp_path, self.data_path)
        self.close()
        self.open() # update self.file
        self.num_records = num_records
        self._save_config()

    @property
    def fields(self):
        return list(self.field_to_length.keys())

    @property
    def MIN_INDEX(self):
        return 0

    @property
    def MAX_INDEX(self):
        return self.num_records - 1

    @property
    def num_fields(self):
        return len(self.fields)

    def _load_config(self):
        """ reads name and field_to_length from config file """
        with open(self.config_path, 'r') as f:
            self.name = f.readline().strip()
            self.num_records = int(f.readline().strip())
            field_lengths = f.readline().strip().split(",")

            self.field_to_length = {}
            for field_length in field_lengths:
                field, length = field_length.split(':')
                self.field_to_length[field] = int(length)

        self.line_size = sum(x for x in self.field_to_length.values()) + 1 # newline
        self.BLANK_RECORD = self.format(['']*self.num_fields)

    def _save_config(self):
        """ stores database name and field_to_length dictionary in config file at path """
        with open(self.config_path, 'w') as config:
            config.write(self.name)
            config.write('\n')
            config.write(str(self.num_records))
            config.write('\n')
            config.write(','.join([
                f'{f}:{w}' for f, w in self.field_to_length.items()
            ]))

    def parse(self, line):
        """ returns list of fields from line in database data file. removes newline at end """
        try:
            values = []
            i = 0
            for length in self.field_to_length.values():
                values.append(line[i:i+length].strip())
                i+=length
            assert not all(x == '' for x in values)
        except (AssertionError, IndexError):
            return None
        else:
            return values

    def format(self, record):
        """ values is a list of strings to store
            widths is a list of int length of each field
            returns a string that is padded with the correct field widths
        """

        # TODO time zip and maybe remove
        return ''.join([fixed_len(x, l) for x, l in zip(record, self.field_to_length.values())]) + '\n'


    def import_data(self, name, csv_path):
        assert not self.initialized

        self.name = name

        fields = None
        max_column_widths = None
        with open(csv_path, 'r') as csv:
            # find field names and max column widths
            num_records = 0
            for i, line in enumerate(csv):
                values = parse_csv_line(line)
                if i == 0:
                    fields = values
                    max_column_widths = [0]*len(values)
                else:
                    widths = [len(x) for x in values]
                    max_column_widths = [max(x,y) for x,y in zip(max_column_widths, widths)]
                    num_records += 1

            self.num_records = num_records*2 + 1
            self.field_to_length = {f: w for f, w in zip(fields, max_column_widths)}
            self._save_config()
            self._load_config()

            # write data from csv file to data file
            csv.seek(0)
            with open(self.data_path, 'w') as data:
                data.write(self.BLANK_RECORD)
                for i, line in enumerate(csv):
                    if i != 0:
                        data.write(self.format(parse_csv_line(line)))
                        data.write(self.BLANK_RECORD)
