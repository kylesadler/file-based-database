import os
from .util import *
from .database import Database

class DatabaseManager:
    """ a class for managing multiple database instances """

    def __init__(self, data_dir):
        self.data_dir = data_dir
        makedir(data_dir)
        self.current_database = None
        self.databases = self.init_databases() # list of Database objects

    def create_database(self, database_name, csv_path):
        if database_name in self.available_databases():
            raise DuplicateDatabaseNameError()

        if not is_csv_file(csv_path):
            raise InvalidPathError()

        database_dir = os.path.join(self.data_dir, database_name)
        database = Database(database_dir)
        database.import_data(database_name, csv_path)
        self.databases.append(database)


    def open_database(self, name):
        if self.database_is_open():
            raise DatabaseOpenError()
        
        options = [d for d in self.databases if d.name == name]
        assert len(options) == 1
        self.current_database = options[0]
        self.current_database.open()


    def close_database(self):
        self.current_database.close()
        self.current_database = None


    def init_databases(self):
        output = []
        for dir_name in os.listdir(self.data_dir):
            path = os.path.join(self.data_dir, dir_name)
            print(path)
            if os.path.isdir(path):
                output.append(Database(path))

        return output


    def database_is_open(self):
        return self.current_database != None

    def available_databases(self):
        return [x.name for x in self.databases]