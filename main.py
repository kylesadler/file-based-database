import os
from file_database import CommandLineInterface

if __name__ == '__main__':
    data_storage_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data')
    cli = CommandLineInterface(data_storage_path=data_storage_path)
    cli.start()