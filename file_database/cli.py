import os
from .database_manager import DatabaseManager
from .util import *

class CommandLineInterface:
    def __init__(self, data_storage_path):
        self.database_manager = DatabaseManager(data_storage_path)

        self.NAME_TO_COMMAND = {
            "create new database":  self.create_database,
            "open database":        self.open_database,
            "close database":       self.close_database,
            "display record":       self.display_record,
            "update record":        self.update_record,
            "create report":        self.create_report,
            "add record":           self.add_record,
            "delete record":        self.delete_record,
            "quit":                 self.quit,
        }

    def start(self):
        self.print_welcome()
        
        while True:
            
            self.print_menu()
            
            input_received = False
            while input_received == False:
                try:
                    command_name = get_option_from_user(
                        "Which command would you like to run? ",
                        list(self.NAME_TO_COMMAND.keys())
                        )
                except (InvalidInputError, EmptyInputError):
                    self.print_input_error()
                else:
                    self.NAME_TO_COMMAND[command_name]()
                    input_received = True

    # user commands
    def create_database(self):
        print("Creating database...")
        
        try:
            name = get_user_input("Enter the name of the database to create (case insensitive, must be unique): ")
        except EmptyInputError:
            print_error("Empty database name. Aborting")
            return
        
        default_csv_path = os.path.join(self.get_main_dir(), f'{name}.csv')
        path = input(f"Enter the CSV file to import ({default_csv_path}): ") or default_csv_path

        try:
            self.database_manager.create_database(name.lower(), path)
        except InvalidPathError:
            print_error(f"Path {path} could not be resolved. Aborting.")
        except DuplicateDatabaseNameError:
            print_error(f"Database name is not unique. Aborting.")

    def open_database(self):
        if self.database_manager.database_is_open():
            print_error(f"Database \"{self.database_manager.current_database.name}\" is already open. Close this database before creating a new one.")
        else:
            available_databases = self.database_manager.available_databases()

            if len(available_databases) == 0:
                print_error("No databases to open. Aborting.")
            else:
                print_options(available_databases)
                try:
                    db_name = get_option_from_user(
                        "Which Database would you like to open? ",
                        available_databases
                    )
                except (InvalidInputError, EmptyInputError):
                    print_error("Empty or invalid database selected. Aborting.")
                else:
                    self.database_manager.open_database(db_name)
                    print(f"Using database {db_name}.")

    def close_database(self):
        if not self.no_databases_open():
            print(f"Closing database {self.database_manager.current_database.name}")
            self.database_manager.close_database()

    def display_record(self):
        index, record = self.prompt_user_to_find_record("display")
        if record is not None: # error messages already printed
            self.print_record(record)

    def update_record(self):
        index, record = self.prompt_user_to_find_record("update")
        if record is None:
            return # error messages already printed
        
        self.print_record(record)
        try:
            field = get_option_from_user(
                "Enter the field to update: ",
                self.database_manager.current_database.fields
            )
        except EmptyInputError:
            print_error("Empty field. Aborting")
            return
        except InvalidInputError:
            print_error(f"Field does not exist. Aborting")
            return
        else:
            # cant update primary key
            if field == self.database_manager.current_database.fields[0]:
                print_error("Cannot update primary Key. Aborting")
                return
            

        try:
            new_value = get_user_input(f"Enter new {field} for record: ")
        except EmptyInputError:
            print_error("Empty value. Aborting")
            return
        
        prompt = f"Are you sure you want to update {field} to \"{new_value}\"? [Y/n] "
        if confirm(prompt, default='y'):
            try:
                self.database_manager.current_database.update(index, record, field, new_value)
            except InvalidRecordSizeError:
                print_error("One or more fields too long to store. Aborting")

    def create_report(self):
        if self.no_databases_open():
            return
        
        default_path = os.path.join(self.get_main_dir(), 'report.txt')
        path = input(f"Enter the file path of the report to generate ({default_path}): ") or default_path

        records = self.database_manager.current_database.find_first_n_records(10)
        with open(path, 'w') as f:
            f.write(self.format_records(records))

        print(f'Report generated at {path}.')

    def add_record(self):
        if self.no_databases_open():
            return
        
        print(f"Creating a new record in {self.database_manager.current_database.name} database...")
        record = []
        for field in self.database_manager.current_database.fields:
            value = input(f'Enter field "{field}": ') or ''
            record.append(value)

        self.print_record(record)

        prompt = f"Are you sure you want to add this record? [Y/n] "
        if confirm(prompt, default='y'):
            try:
                self.database_manager.current_database.insert(record)
            except InvalidRecordSizeError:
                print_error("One or more fields too long to store. Aborting")

    def delete_record(self):
        index, record = self.prompt_user_to_find_record("delete")
        if record is None:
            return # error messages already printed

        self.print_record(record)
        
        prompt = f"Are you sure you want to delete this record? [Y/n] "
        if confirm(prompt, default='y'):
            self.database_manager.current_database.delete(index)

    def quit(self):
        if confirm("Are you sure you want to quit? [Y/n] ", default='y'):
            print("Exiting...")
            exit()
            

    # util methods
    def print_welcome(self):
        print("Welcome to KyleDB!")

    def print_menu(self):
        print_options(list(self.NAME_TO_COMMAND.keys()))

    def print_input_error(self):
        print('Empty or invalid command. Select an integer from 1-9 or type the name of a command.')

    def print_record(self, record):
        print(self.format_records([record]))

    def format_records(self, records):
        spacing = 2
        db = self.database_manager.current_database
        data = []
        data.append(db.fields)
        data.extend(records)
        max_column_widths = [max_len([x[i] for x in data]) for i in range(db.num_fields)]

        output = ''
        for row in data:
            for i in range(db.num_fields):
                output += pad(row[i], max_column_widths[i]+spacing)
            output += '\n'

        return output

    def prompt_user_to_find_record(self, verb):
        """ prompts the user to find a record by primary key
            returns record and index of record if found
        """
        if self.no_databases_open():
            return None, None
        
        try:
            primary_key = get_user_input(f"Enter the primary key of the record to {verb}: ")
            primary_key = int(primary_key) # TODO fix this for non ints
        except EmptyInputError:
            print_error("Empty record key. Aborting.")
            return None, None
        except ValueError:
            print_error("Invalid key. Aborting.")
            return None, None
        
        try:
            return self.database_manager.current_database.find(primary_key)
        except RecordNotFoundError:
            print_error(f"No record found with key {primary_key}")
            return None, None

    def no_databases_open(self):
        """ returns True and prints error message if no databases are open """
        if not self.database_manager.database_is_open():
            print_error("No database open. Aborting.")
            return True
        return False

    def get_main_dir(self):
        """ return path of top-level directory """
        return os.path.abspath(os.path.join(get_current_dir(__file__), '..'))
