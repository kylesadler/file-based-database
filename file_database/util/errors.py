# input errors
class EmptyInputError(Exception):
    """Raised when a user's input is empty"""
    pass

class InvalidInputError(Exception):
    """Raised when a user's input is invalid"""
    pass

# file errors
class InvalidPathError(Exception):
    """Raised when a csv path is not valid"""
    pass

class NoFilesFoundError(Exception):
    """Raised when no config or data files are found in a database's directory"""
    pass

# database errors
class DuplicateDatabaseNameError(Exception):
    """Raised when a database name is not unique"""
    pass

class DuplicatePrimaryKeyError(Exception):
    """Raised when the user tries to insert a record with a duplicat primary key"""
    pass

class RecordNotFoundError(Exception):
    """Raised when no record is found in a database"""
    pass

class InvalidRecordSizeError(Exception):
    """Raised when a record is too long to insert into the database"""
    pass

class NoSpaceToInsertError(Exception):
    """Raised when there is not room to insert a new record in the current database file"""
    pass
