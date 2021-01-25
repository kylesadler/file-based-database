class DatabaseOpenError(Exception):
    """Raised when a user tries to open a database, but there is one already open"""
    pass

class NoDatabaseToCloseError(Exception):
    """Raised when a user tries to close a database, but there is not one open"""
    pass

class EmptyInputError(Exception):
    """Raised when a user's input is empty"""
    pass

class InvalidInputError(Exception):
    """Raised when a user's input is invalid"""
    pass

class InvalidPathError(Exception):
    """Raised when a csv path is not valid"""
    pass

class DuplicateDatabaseNameError(Exception):
    """Raised when a database name is not unique"""
    pass

class NoFilesFoundError(Exception):
    """Raised when no config or data files are found in a database's directory"""
    pass

class RecordNotFoundError(Exception):
    """Raised when no record is found in a database"""
    pass

class InvalidRecordIndexError(Exception):
    """Raised when the user tries to find a record with an index that doesn't exist"""
    pass

class UpdatePrimaryKeyError(Exception):
    """Raised when the user tries to update a record's primary key"""
    pass

class NoSpaceToInsertError(Exception):
    """Raised when there is not room to insert a new record in the current database file"""
    pass

class InvalidRecordSizeError(Exception):
    """Raised when a record is too long to insert into the database"""
    pass
