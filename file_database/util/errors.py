class Error(Exception):
    """ base error class """
    pass

class DatabaseOpenError(Error):
    """Raised when a user tries to open a database, but there is one already open"""
    pass

class NoDatabaseToCloseError(Error):
    """Raised when a user tries to close a database, but there is not one open"""
    pass

class EmptyInputError(Error):
    """Raised when a user's input is empty"""
    pass

class InvalidInputError(Error):
    """Raised when a user's input is invalid"""
    pass

class InvalidPathError(Error):
    """Raised when a csv path is not valid"""
    pass

class DuplicateDatabaseNameError(Error):
    """Raised when a database name is not unique"""
    pass
