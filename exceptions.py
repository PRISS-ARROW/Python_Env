class Wrong_Input(Exception):
    """
    Exception raised for errors in the input salary.

    Attributes:
        table_name -- input of the table name which caused the error
        message -- explanation of the error
    """

    def __init__(self, message="The name for this shouldn´t have a number in it."):
        self.message = message
        super().__init__(self.message)
