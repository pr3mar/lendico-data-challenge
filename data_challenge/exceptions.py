class InconsistentNumberOfRowsException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class InconsistentTablesException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
