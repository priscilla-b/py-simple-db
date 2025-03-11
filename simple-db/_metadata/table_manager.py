class TableManager:
    MAX_NAME = 16  # Maximum length of a table name
    
    def __init__(self, is_new, tx):
        """
        Creates a new catalog manager for the database system.
        If the db is new, two catalog tables are created
        
        :param is_new: true if the database is new
        :param tx: the startup transaction
        """
        pass