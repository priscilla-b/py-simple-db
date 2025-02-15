import os
import logging
from pathlib import Path
from file.file_manager import FileManager
from log.log_manager import LogManager
from buffer.buffer_manager import BufferManager



class SimpleDB:
    BLOCK_SIZE = 400
    BUFFER_SIZE = 8
    LOG_FILE = 'simpledb.log'
    
    def __init__(self, dirname, block_size=BLOCK_SIZE, buffer_size=BUFFER_SIZE):
        """
        Initializes the SimpleDB engine.
        
        :param dirname: Name of the database directory
        :param block_size: Size of database blocks
        :param buffer_size: Number of buffers
        """
        
        self.db_directory = Path(dirname)
        self.db_directory.mkdir(parents=True, exist_ok=True)
        
        self.file_manager = FileManager(self.db_directory, block_size)
        self.log_manager = LogManager(self.file_manager, self.LOG_FILE)
        self.buffer_manager = BufferManager(buffer_size, self.file_manager, self.log_manager)