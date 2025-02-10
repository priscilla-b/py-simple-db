import os
from pathlib import Path

from .block_id import BlockId


class FileManager:
    def __init__(self, db_directory, block_size):
        """
        Initializes the File Manager.
        
        :param db_directory: Path to the database directory
        :param block_size: The size of a single block in bytes
        """
        
        self.db_directory = Path(db_directory)
        self.block_size = block_size
        self.is_new = not self.db_directory.exists()
        self.open_files = {}
        
        # Create directory if new
        if self.is_new:
            self.db_directory.mkdir(parents=True, exist_ok=True)
        
        # Remove leftover temporary files
        for file in self.db_directory.iterdir():
           if file.name.startswith("temp"):
               file.unlink()
               
    
    def read_block(self, block, page):
        """
        Reads a block from disk into memory.
        
        :param block: BlockId object representing the block to read
        :param page: Page object to store the read data
        """
        
        try:
            with self._get_file(block.file_name()) as f:
                f.seek(block.number * self.block_size)
                page.contents[:] = f.read(self.block_size)  # Read into the page buffer
        except Exception as e:
            raise RuntimeError(f"Error reading block {block} from disk: {e}")
    
    
    def write(self, block, page):
        """
        Writes a page to disk.
        
        :param block: BlockId object representing the block to write
        :param page: Page object containing the data to write
        """
        
        try:
            with self._get_file(block.file_name()) as f:
                f.seek(block.number * self.block_size)
                f.write(page.contents)
        except Exception as e:
            raise RuntimeError(f"Error writing block {block} to disk: {e}")
        
    
    def append(self, filename):
        """
        Appends a new empty block to the file.
        
        :param filename: Name of the file to append to
        :return: BlockId of the newly added block
        """
        new_block_num = self.length(filename)
        block = BlockId(filename, new_block_num)
        empty_block = bytearray(self.block_size)
        
        try:
            with self._get_file(filename) as f:
                f.seek(block.number * self.block_size)
                f.write(empty_block)
        except Exception as e:
            raise RuntimeError(f"Error appending block to file {filename}: {e}")
        
        return block
    
    def length(self, filename):
        """
        Returns the number of blocks in a file.
        
        :param filename: Name of the file
        :return: Number of blocks in the file
        """
        
        try:
            with self._get_file(filename) as f:
                return f.seek(0, os.SEEK_END) // self.block_size
        except Exception as e:
            raise RuntimeError(f"Error getting length of file {filename}: {e}")
        
    
    def is_new(self):
        """
        Returns whether this is a new database.
        """
        return self.is_new

    def block_size(self):
        """
        Returns the block size.
        """
        return self.block_size
    
    def _get_file(self, filename):
        """
        Opens or retrieves a file handle for the given filename.
        
        :param filename: The file name
        :return: File handle
        """
        file_path = self.db_directory / filename
        if filename not in self.open_files:
            self.open_files[filename] = open(file_path, "r+b" if file_path.exists() else "w+b")
       
        return self.open_files[filename]

    
    