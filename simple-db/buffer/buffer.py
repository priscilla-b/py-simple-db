from file.page import Page
class Buffer:
    def __init__(self, file_manager, log_manager):
        """
        Initializes the Buffer.

        :param file_manager: Instance of FileManager for file operations.
        :param log_manager: Instance of LogManager for logging operations.
        """
        
        self.file_manager = file_manager
        self.log_manager = log_manager
        
        self.contents = Page(file_manager.block_size)
        self.block = None
        self.pins = 0
        self.txnum = -1
        self.lsn = -1
        
    def contents(self):
        """
        Returns the page stored in this buffer.

        :return: Page object containing buffer data.
        """
        return self.contents
    
    def block(self):
        """
        Returns a reference to the disk block allocated to this buffer.

        :return: BlockId or None if not assigned.
        """
        return self.block
    
    def set_modified(self, txnum, lsn):
        """
        Marks the buffer as modified by a transaction.

        :param txnum: Transaction ID.
        :param lsn: Log Sequence Number.
        """
        
        self.txnum = txnum
        if lsn > 0:
            self.lsn = lsn
    
    def is_pinned(self):
        """
        Checks if the buffer is pinned.

        :return: True if pinned, otherwise False.
        """
        return self.pins > 0
    
    def modifying_tx(self):
        """
        Returns the transaction ID that last modified the buffer.

        :return: Transaction ID.
        """
        return self.txnum
    
    def assign_to_block(self, block):
        """
        Reads the contents of the specified block into the buffer.
        If the buffer was modified, it first flushes its contents.

        :param block: BlockId object to be assigned.
        """
        self.flush()
        self.blk = block
        self.fm.read(self.blk, self.contents)
        self.pins = 0
        
    def flush(self):
        """
        Writes the buffer to disk if it has been modified.
        Ensures all log records up to `lsn` are written before flushing.
        """
        if self.txnum >= 0:
            self.lm.flush(self.lsn)
            self.fm.write(self.blk, self.contents)
            self.txnum = -1  # Reset transaction ID after writing
            
    def pin(self):
        """
        Increases the buffer's pin count.
        """
        self.pins += 1

    def unpin(self):
        """
        Decreases the buffer's pin count.
        """
        self.pins -= 1
    