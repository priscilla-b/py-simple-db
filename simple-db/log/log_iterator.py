from file.block_id import BlockId
from file.page import Page

class LogIterator:
    def __init__(self, file_mgr, blk):
        """
        Initializes the log iterator for traversing log records in reverse order.
        
        :param file_mgr: The FileMgr instance used for reading blocks.
        :param blk: The BlockId where the log starts.
        """
        self.fm = file_mgr
        self.blk = blk
        self.p = Page(block_size=self.fm.block_size())  
        self.move_to_block(self.blk)

    def has_next(self):
        """
        Determines if there are earlier records in the log file.
        
        :return: True if there is an earlier record, False otherwise.
        """
        return self.current_pos < self.fm.block_size() or self.blk.number() > 0

    def next(self):
        """
        Moves to the next log record in the block.
        If there are no more log records in the block, moves to the previous block.

        :return: The next log record as a byte array.
        """
        if self.current_pos == self.fm.block_size():
            self.blk = BlockId(self.blk.file_name(), self.blk.number() - 1)
            self.move_to_block(self.blk)
        
        rec = self.p.get_bytes(self.current_pos)
        self.current_pos += 4 + len(rec)  # Integer size (4 bytes) + record length
        return rec

    def move_to_block(self, blk):
        """
        Moves to the specified log block and positions it at the first record in that block.
        
        :param blk: The BlockId to move to.
        """
        self.fm.read(blk, self.p)
        self.boundary = self.p.get_int(0)
        self.current_pos = self.boundary
