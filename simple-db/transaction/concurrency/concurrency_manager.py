from file.block_id import BlockId

class ConcurrencyManager:
    def __init__(self):
        pass
    
    def x_lock(self, block):
        """
        Obtain an XLock on the block if necessary.
        If the transaction does not have an XLock on that block,
        then the method first gets an SLock on that block, and then
        upgrades it to an XLock
        
        :param block: a reference to the disk block

        """
        if not self.has_s_lock(block):
            self.s_lock(block)
            self.lock_table.x