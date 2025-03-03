
class BufferList:
    def __init__(self, buffer_manager):
        self.buffer_manager = buffer_manager
        self.buffers = {}
        self.pins = []
        
    def get_buffer(self, block):
        """
        Return the buffers pinned to the specified block.
        Returns none if the transaction has not pinned the block.

        :param block: a reference to the disk block
        
        :return: the buffer pinned to that block
        """
        return self.buffers.get(block)
    
    
    def pin(self, block):
        """
        Pin the block and keep track of the buffer internally
        
        :param block: a reference to the disk block

        """
        
        buff = self.buffer_manager.pin(block)
        self.buffers[block] = buff
        self.pins.append(block)
        
        
    def unpin(self, block):
        """
        Unpin the specified block
        
        :param block: a reference to the disk block

        """
        
        buff = self.buffers.get(block)
        self.buffer_manager.unpin(buff)
        self.pins.remove(block)

        if not block in self.pins:
            self.buffers.pop(block)
            
            
    def unpin_all(self):
        """Unpin all buffers still pinned by this transaction
        """
        for block in self.pins:
            buff = self.buffers.get(block)
            self.buffer_manager.unpin(buff)
        
        self.buffers.clear()
        self.pins.clear()
            