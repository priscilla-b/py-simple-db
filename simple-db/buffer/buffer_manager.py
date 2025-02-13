
from .buffer import Buffer
import time

class BufferManager:
    """Manages the pinning and unpinning of buffers to blocks.
    """
    def __init__(self, num_buffers, file_manager, log_manager):
        """
        Creates a buffer manager having the specified number of buffer slots.
        :param num_buffers: Number of buffer slots to allocate.
        :param file_manager: Instance of FileManager for file operations.
        :param log_manager: Instance of LogManager for logging operations.
        """
        self.num_available = num_buffers
        self.buffer_pool = [Buffer(file_manager, log_manager) for _ in range(num_buffers)]
       
    
    def available(self):
        """
        Returns the number of available (i.e. unpinned) buffers.
        :return: the number of available buffers
        """
        return self.num_available
    
    
    def flush_all(self, txnum):
        """
        Flushes the dirty buffers modified by the specified transaction.
        :param txnum: the transaction's id number
        """
        for buffer in self.buffer_pool:
            if buffer.modifying_tx() == txnum:
                buffer.flush()
                
    def unpin(self, buffer: Buffer):
        """
        Unpins the specified data buffer.
        If it's pin count goes to zero, then notify any awaiting threads.
        :param buffer: the buffer to be unpinned
        """
        buffer.unpin()
        if not buffer.is_pinned():
            self.num_available += 1
            
    
    def synchronized(self, block):
        """
        Pins a buffer to the specified block, potentially waiting 
        until a buffer becomes available.
        If no buffer becomes available within a fixed time period, 
        then a BufferAbortException is thrown.
        :param block: the block to which the buffer should be pinned
        :return: the buffer pinned to the block
        """
        pass
        # try:
        #    timestamp = time.now()