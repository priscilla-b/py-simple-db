
from .buffer import Buffer
import time

class BufferManager:
    """Manages the pinning and unpinning of buffers to blocks.
    """
    
    MAX_TIME = 10000  # 10 seconds
    
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
              
    def pin(self, block):
        """
        Pins a buffer to the specified block, potentially waiting 
        until a buffer becomes available.
        If no buffer becomes available within a fixed time period, 
        then a BufferAbortException is thrown.
        :param block: the block to which the buffer should be pinned
        :return: the buffer pinned to the block
        """
        try:
            timestamp = int(time.time() * 1000)  # converts from seconds to milliseconds
            buffer = self.try_to_pin(block)
            
            while buffer is None and not self.waiting_too_long(timestamp):
                time.sleep(self.MAX_TIME)
                buffer = self.try_to_pin(block)
            
            if buffer is None:
                raise BufferAbortException()
            return buffer
        except InterruptedError:
            raise BufferAbortException()
        
    def waiting_too_long(self, start_time):
        """
        Determines whether a certain amount of time has elapsed.
        :param start_time: the starting time
        :return: True if the specified amount of time has elapsed, otherwise False
        """
        return int(time.time() * 1000) - start_time > self.MAX_TIME
        
    def try_to_pin(self, block):
        """
        Tries to pin a buffer to the specified block.
        If there is already a buffer assigned to the block, 
        then that buffer is used.
        Otherwise, an unpinned buffer from the pool is chosen.
        Returns None if there are no available buffers.
        :param block: the block to which the buffer should be pinned
        :return: the buffer pinned to the block, or None if no buffer is available
        """
        buffer = self.find_existing_buffer(block)
        if buffer is None:
            buffer = self.choose_unpinned_buffer()
            if buffer is None:
                return None
            buffer.assign_to_block(block)
            
        if buffer.is_pinned():
            self.num_available -= 1
            buffer.pin()
            return buffer
      
    def find_existing_buffer(self, block):
        """
        Finds an existing buffer assigned to the specified block.
        Returns None if no such buffer exists.
        :param block: the block for which to find the buffer
        :return: the buffer assigned to the block
        """
        for buffer in self.buffer_pool:
            if block == buffer.block():
                return buffer
        return None
    
    def choose_unpinned_buffer(self):
        for buffer in self.buffer_pool:
            if not buffer.is_pinned():
                return buffer
            

class BufferAbortException(RuntimeError):
    """
    Indicates that a transaction needs to abort
    because a buffer request cannot be satisfied.
    """
    pass