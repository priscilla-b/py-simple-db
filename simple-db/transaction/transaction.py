from .recovery.recovery_manager import RecoveryManager
from .concurrency.concurrency_manager import ConcurrencyManager
from .buffer_list import BufferList


class Transaction:
    def __init__(self, file_manager, log_manager, buffer_manager):
        """
        Creates a new transaction along with its associated 
        recovery and concurrency managers.

        This constructor depends on the file, log, and buffer 
        managers provided by the `simpledb.server.SimpleDB` class. 
        These objects are initialized during system startup.

        Therefore, this constructor should not be called until 
        either `SimpleDB.init(dbname)` or 
        `SimpleDB.init_file_log_and_buffer_manager(dbname)` has been 
        called first.
        """
        self.file_manager = file_manager
        self.buffer_manager = buffer_manager
        
        tx_num = self.next_tx_number()
        self.recovery_manager = RecoveryManager(self, tx_num, log_manager, buffer_manager)
        self.concurrency_mgr = ConcurrencyManager()
        self.my_buffers = BufferList(buffer_manager)
        
    
    def commit(self):
        """
        Commits the current transaction.
        Flushes all modified buffers, writes a commit record to the log,
        release all locks, and unpin any pinned buffers
        """
        self.recovery_manager.commit()
        print(f"Transaction {self.tx_num} committed")
        self.concurrency_mgr.release()
        self.my_buffers.unpin_all()
        
    
    def rollback(self):
        """
        Rolls back the current transaction.
        Undoes any changes to the buffers, writes a rollback record to the log,
        release all locks, and unpin any pinned buffers
        """
        self.recovery_manager.rollback()
        print(f"Transaction {self.tx_num} rolled back")
        self.concurrency_mgr.release()
        self.my_buffers.unpin_all()
        
    def recover(self):
        """
        Flush all modified buffers, then go through the log
        and roll back all uncommitted transactions. Finally, 
        write a quiescent checkpoint record to the log.
        Called during systems startup, before user transactions begins
        """
        self.buffer_manager.flush_all(self.tx_num)
        self.recovery_manager.recover() 
        
    def pin(self, block):
        """
        Pins the specified block.
        The transaction manages the buffer internally.
        
        :param block: a reference to the disk block
        """
        self.my_buffers.pin(block)
        
    def unpin(self, block):
        """
        Unpins the specified block.
        The transaction looks up the buffer pinned to this
        block and unpins it.
        
        :param block: a reference to the disk block
        """
        self.my_buffers.unpin(block)    
    
    def get_int(self, block, offset):
        """
        Returns the integer value at the specified offset of the specified block.
        This method first obtains an SLock on the block, then calls the buffer
        to retrieve the value.
        
        :param block: a reference to the disk block
        :param offset: the byte offset within the block
        """
        self.concurrency_mgr.s_lock(block)
        buff = self.my_buffers.get_buffer(block)
        return buff.get_int(offset) 

    
    def next_tx_number(self):
        """
        Returns the next transaction number.
        """
        self.next_txnum += 1
        return self.next_txnum