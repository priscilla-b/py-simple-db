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
        
    
    def next_tx_number(self):
        """
        Returns the next transaction number.
        """
        self.next_txnum += 1
        return self.next_txnum