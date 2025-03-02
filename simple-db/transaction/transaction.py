from .recovery.recovery_manager import RecoveryManager
from .concurrency.concurrency_manager import ConcurrencyManager


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
        recovery_manager = RecoveryManager(self, tx_num, log_manager, buffer_manager)
        concurrency_mgr = ConcurrencyManager()
        # my_buffers = 
        
        pass
    
    def next_tx_number(self):
        """
        Returns the next transaction number.
        """
        self.next_txnum += 1
        return self.next_txnum