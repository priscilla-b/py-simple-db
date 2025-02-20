from start_record import StartRecord
from buffer.buffer_manager import BufferManager
from log.log_manager import LogManager
from commit_record import CommitRecord
from rollback_record import RollbackRecord
from transaction.transaction import Transaction

class RecoveryManager:
    def __init__(self, transaction:Transaction, tx_number, log_manager:LogManager, buffer_manager:BufferManager):
        """
        Create a recovery manager for the specified transaction.
        :param tx_number: the id of the specified transaction
        """
        self.transaction = transaction
        self.tx_number = tx_number
        self.log_manager = log_manager
        self.buffer_manager = buffer_manager

        StartRecord.write_to_log(log_manager, tx_number)
        
    def commit(self):
        """
        Writes a commit record to the log and flushes the log to disk.
        """
        self.buffer_manager.flush_all(self.tx_number)
        lsn = CommitRecord.write_to_log(self.log_manager, self.tx_number)
        self.log_manager.flush(lsn)
        
    def rollback(self):
        """
        Write a rollback record to the log and flush it to disk.
        """
        self.do_rollback()
        self.buffer_manager.flush_all(self.tx_number)
        lsn = RollbackRecord.write_to_log(self.log_manager, self.tx_number)
        self.log_manager.append(lsn)
        
    