from typing import Set, Iterator
from collections import deque

from start_record import StartRecord
from buffer.buffer_manager import BufferManager
from log.log_manager import LogManager
from commit_record import CommitRecord
from rollback_record import RollbackRecord
from log_record import LogRecord
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
        
        
    def recover(self):
        """
        Recover uncompleted transactions from the log
        and then write a quiescent checkpoint record to the log and flush it.
        """
        self.do_recover()
        self.buffer_manager.flush_all(self.tx_number)
        lsn = RollbackRecord.write_to_log(self.log_manager, self.tx_number)
        self.log_manager.flush(lsn)     
    
    def do_recover(self):
        """
        Do a complete database recovery.
        Method iterates through the log records.
        Whenever it finds a log record for an unfinished transaction,
        it calls undo() on that record.
        Method stops when it encounters a checkpoint record or the end of the log.
        """
        
        finished_txs: Set[int] = set()
        iter: Iterator = self.log_manager.iterator()
        
        for bytes_record in iter:
            rec = LogRecord.create_log_record(bytes_record)
            
            if rec.op() == LogRecord.CHECKPOINT:
                return  # stop recovery when we encounter a checkpoint record
            
            if rec.op() in {LogRecord.COMMIT, LogRecord.ROLLBACK}:
                finished_txs.add(rec.tx_number())  # record that this transaction is finished
            elif rec.tx_number() not in finished_txs:
                rec.undo(self.transaction)  # undo the operation if the transaction wasn't committed/rolled back
                
    