from abc import ABC, abstractmethod

from transaction import Transaction
from file.page import Page

from .checkpoint_record import CheckpointRecord
from .start_record import StartRecord
from .commit_record import CommitRecord
from .rollback_record import RollbackRecord

class LogRecord(ABC):
    CHECKPOINT  = 0
    START = 1
    COMMIT = 2
    ROLLBACK = 3
    SETINT = 4
    SETSTRING = 5
    
    @abstractmethod
    def op(self) -> int:
        """
        Returns the log record's type.
        :return: the log record's type
        """
        pass
    
    @abstractmethod
    def tx_number() -> int:
        """
        Returns the transaction id stored with the log record.
        :return: the log record's transaction id
        """
        pass
    
    @abstractmethod
    def undo(self, transaction:Transaction):
        """
        Reverses the operation of this log record.
        The only log record types that need to be undone 
        are SETINT and SETSTRING.
        :param tx_num: the id of the transaction that is performing the undo.
        """
        pass
    
    def create_log_record(self, bytes:bytearray):
        """
        Interprets the bytes returned by the log iterator
        :param bytes: the byte array containing the log values
        """
        page = Page(byte_array=bytes)
        
        match page.get_int(offset=0):
            case self.CHECKPOINT:
                return CheckpointRecord(page)
            case self.START:
                return StartRecord(page)
            case self.COMMIT:
                return CommitRecord(page)
            case self.ROLLBACK:
                return RollbackRecord(page)
        
        