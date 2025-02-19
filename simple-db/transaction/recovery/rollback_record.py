
from log_record import LogRecord
from file.page import Page

class RollbackRecord(LogRecord):
    def __init__(self, page:Page):
        offset = 4
        self.tx_num = page.get_int(offset)
        
    def op(self):
        return self.ROLLBACK
    
    def tx_number(self):
        return self.tx_num
    
    def undo(self, transaction):
        """
        Does nothing. A rollback record contains no undo information.
        """
        pass
    
    def __str__(self):
        return f'<ROLLBACK {self.tx_num}>'
    
    
    def write_to_log(self, log_manager, tx_num):
        """
        Write a rollback record to the log.
        This record contains the ROLLBACK operator, followed by the transaction id.
        :return: the LSN of the last log value
        """
        rec = bytearray(8)
        page = Page(rec)
        page.set_int(0, self.ROLLBACK)
        page.set_int(4, tx_num)
        return log_manager.append(rec)
