from file.page import Page

from .log_record import LogRecord

class StartRecord(LogRecord):
    def __init__(self, page:Page):
        """
        Create a log record by reading one other value from the log.
        :param byte_buffer: ByteBuffer containing the log values.
    
        """
        offset = 4  # transaction number is stored starting at bytes offset 4
        self.tx_num = page.get_int(offset)

    def op(self):
        return self.START
    
    def tx_number(self):
        return self.tx_num
    
    def undo(self, transaction):
        """
        Does nothing. A start record contains no undo information.
        """
        pass

    
    def __str__(self):
        return f'<Start {self.tx_num}>'
    
    def write_to_log(self, log_manager, tx_num):
        """
        Write a start record to the log.
        This record contains the START operator, followed by the transaction id.
        :return: the LSN of the last log value
        """
        rec = bytearray(8)
        page = Page(rec)
        page.set_int(0, self.START)
        page.set_int(4, tx_num)
        return log_manager.append(rec)