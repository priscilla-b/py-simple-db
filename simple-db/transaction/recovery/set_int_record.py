
from log_record import LogRecord
from transaction.transaction import Transaction

from file.page import Page
from file.block_id import BlockId

class SetIntRecord(LogRecord):
    def __init__(self, page:Page):
        """
        Creates a new setint log record.
        """
        tpos = 4
        self.tx_num = page.get_int(tpos)
        
        fpos = tpos + 4
        self.filename = page.get_string(fpos)
        
        bpos = fpos + Page.max_length(len(self.filename))
        self.block_num = page.get_int(bpos)
        
        self.block = BlockId(self.filename, self.block_num)
        
        opos = bpos + 4
        self.offset = page.get_int(opos)
        
        vpos = opos + 4
        self.val = page.get_int(vpos)

        
    def op(self):
        return self.SETINT
    
    def tx_number(self):
        return self.tx_num
    
    def __str__(self):
        return f'<SETINT {self.tx_num} {self.block} {self.offset} {self.val}>'
    
    def undo(self, transaction:Transaction):
        """
        Replace the specified data value with the value saved in this log record.
        Pins a buffer to the specified block, calls setInt to restore the saved value
        and upins the buffer.
        """
        transaction.pin(self.block)
        transaction.set_int(self.block, self.offset, self.val, False)  # don't log the undo!
        transaction.unpin(self.block)
    
    
    def write_to_log(self, log_manager, tx_num, offset, val, oldval):
        """
        Write a setint record to the log.
        This record contains the SETINT operator, followed by the transaction id, offset, value and old value.
        :return: the LSN of the last log value
        """
        tpos = 4
        fpos = tpos + 4
        bpos = fpos + Page.max_length(len(self.filename))
        opos = bpos + 4
        vpos = opos + 4
        
        rec = bytearray(vpos + 4)
        page = Page(rec)
        page.set_int(0, self.SETINT)
        page.set_int(tpos, tx_num)
        page.set_string(fpos, self.block.filename())
        page.set_int(bpos, self.block.number())
        page.set_int(opos, self.offset)
        page.set_int(vpos, self.val)

        return log_manager.append(rec)