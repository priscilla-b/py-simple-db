from .log_record import LogRecord
from file.page import Page

class CheckpointRecord(LogRecord):
    def op(self):
        return self.CHECKPOINT
    
    def tx_number():
        """
        Checkpoint record has no associated transaction
        So return -1
        """
        return -1  # dummy value
    
    def undo(self, transaction):
        """
        A checkpoint record contains no undo information.
        """
        pass
    
    def to_string(self):
        return "<CHECKPOINT>"
    
    def write_to_log(self, log_manager):
        rec = bytearray(4)
        page = Page(rec)
        page.set_int(0, self.CHECKPOINT)
        return log_manager.append(rec)