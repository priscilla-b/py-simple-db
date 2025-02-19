from transaction.transaction import Transaction
from log_record import LogRecord

from file.page import Page
from file.block_id import BlockId

class SetStringRecord(LogRecord):
    def __init__(self):
        super().__init__()