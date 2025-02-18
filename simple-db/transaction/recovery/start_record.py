from file.page import Page
class StartRecord:
    def __init__(self, page:Page):
        """
        Create a log record by reading one other value from the log.
        :param byte_buffer: ByteBuffer containing the log values.
    
        """
        offset = 4  # transaction number is stored starting at bytes offset 4
        self.tx_num = page.get_int(offset)

    def __str__(self):
        return f'StartRecord(transaction_id={self.transaction_id})'