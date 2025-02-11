from file.page import Page
from file.block_id import BlockId
from log.log_iterator import LogIterator


class LogManager:
    def __init__(self, file_mgr, log_file):
        """
        Initializes the Log Manager for the specified log file.
        If the log file doesn't exist, it's created with an empty first block.

        :param file_mgr: The FileManager instance used for reading/writing files.
        :param logfile: The name of the log file.
        """
        self.fm = file_mgr
        self.log_file = log_file
        self.log_page = Page(block_size=self.fm.block_size())  # Assuming Page is already defined
        logsize = self.fm.length(self.log_file)

        if logsize == 0:
            self.current_blk = self.append_new_block()
        else:
            self.current_blk = BlockId(self.log_file, logsize - 1)
            self.fm.read(self.current_blk, self.log_page)

        self.latest_lsn = 0
        self.last_saved_lsn = 0

    def flush(self, lsn):
        """
        Ensures that the log record corresponding to the specified LSN has been written to disk.
        All earlier log records will also be written to disk.

        :param lsn: The LSN (Log Sequence Number) of a log record.
        """
        if lsn >= self.last_saved_lsn:
            self.flush_all()

    def iterator(self):
        """
        Returns an iterator to iterate through the log records in reverse order.

        :return: LogIterator object for iterating over log records.
        """
        self.flush_all()
        return LogIterator(self.fm, self.current_blk)

    def append(self, logrec):
        """
        Appends a log record to the log buffer.

        :param logrec: The byte array representing the log record to be appended.
        :return: The LSN of the appended log record.
        """
        boundary = self.log_page.get_int(0)
        recsize = len(logrec)
        bytes_needed = recsize + 4  # Integer.BYTES is 4

        if boundary - bytes_needed < 4:  # The log record doesn't fit, so move to the next block.
            self.flush_all()
            self.current_blk = self.append_new_block()
            boundary = self.log_page.get_int(0)

        recpos = boundary - bytes_needed
        self.log_page.set_bytes(recpos, logrec)
        self.log_page.set_int(0, recpos)  # The new boundary
        self.latest_lsn += 1
        return self.latest_lsn

    def append_new_block(self):
        """
        Initializes a new block, appends it to the log file, and returns the BlockId.

        :return: The BlockId of the newly created block.
        """
        blk = self.fm.append(self.log_file)
        self.log_page.set_int(0, self.fm.block_size())  # Set boundary to block size
        self.fm.write(blk, self.log_page)
        return blk

    def flush_all(self):
        """
        Writes the current log buffer to the log file and updates the last saved LSN.
        """
        self.fm.write(self.current_blk, self.log_page)
        self.last_saved_lsn = self.latest_lsn
