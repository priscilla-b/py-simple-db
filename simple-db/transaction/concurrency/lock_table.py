import time

class LockTable:
    """
    Provides methods to lock and unlock blocks.
    If a transaction requests a lock that causes a conflict with an existing lock,
    then that transaction is placed on a waitlist.
    There's only one waitlist for all blocks.
    When the last lock on a block is unlocked, all transactions are
    removed from the waitlist and rescheduled.
    If one of those trasanctions discovers that the lock it's waiting for is
    still locked, it will place itself back on the waitlist.
    """
    MAX_TIME = 10000 # 10 seconds
    
    def __init__(self):
        self.locks = {}
    
    
    def s_lock(self, block):
        """
        Requests an SLock on the specified block.
        If the lock request cannot be granted, the method waits
        until it can be granted.
        
        :param block: the reference to the disk block
        """
        
        try:
            timestamp = int(time.time() * 1000)  # converts from seconds to milliseconds
            while self.has_x_lock(block) and not self.wating_too_long(timestamp):
                time.sleep(self.MAX_TIME)
                
            if self.has_x_lock(block):
                raise RuntimeError() # will change this to a LockAbortException later
                
            val = self.get_lock_val(block)
            self.locks[block] = val + 1
            
        except Exception as e:   # will change to the specific exception later
            raise RuntimeError()
        
    
    def x_lock(self, block):
        """
        Grant an XLock on the specified block.
        If a lock of any type exists when the method is called,
        then the calling thread will be placed on a wait list
        until the locks are released.
        If the thread remains on the wait list for a certain
        amount of time, then an exception is thrown.
        
        :param block: a reference to the disk block

        """
        try:
            timestamp = int(time.time() * 1000) 
            while self.has_other_s_locks(block) and not self.waiting_too_long(timestamp):
                time.sleep(self.MAX_TIME)
            
            if self.has_other_s_locks(block):
                raise RuntimeError()
            self.locks[block] = -1
        except Exception as e:
            raise RuntimeError()
        
    