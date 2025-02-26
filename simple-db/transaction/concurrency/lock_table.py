
class LockTable:
    MAX_TIME = 10000 # 10 seconds
    
    def __init__(self):
        self.locks = {}
    
    
    def s_lock(self, blk):
        """
        Requests an SLock on the specified block.
        If the lock request cannot be granted, the method waits
        until it can be granted.
        """
        pass
    