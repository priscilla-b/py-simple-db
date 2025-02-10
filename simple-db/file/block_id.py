
class BlockId:
    def __init__(self, filename, block_number):
        self.filename = filename
        self.block_number = block_number
        
    def filename(self):
        return self.filename
    
    def number(self):
        return self.block_number
    
    def equals(self, obj):
        if not isinstance(obj, BlockId):
            return False
        return self.filename == obj.filename and self.block_number == obj.block_number
    
    def to_string(self):
        return f"[file {self.filename}, block {self.block_number}]"
    
    def hash_code(self):
        return hash((self.to_string()))