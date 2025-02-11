import struct 


class Page:
    CHARSET = 'ascii'
    def __init__(self, block_size=None, byte_array=None):
        """
        Constructor to initialize a Page instance.
        
        :param block_size: The size of the page if creating a new buffer.
        :param byte_array: A byte array for wrapping the page data (used in logging).
        """
        
        if block_size: 
            self.bb  = bytearray(block_size)
        elif byte_array:
            self.bb = byte_array
        else:
            raise ValueError("Must provide either block_size or byte_array")
         
    def get_int(self, offset):
        """
        Gets an integer value from the specified offset.
        
        :param offset: The position from which to read the integer.
        :return: The integer value.
        """
        return struct.unpack_from('>i', self.bb, offset)[0]

    def set_int(self, offset, n):
        """
        Sets an integer value at the specified offset.
        
        :param offset: The position to write the integer.
        :param n: The integer value to store.
        """
        struct.pack_into('>i', self.bb, offset, n)
    
    def get_bytes(self, offset):
        """
        Gets a byte array starting at the specified offset.
        
        :param offset: The position from which to start reading the byte array.
        :return: A byte array.
        """
        length = self.get_int(offset)
        start = offset + 4  # Skip the length integer
        end = start + length
        return self.bb[start:end]
    
    
    def set_bytes(self, offset, b):
        """
        Sets a byte array at the specified offset.
        
        :param offset: The position to write the byte array.
        :param b: The byte array to store.
        """
        self.set_int(offset, len(b))  # Store length first
        self.bb[offset + 4:offset + 4 + len(b)] = b
        
    def get_string(self, offset):
        """
        Gets a string value starting at the specified offset.
        
        :param offset: The position from which to start reading the string.
        :return: The decoded string.
        """
        b = self.get_bytes(offset)
        return b.decode(self.CHARSET)

    def set_string(self, offset, s):
        """
        Sets a string value at the specified offset.
        
        :param offset: The position to write the string.
        :param s: The string to store.
        """
        b = s.encode(self.CHARSET)
        self.set_bytes(offset, b)

    @staticmethod
    def max_length(strlen):
        """
        Returns the maximum length for a string based on the charset.
        
        :param strlen: The length of the string.
        :return: The max length of the page.
        """
        bytes_per_char = len('a'.encode(Page.CHARSET))  # Assuming 1 byte per character for ASCII
        return struct.calcsize('>i') + (strlen * bytes_per_char)

    def contents(self):
        """
        Returns the underlying byte array (equivalent to ByteBuffer contents in Java).
        
        :return: The bytearray containing the page data.
        """
        return self.bb

