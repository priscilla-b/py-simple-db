import enum

class Schema:
    
    def __init__(self):
        self.fields = []
        self.info = {}
        
    def add_field(self, field_name, field_type, length):
        """
        Add a field to the schema with a specified name, type and length.
        If field type is an integer, then the lenght value is irrelevant.

        :param field_name: the name of the field
        :param field_type: the type of the field
        :param length: the length of the field
        """
        self.fields.append(field_name)
        self.info[field_name] = FieldInfo(field_type, length)
        

    def add_int_field(self, field_name):
        """Add an integer field to the schema
        
        :param fieldname: the name of the field

        """
        self.add_field(field_name, SqlType.INTEGER, 0)
        
    def add_string_field(self, field_name, length):
        """
        Add a string field to the schema
        The length of the string is specified by the length parameter
        e.g if a field is defined as varchar(10), then the length is 10
        
        :param fieldname: the name of the field
        :param length: the length of the field

        """
        self.add_field(field_name, SqlType.VARCHAR, length)
        
    
    def add(self, fieldname, schema):
        """
        Add a field to the schema with the same type and length as 
        the corresponding field in another schema

        Args:
            fieldname (string): the name of the field
            schema (Schema): the other schema
        """
        
        _type = schema.type(fieldname)
        length = schema.length(fieldname)
        self.add_field(fieldname, _type, length)
        
        

class FieldInfo:
    def __init__(self, field_type, length):
        self.field_type = field_type
        self.length = length
        
class SqlType(enum.IntEnum):
    INTEGER = 4  
    VARCHAR = 12  