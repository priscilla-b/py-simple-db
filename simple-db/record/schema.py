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
        
    
    def add_all(self, schema):
        """
        Add all fields in the specified schema to this schema

        Args:
            schema (Schema): the other schema
        """
        for field_name in schema.fields:
            self.add(field_name, schema)
            
    @property
    def fields(self):
        """
        Return a collection containing the names 
        of all fields in the schema

        :return: a collection of the schema's field names
        """
        return self.fields
    
    def has_field(self, field_name):
        """
        Check if the schema has a field with the specified name

        :param field_name: the name of the field
        :return: True if the schema has a field with the specified name, False otherwise
        """
        return field_name in self.fields
    
    @property
    def _type(self, field_name):
        """
        Return the type of the specified field

        :param field_name: the name of the field
        :return: the type of the field
        """
        return self.info[field_name].field_type
    
    @property
    def length(self, field_name):
        """
        Return the length of the specified field

        :param field_name: the name of the field
        :return: the length of the field
        """
        return self.info[field_name].length
        

class FieldInfo:
    def __init__(self, field_type, length):
        self.field_type = field_type
        self.length = length
        
class SqlType(enum.IntEnum):
    INTEGER = 4  
    VARCHAR = 12  