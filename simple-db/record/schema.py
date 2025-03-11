
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
        

class FieldInfo:
    def __init__(self, field_type, length):
        self.field_type = field_type
        self.length = length