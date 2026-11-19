import json

def custom_method(doc):
    doc['field_added_by_python'] = "Hello from custom_method.py (method 1)"
    return json.dumps(doc)

def custom_method2(doc):
    doc['field_added_by_python'] = "Hello from custom_method.py (method 2)"
    return json.dumps(doc)