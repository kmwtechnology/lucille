import json

def process_document(doc):
    doc['field_added_by_python'] = "Hello from custom_port.py"
    return json.dumps(doc)
