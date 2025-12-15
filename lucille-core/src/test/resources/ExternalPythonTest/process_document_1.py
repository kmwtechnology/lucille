import json

def process_document(doc):
    doc['field_added_by_python'] = "Hello from process_document_1.py"
    return json.dumps(doc)
