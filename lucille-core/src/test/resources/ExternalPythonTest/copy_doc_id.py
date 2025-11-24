import json

def process_document(doc):
    doc['field_added_by_python'] = doc['id']
    return json.dumps(doc)
