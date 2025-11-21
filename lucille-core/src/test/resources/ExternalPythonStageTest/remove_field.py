import json

def process_document(doc):
    del doc['field2']
    return json.dumps(doc)
