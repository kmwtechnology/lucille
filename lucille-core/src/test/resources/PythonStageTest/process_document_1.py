import json

def process_document(doc):
    print(f"Processing document: {doc}")
    doc['field_added_by_python'] = "Hello from Python!"
    print(f"Returning processed document: {doc}")
    return json.dumps(doc)
