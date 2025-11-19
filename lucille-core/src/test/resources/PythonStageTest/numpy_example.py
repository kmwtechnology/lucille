import numpy as np
import json

def process_document(doc):
  arr = np.array([1, 2, 3, 4], dtype=int)
  total = int(np.sum(arr))

  doc['field_added_by_python'] = total
  return json.dumps(doc)