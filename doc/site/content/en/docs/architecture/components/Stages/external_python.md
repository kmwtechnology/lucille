---
title: ExternalPython
date: 2025-12-16
description: Run a document through an external Py4J Python environment.
---

### Why Use It?

ExternalPython delegates per-document processing to an external Python process using [Py4J](https://www.py4j.org/). Lucille serializes the Document into a request, calls a Python function, receives a JSON response, and applies that response back onto the document.

### When To Use It

Use ExternalPython when you need one or more of the following:
- Real Python compatibility (including packages with native dependencies).
- Dependency management via a requirements.txt installed into a managed venv.
- Process isolation apart from the JVM.

### When To Use EmbeddedPython Instead

Avoid ExternalPython and use EmbeddedPython when you need one or more of the following:
- Minimal operational overhead (ports, subprocess lifecycle, venv creation, pip installs).
- No use of any external Python libraries or native dependencies that require a real Python environment.
- Lightweight field enrichment/transformation.

### Restrictions
Your python file must be in one of the following directories that start in the current working directory that is running lucille:
- `./python`
- `./src/main/resources`
- `./src/test/resources`
- `./src/test/resources/ExternalPythonTest` (for testing)

### Example

#### Input Document

```json
{
  "id": "doc-1",
  "title": "Hello",
  "author": "Test",
  "views": 123
}
```

#### Python Script
```python
def process_document(doc):
    title = doc["title"]
  
    return {
        "title": title.upper()
    }
```

#### Python Returns

```json
{
  "title": "HELLO"
}
```

#### Output Document

```json
{
  "id": "doc-1",
  "title": "HELLO"
}
```

#### Config Parameters

```hocon
{
  name: "ExternalPython-Example"
  class: "com.kmwllc.lucille.stage.ExternalPython"

  scriptPath: "/path/to/my_script.py"

  # Optional
  pythonExecutable: "python3"
  requirementsPath: "/path/to/requirements.txt"
  functionName: "process_document"
  port: 25333
}
```

### Example (NumPy)

#### Input Document

```json
{
  "id": "doc-2",
  "values": [1, 2, 3, 4, 5]
}
```

#### Python Script

```python
import numpy as np

def process_document(doc):
    arr = np.array(doc["values"], dtype=float)
  
    return {
        "values": doc["values"],
        "mean": float(np.mean(arr)),
        "stddev": float(np.std(arr))
    }
```

#### Output Document

```json
{
  "id": "doc-2",
  "values": [1, 2, 3, 4, 5],
  "mean": 3.0,
  "stddev": 1.41
}
```

#### requirements.txt

```
numpy
```

#### Config Parameters

```hocon
{
  name: "ExternalPython-Numpy"
  class: "com.kmwllc.lucille.stage.ExternalPython"

  scriptPath: "/path/to/my_numpy_script.py"
  requirementsPath: "/path/to/requirements.txt"
  
  # Optional
  pythonExecutable: "python3"
  functionName: "process_document"
  port: 25333
}
```