---
title: ExternalPython
weight: 310
date: 2025-12-16
description: Run a document through an external Py4J Python environment.
---

`com.kmwllc.lucille.stage.ExternalPython`

### Why Use It?

ExternalPython delegates per-document processing to an external Python process using [Py4J](https://www.py4j.org/). Lucille serializes the Document into a request, calls a Python function with the document as a dict, receives a JSON response, and applies that response back onto the document.

The returned object replaces the document's fields: every field in the response is set on the document, and every field not in the response is removed. Reserved fields such as `id` are never changed or removed. If the function returns `None`, the document is left unchanged. The function may return a dict or a JSON string. An error in the Python function fails the document.

At startup, Lucille creates a virtual environment at `./python/venv` (relative to the working directory) if one does not exist, installs `py4j` into it, installs `requirementsPath` if set, and launches the script in a separate Python process. The venv is reused across runs. If the venv exists but its pip cannot run (for example, after an interrupted earlier build), Lucille deletes and recreates it, which also removes any previously installed packages. Setup is serialized across processes that share the working directory with a lock file at `./python/.venv.lock`. If a `pip install` fails, the startup error includes pip's output. Python output is also logged to `./python/py4jclient.log`. Lucille copies its Py4J client script to `./python/Py4jClientCopy.py` on every startup, overwriting any existing copy.

The Py4J gateway and callback server listen only on the loopback address (`127.0.0.1`). Each run generates two random authentication tokens, which are passed to the Python process through environment variables rather than command-line arguments. The Python client removes them from its environment before your script runs, and refuses to start without them. Python startup must finish within 30 seconds. If it times out or the Python process exits first, the error includes the captured Python output. A failed startup stops the Python process, so a later attempt can retry. The Python process is also stopped when the JVM shuts down.

All ExternalPython stages in one JVM share a single Python process. They must use the same `scriptPath`, `pythonExecutable`, `requirementsPath`, and `port`, or startup fails. They can call different functions in that script via `functionName`.

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
Your python file must be directly inside (not in a subdirectory of) one of the following directories, relative to the working directory Lucille runs in. Any other location fails at startup.
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

| Parameter | Type | Required | Description |
|---|---|---|---|
| `scriptPath` | String | Yes | Path to the Python script containing the function. Must be directly inside one of the directories listed under Restrictions, and the file name must end in `.py`. |
| `pythonExecutable` | String | No | Python executable used to create the virtual environment. Default: `python3`. |
| `requirementsPath` | String | No | Path to a `requirements.txt` file installed into the virtual environment with `pip install -r` at startup. |
| `functionName` | String | No | Name of the Python function called for each document. Default: `process_document`. |
| `port` | Integer | No | Py4J gateway port. Lucille also uses `port + 1` for the callback server. Startup fails if either port is in use. If not set, the OS assigns a free gateway port and Lucille picks a free port for the callback server. |

```hocon
{
  name: "ExternalPython-Example"
  class: "com.kmwllc.lucille.stage.ExternalPython"

  scriptPath: "python/my_script.py"

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
  "stddev": 1.4142135623730951
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

  scriptPath: "python/my_numpy_script.py"
  requirementsPath: "/path/to/requirements.txt"
  
  # Optional
  pythonExecutable: "python3"
  functionName: "process_document"
  port: 25333
}
```