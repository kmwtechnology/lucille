---
title: EmbeddedPython
date: 2025-12-16
description: Run a document through a Java embedded Graal Python environment.
---

### Why Use It?

EmbeddedPython executes per-document Python code inside the Lucille JVM using GraalPy. Instead of returning a JSON object, your script mutates the current document directly through a Python-friendly proxy bound as doc (and the raw Java document as rawDoc). This avoids ports, subprocesses, venvs, and per-document JSON round trips.

### When To Use It

Use EmbeddedPython when you need one or more of the following:
- Minimal operational overhead (ports, subprocess lifecycle, venv creation, pip installs).
- No use of any external Python libraries or native dependencies that require a real Python environment.
- Lightweight field enrichment/transformation.

### When To Use ExternalPython Instead

Avoid EmbeddedPython and use ExternalPython when you need one or more of the following:
- Real Python compatibility (including packages with native dependencies).
- Dependency management via a requirements.txt installed into a managed venv.
- Process isolation apart from the JVM.

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
doc["title"] = doc["title"].upper()
```

#### Output Document

```json
{
  "id": "doc-1",
  "title": "HELLO",
  "author": "Test",
  "views": 123
}
```

#### Config Parameters

```hocon
{
 name: "EmbeddedPython-Example"
 class: "com.kmwllc.lucille.stage.EmbeddedPython"

 # Specify exactly one of the following:
 script_path: "/path/to/my_script.py"
 script: "doc['title'] = doc['title'].upper()"
}
```