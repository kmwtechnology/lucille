---
title: ParseFilePath
weight: 450
date: 2025-06-09
description: Parses a file path field into filename, folder, normalized path, extension, and the list of parent paths.
---

`com.kmwllc.lucille.stage.ParseFilePath`

Parses the file path in `filePathField` and adds these fields to the Document:

- `filename`: the file name, with extension.
- `folder`: the parent directory.
- `path`: the normalized path.
- `file_extension`: the extension.
- `file_paths`: each parent directory of the file as a separate value, without a leading separator. For `/a/b/c.txt` the values are `a` and `a/b`. Only written when `includeHierarchy` is `true`.

Values are added to any existing values in these fields rather than replacing them. Windows and Unix paths are both parsed, whatever the operating system. Documents without `filePathField` are left unchanged.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `filePathField` | String | No | Field containing the file path. Default: `file_path`. |
| `fileSep` | String | No | Separator used in the output paths. Must be `/` or `\` (escape it as `"\\"` in HOCON). Default: the operating system's separator. |
| `uppercaseExtension` | Boolean | No | Write `file_extension` in uppercase. Default: `true`. |
| `includeHierarchy` | Boolean | No | Populate `file_paths`. Default: `true`. |
