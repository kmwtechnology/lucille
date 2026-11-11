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
- `file_paths`: each parent directory of the file as a separate value, without a leading separator unless `rootedHierarchy` is `true`. For `/a/b/c.txt` the values are `a` and `a/b`. Only written when `includeHierarchy` is `true`.
- `level`: the number of segments in the path, including the file name. `3` for `/a/b/c.txt` and for `C:\a\c.txt`. Segments are counted the same way as for `file_paths`. Only written when `includeLevel` is `true`.
- `scheme` and `authority`: for a URI, its scheme (`s3`) and authority (the bucket or host, or `""` for `file:///a`). Only written for a URI when `parseUris` is `true`.

Values are added to any existing values in these fields rather than replacing them. Windows and Unix paths are both parsed, whatever the operating system. Documents without `filePathField` are left unchanged.

A value that starts with a scheme of two or more letters followed by `://` is parsed as a URI, so the URIs that `FileConnector` writes to `file_path` parse cleanly. The scheme and authority go in their own fields, and the other fields are computed from the URI's path only. A query or fragment is dropped. A one-letter scheme isn't matched, so `C://dir` is still parsed as a Windows drive path. If the URI's path leaves the root (`s3://bucket/../a`), the whole value is parsed as a path instead.

| Input | `scheme` | `authority` | `path` | `folder` | `file_paths` |
|---|---|---|---|---|---|
| `s3://acme-docs/legal/2026/contract.docx` | `s3` | `acme-docs` | `/legal/2026/contract.docx` | `/legal/2026` | `legal`, `legal/2026` |
| `file:///data/reports/q3.pdf` | `file` | `""` | `/data/reports/q3.pdf` | `/data/reports` | `data`, `data/reports` |
| `https://acct.blob.core.windows.net/container/dir/f.txt` | `https` | `acct.blob.core.windows.net` | `/container/dir/f.txt` | `/container/dir` | `container`, `container/dir` |

Set `parseUris` to `false` to parse URIs like any other path, as before: `s3://acme-docs/legal/c.docx` then gives `path` `s3:/acme-docs/legal/c.docx` and `file_paths` `s3:`, `s3:/acme-docs`, `s3:/acme-docs/legal`.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `filePathField` | String | No | Field containing the file path. Default: `file_path`. |
| `fileSeparator` | String | No | Separator used in the output paths. Must be `/` or `\` (escape it as `"\\"` in HOCON). Default: the operating system's separator. |
| `uppercaseExtension` | Boolean | No | Write `file_extension` in uppercase. Default: `true`. |
| `includeHierarchy` | Boolean | No | Populate `file_paths`. Default: `true`. |
| `parseUris` | Boolean | No | Parse a value that starts with a URI scheme as a URI, as described above. Default: `true`. |
| `decodeFilePath` | Boolean | No | URL-decode (UTF-8) the path before parsing it. For a URI, the authority and path are decoded after the URI is split. `+` is kept as is, and a path with an invalid escape such as `100%.txt`, or with an encoded null character (`%00`), is parsed without decoding. Default: `false`. |
| `includeLevel` | Boolean | No | Populate `level`. Default: `false`. |
| `clearInvalidExtension` | Boolean | No | Set `file_extension` to `""` when the extension contains a control character or a character that looks like a slash: `/`, `\`, U+2215, U+2044, U+29F8, U+FF0F, U+FE68, U+2216, U+29F9 or U+FF3C. Default: `false`. |
| `rootedHierarchy` | Boolean | No | Start each `file_paths` value with `fileSeparator`: `/a/b/c.txt` gives `/a` and `/a/b`. Windows paths get one too: `C:\a\c.txt` gives `\C:` and `\C:\a`. Default: `false`. |
