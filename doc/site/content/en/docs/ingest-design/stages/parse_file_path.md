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
- `level`: the number of segments in the path, including the file name. `3` for `/a/b/c.txt` and for `C:\a\c.txt`. Segments are counted the same way as for `file_paths`, so `createRelativeHierarchy` applies here too. Only written when `includeLevel` is `true`.

Values are added to any existing values in these fields rather than replacing them. Windows and Unix paths are both parsed, whatever the operating system. Documents without `filePathField` are left unchanged.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `filePathField` | String | No | Field containing the file path. Default: `file_path`. |
| `fileSep` | String | No | Separator used in the output paths. Must be `/` or `\` (escape it as `"\\"` in HOCON). Default: the operating system's separator. |
| `uppercaseExtension` | Boolean | No | Write `file_extension` in uppercase. Default: `true`. |
| `includeHierarchy` | Boolean | No | Populate `file_paths`. Default: `true`. |
| `decodeFilePath` | Boolean | No | URL-decode (UTF-8) the path before parsing it, and keep the `://` after a URI scheme (`s3://bucket/key`) in `path` and `folder` instead of collapsing it to `:/`. `+` is kept as is, and a path with an invalid escape such as `100%.txt` is parsed without decoding. `file_paths` still splits on runs of separators, so it starts `s3:`, `s3:/bucket`; see `createRelativeHierarchy`. Default: `false`. |
| `createRelativeHierarchy` | Boolean | No | For a URI, build `file_paths` from the path without the scheme and authority: `s3://bucket/a/b/c.txt` gives `a` and `a/b`. `path` and `folder` are not affected, and neither are paths that are not URIs. Default: `false`. |
| `includeLevel` | Boolean | No | Populate `level`. Default: `false`. |
| `clearInvalidExtension` | Boolean | No | Set `file_extension` to `""` when the extension contains a control character or a character that looks like a slash: `/`, `\`, U+2215, U+2044, U+29F8, U+FF0F, U+FE68, U+2216, U+29F9 or U+FF3C. Default: `false`. |
| `rootedHierarchy` | Boolean | No | Start each `file_paths` value with `fileSep`: `/a/b/c.txt` gives `/a` and `/a/b`. Windows paths get one too: `C:\a\c.txt` gives `\C:` and `\C:\a`. Default: `false`. |
