---
title: File Connector
date: 2025-02-28
description: A Connector that, given a path to S3, Azure, Google Cloud, or the local file system, traverses the content at the given path and publishes Lucille documents representing its findings.
---

[Source Code](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/FileConnector.java)

The file connector traverses a file system and publishes Lucille documents representing its findings. In your Configuration, specify
`paths`, representing the path(s) you want to traverse. Each path can be a path to the local file system or a URI for a supported
cloud provider.

### Working with Cloud Storage
When you are providing FileConnector with URIs to cloud storage, you also need to apply the appropriate configuration for any 
cloud providers used. For each provider, you'll need to provide a form of authentication; you can optionally
specify the maximum number of files (`maxNumOfPages`) that Lucille will load into memory for a given request.

* **Azure**: Specify the needed options in `azure` in your Config. You must provide `connectionString`, or you must provide `accountName` and `accountKey`.
* **Google**: Specify the needed options in `gcp` in your Config. You must provide `pathToServiceKey`.
* **S3**: Specify the needed options in `s3` in your Config. You must provide `accessKeyId`, `secretAccessKey`, and `region`. For URIs, `paths` must be percent-encoded for special characters (e.g., `s3://test/folder%20with%20spaces`).
* For each of these providers, in their configuration, you can optionally include `maxNumOfPages` as well.

### Applying FileHandlers
Some of the files that your `FileConnector` encounters will, themselves, contain data that you want to extract more documents from! For example, the FileConnector
may encounter a `.csv` file, where each row itself represents a Document to be published. This is where FileHandlers come in - they will individually process these files
and create more Lucille documents from their data. See [File Handlers](../file_handlers.md) for more. 

In order to use File Handlers, you need to specify the appropriate configuration within your Config - specifically, each File Handler
you want to use will be a map within the `fileOptions` you specify. You can use `csv`, `json`, or `xml`.
See the documentation for each File Handler to see what arguments are needed / accepted.

### File Options
File options determine **how** you handle and process files you encounter during a traversal. Some commonly used options include:
* `getFileContent`:  Whether, during traversal, the FileConnector should add an array of bytes representing the file's contents to the Lucille document it publishes.
                     **This will slow down traversal significantly and is resource intensive. On the cloud, this _will_ download the file contents.**
* `handleArchivedFiles`/`handleCompressedFiles`: Whether you want to handle archive or compressed files, respectively, during your traversal. For cloud files, this _will_ download the file's contents.
* `moveToAfterProcessing`: A path to move files to after processing.
* `moveToErrorFolder`: A path to move files to if an error occurs.

### Filter Options
Filter options determine **which** files will/won't be processed & published in your traversal. All filter options are optional. 
If you specify multiple filter options, files must comply with all of them to be processed & published.
* `includes`: A list of patterns for the only file names that you want to include in your traversal.
* `excludes`: A list of patterns for file names that you want to exclude from your traversal.
* `lastModifiedCutoff`: Filter out files that haven't been modified recently. For example, specify `"1h"`, and only
files modified within the last hour will be processed & published.
* `lastPublishedCutoff`: Filter out files that were recently published by Lucille. For example, specify `"1h"`, and only
files published by Lucille more than an hour ago (or never published) will be processed & published. Requires you to provide **state** configuration,
otherwise, it will not be enforced!

### State
The File Connector can keep track of when files were last known to be published by Lucille. This allows you to use `FilterOptions.lastPublishedCutoff` and
avoid repeatedly publishing the same files in a short period of time.

In order to use state with the File Connector, you'll need to configure a connection to a JDBC-compatible database. The database
can be embedded, or it can be remote.

It's important to note that File Connector state is designed to be efficient and lightweight. As such, keep a few points in mind:
1. Files that were recently moved / renamed files will not have the `lastPublishedCutoff` applied.
2. In your File Connector configuration, it is important that you consistently capitalize directory names in your `paths`, if you are using state.
3. Each database table should be used for only one connector configuration.