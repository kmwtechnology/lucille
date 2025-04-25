---
title: File Connector
date: 2025-02-28
description: A Connector that, given a path to S3, Azure, Google Cloud, or the local file system, traverses the content at the given path and publishes Lucille documents representing its findings.
---

[Source Code](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/FileConnector.java)

The file connector traverses a file system and publishes Lucille documents representing its findings. In your Configuration, specify
`pathsToStorage`, representing the path(s) you want to traverse. Each path can be a path to the local file system or a URI for a supported
cloud provider.

### Working with Cloud Storage
When you are providing FileConnector with URIs to cloud storage, you also need to apply the appropriate configuration for that cloud provider
so Lucille can communicate with the cloud provider. For each provider, you'll need to provide a form of authentication; you can optionally
specify the maximum number of files (`maxNumOfPages`) that Lucille will load into memory for a given request.

* **Azure**: Specify the needed options in `azure` in your Config. You must provide `connectionString`, or you must provide `accountName` and `accountKey`.
* **Google**: Specify the needed options in `gcp` in your Config. You must provide `pathToServiceKey`.
* **S3**: Specify the needed options in `s3` in your Config. You must provide `accessKeyId`, `secretAccessKey`, and `region`.
* For each of these providers, you can optionally include `maxNumOfPages` as well.

### Applying FileHandlers
Some of the files that your `FileConnector` encounters will, themselves, contain data that you want to extract more documents from! For example, the FileConnector
may encounter a `.csv` file that you want to create documents from. This is where FileHandlers come in - they will individually process these files
and create more Lucille documents from their data. See [File Handlers](../file_handlers.md) for more. 

In order to use File Handlers, you need to specify the appropriate configuration within your Config - specifically, each File Handler
you want to use will be a map within the `fileOptions` you specify. You can use `csv`, `json`, `xml`, and `parquet` (coming soon!). 
See the documentation for each File Handler to see what arguments are needed / accepted.

### Parameters for Traversal
There are many different parameters you can specify to customize your traversal of the given path. A few notable ones:

* `includes`/`excludes`: A list of regex patterns for files you want to include in / exclude from your traversal.
* `fileOptions.getFileContent`: Whether, during traversal, the FileConnector should add an array of bytes representing the file's contents to the Lucille document it publishes.
**This will slow down traversal significantly and is resource intensive. On the cloud, this _will_ download the file contents.**
* `fileOptions.moveToAfterProcessing`: A path to move files to after processing. **Only works for the local file system.**
* `fileOptions.moveToErrorFolder`: A path to move files to if an error occurs. **Only works for the local file system.**