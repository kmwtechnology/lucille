---
title: File Connector
date: 2025-02-28
description: A Connector that, given a path to S3, Azure, Google Cloud, or the local file system, traverses the content at the given path and publishes Lucille documents representing its findings.
---

[Source Code](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/FileConnector.java)

The file connector traverses a file system and publishes Lucille documents representing its findings. In your Configuration, specify
a `pathToStorage`, representing the path you want to traverse. The path can be a path to the local file system or a URI for a supported
cloud provider.

### Working with Cloud Storage
When you are providing FileConnector with a URI to cloud storage, you also need to apply the appropriate configuration for that cloud provider
so Lucille can communicate with the cloud provider. For each provider, you'll need to provide a form of authentication; you can optionally
specify the maximum number of files (`maxNumOfPages`) that Lucille will load into memory for a given request.

* **Azure**: Specify the needed options in `azure` in your Config. You must provide `connectionString`, or you must provide `accountName` and `accountKey`.
* **Google**: Specify the needed options in `gcp` in your Config. You must provide `pathToServiceKey`.
* **S3**: Specify the needed options in `s3` in your Config. You must provide `accessKeyId`, `secretAccessKey`, and `region`.
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
* File Handlers: You can include `csv`, `json`, or `xml`, enabling additional processing for these file types. Their configuration will be used to build/configure the File Handler.

### Filter Options
Filter options determine **which** files will/won't be processed & published in your traversal. All filter options are optional. 
If you specify multiple filter options, files must comply with all of them to be processed & published.
* `includes`: A list of patterns for the only file names that you want to include in your traversal.
* `excludes`: A list of patterns for file names that you want to exclude from your traversal.
* `modificationCutoff`: Filter out files that haven't been modified recently. For example, specify `"1h"`, and only
files modified within the last hour will be processed & published.
* `lastPublishedCutoff`: Filter out files that were recently published by Lucille. For example, specify `"1h"`, and only
files published by Lucille more than an hour ago will processed & published. Requires you to provide **state** configuration, otherwise,
it will not be enforced!

### State
The File Connector can keep track of when files were last known to be published by Lucille. This allows you to use `FilterOptions.lastPublishedCutoff` and
avoid repeatedly publishing the same files, for example.

In order to use state with the File Connector, you'll need to configure a connection to a JDBC-compatible database. The database
can be embedded, or it can be remote.

It's important to note that File Connector state is designed to be efficient and lightweight. As such, keep a few points in mind:
1. Lucille will automatically rows for files and directories that appear to have been deleted. This means that moving / renaming files and directories will cause them to have no known last published time!
2. In your File Connector configuration, it is important that you consistently capitalize directory names in your `pathToStorage`, if you are using state.
3. The state database will have tables for each "root" of a file system. So, if you delete a bucket or container, you'll have to delete the table yourself.
   * For the local file system, this is simple - all entries are under "FILE".
   * For S3 and Google Cloud, the table name is a combination of the URI scheme (gs, s3) and the hosting bucket / container's name.
   * For Azure, the table name is a combination of your connection String's storage name and the container's name.