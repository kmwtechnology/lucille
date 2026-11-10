---
title: Developing Storage Clients
weight: 15
date: 2025-05-23
description: >
  How to implement a custom StorageClient for Lucille — adding support for a new storage backend.
---

A StorageClient traverses a storage system, discovers files, and provides access to their content. The `FileConnector` uses StorageClients to list and read files from local disk, S3, Azure Blob Storage, and Google Cloud Storage. If you need to read files from a storage system that Lucille doesn't support out of the box (e.g., SFTP, SMB/CIFS, WebDAV), you implement a StorageClient.

Once implemented, you register your StorageClient for a URI scheme in the `storageClients` config; see [Using Your StorageClient](#using-your-storageclient). You can also register a StorageClient for `s3`, `gs`, or `https` in place of the built-in client, for example to adapt one of the built-in cloud clients; see [Extending a Built-in StorageClient](#extending-a-built-in-storageclient).

---

## How StorageClients Fit In

The `FileConnector` determines which StorageClient to use based on the URI scheme of each path in its `paths` config:

| URI scheme | StorageClient |
|---|---|
| `file` (or no scheme) | `LocalStorageClient` |
| `s3` | `S3StorageClient` |
| `gs` | `GoogleStorageClient` |
| `https` (Azure blob) | `AzureStorageClient` |

A StorageClient you register in `storageClients` adds a row to this mapping for its URI scheme (e.g., `sftp`), or replaces the row for `s3`, `gs`, or `https`. The `file` row can't be replaced.

`StorageClient.createClients()` builds the clients for a config, keyed by scheme, and `StorageClient.clientFor()` looks up the client for a URI in that map. `StorageClient.create()` builds the single client for one URI. All three apply the same rules, and the `FileConnector` and the stages that read files by URI use them.

For `https`, the built-in `AzureStorageClient` only handles URIs whose authority contains one of `azure.allowedHosts` (a substring match), which defaults to `["blob.core.windows.net"]`; `clientFor()` returns null and `create()` throws for any other host. The URI's host only selects the client; `AzureStorageClient` sends requests to the endpoint in its credentials. A StorageClient registered for `https` in `storageClients` handles every `https` URI, so it must apply any host rule of its own.

---

## Extending a Built-in StorageClient

To change how S3, Azure, or Google Cloud Storage files are handled, for example to read from an endpoint that is almost, but not quite, S3, extend `S3StorageClient`, `AzureStorageClient`, or `GoogleStorageClient` and register your subclass in `storageClients` under the built-in scheme (`s3`, `https`, or `gs`). It replaces the built-in client for that scheme:

```hocon
storageClients {
  s3 {
    class: "com.mycompany.lucille.storage.MyS3StorageClient"
    # settings for S3StorageClient
    region: "us-east-1"
    # settings of MyS3StorageClient's own
    endpointOverride: "https://s3.storage.example.internal"
  }
}
```

Your subclass receives the whole entry, so it can read both the built-in client's settings (see the [`FileConnector`]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}) docs) and settings of its own. The built-in client's `validateOptions()` still applies unless you override it. A scheme registered this way can't also have its built-in block: `storageClients.s3` with `s3`, `storageClients.https` with `azure`, or `storageClients.gs` with `gcp` throws an `IllegalArgumentException`. The built-in `s3`, `azure`, and `gcp` blocks only ever configure the built-in clients. For `https`, `azure.allowedHosts` doesn't apply to a registered client, so your client must check the host itself if it should reject some hosts.

```java
public class MyS3StorageClient extends S3StorageClient {

  public MyS3StorageClient(Config config) {
    super(config);
  }

  // override protected methods as needed, reading config.getString("endpointOverride")
}
```

---

## What BaseStorageClient Does for You

To create a StorageClient, extend `BaseStorageClient`. The base class manages the heavy lifting so you only provide the storage-specific operations:

- **Lifecycle management** — Tracks initialization state and ensures `start()` is called before traversal.
- **File filtering** — Applies include/exclude patterns, file size limits, and modification time cutoffs from `TraversalParams`.
- **Archive handling** — With `handleArchivedFiles`, extracts `.zip` and `.tar` archives (and `.tar.gz`, when `handleCompressedFiles` is also set), processing their entries through the appropriate FileHandlers.
- **Compressed file handling** — With `handleCompressedFiles`, decompresses `.gz` files before passing them to FileHandlers. No other compression formats are supported.
- **FileHandler delegation** — Routes files to the correct FileHandler based on extension and publishes the resulting Documents.
- **Incremental mode** — Reads and updates the `FileStateTracker` the `FileConnector` passes to `traverse()`, to track which files have been processed and skip unchanged files on subsequent runs.
- **Success/error directory handling** — Moves files to configured success or error directories after processing.
- **`traversal.maxDocuments`** — `traverse()` enforces the connector's document limit for every subclass, however it traverses. See [Document Limit](#document-limit).
- **Shared traversal** — If your backend has a directory-like listing, `traverseByListing()` handles the descent, `pathsToSkip`, `traversal.threadsPerPath`, and `traversal.listingMode`. See [Using the Shared Traversal](#using-the-shared-traversal).

---

## What You Implement

| Method | Required | Purpose |
|---|---|---|
| `validateOptions(Config config)` | No | Validate that the config has the required credentials/settings for your storage backend. Throw `IllegalArgumentException` if invalid. Has an empty default implementation — override when your backend has config options that necessitate each other (e.g., mutual exclusivity, co-dependency). |
| `initializeStorageClient()` | Yes | Create the client connection (e.g., open an SFTP session). Called by `start()`, once before traversal begins. |
| `shutdownStorageClient()` | Yes | Close the client connection. Called by `stop()`, after traversal completes. |
| `traverseStorageClient(Publisher, TraversalParams, FileStateTracker)` | Yes | List files in the storage system and call `processAndPublishFileIfValid()` for each one, passing the `FileStateTracker` through. It is null when state is not enabled. |
| `getFileContentStreamFromStorage(URI uri)` | Yes | Open and return an `InputStream` for a single file at the given URI. |
| `moveFile(URI filePath, URI folder)` | Yes | Move a file to a different location (used for success/error directories). Throw `UnsupportedOperationException` if your backend doesn't support moves; see [Errors](#errors). |
| `moveTargetInsidePath(URI path, URI folder)` | No | Whether files moved to the folder would land back inside the path being traversed. A move folder for which this returns `true` fails at startup. By default, whether `containsPath(path, folder)`. Override it if your `moveFile()` keeps part of the source path under the folder. |

`traverse()` and `traverseStorageClient()` declare `IOException` and `ConnectorException`. Wrap any other checked exception, such as a `PublisherException` from publishing directly, in a `ConnectorException`.

If your client has settings that can't work with some traversals, override `validateTraversal(TraversalParams params)` and throw an `IllegalArgumentException`. The `FileConnector` calls it at startup for each path, and adds the path to the message. If you extend `BaseStorageClient`, call `super.validateTraversal(params)`: it rejects `traversal.threadsPerPath` above `1` and `traversal.listingMode` unless your client uses the shared traversal.

Constants for the Document field names and config keys are on `StorageClientFields` and `StorageClientOptions` in `com.kmwllc.lucille.connector.storageclient`. Import them from there rather than from `FileConnector`, which keeps aliases of them.

---

## What's in `config`

The `Config` passed to your StorageClient constructor is your scheme's entry in `storageClients`, extracted from the connector config. For example, if the connector config contains:

```hocon
storageClients: {
  sftp: {
    class: "com.mycompany.lucille.storage.SftpStorageClient"
    host: "files.example.com"
    port: 22
    username: "ingest"
    privateKeyPath: "/home/ingest/.ssh/id_rsa"
  }
}
```

...your constructor receives a Config containing `class`, `host`, `port`, `username`, and `privateKeyPath`. It does not contain the connector config or the full Lucille config. Lucille doesn't validate the settings in the entry beyond `class`, so check them in `validateOptions()`.

---

## Using the Shared Traversal

If your backend can list one directory at a time, you don't need to write the descent. Implement `traverseStorageClient()` by calling `traverseByListing()`, and override the listing methods:

| Method | Required | Purpose |
|---|---|---|
| `listDirectory(String prefix, TraversalParams params)` | Yes | Return a `Stream<ListingEntry>` of the files and child directories directly under the prefix: `ListingEntry.file(name, referenceSupplier)` or `ListingEntry.directory(prefix)`. Make it lazy, fetching pages as it is consumed, because the traversal stops consuming it at `maxDocuments`. |
| `directoryURI(String prefix, TraversalParams params)` | Yes | Return the URI of a directory, which `pathsToSkip` is compared against. |
| `listFlat(String prefix, TraversalParams params)` | No | Return every file under the prefix, for `listingMode: flat`. Throws `UnsupportedOperationException` by default. |
| `getStartingDirectory(TraversalParams params)` | No | Return the key to start from. By default, the URI's path without its leading `/`. |
| `defaultListingMode()` | No | The mode when `listingMode` isn't configured. By default, `HIERARCHICAL`. |

```java
@Override
protected void traverseStorageClient(Publisher publisher, TraversalParams params, FileStateTracker stateMgr)
    throws ConnectorException {
  traverseByListing(publisher, params, stateMgr);
}

@Override
protected Stream<ListingEntry> listDirectory(String prefix, TraversalParams params) {
  return client.list(prefix).stream().map(item -> item.isDirectory()
      ? ListingEntry.directory(item.key())
      : ListingEntry.file(item.key(), () -> new MyFileReference(item, params)));
}
```

Also override `traversesByListing()` to return `true`:

```java
@Override
protected boolean traversesByListing() {
  return true;
}
```

The traversal reads `threadsPerPath` and `listingMode` from the connector's `traversal` block, through `TraversalParams`. Only a client that calls `traverseByListing()` honors them, so `validateTraversal()` rejects them at startup unless `traversesByListing()` returns `true`. With `threadsPerPath` above `1`:

- `listDirectory()`, `directoryURI()`, `getContentStream()`, and `moveFile()` are called from several threads, so your backend's client must be safe to share, or you must keep one per thread.
- A `FileStateTracker` passed to `traverse()` must implement `ThreadBoundFileStateTracker`, which opens and closes state for each traversal thread. A plain `FileStateTracker` fails the traversal, since it may not be thread-safe. `FileConnector`'s does.
- `stop()` interrupts the traversal's threads and waits for them to end, and the traversal fails, since it didn't reach every file.

To visit a single level without descending, call `traverseOneDirectory(prefix, publisher, params, stateMgr)`. It publishes the directory's files and returns its child directories.

### Document Limit

`traverse()` enforces `traversal.maxDocuments` for every subclass of `BaseStorageClient`, whether or not it uses the shared traversal. When a limit is set, the `Publisher` passed to `traverseStorageClient()` throws an unchecked exception once the limit is reached. `processAndPublishFileIfValid()` lets that exception through rather than handling it as a failed file, and `traverse()` then ends the traversal without failing it. If your traversal catches exceptions itself, rethrow unchecked ones (wrapping one is fine) so that the limit can end it. Otherwise every remaining file would fail the same way, and with `moveToErrorFolder` set, be moved to the error folder.

The shared traversal also stops listing once the limit is reached. A client that walks its storage another way stops at the next document it publishes.

A client that implements `StorageClient` directly, rather than extending `BaseStorageClient`, doesn't get this. The interface's default `validateTraversal()` rejects `maxDocuments`, `threadsPerPath` above `1` and `listingMode` at startup, so a setting is either honored or refused, never ignored.

---

## FileReference

When traversing, you call `processAndPublishFileIfValid()` for each file. This method expects a `FileReference` — an object that describes a file's metadata and provides access to its content. Extend `BaseFileReference` for your storage system:

```java
public class SftpFileReference extends BaseFileReference {

  private final ChannelSftp channel;
  private final ChannelSftp.LsEntry entry;
  private final String remotePath;

  // directoryPath must end with "/"
  public SftpFileReference(ChannelSftp.LsEntry entry, String directoryPath, ChannelSftp channel, TraversalParams params)
      throws URISyntaxException {
    super(
        // the multi-argument URI constructor percent-encodes the path, so the URI is canonical
        new URI("sftp", params.getURI().getAuthority(), directoryPath + entry.getFilename(), null, null),
        Instant.ofEpochSecond(entry.getAttrs().getMTime()),  // last modified
        entry.getAttrs().getSize(),                          // file size
        null                                                 // creation time (not available via SFTP)
    );
    this.channel = channel;
    this.entry = entry;
    this.remotePath = directoryPath + entry.getFilename();
  }

  @Override
  public String getName() {
    return entry.getFilename();
  }

  @Override
  public boolean isValidFile() {
    return !entry.getAttrs().isDir();
  }

  @Override
  public InputStream getContentStream(TraversalParams params) throws IOException {
    try {
      return channel.get(remotePath);
    } catch (SftpException e) {
      throw new IOException("Failed to open " + remotePath, e);
    }
  }

  @Override
  protected byte[] getFileContent(TraversalParams params) throws IOException {
    try (InputStream is = getContentStream(params)) {
      return is.readAllBytes();
    }
  }
}
```

What each method must return:

| Method | Contract |
|---|---|
| `getFullPath()` | The file's full URI. The Document ID and the state key are derived from it; see [Document IDs and State Keys](#document-ids-and-state-keys). Passed to the `BaseFileReference` constructor. |
| `getName()` | The file's leaf name, the last segment of its path (`a.json` for `sftp://host/data/a.json`). Used for the file's extension, which selects its FileHandler. |
| `getFilterName()` | The name `filterOptions.includes` and `excludes` are matched against. Defaults to `getName()`. The built-in S3, Google, and Azure clients return the whole object key (`data/a.json`), so patterns can match on folders; override it if your backend should do the same. |
| `getLastModified()` | The last-modified time, or null if your backend doesn't report one. A file with no last-modified time passes `lastModifiedCutoff` and, in incremental mode, is published on every run, since it can't be shown to be unchanged. |
| `getContentStream()` / `getFileContent()` | The file's contents. Throw `IOException` if they can't be read; see [Errors](#errors). |

---

## Fields Your Client Emits

For a file with no FileHandler, `BaseFileReference.asDoc()` builds the Document the `FileConnector` publishes, with the fields listed under [Document Fields]({{< relref "docs/ingest-design/connectors/file_connector#document-fields" >}}): `file_path`, `file_modification_date`, `file_size_bytes`, `file_creation_date`, and `file_content`. A field whose value your `FileReference` doesn't provide (a null last-modified time, size, or creation time) is left out. These names are part of the StorageClient contract: stages such as `FetchFileContent` and `ApplyFileHandlers` read `file_path` and `file_content` by default. If your client builds Documents itself, use the names on `StorageClientFields` so they match.

---

## Document IDs and State Keys

The `FileConnector` derives both the Document ID and the incremental state for a file from `getFullPath().toString()`:

- **Document ID:** the connector's `docIdPrefix` plus the MD5 hash of the full path. For an entry in an archive or a decompressed file, the full path is the container's full path, `!` (`StorageClientFields.ARCHIVE_FILE_SEPARATOR`), and the entry's name, e.g. `sftp://host/data/files.zip!a.json`.
- **State key:** the full path itself is the row in the state database, so it must fit in `state.pathLength`.

So `getFullPath()` must be a canonical URI that is the same for the same file on every run and on every platform:

- Build it with the multi-argument `java.net.URI` constructors, which percent-encode the path, rather than by concatenating strings. A path with a space then always becomes `%20`.
- Use the same scheme and authority as the path configured in `paths`, so that `containsPath()` and the configured path agree.
- Use `/` as the separator, and don't let it depend on the listing order, the local platform, or a session-specific host name or token.

If the full path of a file changes between runs, the file is published under a new ID, and in incremental mode its old path is treated as deleted (and gets a tombstone when `sendTombstones` is set).

---

## Errors

`processAndPublishFileIfValid()` handles errors for one file at a time:

- An exception while processing a file, such as an `IOException` from `getContentStream()`, a file over `maxSizeBytes`, or an error in a FileHandler, is logged. The file is moved to `moveToErrorFolder`, if set, and the traversal continues with the next file. This includes an `UnsupportedOperationException` thrown while processing one file.
- If `moveToAfterProcessing` or `moveToErrorFolder` is set and your `moveFile()` throws `UnsupportedOperationException`, the traversal stops with an `UnsupportedOperationException` naming the option, since every file would fail the same way.
- A path longer than `state.pathLength` stops the traversal.

An exception thrown by `traverseStorageClient()` itself, for example when a listing call fails, stops the traversal and fails the run. So does an exception from a listing during `traverseByListing()`, on any thread. A file whose `ListingEntry` reference can't be built is logged and skipped.

---

## Skeleton

```java
package com.mycompany.lucille.storage;

import com.jcraft.jsch.*;
import com.kmwllc.lucille.connector.storageclient.BaseStorageClient;
import com.kmwllc.lucille.connector.storageclient.FileStateTracker;
import com.kmwllc.lucille.connector.storageclient.TraversalParams;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Vector;

/**
 * A StorageClient for SFTP servers. Traverses a remote directory and publishes files
 * through the standard FileHandler pipeline.
 */
public class SftpStorageClient extends BaseStorageClient {

  private Session session;
  private ChannelSftp channel;

  public SftpStorageClient(Config config) {
    super(config);
  }

  @Override
  protected void validateOptions(Config config) {
    if (!config.hasPath("host")) {
      throw new IllegalArgumentException("SFTP StorageClient requires 'host' in config.");
    }
    if (!config.hasPath("username")) {
      throw new IllegalArgumentException("SFTP StorageClient requires 'username' in config.");
    }
  }

  @Override
  protected void initializeStorageClient() throws IOException {
    try {
      String host = config.getString("host");
      int port = config.hasPath("port") ? config.getInt("port") : 22;
      String username = config.getString("username");

      JSch jsch = new JSch();
      if (config.hasPath("privateKeyPath")) {
        jsch.addIdentity(config.getString("privateKeyPath"));
      }

      session = jsch.getSession(username, host, port);
      session.setConfig("StrictHostKeyChecking", "no");

      if (config.hasPath("password")) {
        session.setPassword(config.getString("password"));
      }

      session.connect();
      channel = (ChannelSftp) session.openChannel("sftp");
      channel.connect();
    } catch (JSchException e) {
      throw new IOException("Failed to connect to SFTP server", e);
    }
  }

  @Override
  protected void shutdownStorageClient() throws IOException {
    if (channel != null) channel.disconnect();
    if (session != null) session.disconnect();
  }

  @Override
  protected void traverseStorageClient(Publisher publisher, TraversalParams params,
      FileStateTracker stateMgr) throws IOException {
    String path = params.getURI().getPath();
    String directoryPath = path.endsWith("/") ? path : path + "/";

    Vector<ChannelSftp.LsEntry> entries;
    try {
      @SuppressWarnings("unchecked")
      Vector<ChannelSftp.LsEntry> listed = channel.ls(directoryPath);
      entries = listed;
    } catch (SftpException e) {
      throw new IOException("Error listing " + directoryPath, e);
    }

    for (ChannelSftp.LsEntry entry : entries) {
      if (entry.getFilename().startsWith(".")) continue;
      if (entry.getAttrs().isDir()) continue;

      SftpFileReference ref = new SftpFileReference(entry, directoryPath, channel, params);
      processAndPublishFileIfValid(publisher, ref, params, stateMgr);
    }
  }

  @Override
  protected InputStream getFileContentStreamFromStorage(URI uri) throws IOException {
    try {
      return channel.get(uri.getPath());
    } catch (SftpException e) {
      throw new IOException("Failed to read " + uri, e);
    }
  }

  @Override
  public void moveFile(URI filePath, URI folder) throws IOException {
    try {
      String source = filePath.getPath();
      String destDir = folder.getPath();
      String fileName = source.substring(source.lastIndexOf('/') + 1);
      channel.rename(source, destDir + "/" + fileName);
    } catch (SftpException e) {
      throw new IOException("Failed to move " + filePath + " to " + folder, e);
    }
  }
}
```

---

## Using Your StorageClient

Add an entry to `storageClients`, keyed by your URI scheme, with `class` set to your StorageClient:

```hocon
connectors: [{
  name: "sftp-ingest"
  class: "com.kmwllc.lucille.connector.FileConnector"
  pipeline: "my-pipeline"
  paths: ["sftp://files.example.com/data/incoming/"]
  storageClients: {
    sftp: {
      class: "com.mycompany.lucille.storage.SftpStorageClient"
      host: "files.example.com"
      port: 22
      username: "ingest"
      privateKeyPath: "/home/ingest/.ssh/id_rsa"
    }
  }
  fileHandlers: {
    csv: {}
    json: {}
  }
}]
```

- `class` must implement `StorageClient` (usually by extending `BaseStorageClient`), must not be abstract, and must have a public constructor that takes a `Config`. Your class must be on the classpath.
- The scheme must be a valid URI scheme of at least two characters. One-letter schemes are read as Windows drive letters (e.g. `C:/data`).
- A StorageClient for `s3`, `gs`, or `https` replaces the built-in client, and that scheme's built-in block (`s3`, `gcp`, or `azure`) can't also be given. See [Extending a Built-in StorageClient](#extending-a-built-in-storageclient).
- `file` can't be configured in `storageClients`.
- Stages that read files by URI, such as `FetchFileContent`, `ApplyFileHandlers`, `ApplyJavascript`, and the dictionary-based stages, accept the same `storageClients` config.

`StorageClient.create()` and `StorageClient.createClients()` throw an `IllegalArgumentException` if an entry breaks one of these rules or its class can't be created.

---

## Guidelines

- **Call `processAndPublishFileIfValid()` for each file** — The base class handles filtering, archive extraction, FileHandler delegation, and state management. Don't bypass it.
- **Implement a `FileReference` subclass** — Provide the file's full URI, size, modification time, and a method to open its content stream. The base class uses this metadata for filtering and incremental mode.
- **Use `maxResultsPerPage`** — The base class exposes this config value as the number of results to request per listing call (default: 100, or a provider-specific default passed to the protected `BaseStorageClient(Config, int)` constructor, as `S3StorageClient` does with 1000).
- **Handle `moveFile()` appropriately** — If your storage system doesn't support moves (e.g., a read-only archive), throw `UnsupportedOperationException` and note in your documentation that success/error directories are not supported. A traversal with `moveToAfterProcessing` or `moveToErrorFolder` set then stops at the first move.
- **Keep connections open across traversal** — Open the connection in `initializeStorageClient()` and close it in `shutdownStorageClient()`. Don't reconnect per file.
- **Validate eagerly** — Check credentials and required config in `validateOptions()` so errors surface at startup, not mid-traversal.

---

## Testing Your StorageClient

You can test a StorageClient without a `FileConnector`, a database, or a live backend:

- Build `TraversalParams` directly with `new TraversalParams(config, uri, docIdPrefix)`, where `config` holds any `fileOptions` and `filterOptions` you want to test.
- Publish to a `TestMessenger` (in `com.kmwllc.lucille.message`) through a `PublisherImpl`, and check `getDocsSentForProcessing()`.
- To test incremental mode with one thread, implement `FileStateTracker` in memory (four methods) and pass it to `traverse(publisher, params, state)`. Run the traversal twice to check that unchanged files are skipped.
- To test against a mock of your backend's SDK client, inject the mock and call `initializeForTesting()`, which marks the client as started without calling `initializeStorageClient()`.
