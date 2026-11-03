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

- **Lifecycle management** — Tracks initialization state and ensures `init()` is called before traversal.
- **File filtering** — Applies include/exclude patterns, file size limits, and modification time cutoffs from `TraversalParams`.
- **Archive handling** — With `handleArchivedFiles`, extracts `.zip` and `.tar` archives (and `.tar.gz`, when `handleCompressedFiles` is also set), processing their entries through the appropriate FileHandlers.
- **Compressed file handling** — With `handleCompressedFiles`, decompresses `.gz` files before passing them to FileHandlers. No other compression formats are supported.
- **FileHandler delegation** — Routes files to the correct FileHandler based on extension and publishes the resulting Documents.
- **Incremental mode** — Integrates with `FileConnectorStateManager` to track which files have been processed and skip unchanged files on subsequent runs.
- **Success/error directory handling** — Moves files to configured success or error directories after processing.

---

## What You Implement

| Method | Required | Purpose |
|---|---|---|
| `validateOptions(Config config)` | No | Validate that the config has the required credentials/settings for your storage backend. Throw `IllegalArgumentException` if invalid. Has an empty default implementation — override when your backend has config options that necessitate each other (e.g., mutual exclusivity, co-dependency). |
| `initializeStorageClient()` | Yes | Create the client connection (e.g., open an SFTP session). Called once before traversal begins. |
| `shutdownStorageClient()` | Yes | Close the client connection. Called after traversal completes. |
| `traverseStorageClient(Publisher, TraversalParams, FileConnectorStateManager)` | Yes | List files in the storage system and call `processAndPublishFileIfValid()` for each one. |
| `getFileContentStreamFromStorage(URI uri)` | Yes | Open and return an `InputStream` for a single file at the given URI. |
| `moveFile(URI filePath, URI folder)` | Yes | Move a file to a different location (used for success/error directories). Throw `UnsupportedOperationException` if your backend doesn't support moves. |

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

## FileReference

When traversing, you call `processAndPublishFileIfValid()` for each file. This method expects a `FileReference` — an object that describes a file's metadata and provides access to its content. Extend `BaseFileReference` for your storage system:

```java
public class SftpFileReference extends BaseFileReference {

  private final ChannelSftp channel;
  private final String remotePath;

  public SftpFileReference(LsEntry entry, String basePath, ChannelSftp channel, TraversalParams params) {
    super(
        URI.create("sftp://" + channel.getSession().getHost() + basePath + entry.getFilename()),
        entry.getAttrs().getMTime(),   // last modified (epoch seconds)
        entry.getAttrs().getSize(),    // file size
        null                           // creation time (not available via SFTP)
    );
    this.channel = channel;
    this.remotePath = basePath + entry.getFilename();
  }

  @Override
  public String getName() {
    return remotePath;
  }

  @Override
  public boolean isValidFile() {
    return !remotePath.endsWith("/");
  }

  @Override
  public InputStream getContentStream(TraversalParams params) {
    try {
      return channel.get(remotePath);
    } catch (SftpException e) {
      throw new RuntimeException("Failed to open " + remotePath, e);
    }
  }

  @Override
  protected byte[] getFileContent(TraversalParams params) {
    try (InputStream is = getContentStream(params)) {
      return is.readAllBytes();
    } catch (Exception e) {
      throw new RuntimeException("Failed to read " + remotePath, e);
    }
  }
}
```

---

## Skeleton

```java
package com.mycompany.lucille.storage;

import com.jcraft.jsch.*;
import com.kmwllc.lucille.connector.FileConnectorStateManager;
import com.kmwllc.lucille.connector.storageclient.BaseStorageClient;
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
      FileConnectorStateManager stateMgr) throws Exception {
    String remotePath = params.getURI().getPath();

    @SuppressWarnings("unchecked")
    Vector<ChannelSftp.LsEntry> entries = channel.ls(remotePath);

    for (ChannelSftp.LsEntry entry : entries) {
      if (entry.getFilename().startsWith(".")) continue;
      if (entry.getAttrs().isDir()) continue;

      SftpFileReference ref = new SftpFileReference(entry, remotePath, channel, params);
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
- **Handle `moveFile()` appropriately** — If your storage system doesn't support moves (e.g., a read-only archive), throw `UnsupportedOperationException` and note in your documentation that success/error directories are not supported.
- **Keep connections open across traversal** — Open the connection in `initializeStorageClient()` and close it in `shutdownStorageClient()`. Don't reconnect per file.
- **Validate eagerly** — Check credentials and required config in `validateOptions()` so errors surface at startup, not mid-traversal.
