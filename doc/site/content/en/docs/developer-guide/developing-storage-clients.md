---
title: Developing Storage Clients
weight: 15
date: 2025-05-23
description: >
  How to implement a custom StorageClient for Lucille — adding support for a new storage backend.
---

A StorageClient traverses a storage system, discovers files, and provides access to their content. The `FileConnector` uses StorageClients to list and read files from local disk, S3, Azure Blob Storage, and Google Cloud Storage. If you need to read files from a storage system that Lucille doesn't support out of the box (e.g., SFTP, SMB/CIFS, WebDAV), you implement a StorageClient.

> **Note:** It is not currently possible to reference a custom StorageClient from a Lucille config. The mapping from URI scheme to StorageClient implementation is hardcoded in `StorageClient.create()`. Adding a new storage backend today requires a PR to lucille-core — see the [Contributor Guide]({{< relref "docs/contributing" >}}) for project structure and contribution workflow. Config-driven pluggable StorageClients are a roadmap feature. This page is included in the Component Developer Guide because the implementation pattern is the same regardless of how the client is registered.

---

## How StorageClients Fit In

The `FileConnector` determines which StorageClient to use based on the URI scheme of each path in its `paths` config:

| URI scheme | StorageClient |
|---|---|
| `file` (or no scheme) | `LocalStorageClient` |
| `s3` | `S3StorageClient` |
| `gs` | `GoogleStorageClient` |
| `https` (Azure blob) | `AzureStorageClient` |

When you add a new StorageClient, you add a new case to this mapping for your URI scheme (e.g., `sftp`).

---

## What BaseStorageClient Does for You

To create a StorageClient, extend `BaseStorageClient`. The base class manages the heavy lifting so you only provide the storage-specific operations:

- **Lifecycle management** — Tracks initialization state and ensures `init()` is called before traversal.
- **File filtering** — Applies include/exclude patterns, file size limits, and modification time cutoffs from `TraversalParams`.
- **Archive handling** — Automatically detects and extracts `.zip`, `.tar`, `.tar.gz`, and `.gz` files, processing their contents through the appropriate FileHandlers.
- **Compressed file handling** — Decompresses `.gz` and `.bz2` files transparently before passing to FileHandlers.
- **FileHandler delegation** — Routes files to the correct FileHandler based on extension and publishes the resulting Documents.
- **Incremental mode** — Integrates with `FileConnectorStateManager` to track which files have been processed and skip unchanged files on subsequent runs.
- **Success/error directory handling** — Moves files to configured success or error directories after processing.

---

## What You Implement

| Method | Required | Purpose |
|---|---|---|
| `validateOptions(Config config)` | Yes | Validate that the config has the required credentials/settings for your storage backend. Throw `IllegalArgumentException` if invalid. |
| `initializeStorageClient()` | Yes | Create the client connection (e.g., open an SFTP session). Called once before traversal begins. |
| `shutdownStorageClient()` | Yes | Close the client connection. Called after traversal completes. |
| `traverseStorageClient(Publisher, TraversalParams, FileConnectorStateManager)` | Yes | List files in the storage system and call `processAndPublishFileIfValid()` for each one. |
| `getFileContentStreamFromStorage(URI uri)` | Yes | Open and return an `InputStream` for a single file at the given URI. |
| `moveFile(URI filePath, URI folder)` | Yes | Move a file to a different location (used for success/error directories). Throw `UnsupportedOperationException` if your backend doesn't support moves. |

---

## What's in `config`

The `Config` passed to your StorageClient constructor is the cloud/storage-provider-specific config block extracted from the connector config. For example, if the connector config contains:

```hocon
sftp: {
  host: "files.example.com"
  port: 22
  username: "ingest"
  privateKeyPath: "/home/ingest/.ssh/id_rsa"
}
```

...your constructor receives a Config containing `host`, `port`, `username`, and `privateKeyPath`. It does not contain the connector config or the full Lucille config.

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

## How It Would Be Used (Once Pluggable)

When config-driven StorageClient registration is available, the config would look like:

```hocon
connectors: [{
  name: "sftp-ingest"
  class: "com.kmwllc.lucille.connector.FileConnector"
  pipeline: "my-pipeline"
  paths: ["sftp://files.example.com/data/incoming/"]
  sftp: {
    host: "files.example.com"
    port: 22
    username: "ingest"
    privateKeyPath: "/home/ingest/.ssh/id_rsa"
  }
  fileHandlers: {
    csv: {}
    json: {}
  }
}]
```

Until then, adding a new StorageClient requires modifying the `StorageClient.create()` and `StorageClient.createClients()` factory methods in lucille-core to add a case for your URI scheme.

---

## Guidelines

- **Call `processAndPublishFileIfValid()` for each file** — The base class handles filtering, archive extraction, FileHandler delegation, and state management. Don't bypass it.
- **Implement a `FileReference` subclass** — Provide the file's full URI, size, modification time, and a method to open its content stream. The base class uses this metadata for filtering and incremental mode.
- **Use `maxNumOfPages`** — The base class exposes this config value (default: 100) for controlling pagination when listing large directories.
- **Handle `moveFile()` appropriately** — If your storage system doesn't support moves (e.g., a read-only archive), throw `UnsupportedOperationException` and note in your documentation that success/error directories are not supported.
- **Keep connections open across traversal** — Open the connection in `initializeStorageClient()` and close it in `shutdownStorageClient()`. Don't reconnect per file.
- **Validate eagerly** — Check credentials and required config in `validateOptions()` so errors surface at startup, not mid-traversal.
