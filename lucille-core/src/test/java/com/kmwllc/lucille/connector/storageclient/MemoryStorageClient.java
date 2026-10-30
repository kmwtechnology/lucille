package com.kmwllc.lucille.connector.storageclient;

import com.kmwllc.lucille.connector.FileConnectorStateManager;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * A StorageClient for tests, registered for a custom URI scheme through "storageClients". Requires "content" in its config.
 * Traversal publishes one document, whose ID is the traversed URI and whose "content" field holds the configured content.
 * Every file's content is the configured content.
 */
public class MemoryStorageClient extends BaseStorageClient {

  public MemoryStorageClient(Config config) {
    super(config);
  }

  @Override
  protected void validateOptions(Config config) {
    if (!config.hasPath("content")) {
      throw new IllegalArgumentException("MemoryStorageClient requires 'content' in config.");
    }
  }

  public Config getConfig() {
    return config;
  }

  @Override
  protected void initializeStorageClient() { }

  @Override
  protected void shutdownStorageClient() { }

  @Override
  protected void traverseStorageClient(Publisher publisher, TraversalParams params, FileConnectorStateManager stateMgr) throws Exception {
    Document doc = Document.create(createDocId(params.getURI().toString(), params));
    doc.setField("content", config.getString("content"));
    publisher.publish(doc);
  }

  @Override
  protected InputStream getFileContentStreamFromStorage(URI uri) {
    return new ByteArrayInputStream(config.getString("content").getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void moveFile(URI filePath, URI folder) {
    throw new UnsupportedOperationException("MemoryStorageClient does not support moving files.");
  }
}
