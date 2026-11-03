package com.kmwllc.lucille.connector.storageclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;

/**
 * Tests the per-file contract BaseStorageClient applies to any StorageClient's FileReferences, using a client that
 * traverses a fixed list of references.
 */
public class BaseStorageClientTest {

  private static final URI ROOT = URI.create("list://root/");

  @Test
  public void testIOExceptionFromContentStreamSkipsOnlyThatFile() throws Exception {
    ListStorageClient client = new ListStorageClient(List.of(
        new TestFileReference("folder/a.txt").failingWith(new IOException("can't read a.txt")),
        new TestFileReference("folder/b.txt")));
    Config config = ConfigFactory.parseMap(Map.of("fileOptions", Map.of("moveToErrorFolder", "list://root/errors/")));

    List<String> published = traverse(client, config);

    assertEquals(List.of("list://root/folder/b.txt"), published);
    // the failed file is moved to the error folder
    assertEquals(List.of("list://root/folder/a.txt -> list://root/errors/"), client.moves);
  }

  @Test
  public void testUnsupportedOperationFromAFileSkipsOnlyThatFile() throws Exception {
    ListStorageClient client = new ListStorageClient(List.of(
        new TestFileReference("a.txt").failingWith(new UnsupportedOperationException("not for a.txt")),
        new TestFileReference("b.txt")));

    assertEquals(List.of("list://root/b.txt"), traverse(client, ConfigFactory.empty()));
  }

  @Test
  public void testUnsupportedMoveStopsTraversal() {
    ListStorageClient client = new ListStorageClient(List.of(new TestFileReference("a.txt"), new TestFileReference("b.txt")));
    client.supportsMove = false;
    Config config = ConfigFactory.parseMap(Map.of("fileOptions", Map.of("moveToAfterProcessing", "list://root/done/")));

    UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class, () -> traverse(client, config));
    assertTrue(e.getMessage(), e.getMessage().contains("moveToAfterProcessing"));
  }

  @Test
  public void testUnsupportedMoveToErrorFolderStopsTraversal() {
    ListStorageClient client = new ListStorageClient(List.of(
        new TestFileReference("a.txt").failingWith(new IOException("can't read a.txt")),
        new TestFileReference("b.txt")));
    client.supportsMove = false;
    Config config = ConfigFactory.parseMap(Map.of("fileOptions", Map.of("moveToErrorFolder", "list://root/errors/")));

    UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class, () -> traverse(client, config));
    assertTrue(e.getMessage(), e.getMessage().contains("moveToErrorFolder"));
  }

  @Test
  public void testFilterPatternsMatchFilterName() throws Exception {
    ListStorageClient client = new ListStorageClient(List.of(
        new TestFileReference("keep/a.txt"),
        new TestFileReference("skip/b.txt")));
    Config config = ConfigFactory.parseMap(Map.of("filterOptions", Map.of("includes", List.of("keep/.*"))));

    assertEquals(List.of("list://root/keep/a.txt"), traverse(client, config));
  }

  @Test
  public void testNullLastModifiedIsPublishedWithStateAndCutoff() throws Exception {
    ListStorageClient client = new ListStorageClient(List.of(new TestFileReference("a.txt").withLastModified(null)));
    Config config = ConfigFactory.parseMap(Map.of("filterOptions", Map.of("publishMode", "incremental", "lastModifiedCutoff", "1h")));
    InMemoryFileStateTracker state = new InMemoryFileStateTracker();

    assertEquals(List.of("list://root/a.txt"), traverse(client, config, state));
    // published again on the next run, since it can't be shown to be unchanged
    state.startNewRun();
    assertEquals(List.of("list://root/a.txt"), traverse(client, config, state));
  }

  private static List<String> traverse(ListStorageClient client, Config config) throws Exception {
    return traverse(client, config, null);
  }

  private static List<String> traverse(ListStorageClient client, Config config, FileStateTracker state) throws Exception {
    TestMessenger messenger = new TestMessenger();
    client.start();
    try {
      client.traverse(new PublisherImpl(ConfigFactory.empty(), messenger, "run", "pipeline"),
          new TraversalParams(config, ROOT, ""), state);
    } finally {
      client.stop();
    }
    return messenger.getDocsSentForProcessing().stream()
        .map(doc -> doc.getString(StorageClientFields.FILE_PATH))
        .collect(Collectors.toList());
  }

  private static class ListStorageClient extends BaseStorageClient {

    private final List<TestFileReference> references;
    private final List<String> moves = new ArrayList<>();
    private boolean supportsMove = true;

    ListStorageClient(List<TestFileReference> references) {
      super(ConfigFactory.empty());
      this.references = references;
    }

    @Override
    protected void initializeStorageClient() { }

    @Override
    protected void shutdownStorageClient() { }

    @Override
    protected void traverseStorageClient(com.kmwllc.lucille.core.Publisher publisher, TraversalParams params, FileStateTracker stateMgr) {
      for (TestFileReference reference : references) {
        processAndPublishFileIfValid(publisher, reference, params, stateMgr);
      }
    }

    @Override
    protected InputStream getFileContentStreamFromStorage(URI uri) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void moveFile(URI filePath, URI folder) {
      if (!supportsMove) {
        throw new UnsupportedOperationException("ListStorageClient can't move files.");
      }
      moves.add(filePath + " -> " + folder);
    }
  }

  private static class TestFileReference extends BaseFileReference {

    private final String key;
    private Exception failure;
    private Instant lastModified = Instant.parse("2026-01-01T00:00:00Z");

    TestFileReference(String key) {
      super(ROOT.resolve(key), null, null, null);
      this.key = key;
    }

    TestFileReference failingWith(Exception failure) {
      this.failure = failure;
      return this;
    }

    TestFileReference withLastModified(Instant lastModified) {
      this.lastModified = lastModified;
      return this;
    }

    @Override
    public String getName() {
      return lastSegment(key);
    }

    @Override
    public String getFilterName() {
      return key;
    }

    @Override
    public Instant getLastModified() {
      return lastModified;
    }

    @Override
    public boolean isValidFile() {
      return true;
    }

    @Override
    public InputStream getContentStream(TraversalParams params) throws IOException {
      if (failure instanceof IOException ioException) {
        throw ioException;
      }
      if (failure instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      return new ByteArrayInputStream(key.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    protected byte[] getFileContent(TraversalParams params) throws IOException {
      try (InputStream in = getContentStream(params)) {
        return in.readAllBytes();
      }
    }

    @Override
    public Document asDoc(TraversalParams params) throws IOException {
      return super.asDoc(params);
    }
  }

  @Test
  public void testDestinationKey() {
    // the folder's trailing '/' is optional, and never doubled
    assertEquals("processed/data/a.csv", BaseStorageClient.destinationKey("processed", "data/a.csv"));
    assertEquals("processed/data/a.csv", BaseStorageClient.destinationKey("processed/", "data/a.csv"));
    assertEquals("done/2024/a.csv", BaseStorageClient.destinationKey("done/2024", "a.csv"));
    // an empty folder is the root of the bucket or container
    assertEquals("data/a.csv", BaseStorageClient.destinationKey("", "data/a.csv"));
  }
}
