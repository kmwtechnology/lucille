package com.kmwllc.lucille.connector.storageclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.MessengerException;
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

  @Test
  public void testMessengerFailureStopsTraversal() {
    ListStorageClient client = new ListStorageClient(List.of(
        new TestFileReference("a.txt"), new TestFileReference("b.txt"), new TestFileReference("c.txt")));
    Config config = ConfigFactory.parseMap(Map.of("fileOptions", Map.of("moveToErrorFolder", "list://root/errors/")));
    // how KafkaPublisherMessenger reports a send that failed after its retries: on every later send
    MessengerException failure = new MessengerException("Kafka send failed", new RuntimeException("buffer exhausted"));
    FailingMessenger messenger = new FailingMessenger(path -> !path.endsWith("a.txt"), failure);

    RuntimeException e = assertThrows(RuntimeException.class, () -> traverse(client, config, null, messenger));

    assertTrue(e.getMessage(), e.getMessage().contains("list://root/b.txt"));
    assertTrue("expected the messenger's failure in the cause chain of " + e, hasCause(e, failure));
    // c.txt is never tried, and b.txt isn't moved to the error folder, since it isn't the file that failed
    assertEquals(List.of("list://root/a.txt", "list://root/b.txt"), messenger.attempted);
    assertEquals(List.of("list://root/a.txt"), paths(messenger));
    assertEquals(List.of(), client.moves);
  }

  @Test
  public void testDocumentSpecificMessengerFailureSkipsOnlyThatFile() throws Exception {
    ListStorageClient client = new ListStorageClient(List.of(
        new TestFileReference("a.txt"), new TestFileReference("b.txt"), new TestFileReference("c.txt")));
    Config config = ConfigFactory.parseMap(Map.of("fileOptions", Map.of("moveToErrorFolder", "list://root/errors/")));
    FailingMessenger messenger = new FailingMessenger(path -> path.endsWith("b.txt"),
        new MessengerException("b.txt is too large", new RuntimeException("record too large"), true));

    assertEquals(List.of("list://root/a.txt", "list://root/c.txt"), traverse(client, config, null, messenger));
    assertEquals(List.of("list://root/b.txt -> list://root/errors/"), client.moves);
  }

  private static boolean hasCause(Throwable e, Throwable cause) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t == cause) {
        return true;
      }
    }
    return false;
  }

  /** A TestMessenger whose sendForProcessing throws the given exception for the matching file paths. */
  private static class FailingMessenger extends TestMessenger {
    private final java.util.function.Predicate<String> failsFor;
    private final MessengerException failure;
    private final List<String> attempted = new ArrayList<>();

    FailingMessenger(java.util.function.Predicate<String> failsFor, MessengerException failure) {
      this.failsFor = failsFor;
      this.failure = failure;
    }

    @Override
    public void sendForProcessing(Document document) throws MessengerException {
      String path = document.getString(StorageClientFields.FILE_PATH);
      attempted.add(path);
      if (failsFor.test(path)) {
        throw failure;
      }
      super.sendForProcessing(document);
    }
  }

  private static List<String> traverse(ListStorageClient client, Config config) throws Exception {
    return traverse(client, config, null);
  }

  private static List<String> traverse(ListStorageClient client, Config config, FileStateTracker state) throws Exception {
    return traverse(client, config, state, new TestMessenger());
  }

  private static List<String> paths(TestMessenger messenger) {
    return messenger.getDocsSentForProcessing().stream()
        .map(doc -> doc.getString(StorageClientFields.FILE_PATH))
        .collect(Collectors.toList());
  }

  private static List<String> traverse(ListStorageClient client, Config config, FileStateTracker state,
      TestMessenger messenger) throws Exception {
    client.start();
    try {
      client.traverse(new PublisherImpl(ConfigFactory.empty(), messenger, "run", "pipeline"),
          new TraversalParams(config, ROOT, ""), state);
    } finally {
      client.stop();
    }
    return paths(messenger);
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

  // An object-store path is a raw key prefix, so a parent contains every path whose key starts with its own.
  @Test
  public void testContainsPathUsesRawPrefixes() {
    S3StorageClient client = new S3StorageClient(ConfigFactory.empty());

    assertTrue(client.containsPath(URI.create("s3://b/logs"), URI.create("s3://b/logs/2024")));
    assertTrue(client.containsPath(URI.create("s3://b/logs"), URI.create("s3://b/logs-old")));
    assertTrue(client.containsPath(URI.create("s3://b/logs"), URI.create("s3://b/logs")));
    assertTrue(client.containsPath(URI.create("s3://b/logs/"), URI.create("s3://b/logs/2024/")));
    assertTrue(client.containsPath(URI.create("s3://b"), URI.create("s3://b/anything")));
    assertTrue(client.containsPath(URI.create("s3://b/"), URI.create("s3://b/anything")));

    assertFalse(client.containsPath(URI.create("s3://b/logs/"), URI.create("s3://b/logs-old/")));
    // "logs" also lists "logs-old", which "logs/" doesn't
    assertFalse(client.containsPath(URI.create("s3://b/logs/"), URI.create("s3://b/logs")));
    assertFalse(client.containsPath(URI.create("s3://b/logs"), URI.create("s3://other/logs")));
    assertFalse(client.containsPath(URI.create("s3://b/logs"), URI.create("gs://b/logs")));
  }

  // Decided by where a moved file lands, the folder and then the object's full key, not by where the folder is.
  @Test
  public void testMoveTargetInsidePath() {
    S3StorageClient client = new S3StorageClient(ConfigFactory.empty());

    // the folder is inside the path
    assertTrue(client.moveTargetInsidePath(URI.create("s3://b/in"), URI.create("s3://b/in/done")));
    assertTrue(client.moveTargetInsidePath(URI.create("s3://b/in"), URI.create("s3://b/in-done/")));
    assertTrue(client.moveTargetInsidePath(URI.create("s3://b/in/"), URI.create("s3://b/in/")));
    assertTrue(client.moveTargetInsidePath(URI.create("s3://b"), URI.create("s3://b/done")));

    // the folder is outside the path, but the moved key isn't
    assertTrue(client.moveTargetInsidePath(URI.create("s3://b/in/"), URI.create("s3://b/in"))); // in/in/a.csv
    assertTrue(client.moveTargetInsidePath(URI.create("s3://b/a/a"), URI.create("s3://b/a"))); // a/a/a/x.csv
    assertTrue(client.moveTargetInsidePath(URI.create("s3://b/in/"), URI.create("s3://b"))); // in/a.csv, its own key
    assertTrue(client.moveTargetInsidePath(URI.create("s3://b/in/"), URI.create("s3://b/")));

    assertFalse(client.moveTargetInsidePath(URI.create("s3://b/in/"), URI.create("s3://b/done")));
    assertFalse(client.moveTargetInsidePath(URI.create("s3://b/aa"), URI.create("s3://b/a"))); // a/aa/x.csv
    assertFalse(client.moveTargetInsidePath(URI.create("s3://b/in/"), URI.create("s3://other")));
    assertFalse(client.moveTargetInsidePath(URI.create("s3://b/in/"), URI.create("s3://other/in/")));
  }
}
