package com.kmwllc.lucille.connector.storageclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherException;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.core.filehandler.FileHandler;
import com.kmwllc.lucille.core.filehandler.FileHandlerException;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.Test;

/**
 * Tests the traversal BaseStorageClient shares with the cloud clients, against a client that lists a fixed set of keys.
 */
public class SharedTraversalTest {

  private static final String ROOT = "tree://bucket/";
  private static final long TIMEOUT_SECONDS = 20;

  // 6 directories, 2 of them nested, each with 5 files, and 5 files at the top
  private static List<String> manyKeys() {
    List<String> keys = new ArrayList<>();
    for (String directory : List.of("", "a/", "b/", "c/", "a/x/", "a/y/", "b/z/")) {
      for (int i = 0; i < 5; i++) {
        keys.add(directory + "file" + i + ".txt");
      }
    }
    return keys;
  }

  private static Config clientConfig(List<String> keys) {
    return ConfigFactory.parseMap(Map.of("keys", keys));
  }

  private static TreeStorageClient client(List<String> keys) {
    return new TreeStorageClient(clientConfig(keys));
  }

  private static TraversalParams params(String path) {
    return new TraversalParams(ConfigFactory.empty(), URI.create(path), "");
  }

  private static TraversalParams params(String path, Map<String, Object> connectorConfig) {
    return new TraversalParams(ConfigFactory.parseMap(connectorConfig), URI.create(path), "");
  }

  // params for the root, with the given settings in the connector's traversal block
  private static TraversalParams traversal(Map<String, Object> settings) {
    return params(ROOT, Map.of("traversal", settings));
  }

  private static TraversalParams threads(int threadsPerPath) {
    return traversal(Map.of("threadsPerPath", threadsPerPath));
  }

  // with the connector's other settings too
  private static TraversalParams traversal(String path, Map<String, Object> settings, Map<String, Object> connectorConfig) {
    Map<String, Object> config = new HashMap<>(connectorConfig);
    config.put("traversal", settings);
    return params(path, config);
  }

  private static List<String> traverse(TreeStorageClient client, TraversalParams params) throws Exception {
    return traverse(client, params, null);
  }

  private static List<String> traverse(TreeStorageClient client, TraversalParams params, FileStateTracker state) throws Exception {
    TestMessenger messenger = new TestMessenger();
    // as FileConnector does at startup
    client.validateTraversal(params);
    client.start();
    try {
      client.traverse(new PublisherImpl(ConfigFactory.empty(), messenger, "run", "pipeline"), params, state);
    } finally {
      client.stop();
    }
    return messenger.getDocsSentForProcessing().stream()
        .map(doc -> doc.getString(StorageClientFields.FILE_PATH))
        .collect(Collectors.toList());
  }

  private static List<String> sorted(List<String> values) {
    List<String> copy = new ArrayList<>(values);
    Collections.sort(copy);
    return copy;
  }

  private static List<String> uris(String... keys) {
    return Stream.of(keys).map(key -> ROOT + key).collect(Collectors.toList());
  }

  @Test
  public void testParallelPublishesTheSameDocumentsAsSequential() throws Exception {
    List<String> sequential = traverse(client(manyKeys()), params(ROOT));
    List<String> parallel = traverse(client(manyKeys()), threads(4));

    assertEquals(35, sequential.size());
    // compares lists, so a file published twice fails too
    assertEquals(sorted(sequential), sorted(parallel));
  }

  // Each listing of a sibling waits for the other, which only finishes if they run at the same time.
  @Test
  public void testDirectoriesAreListedConcurrently() throws Exception {
    CyclicBarrier bothListing = new CyclicBarrier(2);
    TreeStorageClient client = new TreeStorageClient(clientConfig(List.of("a/1.txt", "b/2.txt"))) {
      @Override
      protected void beforeListing(String prefix) {
        if (!prefix.isEmpty()) {
          try {
            bothListing.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
          } catch (Exception e) {
            throw new IllegalStateException("The siblings weren't listed at the same time.", e);
          }
        }
      }
    };

    assertEquals(uris("a/1.txt", "b/2.txt"), sorted(traverse(client, threads(2))));
    assertEquals(2, client.listingThreads.size());
  }

  @Test
  public void testSequentialTraversalStaysOnTheCallingThread() throws Exception {
    TreeStorageClient client = client(manyKeys());

    traverse(client, params(ROOT));

    assertEquals(Set.of(Thread.currentThread()), client.listingThreads);
  }

  // a directory's files first, then its children, depth first in listing order
  @Test
  public void testSequentialOrder() throws Exception {
    List<String> keys = List.of("b/y/4.txt", "b/3.txt", "a/2.txt", "1.txt");

    assertEquals(uris("1.txt", "a/2.txt", "b/3.txt", "b/y/4.txt"), traverse(client(keys), params(ROOT)));
  }

  @Test
  public void testPathsToSkipAppliesToTheStartDirectory() throws Exception {
    for (int threads : List.of(1, 4)) {
      TreeStorageClient client = client(manyKeys());
      Map<String, Object> skip = Map.of("filterOptions", Map.of("pathsToSkip", List.of(ROOT + "a")));

      assertEquals(List.of(), traverse(client, traversal(ROOT + "a/", Map.of("threadsPerPath", threads), skip)));
      assertTrue(client.listedPrefixes.isEmpty());
    }
  }

  // "logs" is a raw prefix that also covers logs-old/, so an entry for the logs/ directory must not end the traversal
  @Test
  public void testPathsToSkipDoesNotApplyToARawPrefixStart() throws Exception {
    List<String> keys = List.of("logs/1.txt", "logs-old/2.txt", "logs.txt");
    Map<String, Object> skip = Map.of("filterOptions", Map.of("pathsToSkip", List.of(ROOT + "logs/")));

    assertEquals(uris("logs-old/2.txt", "logs.txt"), sorted(traverse(client(keys), params(ROOT + "logs", skip))));
  }

  @Test
  public void testPathsToSkipAppliesAtEveryDepth() throws Exception {
    for (int threads : List.of(1, 4)) {
      TreeStorageClient client = client(List.of("1.txt", "a/2.txt", "a/x/3.txt", "a/y/4.txt", "b/5.txt"));
      Map<String, Object> skip = Map.of("filterOptions", Map.of("pathsToSkip", List.of(ROOT + "a/x/", ROOT + "b")));

      assertEquals(uris("1.txt", "a/2.txt", "a/y/4.txt"), sorted(traverse(client, traversal(ROOT, Map.of("threadsPerPath", threads), skip))));
      assertFalse(client.listedPrefixes.contains("a/x/"));
      assertFalse(client.listedPrefixes.contains("b/"));
    }
  }

  @Test
  public void testDirectoryCallbackVerdict() throws Exception {
    Set<String> asked = ConcurrentHashMap.newKeySet();
    TreeStorageClient client = new TreeStorageClient(clientConfig(List.of("start/1.txt", "start/a/2.txt", "start/b/3.txt", "start/c/4.txt"))) {
      @Override
      protected DirectoryVerdict onDirectory(URI directory, TraversalParams params) {
        asked.add(directory.toString());
        return directory.toString().endsWith("/a/") ? DirectoryVerdict.SKIP : DirectoryVerdict.DESCEND;
      }
    };
    Map<String, Object> skip = Map.of("filterOptions", Map.of("pathsToSkip", List.of(ROOT + "start/c/")));

    assertEquals(uris("start/1.txt", "start/b/3.txt"), sorted(traverse(client, params(ROOT + "start/", skip))));
    // the start is asked about, and a directory in pathsToSkip is not
    assertEquals(Set.of(ROOT + "start/", ROOT + "start/a/", ROOT + "start/b/"), asked);
  }

  @Test
  public void testStartDirectoryCallbackCanSkipTheTraversal() throws Exception {
    TreeStorageClient client = new TreeStorageClient(clientConfig(manyKeys())) {
      @Override
      protected DirectoryVerdict onDirectory(URI directory, TraversalParams params) {
        return DirectoryVerdict.SKIP;
      }
    };

    assertEquals(List.of(), traverse(client, params(ROOT)));
  }

  // One step visits one level, publishing its files and returning its children, with no descent.
  @Test
  public void testOneDirectoryStepIsCallableOnItsOwn() throws Exception {
    TreeStorageClient client = client(List.of("a/1.txt", "a/x/2.txt", "a/y/3.txt", "4.txt"));
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run", "pipeline");
    client.start();

    List<String> children = client.traverseOneDirectory("a/", publisher, params(ROOT), null);

    assertEquals(List.of("a/x/", "a/y/"), children);
    assertEquals(1, messenger.getDocsSentForProcessing().size());
    assertEquals(ROOT + "a/1.txt", messenger.getDocsSentForProcessing().get(0).getString(StorageClientFields.FILE_PATH));
    assertEquals(List.of("a/"), List.copyOf(client.listedPrefixes));
  }

  @Test
  public void testUnbuildableReferenceSkipsOnlyThatFile() throws Exception {
    TreeStorageClient client = new TreeStorageClient(clientConfig(List.of("good.txt"))) {
      @Override
      protected Stream<ListingEntry> listDirectory(String prefix, TraversalParams params) {
        return Stream.concat(
            Stream.of(ListingEntry.file("bad", () -> {
              throw new IllegalArgumentException("Not a valid key");
            })),
            super.listDirectory(prefix, params));
      }
    };

    assertEquals(uris("good.txt"), traverse(client, params(ROOT)));
  }

  @Test
  public void testMaxDocumentsIsExactUnderParallelism() throws Exception {
    List<String> keys = IntStream.range(0, 500).mapToObj(i -> "dir" + (i % 25) + "/file" + i + ".txt").collect(Collectors.toList());

    List<String> published = traverse(client(keys), traversal(Map.of("threadsPerPath", 8, "maxDocuments", 37)));

    assertEquals(37, published.size());
    assertEquals(37, new HashSet<>(published).size());
  }

  @Test
  public void testMaxDocumentsStopsListing() throws Exception {
    TreeStorageClient client = client(List.of("1.txt", "2.txt", "a/3.txt", "b/4.txt"));

    assertEquals(uris("1.txt", "2.txt"), traverse(client, traversal(Map.of("maxDocuments", 2))));
    assertEquals(List.of(""), List.copyOf(client.listedPrefixes));
  }

  @Test
  public void testMaxDocumentsAboveTheFileCountPublishesEverything() throws Exception {
    assertEquals(35, traverse(client(manyKeys()), traversal(Map.of("maxDocuments", 1000))).size());
    assertEquals(35, traverse(client(manyKeys()), traversal(Map.of("maxDocuments", 35, "threadsPerPath", 4))).size());
  }

  @Test
  public void testMaxDocumentsAppliesToAFlatListing() throws Exception {
    TreeStorageClient client = client(manyKeys());

    assertEquals(3, traverse(client, traversal(Map.of("maxDocuments", 3, "listingMode", "flat"))).size());
  }

  // A document that fails to publish only skips its file, so it mustn't use up one of the maxDocuments.
  @Test
  public void testAFailedPublishDoesNotCountTowardMaxDocuments() throws Exception {
    TreeStorageClient client = client(List.of("1.txt", "2.txt", "3.txt", "4.txt", "5.txt"));
    TestMessenger messenger = new TestMessenger();
    AtomicBoolean failed = new AtomicBoolean();
    Publisher failsOnce = new PublisherImpl(ConfigFactory.empty(), messenger, "run", "pipeline") {
      @Override
      public void publish(Document document) throws PublisherException {
        if (failed.compareAndSet(false, true)) {
          throw new PublisherException("Simulated failure publishing " + document.getId());
        }
        super.publish(document);
      }
    };

    client.start();
    try {
      client.traverse(failsOnce, traversal(Map.of("maxDocuments", 3)), null);
    } finally {
      client.stop();
    }

    assertTrue(failed.get());
    assertEquals(3, messenger.getDocsSentForProcessing().size());
  }

  // Documents from a file handler count one by one, and a file that was cut off isn't moved as if it had been processed.
  @Test
  public void testMaxDocumentsCountsFileHandlerDocumentsAndLeavesACutOffFile() throws Exception {
    TreeStorageClient client = new TreeStorageClient(clientConfig(List.of("rows.csv", "other.txt"))) {
      @Override
      protected String contentFor(String key) {
        return "id,value\n1,a\n2,b\n3,c\n4,d\n5,e\n";
      }
    };
    TraversalParams params = traversal(ROOT, Map.of("maxDocuments", 3), Map.of(
        "fileHandlers", Map.of("csv", Map.of("idField", "id")),
        "fileOptions", Map.of("moveToAfterProcessing", ROOT + "done/", "moveToErrorFolder", ROOT + "errors/")));

    TestMessenger messenger = new TestMessenger();
    client.start();
    client.traverse(new PublisherImpl(ConfigFactory.empty(), messenger, "run", "pipeline"), params, null);
    client.stop();

    assertEquals(3, messenger.getDocsSentForProcessing().size());
    assertTrue(client.moves.isEmpty());
  }

  // Ending on the limit exactly is still a complete file, so it is moved.
  @Test
  public void testAFileFinishedAtTheLimitIsMoved() throws Exception {
    TreeStorageClient client = client(List.of("1.txt", "2.txt"));
    TraversalParams params = traversal(ROOT, Map.of("maxDocuments", 1), Map.of("fileOptions", Map.of("moveToAfterProcessing", "other://bucket/done/")));

    assertEquals(uris("1.txt"), traverse(client, params));
    assertEquals(uris("1.txt"), List.copyOf(client.moves));
  }

  @Test
  public void testInvalidSettingsAreRejected() {
    assertThrows(IllegalArgumentException.class, () -> threads(0));
    assertThrows(IllegalArgumentException.class, () -> traversal(Map.of("maxDocuments", 0)));
    assertThrows(IllegalArgumentException.class, () -> traversal(Map.of("listingMode", "sideways")));
  }

  @Test
  public void testValidateTraversal() {
    TreeStorageClient client = client(manyKeys());
    Map<String, Object> skip = Map.of("filterOptions", Map.of("pathsToSkip", List.of(ROOT + "a/")));

    client.validateTraversal(traversal(ROOT, Map.of("threadsPerPath", 2, "maxDocuments", 2, "listingMode", "hierarchical"), skip));
    client.validateTraversal(traversal(Map.of("listingMode", "flat")));

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> client.validateTraversal(traversal(Map.of("listingMode", "flat", "threadsPerPath", 2))));
    assertTrue(e.getMessage(), e.getMessage().contains("traversal.threadsPerPath"));

    e = assertThrows(IllegalArgumentException.class,
        () -> client.validateTraversal(traversal(ROOT, Map.of("listingMode", "flat"), skip)));
    assertTrue(e.getMessage(), e.getMessage().contains("pathsToSkip"));
  }

  // A client that walks its storage itself would ignore the listing settings, so it refuses them. It accepts
  // maxDocuments, which traverse() enforces for every BaseStorageClient.
  @Test
  public void testAClientThatDoesNotTraverseByListingRejectsItsSettings() {
    MemoryStorageClient client = new MemoryStorageClient(ConfigFactory.parseMap(Map.of("content", "x")));

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> client.validateTraversal(threads(2)));
    assertTrue(e.getMessage(), e.getMessage().contains("traversal.threadsPerPath above 1 isn't supported by " + MemoryStorageClient.class.getName()));
    e = assertThrows(IllegalArgumentException.class, () -> client.validateTraversal(traversal(Map.of("listingMode", "flat"))));
    assertTrue(e.getMessage(), e.getMessage().contains("traversal.listingMode"));

    client.validateTraversal(threads(1));
    client.validateTraversal(traversal(Map.of("maxDocuments", 2)));
  }

  // The interface's default refuses everything only BaseStorageClient honors, so a limit is never silently ignored.
  @Test
  public void testAClientImplementingTheInterfaceDirectlyRejectsTheSettings() {
    StorageClient direct = mock(StorageClient.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));

    direct.validateTraversal(params(ROOT));
    for (Map<String, Object> settings : List.<Map<String, Object>>of(Map.of("threadsPerPath", 2), Map.of("listingMode", "hierarchical"),
        Map.of("maxDocuments", 5))) {
      String setting = "traversal." + settings.keySet().iterator().next();
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> direct.validateTraversal(traversal(settings)));
      assertTrue(e.getMessage(), e.getMessage().contains(setting));
    }
  }

  @Test
  public void testExplicitFlatListing() throws Exception {
    TreeStorageClient client = client(manyKeys());

    assertEquals(35, traverse(client, traversal(Map.of("listingMode", "flat"))).size());
    assertEquals(List.of("flat:"), List.copyOf(client.listedPrefixes));
  }

  // A provider returning a prefix as its own child, or as a child of its child, doesn't loop.
  @Test(timeout = TIMEOUT_SECONDS * 1000)
  public void testLoopGuard() throws Exception {
    for (int threads : List.of(1, 4)) {
      TreeStorageClient client = new TreeStorageClient(clientConfig(List.of("a/1.txt", "2.txt"))) {
        @Override
        protected Stream<ListingEntry> listDirectory(String prefix, TraversalParams params) {
          return Stream.concat(super.listDirectory(prefix, params), Stream.of(ListingEntry.directory(prefix), ListingEntry.directory("")));
        }
      };

      assertEquals(uris("2.txt", "a/1.txt"), sorted(traverse(client, threads(threads))));
      assertEquals(2, client.listedPrefixes.size());
    }
  }

  @Test
  public void testAListingFailureFailsTheTraversal() {
    for (int threads : List.of(1, 4)) {
      TreeStorageClient client = new TreeStorageClient(clientConfig(manyKeys())) {
        @Override
        protected void beforeListing(String prefix) {
          if (prefix.equals("b/")) {
            throw new IllegalStateException("Listing b/ failed.");
          }
        }
      };

      IllegalStateException e = assertThrows(IllegalStateException.class, () -> traverse(client, threads(threads)));
      assertEquals("Listing b/ failed.", e.getMessage());
    }
  }

  @Test
  public void testNoThreadsLeftAfterATraversal() throws Exception {
    TreeStorageClient client = client(manyKeys());
    client.start();
    client.traverse(new PublisherImpl(ConfigFactory.empty(), new TestMessenger(), "run", "pipeline"), threads(4), null);

    // no stop() needed: a traversal cleans up after itself
    assertAllEnded(client.listingThreads);
    client.stop();
  }

  @Test
  public void testNoThreadsLeftAfterAFailure() {
    TreeStorageClient client = new TreeStorageClient(clientConfig(manyKeys())) {
      @Override
      protected void beforeListing(String prefix) {
        throw new IllegalStateException("Listing failed.");
      }
    };

    assertThrows(IllegalStateException.class, () -> traverse(client, threads(4)));
    assertAllEnded(client.listingThreads);
  }

  private static void assertAllEnded(Set<Thread> threads) {
    assertFalse(threads.isEmpty());
    for (Thread thread : threads) {
      try {
        thread.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
      assertFalse(thread.getName() + " is still running.", thread.isAlive());
    }
  }

  // A client whose listings block until they're interrupted, so that a traversal is certain to be running.
  private static class BlockingTreeClient extends TreeStorageClient {

    private final CountDownLatch listing = new CountDownLatch(1);

    BlockingTreeClient() {
      super(clientConfig(List.of("a/1.txt", "b/2.txt")));
    }

    @Override
    protected void beforeListing(String prefix) {
      listing.countDown();
      try {
        new CountDownLatch(1).await();
      } catch (InterruptedException e) {
        throw new IllegalStateException("Interrupted while listing.", e);
      }
    }
  }

  // Runs the client's traversal on another thread, returning what it threw once it has started listing.
  private static Thread traverseInBackground(BlockingTreeClient client, int threadsPerPath, AtomicReference<Throwable> thrown) throws Exception {
    client.start();
    Thread traversal = new Thread(() -> {
      try {
        client.traverse(new PublisherImpl(ConfigFactory.empty(), new TestMessenger(), "run", "pipeline"), threads(threadsPerPath), null);
      } catch (Throwable t) {
        thrown.set(t);
      }
    });
    traversal.start();
    assertTrue(client.listing.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    return traversal;
  }

  @Test
  public void testStopEndsARunningTraversalAndItsThreads() throws Exception {
    BlockingTreeClient client = new BlockingTreeClient();
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    Thread traversal = traverseInBackground(client, 3, thrown);

    client.stop();

    assertAllEnded(client.listingThreads);
    traversal.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    assertFalse(traversal.isAlive());
    // an unfinished traversal must fail, or the files it didn't reach would look deleted
    assertTrue(String.valueOf(thrown.get()), thrown.get() instanceof ConnectorException);
  }

  // The first thread can reach its listing while the others are still being started, and stop() can land right then.
  @Test
  public void testStopWhileThreadsAreStillStarting() throws Exception {
    for (int i = 0; i < 25; i++) {
      BlockingTreeClient client = new BlockingTreeClient();
      AtomicReference<Throwable> thrown = new AtomicReference<>();
      Thread traversal = traverseInBackground(client, 32, thrown);

      client.stop();

      traversal.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
      assertFalse(traversal.isAlive());
      assertTrue(String.valueOf(thrown.get()), thrown.get() instanceof ConnectorException);
    }
  }

  @Test
  public void testInterruptingTheCallerEndsTheTraversalAndItsThreads() throws Exception {
    BlockingTreeClient client = new BlockingTreeClient();
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    Thread traversal = traverseInBackground(client, 3, thrown);

    traversal.interrupt();

    traversal.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    assertFalse(traversal.isAlive());
    assertAllEnded(client.listingThreads);
    assertTrue(String.valueOf(thrown.get()), thrown.get() instanceof ConnectorException);
    client.stop();
  }

  // A read that fails because the traversal is being stopped says nothing about the file, so it isn't moved to the error folder.
  @Test
  public void testStoppingDoesNotMoveTheFileBeingReadToTheErrorFolder() throws Exception {
    CountDownLatch reading = new CountDownLatch(1);
    TreeStorageClient client = new TreeStorageClient(clientConfig(List.of("a.txt"))) {
      @Override
      protected String contentFor(String key) {
        reading.countDown();
        try {
          new CountDownLatch(1).await();
        } catch (InterruptedException e) {
          throw new IllegalStateException("Interrupted while reading.", e);
        }
        return key;
      }
    };
    TraversalParams params = traversal(ROOT, Map.of("threadsPerPath", 2), Map.of("fileOptions", Map.of("moveToErrorFolder", "other://bucket/errors/")));

    client.start();
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    Thread traversal = new Thread(() -> {
      try {
        client.traverse(new PublisherImpl(ConfigFactory.empty(), new TestMessenger(), "run", "pipeline"), params, null);
      } catch (Throwable t) {
        thrown.set(t);
      }
    });
    traversal.start();
    assertTrue(reading.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

    client.stop();
    traversal.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));

    assertFalse(traversal.isAlive());
    assertTrue(String.valueOf(thrown.get()), thrown.get() instanceof ConnectorException);
    assertTrue(client.moves.isEmpty());
  }

  // Records which threads opened state, and fails any per-file call from a thread that didn't.
  private static class RecordingTracker implements ThreadBoundFileStateTracker {

    final Set<String> encountered = ConcurrentHashMap.newKeySet();
    final Set<String> published = ConcurrentHashMap.newKeySet();
    final Set<Thread> opened = ConcurrentHashMap.newKeySet();
    final Set<Thread> closed = ConcurrentHashMap.newKeySet();
    final Set<Thread> usedWithoutOpening = ConcurrentHashMap.newKeySet();

    private void check() {
      if (!opened.contains(Thread.currentThread()) || closed.contains(Thread.currentThread())) {
        usedWithoutOpening.add(Thread.currentThread());
      }
    }

    @Override
    public void openForThread() throws IOException {
      opened.add(Thread.currentThread());
    }

    @Override
    public void closeForThread() {
      closed.add(Thread.currentThread());
    }

    @Override
    public void markFileEncountered(String fullPathStr) {
      check();
      encountered.add(fullPathStr);
    }

    @Override
    public void markAllEntriesEncountered(String prefix) {
      check();
    }

    @Override
    public Instant getLastPublished(String fullPathStr) {
      check();
      return null;
    }

    @Override
    public void successfullyPublishedFile(String fullPathStr) {
      check();
      published.add(fullPathStr);
    }
  }

  @Test
  public void testEachThreadOpensAndClosesItsOwnState() throws Exception {
    RecordingTracker tracker = new RecordingTracker();
    TreeStorageClient client = client(manyKeys());

    List<String> published = traverse(client, threads(4), tracker);

    assertEquals(35, published.size());
    assertEquals(Set.copyOf(published), tracker.encountered);
    assertEquals(Set.copyOf(published), tracker.published);
    assertTrue(tracker.usedWithoutOpening.isEmpty());
    assertEquals(4, tracker.opened.size());
    assertEquals(tracker.opened, tracker.closed);
    assertFalse(tracker.opened.contains(Thread.currentThread()));
  }

  // Without this check a state failure would be swallowed per file, leaving every file unmarked and so looking deleted.
  @Test
  public void testFailingToOpenStateFailsTheTraversal() {
    IOException failure = new IOException("No connection available.");
    RecordingTracker tracker = new RecordingTracker() {
      @Override
      public void openForThread() throws IOException {
        throw failure;
      }
    };
    TreeStorageClient client = client(manyKeys());

    ConnectorException e = assertThrows(ConnectorException.class, () -> traverse(client, threads(4), tracker));

    assertSame(failure, e.getCause());
  }

  @Test
  public void testATrackerThatIsNotThreadBoundIsRejectedWithThreads() throws Exception {
    TreeStorageClient parallel = client(manyKeys());
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> traverse(parallel, threads(2), new InMemoryFileStateTracker()));
    assertTrue(e.getMessage(), e.getMessage().contains("traversal.threadsPerPath"));

    // and works with one thread, as it always has
    assertEquals(35, traverse(client(manyKeys()), params(ROOT), new InMemoryFileStateTracker()).size());
  }

  /** A FileHandler that records the threads each instance is used on. */
  public static class ThreadRecordingFileHandler implements FileHandler {

    static final Map<ThreadRecordingFileHandler, Set<Thread>> THREADS_BY_INSTANCE = new ConcurrentHashMap<>();

    public ThreadRecordingFileHandler(Config config) { }

    @Override
    public Iterator<Document> processFile(InputStream inputStream, String pathStr) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void processFileAndPublish(Publisher publisher, InputStream inputStream, String pathStr) {
      THREADS_BY_INSTANCE.computeIfAbsent(this, handler -> ConcurrentHashMap.newKeySet()).add(Thread.currentThread());
      try (inputStream) {
        publisher.publish(Document.create(new String(inputStream.readAllBytes(), StandardCharsets.UTF_8)));
      } catch (IOException | PublisherException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  /** A FileHandler that publishes one document per line of its file, and never closes the stream it's given. */
  public static class LinePerDocumentFileHandler implements FileHandler {

    public LinePerDocumentFileHandler(Config config) { }

    @Override
    public Iterator<Document> processFile(InputStream inputStream, String pathStr) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void processFileAndPublish(Publisher publisher, InputStream inputStream, String pathStr) throws FileHandlerException {
      try {
        for (String line : new String(inputStream.readNBytes(1000), StandardCharsets.UTF_8).split("\n")) {
          if (line.equals("fail")) {
            throw new FileHandlerException("Failed on purpose.");
          }
          publisher.publish(Document.create(pathStr + "-" + line));
        }
      } catch (IOException | PublisherException e) {
        throw new FileHandlerException("Error handling " + pathStr, e);
      }
    }
  }

  // A tree client whose content streams record whether they were closed.
  private static class ClosingTrackedClient extends TreeStorageClient {

    final Map<String, AtomicBoolean> closed = new ConcurrentHashMap<>();

    ClosingTrackedClient(List<String> keys) {
      super(clientConfig(keys));
    }

    @Override
    protected String contentFor(String key) {
      return key.startsWith("bad") ? "1\nfail\n3" : "1\n2\n3\n4\n5";
    }

    @Override
    protected InputStream getFileContentStreamFromStorage(URI uri) {
      AtomicBoolean wasClosed = new AtomicBoolean();
      closed.put(keyPrefix(uri), wasClosed);
      return new java.io.FilterInputStream(super.getFileContentStreamFromStorage(uri)) {
        @Override
        public void close() throws IOException {
          wasClosed.set(true);
          super.close();
        }
      };
    }
  }

  private static TraversalParams lineHandlerParams(Map<String, Object> traversalSettings) {
    return traversal(ROOT, traversalSettings, Map.of("fileHandlers", Map.of("lines", Map.of("class", LinePerDocumentFileHandler.class.getName()))));
  }

  // On S3 and Azure, an open content stream holds an HTTP connection, so one left open by a failing handler leaks it.
  @Test
  public void testAFileHandlerStreamIsClosedWhenTheHandlerFails() throws Exception {
    ClosingTrackedClient client = new ClosingTrackedClient(List.of("bad.lines", "good.lines"));

    List<String> published = traverse(client, lineHandlerParams(Map.of()));

    assertEquals(6, published.size());
    assertEquals(Set.of("bad.lines", "good.lines"), client.closed.keySet());
    assertTrue("the failed file's stream was left open", client.closed.get("bad.lines").get());
    assertTrue(client.closed.get("good.lines").get());
  }

  @Test
  public void testAFileHandlerStreamIsClosedWhenMaxDocumentsCutsItOff() throws Exception {
    ClosingTrackedClient client = new ClosingTrackedClient(List.of("first.lines", "second.lines"));

    List<String> published = traverse(client, lineHandlerParams(Map.of("maxDocuments", 7)));

    assertEquals(7, published.size());
    assertEquals(Set.of("first.lines", "second.lines"), client.closed.keySet());
    assertTrue("the cut-off file's stream was left open", client.closed.get("second.lines").get());
  }

  @Test
  public void testThreadsDoNotShareFileHandlers() throws Exception {
    ThreadRecordingFileHandler.THREADS_BY_INSTANCE.clear();
    List<String> keys = IntStream.range(0, 40).mapToObj(i -> "dir" + i + "/file.rec").collect(Collectors.toList());
    TraversalParams params = traversal(ROOT, Map.of("threadsPerPath", 4),
        Map.of("fileHandlers", Map.of("rec", Map.of("class", ThreadRecordingFileHandler.class.getName()))));

    TestMessenger messenger = new TestMessenger();
    TreeStorageClient client = client(keys);
    client.start();
    client.traverse(new PublisherImpl(ConfigFactory.empty(), messenger, "run", "pipeline"), params, null);
    client.stop();

    assertEquals(40, messenger.getDocsSentForProcessing().size());
    assertFalse(ThreadRecordingFileHandler.THREADS_BY_INSTANCE.isEmpty());
    ThreadRecordingFileHandler.THREADS_BY_INSTANCE.values().forEach(threads -> assertEquals(1, threads.size()));
  }
}
