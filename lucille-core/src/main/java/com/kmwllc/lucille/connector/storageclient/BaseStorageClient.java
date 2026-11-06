package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.storageclient.StorageClientFields.ARCHIVE_FILE_SEPARATOR;
import static com.kmwllc.lucille.connector.storageclient.StorageClientFields.CONTENT;
import static com.kmwllc.lucille.connector.storageclient.StorageClientFields.FILE_PATH;
import static com.kmwllc.lucille.connector.storageclient.StorageClientFields.MODIFIED;
import static com.kmwllc.lucille.connector.storageclient.StorageClientFields.SIZE;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.LISTING_MODE;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.MAX_DOCUMENTS;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.MAX_RESULTS_PER_PAGE;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.MOVE_TO_AFTER_PROCESSING;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.MOVE_TO_ERROR_FOLDER;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.TRAVERSAL_THREADS;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.connector.StatePathTooLongException;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.filehandler.FileHandler;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherException;
import com.kmwllc.lucille.util.FileUtils;
import com.kmwllc.lucille.util.FileUtils.ContentTooLargeException;
import com.kmwllc.lucille.util.ThreadNameUtils;
import com.typesafe.config.Config;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

/**
 * The base implementation for StorageClients. Must be started before traversal / getting a file's contents using
 * {@link BaseStorageClient#start()}. Call {@link BaseStorageClient#traverse(Publisher, TraversalParams)} to process and publish
 * Documents. Call {@link BaseStorageClient#getFileContentStream(URI)} to get the contents of a single file at the given URI.
 */
public abstract class BaseStorageClient implements StorageClient {

  private static final Logger log = LoggerFactory.getLogger(BaseStorageClient.class);
  private static final Logger docLogger = LoggerFactory.getLogger("com.kmwllc.lucille.core.DocLogger");

  protected final Config config;

  protected final int maxResultsPerPage;
  // volatile because stop() can be called while another thread is traversing
  private volatile boolean initialized = false;

  private final int traversalThreads;
  // null means no limit
  private final Long maxDocuments;
  // null means this client's default, see defaultListingMode()
  private final ListingMode configuredListingMode;

  // the multi-threaded traversals running now, which stop() has to end
  private final Set<ParallelDescent> activeDescents = ConcurrentHashMap.newKeySet();

  // the descent a pool thread works for, so that processing a file can tell an aborted traversal from a bad file
  private final ThreadLocal<ParallelDescent> workerDescent = new ThreadLocal<>();

  // how long to wait for traversal threads to end once they have been interrupted
  private static final long TRAVERSAL_THREAD_STOP_SECONDS = 10;

  // how often an idle traversal thread checks whether the others have finished
  private static final long IDLE_POLL_MILLIS = 50;

  /**
   * Creates a base implementation of a Storage client from the given config. Validates the provided options, throwing an
   * IllegalArgumentException if they are invalid for the specific implementation.
   */
  public BaseStorageClient(Config config) {
    this(config, 100);
  }

  /**
   * Creates a base implementation of a Storage client from the given config, using the given page size when the config
   * does not set "maxResultsPerPage".
   */
  protected BaseStorageClient(Config config, int defaultMaxResultsPerPage) {
    validateOptions(config);
    this.config = config;

    // only matters for traversals
    this.maxResultsPerPage = ConfigUtils.getIntOrDefault(config, MAX_RESULTS_PER_PAGE, defaultMaxResultsPerPage);

    this.traversalThreads = ConfigUtils.getIntOrDefault(config, TRAVERSAL_THREADS, 1);
    if (traversalThreads < 1) {
      throw new IllegalArgumentException(TRAVERSAL_THREADS + " must be at least 1, but was " + traversalThreads);
    }

    this.maxDocuments = ConfigUtils.getLongOrDefault(config, MAX_DOCUMENTS, null);
    if (maxDocuments != null && maxDocuments < 1) {
      throw new IllegalArgumentException(MAX_DOCUMENTS + " must be at least 1, but was " + maxDocuments);
    }

    this.configuredListingMode = config.hasPath(LISTING_MODE) ? ListingMode.fromString(config.getString(LISTING_MODE)) : null;
    if (configuredListingMode == ListingMode.FLAT && traversalThreads > 1) {
      throw new IllegalArgumentException(LISTING_MODE + " '" + ListingMode.FLAT + "' is one listing, which can't be divided among threads, so it can't be "
          + "combined with " + TRAVERSAL_THREADS + " above 1.");
    }
  }

  /**
   * Validate that the given config is sufficient to construct an instance of this StorageClient.
   * Catches cases in which a config option being set requires the presence or absence of another option(s), which is not covered by
   * the Spec.
   * @throws IllegalArgumentException If the configuration is not sufficient for this StorageClient.
   */
  protected void validateOptions(Config config) { }

  // Wrapping around start, stop, and traverse to manage "initialized" before / after doing the actual operations in a
  // StorageClient-specific way.

  @Override
  public final void start() throws IOException {
    if (!initialized) {
      initializeStorageClient();
      initialized = true;
    }
  }

  // StorageClient-specific operations for start()
  protected abstract void initializeStorageClient() throws IOException;

  @Override
  public final void stop() throws IOException {
    if (initialized) {
      initialized = false;
      stopActiveDescents();
      shutdownStorageClient();
    }
  }

  // StorageClient-specific operations for stop()
  protected abstract void shutdownStorageClient() throws IOException;

  public boolean isInitialized() {
    return this.initialized;
  }

  // Convenience method for traversing without state.
  @Override
  public final void traverse(Publisher publisher, TraversalParams params) throws IOException, ConnectorException {
    traverse(publisher, params, null);
  }

  @Override
  public final void traverse(Publisher publisher, TraversalParams params, FileStateTracker stateMgr) throws IOException, ConnectorException {
    if (!isInitialized()) {
      throw new IllegalStateException("This StorageClient has not been started. Call start() first.");
    }

    traverseStorageClient(publisher, params, stateMgr);
  }

  /**
   * Performs the traversal. A client whose storage has a directory-like listing can implement this by calling
   * {@link #traverseByListing(Publisher, TraversalParams, FileStateTracker)} and overriding {@link #listDirectory(String, TraversalParams)}.
   */
  protected abstract void traverseStorageClient(Publisher publisher, TraversalParams params, FileStateTracker stateMgr) throws IOException, ConnectorException;

  /**
   * {@inheritDoc}
   * <p> Checks the settings {@link #traverseByListing(Publisher, TraversalParams, FileStateTracker)} reads.
   */
  @Override
  public void validateTraversal(TraversalParams params, boolean stateEnabled, boolean collapsing) {
    // A collapsing Publisher is not safe for concurrent publish() calls - it would merge and drop documents silently
    if (traversalThreads > 1 && collapsing) {
      throw new IllegalArgumentException(TRAVERSAL_THREADS + " above 1 can't be combined with collapse.");
    }

    // the files a capped traversal never reaches would look deleted, and be tombstoned and removed from state
    if (maxDocuments != null && stateEnabled) {
      throw new IllegalArgumentException(MAX_DOCUMENTS + " can't be combined with state: files the traversal doesn't reach would be treated as deleted.");
    }

    if (configuredListingMode == ListingMode.FLAT && !params.getPathsToSkip().isEmpty()) {
      throw new IllegalArgumentException(LISTING_MODE + " '" + ListingMode.FLAT + "' lists no directories, so it can't be combined with filterOptions.pathsToSkip.");
    }
  }

  // -- The shared traversal --

  /**
   * How a traversal lists its path: {@link #HIERARCHICAL}, one listing per directory, or {@link #FLAT}, one listing of
   * every key under the path.
   */
  public enum ListingMode {
    HIERARCHICAL, FLAT;

    @Override
    public String toString() {
      return name().toLowerCase(Locale.ROOT);
    }

    static ListingMode fromString(String name) {
      for (ListingMode mode : values()) {
        if (mode.name().equalsIgnoreCase(name.trim())) {
          return mode;
        }
      }
      throw new IllegalArgumentException("Unknown " + LISTING_MODE + " \"" + name + "\"; expected one of hierarchical, flat");
    }
  }

  /**
   * What a traversal does with a directory it found: {@link #DESCEND} lists it, and {@link #SKIP} leaves it and everything
   * under it unvisited.
   */
  public enum DirectoryVerdict {
    DESCEND, SKIP
  }

  /**
   * One result of a listing: a file, whose reference is only built when the traversal reaches it, or a child directory.
   */
  public static final class ListingEntry {

    private final String name;
    private final Supplier<FileReference> reference;

    private ListingEntry(String name, Supplier<FileReference> reference) {
      this.name = name;
      this.reference = reference;
    }

    /**
     * @param name The file's key, used to log an error if its reference can't be built.
     * @param reference Builds the file's reference. If it throws, the traversal logs the error and skips only this file.
     */
    public static ListingEntry file(String name, Supplier<FileReference> reference) {
      return new ListingEntry(name, reference);
    }

    /**
     * @param prefix The child directory's key, as given to {@link #listDirectory(String, TraversalParams)} to list it.
     */
    public static ListingEntry directory(String prefix) {
      return new ListingEntry(prefix, null);
    }

    private boolean isDirectory() {
      return reference == null;
    }
  }

  /**
   * Lists one directory, without descending into its children. The stream should be lazy, fetching further pages only as
   * it is consumed, because the traversal stops consuming it at <code>maxDocuments</code>. Called from several threads when
   * <code>traversalThreads</code> is above 1.
   *
   * @param prefix The directory's key. For the start of a traversal, this is {@link #getStartingDirectory(TraversalParams)},
   *               which may be a raw prefix rather than a directory.
   * @param params Parameters for the traversal.
   * @return The files and child directories directly under the prefix.
   */
  protected Stream<ListingEntry> listDirectory(String prefix, TraversalParams params) {
    throw new UnsupportedOperationException(getClass().getName() + " does not implement listDirectory.");
  }

  /**
   * Lists every file under the prefix in one listing, for {@link ListingMode#FLAT}. Lazy, like {@link #listDirectory}.
   *
   * @param prefix The key prefix to list.
   * @param params Parameters for the traversal.
   * @return The files under the prefix.
   */
  protected Stream<ListingEntry> listFlat(String prefix, TraversalParams params) {
    throw new UnsupportedOperationException(getClass().getName() + " does not implement listFlat.");
  }

  /**
   * Returns the URI of a directory found by {@link #listDirectory}, which <code>pathsToSkip</code> is compared against.
   *
   * @param prefix The directory's key.
   * @param params Parameters for the traversal.
   * @return The directory's URI.
   */
  protected URI directoryURI(String prefix, TraversalParams params) {
    throw new UnsupportedOperationException(getClass().getName() + " does not implement directoryURI.");
  }

  /**
   * Returns the key prefix a traversal starts from: by default, the path of the traversal's URI without its leading '/'.
   *
   * @param params Parameters for the traversal.
   * @return The key prefix to start from. Empty for the root of the bucket or container.
   */
  protected String getStartingDirectory(TraversalParams params) {
    return keyPrefix(params.getURI());
  }

  /**
   * The listing mode used when <code>listingMode</code> is not configured. A {@link ListingMode#FLAT} default gives way to
   * a hierarchical listing when <code>pathsToSkip</code> is set or <code>traversalThreads</code> is above 1, which a single
   * listing can't serve.
   */
  protected ListingMode defaultListingMode() {
    return ListingMode.HIERARCHICAL;
  }

  /**
   * Asked about each directory a hierarchical traversal could descend into, after <code>pathsToSkip</code>. Includes the
   * start of the traversal when its path names a directory, meaning it is empty or ends in '/'. Called from several threads
   * when <code>traversalThreads</code> is above 1. Not called for a flat listing, which sees no directories.
   *
   * @param directory The directory's URI.
   * @param publisher The traversal's Publisher, for publishing a document for the directory. Such a document counts toward
   *                  <code>maxDocuments</code>, and isn't recorded in state.
   * @param params Parameters for the traversal.
   * @return Whether to descend. By default, always {@link DirectoryVerdict#DESCEND}.
   * @throws ConnectorException To stop the traversal and fail the run.
   */
  protected DirectoryVerdict onDirectory(URI directory, Publisher publisher, TraversalParams params) throws ConnectorException {
    return DirectoryVerdict.DESCEND;
  }

  /**
   * Lists one directory and publishes its files, without descending, returning its child directories. Applies the same
   * per-file handling as a traversal, including skipping a file whose reference can't be built. The descent in
   * {@link #traverseByListing} is built on this, and it can be called on its own to visit a single level.
   *
   * @param prefix The directory's key.
   * @return The keys of the child directories, in listing order. <code>pathsToSkip</code> and {@link #onDirectory} have not
   * been applied to them.
   */
  protected final List<String> traverseOneDirectory(String prefix, Publisher publisher, TraversalParams params, FileStateTracker stateMgr) {
    return publishEntries(listDirectory(prefix, params), publisher, params, stateMgr, () -> false);
  }

  /**
   * The traversal shared by the cloud clients. Lists the path with {@link #listFlat} or, one directory at a time, with
   * {@link #listDirectory}, on <code>traversalThreads</code> threads, and stops at <code>maxDocuments</code>. Reaching
   * <code>maxDocuments</code> is not a failure. Otherwise, the first failure stops the traversal and is thrown.
   */
  protected final void traverseByListing(Publisher publisher, TraversalParams params, FileStateTracker stateMgr) throws ConnectorException {
    validateTraversal(params, stateMgr != null, false);

    if (traversalThreads > 1 && stateMgr != null && !(stateMgr instanceof ThreadBoundFileStateTracker)) {
      throw new IllegalArgumentException(TRAVERSAL_THREADS + " above 1 requires a " + ThreadBoundFileStateTracker.class.getSimpleName()
          + ", but was given a " + stateMgr.getClass().getName() + ".");
    }

    DocumentLimitPublisher limited = maxDocuments == null ? null : new DocumentLimitPublisher(publisher, maxDocuments);
    Publisher target = limited == null ? publisher : limited;
    BooleanSupplier limitReached = limited == null ? () -> false : limited::limitReached;
    String start = getStartingDirectory(params);

    try {
      if (listingModeFor(params) == ListingMode.FLAT) {
        publishEntries(listFlat(start, params), target, params, stateMgr, limitReached);
        return;
      }

      // Without a trailing '/' the start is a raw prefix, not a directory: "logs" also covers "logs-old/". A pathsToSkip
      // entry for "logs/" is applied when the listing returns that as a child, and must not end the whole traversal.
      if ((start.isEmpty() || start.endsWith("/")) && verdictFor(params.getURI(), target, params) == DirectoryVerdict.SKIP) {
        log.info("Not traversing '{}': the directory is skipped.", params.getURI());
        return;
      }

      if (traversalThreads == 1) {
        descendSequentially(start, target, params, stateMgr, limitReached);
      } else {
        new ParallelDescent(start, target, params, stateMgr, limitReached).run();
      }
    } catch (DocumentLimitReachedException e) {
      log.info("Stopped traversing '{}': reached {} of {}.", params.getURI(), MAX_DOCUMENTS, maxDocuments);
    }
  }

  private ListingMode listingModeFor(TraversalParams params) {
    if (configuredListingMode != null) {
      return configuredListingMode;
    }

    return traversalThreads == 1 && params.getPathsToSkip().isEmpty() ? defaultListingMode() : ListingMode.HIERARCHICAL;
  }

  private DirectoryVerdict verdictFor(URI directory, Publisher publisher, TraversalParams params) throws ConnectorException {
    return isSkippedDirectory(directory, params) ? DirectoryVerdict.SKIP : onDirectory(directory, publisher, params);
  }

  // Publishes the files in the listing and returns its directories. A file is reached only if the limit wasn't.
  private List<String> publishEntries(Stream<ListingEntry> listing, Publisher publisher, TraversalParams params, FileStateTracker stateMgr,
      BooleanSupplier stop) {
    List<String> directories = new ArrayList<>();

    try (Stream<ListingEntry> entries = listing) {
      entries.takeWhile(entry -> !stop.getAsBoolean()).forEachOrdered(entry -> {
        if (entry.isDirectory()) {
          directories.add(entry.name);
        } else {
          FileReference fileRef = buildReferenceOrSkip(entry.name, entry.reference);
          if (fileRef != null) {
            processAndPublishFileIfValid(publisher, fileRef, params, stateMgr);
          }
        }
      });
    }

    return directories;
  }

  // Returns the children to descend into. The visited set guards against a provider returning a prefix as its own descendant.
  private List<String> childrenToDescend(List<String> children, Set<String> visited, Publisher publisher, TraversalParams params)
      throws ConnectorException {
    List<String> toDescend = new ArrayList<>();

    for (String child : children) {
      if (!visited.add(child)) {
        log.warn("Directory '{}' of '{}' was already visited in this traversal, so it isn't visited again.", child, params.getURI());
      } else if (verdictFor(directoryURI(child, params), publisher, params) == DirectoryVerdict.DESCEND) {
        toDescend.add(child);
      }
    }

    return toDescend;
  }

  private void descendSequentially(String start, Publisher publisher, TraversalParams params, FileStateTracker stateMgr, BooleanSupplier limitReached)
      throws ConnectorException {
    Set<String> visited = ConcurrentHashMap.newKeySet();
    visited.add(start);

    Deque<String> stack = new ArrayDeque<>();
    stack.push(start);

    while (!stack.isEmpty() && !limitReached.getAsBoolean()) {
      String prefix = stack.pop();
      List<String> children = publishEntries(listDirectory(prefix, params), publisher, params, stateMgr, limitReached);
      List<String> toDescend = childrenToDescend(children, visited, publisher, params);

      // so that the first child listed is the next one visited
      for (int i = toDescend.size() - 1; i >= 0; i--) {
        stack.push(toDescend.get(i));
      }
    }
  }

  /**
   * Runs the per-directory step on a pool of threads. A pool thread holds its state connection and its own FileHandlers
   * for as long as it runs, so each one loops over a shared queue rather than running one task per directory.
   */
  private final class ParallelDescent {

    private final Publisher publisher;
    private final TraversalParams params;
    private final FileStateTracker stateMgr;
    private final BooleanSupplier limitReached;

    private final Set<String> visited = ConcurrentHashMap.newKeySet();
    private final LinkedBlockingDeque<String> queue = new LinkedBlockingDeque<>();
    // directories queued or being processed. A directory's children are counted before it is uncounted, so this is only
    // zero when the descent is complete.
    private final AtomicInteger pending = new AtomicInteger();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final ExecutorService executor;

    private volatile boolean finishing = false;
    private volatile boolean aborted = false;

    private ParallelDescent(String start, Publisher publisher, TraversalParams params, FileStateTracker stateMgr, BooleanSupplier limitReached) {
      this.publisher = publisher;
      this.params = params;
      this.stateMgr = stateMgr;
      this.limitReached = limitReached;
      this.executor = Executors.newFixedThreadPool(traversalThreads, new BasicThreadFactory.Builder()
          .namingPattern(ThreadNameUtils.createName("DirectoryTraversal") + "-%d")
          .build());

      visited.add(start);
      pending.incrementAndGet();
      queue.add(start);
    }

    private void run() throws ConnectorException {
      activeDescents.add(this);

      // a stop() from before the line above didn't see this descent
      if (!initialized) {
        abort();
      }

      try {
        List<Future<?>> workers = new ArrayList<>();
        try {
          for (int i = 0; i < traversalThreads; i++) {
            workers.add(executor.submit(this::work));
          }
        } catch (RejectedExecutionException e) {
          // stop() can arrive while the threads are still being started, once the first of them is listing
          if (!aborted) {
            throw e;
          }
        }
        executor.shutdown();

        for (Future<?> worker : workers) {
          worker.get();
        }
      } catch (InterruptedException e) {
        abort();
        awaitWorkers();
        Thread.currentThread().interrupt();
        throw new ConnectorException("Interrupted while traversing " + params.getURI() + ".", e);
      } catch (ExecutionException e) {
        // work() records everything it catches, so this isn't expected
        failure.compareAndSet(null, e.getCause());
      } finally {
        executor.shutdownNow();
        activeDescents.remove(this);
      }

      Throwable thrown = failure.get();
      if (thrown instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      if (thrown instanceof Error error) {
        throw error;
      }
      if (thrown instanceof ConnectorException connectorException) {
        throw connectorException;
      }
      if (thrown != null) {
        throw new ConnectorException("Error occurred while traversing " + params.getURI() + ".", thrown);
      }
      // unfinished directories must fail the run: their files would otherwise look deleted
      if (aborted) {
        throw new ConnectorException("The traversal of " + params.getURI() + " was stopped before it finished.");
      }
    }

    private void work() {
      workerDescent.set(this);
      boolean stateOpen = false;

      try {
        if (stateMgr != null) {
          ((ThreadBoundFileStateTracker) stateMgr).openForThread();
          stateOpen = true;
        }

        // FileHandlers aren't thread-safe, so each thread gets its own
        TraversalParams threadParams = params.withOwnFileHandlers();

        while (!finishing && !limitReached.getAsBoolean() && pending.get() > 0) {
          String prefix = queue.pollFirst(IDLE_POLL_MILLIS, TimeUnit.MILLISECONDS);
          if (prefix != null) {
            processDirectory(prefix, threadParams);
          }
        }
      } catch (DocumentLimitReachedException e) {
        finishing = true;
      } catch (Throwable t) {
        // when aborted, this is a consequence of the interrupt, and run() reports the abort
        if (!aborted) {
          failure.compareAndSet(null, t);
        }
        finishing = true;
      } finally {
        if (stateOpen) {
          ((ThreadBoundFileStateTracker) stateMgr).closeForThread();
        }
        workerDescent.remove();
      }
    }

    private void processDirectory(String prefix, TraversalParams threadParams) throws ConnectorException {
      try {
        List<String> children = publishEntries(listDirectory(prefix, threadParams), publisher, threadParams, stateMgr,
            () -> finishing || limitReached.getAsBoolean());

        for (String child : childrenToDescend(children, visited, publisher, threadParams)) {
          pending.incrementAndGet();
          // at the front, so the queue stays small by finishing a subtree before starting another
          queue.addFirst(child);
        }
      } finally {
        pending.decrementAndGet();
      }
    }

    // Ends the descent now, rather than after each thread finishes its current directory.
    private void abort() {
      aborted = true;
      finishing = true;
      executor.shutdownNow();
    }

    private void awaitWorkers() {
      boolean interrupted = Thread.interrupted();
      try {
        if (!executor.awaitTermination(TRAVERSAL_THREAD_STOP_SECONDS, TimeUnit.SECONDS)) {
          log.error("Traversal threads for '{}' did not stop within {} seconds of being interrupted.", params.getURI(), TRAVERSAL_THREAD_STOP_SECONDS);
        }
      } catch (InterruptedException e) {
        interrupted = true;
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void stopActiveDescents() {
    for (ParallelDescent descent : activeDescents) {
      descent.abort();
    }
    for (ParallelDescent descent : activeDescents) {
      descent.awaitWorkers();
    }
  }

  /**
   * Thrown by a {@link DocumentLimitPublisher} when a document would exceed the limit. Ends the traversal without failing
   * it, and leaves the file being processed alone: it isn't moved, because some of its documents weren't published.
   */
  private static class DocumentLimitReachedException extends RuntimeException {
    DocumentLimitReachedException() {
      super("maxDocuments reached.");
    }
  }

  /**
   * Publishes at most the given number of documents, counted across all of a traversal's threads. Wraps the Publisher
   * given to a traversal because FileHandlers publish to it directly.
   */
  private static final class DocumentLimitPublisher implements Publisher {

    private final Publisher delegate;
    private final long limit;
    private final AtomicLong published = new AtomicLong();

    private DocumentLimitPublisher(Publisher delegate, long limit) {
      this.delegate = delegate;
      this.limit = limit;
    }

    private boolean limitReached() {
      return published.get() >= limit;
    }

    @Override
    public void publish(Document document) throws PublisherException {
      if (published.incrementAndGet() > limit) {
        throw new DocumentLimitReachedException();
      }
      delegate.publish(document);
    }

    @Override
    public void flush() throws PublisherException {
      delegate.flush();
    }

    @Override
    public long numPublished() {
      return delegate.numPublished();
    }

    @Override
    public long numReceived() {
      return delegate.numReceived();
    }

    @Override
    public long numPending() {
      return delegate.numPending();
    }

    @Override
    public Integer getMaxPendingDocs() {
      return delegate.getMaxPendingDocs();
    }

    @Override
    public boolean hasPending() {
      return delegate.hasPending();
    }

    @Override
    public long numCreated() {
      return delegate.numCreated();
    }

    @Override
    public long numSucceeded() {
      return delegate.numSucceeded();
    }

    @Override
    public long numFailed() {
      return delegate.numFailed();
    }

    @Override
    public long numDropped() {
      return delegate.numDropped();
    }

    @Override
    public void pause() {
      delegate.pause();
    }

    @Override
    public void resume() {
      delegate.resume();
    }

    @Override
    public boolean isPaused() {
      return delegate.isPaused();
    }

    @Override
    public boolean isPublishingStarted() {
      return delegate.isPublishingStarted();
    }

    @Override
    public boolean isPublishingEnded() {
      return delegate.isPublishingEnded();
    }

    @Override
    public void preClose() {
      delegate.preClose();
    }
  }

  /**
   * {@inheritDoc}
   * <p> For object stores, where a path is a raw key prefix: the parent contains the child when the child's key prefix
   * starts with the parent's. So <code>s3://bucket/data</code> contains both <code>s3://bucket/data/2024</code> and
   * <code>s3://bucket/data2</code>, while <code>s3://bucket/data/</code> contains only the first. A URI with an empty
   * path is the root of its bucket, and contains everything in it.
   */
  @Override
  public boolean containsPath(URI parent, URI child) {
    URI normalizedParent = parent.normalize();
    URI normalizedChild = child.normalize();

    if (!Objects.equals(normalizedParent.getScheme(), normalizedChild.getScheme())
        || !Objects.equals(normalizedParent.getAuthority(), normalizedChild.getAuthority())) {
      return false;
    }

    return keyPrefix(normalizedChild).startsWith(keyPrefix(normalizedParent));
  }

  /**
   * Returns the object-store key a URI names: its path without the leading '/'. This is the key prefix a traversal path
   * lists, or the folder key a move uses (see {@link #destinationKey}). Empty for the root of the bucket or container,
   * with or without a trailing '/'.
   */
  static String keyPrefix(URI uri) {
    String path = uri.getPath();

    if (path == null || path.isEmpty()) {
      return "";
    }

    return path.startsWith("/") ? path.substring(1) : path;
  }

  /**
   * Implements {@link #moveTargetInsidePath(URI, URI)} for object stores that move files by {@link #destinationKey}.
   * Different buckets never overlap.
   */
  static boolean movedKeyInsideBucketPath(URI path, URI folder) {
    URI normalizedPath = path.normalize();
    URI normalizedFolder = folder.normalize();

    return Objects.equals(normalizedPath.getScheme(), normalizedFolder.getScheme())
        && Objects.equals(normalizedPath.getAuthority(), normalizedFolder.getAuthority())
        && movedKeyInsidePrefix(keyPrefix(normalizedPath), keyPrefix(normalizedFolder));
  }

  /**
   * Returns whether objects listed under the given key prefix would still be under it after moving to the given folder.
   * Every listed key starts with the prefix, so the destination of the prefix itself decides for all of them. This covers
   * a folder under the prefix, the prefix without its trailing '/' (the file lands at <code>incoming/incoming/a.csv</code>)
   * and the root of the bucket (the file lands on its own key).
   */
  static boolean movedKeyInsidePrefix(String prefix, String folderKey) {
    return destinationKey(folderKey, prefix).startsWith(prefix);
  }

  /**
   * Returns whether the provided URI, which represents a directory in some storage provider, is a directory
   * that should be skipped and not traversed at all, based on the <code>pathsToSkip</code> in <code>params</code>.
   * @param directoryURI A URI representing a directory (or the notion of a directory) in a storage provider.
   * @param params Parameters for a traversal.
   * @return Whether the directory at the provided URI ought to be skipped.
   */
  protected boolean isSkippedDirectory(URI directoryURI, TraversalParams params) {
    String normalized = ensureTrailingSlash(directoryURI.toString());
    return params.getPathsToSkip().stream()
        .anyMatch(skipDir -> ensureTrailingSlash(skipDir.toString()).equals(normalized));
  }

  private static String ensureTrailingSlash(String s) {
    return s.endsWith("/") ? s : s + "/";
  }

  @Override
  public final InputStream getFileContentStream(URI uri) throws IOException {
    if (!isInitialized()) {
      throw new IllegalStateException("This StorageClient has not been started. Call start() first.");
    }

    return getFileContentStreamFromStorage(uri);
  }

  // Performs a StorageClient-specific approach to get the contents of the file at the given URI (in an InputStream),
  protected abstract InputStream getFileContentStreamFromStorage(URI uri) throws IOException;

  /**
   * Builds the Document published for a file that no FileHandler, archive or decompression step applies to. Override this to
   * change those documents, such as to add fields. By default, {@link FileReference#asDoc(TraversalParams)}. Called from
   * several threads when <code>traversalThreads</code> is above 1.
   *
   * @param fileReference The file.
   * @param params Parameters for the traversal.
   * @return The Document for the file.
   * @throws IOException If an error occurs reading the file's content.
   */
  protected Document buildDocument(FileReference fileReference, TraversalParams params) throws IOException {
    return fileReference.asDoc(params);
  }

  /**
   * Applies a traversal's filters to one file and builds its Document, without publishing it, updating state, or moving the
   * file. For use outside a traversal, for example by a stage that emits the Document as a child. The client must be
   * started if the file's content is needed. Does not apply FileHandlers or unpack archives or compressed files.
   *
   * @param fileReference The file.
   * @param params Parameters holding the filters and file options to apply.
   * @param lastPublished When the file was last published, for an incremental <code>params</code>. May be null.
   * @return The file's Document, or null if the file is not valid or the filters exclude it.
   * @throws IOException If an error occurs reading the file's content.
   */
  public final Document buildDocumentIfIncluded(FileReference fileReference, TraversalParams params, Instant lastPublished) throws IOException {
    if (!fileReference.isValidFile()
        || !params.includeFile(fileReference.getFilterName(), fileReference.getLastModified(), lastPublished)) {
      return null;
    }

    return buildDocument(fileReference, params);
  }

  /**
   * If the file is valid, it will be processed and published. Also, perform any preprocessing, error handling, and post-processing.
   */
  protected void processAndPublishFileIfValid(Publisher publisher, FileReference fileReference, TraversalParams params, FileStateTracker stateMgr) {
    URI fullPath = fileReference.getFullPath();
    String fileExtension = fileReference.getFileExtension();

    try {
      // We always mark files as encountered, even if they don't comply with filter options
      if (stateMgr != null) {
        stateMgr.markFileEncountered(fullPath.toString());
      }

      Instant lastPublished = stateMgr == null ? null : stateMgr.getLastPublished(fullPath.toString());

      // Skip files that aren't valid (e.g. directories)
      if (!fileReference.isValidFile()) {
        return;
      }

      // Skip files that don't pass the filter options
      if (!params.includeFile(fileReference.getFilterName(), fileReference.getLastModified(), lastPublished)) {
        // If this is a previously-published archive or compressed file, its entries won't be traversed
        // this run. Mark all entries as encountered so they are not incorrectly marked as tombstones
        if (stateMgr != null && lastPublished != null) {
          if ((params.getHandleArchivedFiles() && isSupportedArchiveFileType(fullPath.toString()))
              || (params.getHandleCompressedFiles() && isSupportedCompressedFileType(fullPath.toString()))) {
            stateMgr.markAllEntriesEncountered(fullPath + ARCHIVE_FILE_SEPARATOR);
          }
        }
        return;
      }

      // Skip the file if pre-processing fails.
      // (preprocessing is currently a NO-OP unless a subclass overrides it)
      if (!beforeProcessingFile(fullPath)) {
        return;
      }

      // handle compressed files if needed to the end
      if (params.getHandleCompressedFiles() && isSupportedCompressedFileType(fullPath.toString())) {
        // unzip the file, compressorStream will be closed when try block is exited
        try (BufferedInputStream bis = new BufferedInputStream(fileReference.getContentStream(params));
            CompressorInputStream compressorStream = new CompressorStreamFactory().createCompressorInputStream(bis)) {
          // we can remove the last extension from path knowing before we confirmed that it has a compressed extension
          String decompressedPath = FilenameUtils.removeExtension(fullPath.toString());
          String resolvedExtension = FilenameUtils.getExtension(decompressedPath);

          // if detected to be an archive type after decompression
          if (params.getHandleArchivedFiles() && isSupportedArchiveFileType(decompressedPath)) {
            handleArchiveFiles(publisher, compressorStream, fullPath, params, stateMgr);
          } else {
            String compressedFileFullPath = fullPath + ARCHIVE_FILE_SEPARATOR + FilenameUtils.getName(decompressedPath);

            if (stateMgr != null) {
              stateMgr.markFileEncountered(compressedFileFullPath);
            }

            // if file is a supported file type that should be handled by a file handler
            if (params.supportsFileExtension(resolvedExtension)) {
              handleStreamExtensionFiles(publisher, resolvedExtension, params, compressorStream, compressedFileFullPath);
            } else {
              Document doc = fileReference.decompressedFileAsDoc(compressorStream, compressedFileFullPath, params);
              if (docLogger.isInfoEnabled()) {
                try (MDCCloseable mdc = MDC.putCloseable(Document.ID_FIELD, doc.getId())) {
                  docLogger.info("StorageClient to publish Document {}.", doc.getId());
                }
              }
              publisher.publish(doc);
            }

            if (stateMgr != null) {
              stateMgr.successfullyPublishedFile(compressedFileFullPath);
            }
          }
        }

        // Regardless of what actually gets published in the above code, the compressed file itself was considered / processed by Lucille.
        if (stateMgr != null) {
          stateMgr.successfullyPublishedFile(fullPath.toString());
        }

        afterProcessingFile(fullPath, params);
        return;
      }

      // handle archived files if needed to the end
      if (params.getHandleArchivedFiles() && isSupportedArchiveFileType(fullPath.toString())) {
        try (InputStream is = fileReference.getContentStream(params)) {
          handleArchiveFiles(publisher, is, fullPath, params, stateMgr);
        }

        // Regardless of what is extracted & published in handleArchiveFiles, the archive file itself was considered / processed by Lucille.
        if (stateMgr != null) {
          stateMgr.successfullyPublishedFile(fullPath.toString());
        }

        afterProcessingFile(fullPath, params);
        return;
      }

      // handle file types using fileHandler if needed to the end
      if (params.supportsFileExtension(fileExtension)) {
        // Get a stream for the file content, so we don't have to load it all at once.
        InputStream contentStream = fileReference.getContentStream(params);
        // get the right FileHandler and publish based on content
        publishUsingFileHandler(publisher, fileExtension, params, contentStream, fullPath);

        if (stateMgr != null) {
          stateMgr.successfullyPublishedFile(fullPath.toString());
        }

        afterProcessingFile(fullPath, params);
        return;
      }

      // handle normal files
      Document doc = buildDocument(fileReference, params);
      if (docLogger.isInfoEnabled()) {
        try (MDCCloseable mdc = MDC.putCloseable(Document.ID_FIELD, doc.getId())) {
          docLogger.info("StorageClient to publish Document {}.", doc.getId());
        }
      }
      publisher.publish(doc);

      if (stateMgr != null) {
        stateMgr.successfullyPublishedFile(fullPath.toString());
      }

      afterProcessingFile(fullPath, params);
    } catch (MoveNotSupportedException e) {
      // every file would fail the same way, so stop the traversal. Any other UnsupportedOperationException is
      // handled like other errors processing a single file, below.
      throw e;
    } catch (StatePathTooLongException e) {
      throw e;
    } catch (Exception e) {
      // FileHandlers wrap what publish() throws, so look through the causes
      int limitIndex = ExceptionUtils.indexOfType(e, DocumentLimitReachedException.class);
      if (limitIndex != -1) {
        throw (DocumentLimitReachedException) ExceptionUtils.getThrowables(e)[limitIndex];
      }

      // the interrupt that ends an aborted traversal can surface here as a read failure, which says nothing about the file
      ParallelDescent descent = workerDescent.get();
      if (descent != null && descent.aborted) {
        throw new IllegalStateException("The traversal was stopped while processing '" + fullPath + "'.", e);
      }

      try {
        errorProcessingFile(fullPath, params);
      } catch (IOException ex) {
        log.error("Error occurred while performing error operations on file '{}'", fullPath, ex);
      }
      log.error("Unable to publish document '{}', SKIPPING", fullPath, e);
    }
  }

  /**
   * Returns the key an object moves to under the given folder key, for object stores: the folder, a '/', then the
   * object's full key. An empty folder key is the root of the bucket or container. A folder key ending in '/' gets no
   * second one.
   *
   * @param folderKey The destination folder's key, without a leading '/'. May be empty.
   * @param sourceKey The full key of the object being moved.
   * @return The destination key.
   */
  static String destinationKey(String folderKey, String sourceKey) {
    if (folderKey.isEmpty()) {
      return sourceKey;
    }

    return folderKey.endsWith("/") ? folderKey + sourceKey : folderKey + "/" + sourceKey;
  }

  /**
   * Builds the FileReference for one listed object, or returns null, logging the error, if it can't be built (e.g. the
   * object's name can't form a URI). The caller skips that object, so one bad object doesn't fail the whole traversal.
   * It is never published or marked encountered in state, because it has no full path to record it under. So if an
   * object with that name was published by an earlier run, its row is treated like any file not encountered: once it
   * has gone unencountered for <code>state.runsBeforeExpiration</code> runs, it is tombstoned (with
   * <code>sendTombstones</code>) and deleted from state.
   */
  static FileReference buildReferenceOrSkip(String objectName, Supplier<FileReference> builder) {
    try {
      return builder.get();
    } catch (Exception e) {
      log.error("Unable to build a reference to '{}', SKIPPING", objectName, e);
      return null;
    }
  }

  /**
   * This handles all entries of an archive file. Performs validity of an entry using includes/excludes as configured
   * in the config file before processing. Entries do not have a created date, and depending on the implementation
   * of the ArchiveInputStream, may return size as -1. Does not support recursive archives.
   *
   * @param publisher publisher used to publish documents
   * @param inputStream input stream of the archive file. Used to create an ArchiveInputStream
   * @param fullPathURI full path of the archive file including the extension. Can be a cloud path or local path.
   *                    Cloud path would include schema and bucket/container name
   *                    e.g gs://bucket-name/folder/file.zip or s3://bucket-name/file.tar
   * @param params Parameters to customize the traversal / handling of files.
   *
   * @throws ArchiveException If an error occurs getting the archive file's entries.
   * @throws IOException If an error regarding file I/O occurs
   * @throws ConnectorException If an error occurs handling subsequent archive entries.
   */
  private void handleArchiveFiles(Publisher publisher, InputStream inputStream, URI fullPathURI, TraversalParams params, FileStateTracker stateMgr) throws ArchiveException, IOException, ConnectorException {
    try (BufferedInputStream bis = new BufferedInputStream(inputStream);
        ArchiveInputStream<? extends ArchiveEntry> in = new ArchiveStreamFactory().createArchiveInputStream(bis)) {
      ArchiveEntry entry = null;

      while ((entry = in.getNextEntry()) != null) {
        String entryFullPathStr = getArchiveEntryFullPath(fullPathURI, entry.getName());

        if (stateMgr != null) {
          stateMgr.markFileEncountered(entryFullPathStr);
        }

        if (!in.canReadEntryData(entry)) {
          log.info("Cannot read entry data for entry: '{}' in '{}'. Skipping...", entry.getName(), fullPathURI);
          continue;
        }

        Instant archiveLastPublished = stateMgr == null ? null : stateMgr.getLastPublished(entryFullPathStr);
        // some archive formats and entries have no modification date
        Instant entryLastModified = entry.getLastModifiedDate() == null ? null : entry.getLastModifiedDate().toInstant();
        // checking validity only for the entries
        if (!entry.isDirectory() && params.includeFile(entry.getName(), entryLastModified, archiveLastPublished)) {
          String entryExtension = FilenameUtils.getExtension(entry.getName());
          if (params.supportsFileExtension(entryExtension)) {
            handleStreamExtensionFiles(publisher, entryExtension, params, in, entryFullPathStr);
          } else {
            // handle entry to be published as a normal document
            // note that if there exists a file within the same parent directory with the same name as the entries, it will have the same id
            Document doc = Document.create(StorageClient.createDocId(DigestUtils.md5Hex(entryFullPathStr), params));
            doc.setField(FILE_PATH, entryFullPathStr);
            if (entryLastModified != null) {
              doc.setField(MODIFIED, entryLastModified);
            }
            // entry does not have creation date
            // note that some ArchiveEntry implementations may not have a size so will return -1
            doc.setField(SIZE, entry.getSize());
            if (params.shouldGetFileContent()) {
              Long maxSizeBytes = params.getMaxSizeBytes();
              try {
                if (maxSizeBytes != null && entry.getSize() > maxSizeBytes) {
                  throw new ContentTooLargeException(
                      String.format("Entry of %d bytes exceeds maxSizeBytes of %d.", entry.getSize(), maxSizeBytes));
                }
                doc.setField(CONTENT, FileUtils.readAllBytes(in, maxSizeBytes));
              } catch (ContentTooLargeException e) {
                // skip just this entry rather than failing the whole archive
                log.error("Skipping entry '{}' in '{}'. {}", entry.getName(), fullPathURI, e.getMessage());
                continue;
              }
            }
            try {
              if (docLogger.isInfoEnabled()) {
                try (MDCCloseable mdc = MDC.putCloseable(Document.ID_FIELD, doc.getId())) {
                  docLogger.info("StorageClient to publish Document {}.", doc.getId());
                }
              }
              publisher.publish(doc);
            } catch (Exception e) {
              throw new ConnectorException("Error occurred while publishing archive entry: " + entry.getName() + " in " + fullPathURI, e);
            }
          }

          if (stateMgr != null) {
            stateMgr.successfullyPublishedFile(entryFullPathStr);
          }
        }
      }
    }
  }

  /**
   * This processes the file after it has been decompressed or extracted from an archive. This method is only called when
   * the resolved file has a supported extension and user has configured the appropriate file handler.
   *
   * @param publisher publisher used to publish documents
   * @param in An InputStream for an archive / compressed file. This InputStream will <b>NOT</b> be closed by this method.
   * @param fileExtension The extension of the file you're processing.
   * @param fullPathStr can be entry full path or decompressed full path. Can be a cloud path or local path.
   *                    e.g. gs://bucket-name/folder/file.zip!entry.json OR path/to/example.csv
   * @throws ConnectorException If an error occurs processing / handling the file.
   */
  private void handleStreamExtensionFiles(Publisher publisher, String fileExtension, TraversalParams params, InputStream in, String fullPathStr)
      throws ConnectorException {
    try {
      // Shield the archive / compressor stream from close() calls, but delegate everything else (notably bulk reads).
      InputStream wrappedNonClosingStream = CloseShieldInputStream.wrap(in);

      FileHandler handler = params.handlerForExtension(fileExtension);
      handler.processFileAndPublish(publisher, wrappedNonClosingStream, fullPathStr);
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while handling / processing file: " + fullPathStr, e);
    }
  }

  /**
   * Publishes a file using a file handler and an InputStream to its contents.
   * @throws Exception If an error occurs or the file extension doesn't have a file handler to use.
   */
  private void publishUsingFileHandler(Publisher publisher, String fileExtension, TraversalParams params, InputStream inputStream, URI pathURI) throws Exception {
    FileHandler handler = params.handlerForExtension(fileExtension);
    if (handler == null) {
      throw new ConnectorException("No file handler found for file extension: " + fileExtension);
    }

    try {
      handler.processFileAndPublish(publisher, inputStream, pathURI.toString());
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while processing or publishing file: " + pathURI, e);
    }
  }

  /**
   * helper method to get the full path of an entry in an archived file. Only used for archive files.
   * @param fullPathURI A URI representing the full path to the archive file.
   * @param entryName The name of the file extracted from the archive file.
   * @return A String representing the full path to this archive entry, including the full path to the
   * archive file, the archive file separator, and then the entry's name.
   */
  private String getArchiveEntryFullPath(URI fullPathURI, String entryName) {
    return fullPathURI + ARCHIVE_FILE_SEPARATOR + entryName;
  }

  /**
   * Performs preprocessing operations. Called before processing each file in a traversal.
   * @return Whether preprocessing was successful, and the file should be processed further.
   */
  private boolean beforeProcessingFile(URI filePath) throws Exception {
    // Base implementation, process all files. 
    return true;
  }

  /**
   * method for performing operations after processing files. Additional operations can be added
   * in the implementation of this method. Will be called after processing each file in traversal.
   *
   * @throws IOException If an error occurs performing post-processing operations on the file. (Namely, a <code>moveToAfterProcessing</code>
   * directory is specified and an error occurs.)
   */
  private void afterProcessingFile(URI filePath, TraversalParams params) throws IOException {
    if (params.getMoveToAfterProcessing() != null) {
      // move to processed folder
      moveFileOrFail(filePath, params.getMoveToAfterProcessing(), MOVE_TO_AFTER_PROCESSING);
    }
  }

  /**
   * method for performing operations when encountering an error while processing files. Additional operations can be added
   * in the implementation of this method. Will be called in the catch block for each file in traversal
   * in the tryProcessAndPublishFile method.
   */
  private void errorProcessingFile(URI filePath, TraversalParams params) throws IOException {
    if (params.getMoveToErrorFolder() != null) {
      // move to error folder
      moveFileOrFail(filePath, params.getMoveToErrorFolder(), MOVE_TO_ERROR_FOLDER);
    }
  }

  // Moves the file, throwing a MoveNotSupportedException, which stops the traversal, if this client doesn't support
  // moving files.
  private void moveFileOrFail(URI filePath, URI folder, String option) throws IOException {
    try {
      moveFile(filePath, folder);
    } catch (UnsupportedOperationException e) {
      throw new MoveNotSupportedException(getClass().getName() + " does not support moving files, which fileOptions."
          + option + " requires.", e);
    }
  }

  /**
   * Thrown when fileOptions.moveToAfterProcessing or fileOptions.moveToErrorFolder is set but this client's
   * {@link #moveFile(URI, URI)} throws an UnsupportedOperationException. Stops the traversal.
   */
  private static class MoveNotSupportedException extends UnsupportedOperationException {
    MoveNotSupportedException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * helper method to check if the file is a supported compressed file type.
   * note that the commented following are supported by apache-commons compress, but have yet to been tested, so commented out for now
   */
  private boolean isSupportedCompressedFileType(String pathStr) {
    return pathStr.toLowerCase(Locale.ROOT).endsWith(".gz");
    // string.endsWith(".bz2") ||
    // string.endsWith(".xz") ||
    // string.endsWith(".lzma") ||
    // string.endsWith(".br") ||
    // string.endsWith(".pack") ||
    // string.endsWith(".zst") ||
    // string.endsWith(".Z");
  }

  /**
   * helper method to check if the file is a supported archived file type.
   * note that the commented following are supported by apache-commons compress, but have yet to been tested, so commented out for now
   */
  private boolean isSupportedArchiveFileType(String pathStr) {
    String lowerCasePath = pathStr.toLowerCase(Locale.ROOT);
    return lowerCasePath.endsWith(".tar") ||
        lowerCasePath.endsWith(".zip");
    // string.endsWith(".7z") ||
    // string.endsWith(".ar") ||
    // string.endsWith(".arj") ||
    // string.endsWith(".cpio") ||
    // string.endsWith(".dump") ||
    // string.endsWith(".dmp");
  }

  /**
   * Creates a document ID using the given initial docID and the params.
   *
   * @param docId The document ID you want to use.
   * @param params Parameters for the traversal.
   * @return The document ID with the prefix added if needed / appropriate.
   */
  //should sync with abstract connector class?
  protected String createDocId(String docId, TraversalParams params) {
    return params.getDocIdPrefix() + docId;
  }

  /**
   * Should only be used for testing. Marks this storage client as initialized without calling
   * {@link #initializeStorageClient()}, for a test that supplies its own mock of the provider's SDK client.
   */
  protected void initializeForTesting() {
    this.initialized = true;
  }
}
