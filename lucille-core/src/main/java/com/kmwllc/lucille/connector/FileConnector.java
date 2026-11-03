package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kmwllc.lucille.util.ThreadNameUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.connector.storageclient.BaseFileReference;
import com.kmwllc.lucille.connector.storageclient.StorageClient;
import com.kmwllc.lucille.connector.storageclient.StorageClientFields;
import com.kmwllc.lucille.connector.storageclient.StorageClientOptions;
import com.kmwllc.lucille.connector.storageclient.TraversalParams;
import com.kmwllc.lucille.connector.storageclient.TraversalParams.PublishMode;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;

/**
 * Traverses local and cloud storage (S3, GCP, Azure) from one or more roots and publishes a Document for each file encountered.
 * Supports include/exclude regex filters, recency cutoffs, optional content fetching, archive/compressed file handling, file moves
 * after processing or on error, and optional JDBC-backed state to avoid republishing recently handled files. Only files matching
 * all filter criteria are processed. Durations use HOCON-style strings like "1h", "2d", "3s".
 *
 * For archive/compressed files, modification/publish cutoffs apply to both the container and its entries. When state is enabled,
 * traversal may be slower. Files that are moved/renamed are always republished regardless of lastPublishedCutoff. You can enable
 * state without specifying lastPublishedCutoff to keep publish times updated.
 *
 * State tracks file paths and last publish timestamps to support filterOptions.lastPublishedCutoff and to avoid duplicate publications
 * across runs. You can connect to your own JDBC database by providing driver, connectionString, jdbcUser, and tableName. If
 * connectionString is omitted, an embedded database is created at ./state/{CONNECTOR_NAME}. A run fails at startup if another run
 * in the same JVM is already using the same connectionString and tableName. With state enabled, traversal may be slower, and
 * files that are moved or renamed are always republished. The lastPublishedCutoff setting requires state and incremental publishMode;
 * configuring it without them is an error. You may enable state without lastPublishedCutoff and publish times will still be updated.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>paths (List&lt;String&gt;, Required) : Paths or URIs to traverse (local paths or cloud storage URIs). s3 URIs must be
 *   percent-encoded; unencoded spaces or special characters will not be recognized. For example, use s3://test/folder%20with%20spaces.</li>
 *   <li>maxThreads (Int, Optional) : Number of threads to traverse the paths on. 1 traverses them sequentially.
 *   A larger value traverses them concurrently on up to that many threads, with paths beyond the cap waiting for a free
 *   thread. -1 uses one thread per path. Concurrent traversal requires that the paths do not overlap. Must be -1 or at
 *   least 1. Defaults to 1.</li>
 *   <li>filterOptions.includes (List&lt;String&gt;, Optional) : Regex patterns to include files.</li>
 *   <li>filterOptions.excludes (List&lt;String&gt;, Optional) : Regex patterns to exclude files.</li>
 *   <li>filterOptions.pathsToSkip (List&lt;String&gt;, Optional) : URIs of paths to directories that should be skipped and not traversed.</li>
 *   <li>filterOptions.lastModifiedCutoff (String, Optional) : Duration string to include only files modified within this period (e.g., "1h").</li>
 *   <li>filterOptions.lastPublishedCutoff (String, Optional) : Duration string to include only files not published within this period.
 *   Requires state and publishMode 'incremental'; the connector fails to start if either is missing.</li>
 *   <li>filterOptions.publishMode (String, Optional) : Set as 'incremental' or 'full' to choose mode of publishing.</li>
 *   <li>fileOptions.getFileContent (Boolean, Optional) : Fetch file content during traversal. Defaults to true.</li>
 *   <li>fileOptions.maxSizeBytes (Long, Optional) : The maximum size, in bytes, of file content to fetch. Must be positive and at
 *   most Integer.MAX_VALUE - 9. A file larger than this is not published and is handled as a processing error (e.g. moved to
 *   moveToErrorFolder, if set). An oversized archive entry is skipped, and the rest of the archive is still processed. Does
 *   not apply to files processed by a file handler; see the handler's own limits. No limit by default.</li>
 *   <li>fileOptions.handleArchivedFiles (Boolean, Optional) : Process archive files. Defaults to false.</li>
 *   <li>fileOptions.handleCompressedFiles (Boolean, Optional) : Process compressed files. Defaults to false.</li>
 *   <li>fileOptions.moveToAfterProcessing (String, Optional) : URI to move files after successful processing (single input path only).</li>
 *   <li>fileOptions.moveToErrorFolder (String, Optional) : URI to move files if processing fails (single input path only).</li>
 *   <li>state.enabled (Boolean, Optional) : Set state database to be enabled or disabled (null). Defaults to true.</li>
 *   <li>state.driver (String, Optional) : JDBC driver class. Defaults to "org.h2.Driver".</li>
 *   <li>state.connectionString (String, Optional) : JDBC connection string. Defaults to "jdbc:h2:./state/{CONNECTOR_NAME}".</li>
 *   <li>state.jdbcUser (String, Optional) : Database username. Defaults to "".</li>
 *   <li>state.jdbcPassword (String, Optional) : Database password. Defaults to "".</li>
 *   <li>state.tableName (String, Optional) : Table name for state. Defaults to the connector name.</li>
 *   <li>state.performDeletions (Boolean, Optional) : Delete rows for files removed from storage. Defaults to true.</li>
 *   <li>state.runsBeforeExpiration (Int, Optional) : After a file is not encountered for this number of runs, it will be marked
 *   as expired. Must be at least 1. Defaults to 1.</li>
 *   <li>state.pathLength (Int, Optional) : Max length for stored file paths when Lucille creates the table. Defaults to 768. A longer path fails the run.</li>
 *   <li>gcp.pathToServiceKey (String, Required) : Path to the Google Cloud service key JSON.</li>
 *   <li>gcp.maxResultsPerPage (Int, Optional) : Maximum number of results returned per listing request (page size). Defaults to 100.</li>
 *   <li>s3.accessKeyId (String, Optional) : AWS access key ID (omit to use default credentials).</li>
 *   <li>s3.secretAccessKey (String, Optional) : AWS secret access key (omit to use default credentials).</li>
 *   <li>s3.region (String, Optional) : AWS region for S3.</li>
 *   <li>s3.anonymous (Boolean, Optional) : Send unsigned requests, for public buckets that need no
 *   credentials. Cannot be combined with accessKeyId/secretAccessKey. When set and no region is given,
 *   region defaults to us-east-1, S3's global endpoint. Defaults to false.</li>
 *   <li>s3.maxResultsPerPage (Int, Optional) : Maximum number of results returned per S3 LIST call (page size). Defaults to 1000, which is also
 *   the most S3 will return per call; larger values have no further effect. Without filterOptions.pathsToSkip, every key under the
 *   path is listed in one paginated listing, so files may be published in a different order than a directory-by-directory walk.</li>
 *   <li>azure.connectionString (String, Optional) : Azure connection string.</li>
 *   <li>azure.accountName (String, Optional) : Azure account name.</li>
 *   <li>azure.accountKey (String, Optional) : Azure account key.</li>
 *   <li>azure.maxResultsPerPage (Int, Optional) : Maximum number of results returned per listing request (page size). Defaults to 100.</li>
 *   <li>azure.allowedHosts (List&lt;String&gt;, Optional) : An https URI is only read with the azure client if its authority
 *   contains one of these values (a substring match). Applies to traversed paths and to files fetched by stages. Only
 *   selects the client: requests go to the endpoint in the azure credentials, such as connectionString. Replaces the default
 *   rather than adding to it. Defaults to ["blob.core.windows.net"].</li>
 *   <li>storageClients (Map&lt;String, Map&lt;String, Object&gt;&gt;, Optional) : Custom StorageClients, keyed by URI scheme
 *   (e.g. sftp, or s3). Each entry must contain "class", the fully qualified name of a StorageClient implementation with a
 *   public constructor that takes a Config. The client receives its entry, which may contain any other settings it needs.
 *   A client for s3, gs, or https replaces the built-in client for that scheme, so the scheme's built-in config (s3, gcp,
 *   or azure) can't also be given, and azure.allowedHosts does not apply to a custom https client. Schemes must be at
 *   least two characters long, and file can't be configured.</li>
 *   <li>fileHandlers (Map&lt;String, Map&lt;String, Object&gt;&gt;, Optional) : Per-type FileHandler configuration (e.g., csv, json, xml).
 *   Supply a class to override the default handler. Otherwise the built-in handler for csv/json/xml is used. Configure docIdPrefix inside
 *   each handler's config as needed.</li>
 * </ul>
 */
public class FileConnector extends AbstractConnector {

  // The document field names and config keys below are aliases. They are defined on StorageClientFields and
  // StorageClientOptions, which a StorageClient should import instead of FileConnector.
  public static final String FILE_PATH = StorageClientFields.FILE_PATH;
  public static final String MODIFIED = StorageClientFields.MODIFIED;
  public static final String CREATED = StorageClientFields.CREATED;
  public static final String SIZE = StorageClientFields.SIZE;
  public static final String CONTENT = StorageClientFields.CONTENT;
  public static final String EXPIRED = StorageClientFields.EXPIRED;
  public static final String ARCHIVE_FILE_SEPARATOR = StorageClientFields.ARCHIVE_FILE_SEPARATOR;

  // cloudOption Keys
  public static final String AZURE_CONNECTION_STRING = StorageClientOptions.AZURE_CONNECTION_STRING;
  public static final String AZURE_ACCOUNT_NAME = StorageClientOptions.AZURE_ACCOUNT_NAME;
  public static final String AZURE_ACCOUNT_KEY = StorageClientOptions.AZURE_ACCOUNT_KEY;
  public static final String AZURE_ALLOWED_HOSTS = StorageClientOptions.AZURE_ALLOWED_HOSTS;
  public static final List<String> DEFAULT_AZURE_ALLOWED_HOSTS = StorageClientOptions.DEFAULT_AZURE_ALLOWED_HOSTS;
  public static final String S3_REGION = StorageClientOptions.S3_REGION;
  public static final String S3_ACCESS_KEY_ID = StorageClientOptions.S3_ACCESS_KEY_ID;
  public static final String S3_SECRET_ACCESS_KEY = StorageClientOptions.S3_SECRET_ACCESS_KEY;
  public static final String S3_ANONYMOUS = StorageClientOptions.S3_ANONYMOUS;
  public static final String GOOGLE_SERVICE_KEY = StorageClientOptions.GOOGLE_SERVICE_KEY;
  public static final String MAX_RESULTS_PER_PAGE = StorageClientOptions.MAX_RESULTS_PER_PAGE;
  public static final String STORAGE_CLIENT_CLASS = StorageClientOptions.STORAGE_CLIENT_CLASS;
  public static final String STORAGE_CLIENTS = StorageClientOptions.STORAGE_CLIENTS;

  // fileOption Config Options
  public static final String GET_FILE_CONTENT = StorageClientOptions.GET_FILE_CONTENT;
  public static final String MAX_SIZE_BYTES = StorageClientOptions.MAX_SIZE_BYTES;
  public static final String HANDLE_ARCHIVED_FILES = StorageClientOptions.HANDLE_ARCHIVED_FILES;
  public static final String HANDLE_COMPRESSED_FILES = StorageClientOptions.HANDLE_COMPRESSED_FILES;
  public static final String MOVE_TO_AFTER_PROCESSING = StorageClientOptions.MOVE_TO_AFTER_PROCESSING;
  public static final String MOVE_TO_ERROR_FOLDER = StorageClientOptions.MOVE_TO_ERROR_FOLDER;

  // parent specs for cloud provider configs
  private static final Spec GCP_PARENT_SPEC = SpecBuilder.withoutDefaults()
      .requiredString("pathToServiceKey")
      .optionalNumber(MAX_RESULTS_PER_PAGE).build();
  private static final Spec S3_PARENT_SPEC = SpecBuilder.withoutDefaults()
      .optionalString("accessKeyId", "secretAccessKey", "region")
      .optionalBoolean("anonymous")
      .optionalNumber(MAX_RESULTS_PER_PAGE).build();
  private static final Spec AZURE_PARENT_SPEC = SpecBuilder.withoutDefaults()
      .optionalString("connectionString", "accountName", "accountKey")
      .optionalList(AZURE_ALLOWED_HOSTS, new TypeReference<List<String>>(){})
      .optionalNumber(MAX_RESULTS_PER_PAGE).build();

  /**
   * The storage configuration accepted by every component that reads files through StorageClients: the gcp, azure,
   * and s3 blocks, and storageClients (custom storage clients, keyed by URI scheme, whose settings are not validated
   * here). Add it to a component's Spec with {@link SpecBuilder#include(Spec)}.
   */
  public static final Spec STORAGE_SPEC = SpecBuilder.withoutDefaults()
      .optionalParent("gcp", GCP_PARENT_SPEC)
      .optionalParent("azure", AZURE_PARENT_SPEC)
      .optionalParent("s3", S3_PARENT_SPEC)
      .optionalParent(STORAGE_CLIENTS, new TypeReference<Map<String, Map<String, Object>>>(){}).build();

  private static final Logger log = LoggerFactory.getLogger(FileConnector.class);

  public static final Spec SPEC = SpecBuilder.connector()
      .requiredList("paths", new TypeReference<List<String>>(){})
      .optionalNumber("maxThreads")
      .optionalParent("filterOptions", SpecBuilder.withoutDefaults()
          .optionalList("includes", new TypeReference<List<String>>(){})
          .optionalList("excludes", new TypeReference<List<String>>(){})
          .optionalList("pathsToSkip", new TypeReference<List<String>>(){})
          // durations are strings.
          .optionalString("lastModifiedCutoff", "lastPublishedCutoff", "publishMode")
          .optionalBoolean("sendTombstones").build())
      .optionalParent("fileOptions", SpecBuilder.withoutDefaults()
          .optionalBoolean("getFileContent", "handleArchivedFiles", "handleCompressedFiles")
          .optionalNumber(MAX_SIZE_BYTES)
          .optionalString("moveToAfterProcessing", "moveToErrorFolder").build())
      .optionalParent("state", SpecBuilder.withoutDefaults()
          .optionalString("driver", "connectionString", "jdbcUser", "jdbcPassword", "tableName")
          .optionalBoolean("performDeletions", "enabled")
          .optionalNumber("pathLength", "runsBeforeExpiration").build())
      .include(STORAGE_SPEC)
      .optionalParent("fileHandlers", new TypeReference<Map<String, Map<String, Object>>>(){}).build();

  private final List<URI> storageURIs;
  private final Map<String, StorageClient> storageClientMap;

  private final FileConnectorStateManager stateManager;

  private final int maxThreads;
  private final boolean concurrent;

  public FileConnector(Config config) throws ConnectorException {
    super(config);

    this.maxThreads = ConfigUtils.getIntOrDefault(config, "maxThreads", 1);

    if (maxThreads < 1 && maxThreads != -1) {
      throw new IllegalArgumentException("maxThreads must be -1 or >= 1.");
    }

    this.concurrent = maxThreads != 1;

    List<String> paths = config.getStringList("paths");
    this.storageURIs = new ArrayList<>();

    for (String path : paths) {
      try {
        URI newStorageURI = TraversalParams.parsePathOrURI(path);
        storageURIs.add(newStorageURI);
        log.debug("FileConnector to use path {} with scheme {}", path, newStorageURI.getScheme());
      } catch (URISyntaxException e) {
        throw new ConnectorException("Invalid path to storage: " + path, e);
      }
    }

    this.stateManager =
        config.hasPath("state") && ConfigUtils.getOrDefault(config, "state.enabled", true)
            ? new FileConnectorStateManager(config.getConfig("state"), getName())
            : null;

    this.storageClientMap = StorageClient.createClients(config);

    // an https path on a host outside azure.allowedHosts would otherwise only fail once traversal starts
    for (URI uri : storageURIs) {
      if (StorageClient.clientFor(uri, storageClientMap, config) == null && storageClientMap.containsKey(clientKeyFor(uri))) {
        throw new IllegalArgumentException("No StorageClient is available for " + uri + ": its host doesn't contain any of azure."
            + AZURE_ALLOWED_HOSTS + ".");
      }
    }


    // incremental mode requires state tracking in order to function correctly
    if (config.hasPath("filterOptions.publishMode")) {
      PublishMode mode = PublishMode.fromString(config.getString("filterOptions.publishMode"));
      if (mode == PublishMode.INCREMENTAL && !config.hasPath("state")) {
        throw new IllegalArgumentException("filterOptions.publishMode of 'incremental' requires state configuration.");
      }
    }

    if (config.hasPath("filterOptions.sendTombstones") && config.getBoolean("filterOptions.sendTombstones") &&
        (!config.hasPath("filterOptions.publishMode") ||
            PublishMode.fromString(config.getString("filterOptions.publishMode")) == PublishMode.FULL)) {
      throw new IllegalArgumentException("publishMode must be set and be incremental to use the sendTombstones toggle.");
    }

    // Cannot specify multiple storage paths and a moveTo of some kind
    if (storageURIs.size() > 1 && (config.hasPath("fileOptions.moveToAfterProcessing") || config.hasPath("fileOptions.moveToErrorFolder"))) {
      throw new IllegalArgumentException("FileConnector does not support multiple paths and moveToAfterProcessing / moveToErrorFolder. Create individual FileConnectors.");
    }

    validateMoveTargetOutsidePath(MOVE_TO_AFTER_PROCESSING);
    validateMoveTargetOutsidePath(MOVE_TO_ERROR_FOLDER);

    // A collapsing Publisher is not safe for concurrent publish() calls - it would merge and drop documents silently
    if (concurrent && requiresCollapsingPublisher()) {
      throw new IllegalArgumentException("FileConnector does not support concurrent traversal and collapse. Set maxThreads to 1 or disable collapse.");
    }

    if (concurrent) {
      validateNonOverlappingPaths();
    }

    // lastPublishedCutoff is only applied in incremental mode with state; anywhere else it would be silently ignored
    if (config.hasPath("filterOptions.lastPublishedCutoff")) {
      if (!config.hasPath("state")) {
        throw new IllegalArgumentException("filterOptions.lastPublishedCutoff requires state configuration.");
      }
      if (!config.hasPath("filterOptions.publishMode")
          || PublishMode.fromString(config.getString("filterOptions.publishMode")) == PublishMode.FULL) {
        throw new IllegalArgumentException(
            "filterOptions.lastPublishedCutoff requires filterOptions.publishMode of 'incremental'; it is not applied in full mode.");
      }
    }
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    initialize();

    // discover and publish all valid file candidates
    try {
      if (concurrent && storageURIs.size() > 1) {
        traversePathsConcurrently(publisher);
      } else {
        for (URI resource : storageURIs) {
          traverseStoragePath(publisher, resource);
        }
      }
    } finally {
      if (stateManager != null) {
        stateManager.closeStateForThread();
      }
    }

    // bump runs_not_encountered so listExpiredFiles and shutdown's deletion see up-to-date counters
    if (stateManager != null) {
      try {
        stateManager.incrementRunsNotEncountered();
      } catch (SQLException e) {
        throw new ConnectorException("Error finalizing state after traversal.", e);
      }
    }

    if (config.hasPath("filterOptions.sendTombstones") &&
        config.getBoolean("filterOptions.sendTombstones")) {
      // find files no longer in datastore that need to be removed from index
      sendExpiredFileTombstones(publisher);
    }
  }

  // stateful only: publish tombstones for files seen during prior ingests but not the current
  private void sendExpiredFileTombstones(Publisher publisher) throws ConnectorException {
    // skip if state not being managed
    if (stateManager == null) {
      return;
    }

    List<String> expiredFiles;
    try {
      expiredFiles = stateManager.listExpiredFiles();
    } catch (SQLException e) {
      // keep the expired rows, so their tombstones are published next run rather than deleted without one
      stateManager.retainExpiredRows();
      log.warn("Error occurred while listing expired files. Their tombstones will be published on the next run.", e);
      return;
    }

    if (expiredFiles.isEmpty()) {
      return;
    }

    int expiredFileCount = expiredFiles.size();
    int publishedTombstoneCount = 0;
    log.info("{} previously published files now missing/expired, publishing document tombstones...", expiredFileCount);

    // tombstones only take the docIdPrefix from the params, so one set serves them all
    TraversalParams tombstoneParams = buildTraversalParams(storageURIs.get(0));

    for (String expiredFile : expiredFiles) {
      Document doc = buildTombstoneDoc(expiredFile, tombstoneParams);
      try {
        publisher.publish(doc);
        publishedTombstoneCount++;
      } catch (Exception e) {
        stateManager.retainExpiredRows();
        throw new ConnectorException("Error publishing document tombstone for file: " + expiredFile, e);
      }
    }
    log.info("Published {} of {} document tombstones for tracking and index removal", publishedTombstoneCount, expiredFileCount);
  }

  private void initialize() throws ConnectorException {
    try {
      for (StorageClient client : storageClientMap.values()) {
        client.start();
      }
    } catch (IOException e) {
      throw new ConnectorException("Error initializing a StorageClient.", e);
    }
    if (stateManager != null) {
      try {
        stateManager.init();
      } catch (Exception e) {
        throw new ConnectorException("Error occurred initializing StorageClientStateManager.", e);
      }
    }
  }

  @Override
  public void close() {
    if (stateManager != null) {
      try {
        stateManager.shutdown();
      } catch (SQLException e) {
        log.warn("Error occurred while shutting down FileConnectorStateManager.", e);
      }
    }
    for (StorageClient client : storageClientMap.values()) {
      try {
        client.stop();
      } catch (IOException e) {
        log.warn("Error shutting down StorageClient.", e);
      }
    }
  }

  /**
   * Traverses the storage paths concurrently, on up to maxThreads threads. Requires the paths to not overlap, so that
   * concurrent traversals never touch the same row of the state database.
   */
  private void traversePathsConcurrently(Publisher publisher) throws ConnectorException {
    ThreadFactory threadFactory = new BasicThreadFactory.Builder()
        .namingPattern(ThreadNameUtils.createName("PathTraversal") + "-%d")
        .build();
    // -1 means one thread per path
    int poolSize = maxThreads == -1 ? storageURIs.size() : Math.min(maxThreads, storageURIs.size());
    ExecutorService executor = Executors.newFixedThreadPool(poolSize, threadFactory);

    try {
      List<Future<?>> traversals = new ArrayList<>();

      for (URI resource : storageURIs) {
        traversals.add(executor.submit(() -> {
          traverseStoragePathOnThisThread(publisher, resource);
          return null;
        }));
      }

      // Every traversal runs to completion, even after one fails. Aborting the others would leave some of this run's
      // files unmarked in the state database, and they would then be expired, tombstoned and deleted.
      ConnectorException failure = null;

      // traversals is in the same order as storageURIs, so the index identifies the path that failed
      for (int i = 0; i < traversals.size(); i++) {
        URI resource = storageURIs.get(i);

        try {
          traversals.get(i).get();
          log.info("Finished traversing '{}'.", resource);
        } catch (ExecutionException e) {
          log.error("Error occurred while traversing '{}', CONTINUING", resource, e.getCause());

          if (failure == null) {
            failure = new ConnectorException("One or more traversals failed.", e.getCause());
          } else {
            failure.addSuppressed(e.getCause());
          }
        }
      }

      if (failure != null) {
        throw failure;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectorException("Interrupted while waiting for traversals to finish.", e);
    } finally {
      executor.shutdownNow();
    }
  }

  // Gives the calling thread its own TraversalState for the traversal. A JDBC Connection is not thread-safe, so each
  // traversal thread needs its own.
  private void traverseStoragePathOnThisThread(Publisher publisher, URI pathToTraverse) throws Exception {
    if (stateManager == null) {
      traverseStoragePath(publisher, pathToTraverse);
      return;
    }

    stateManager.openStateForThread();

    try {
      traverseStoragePath(publisher, pathToTraverse);
    } finally {
      stateManager.closeStateForThread();
    }
  }

  /**
   * Rejects overlapping paths. Threads traversing overlapping paths would touch the same rows of the state database,
   * and would publish the files under the shared paths more than once.
   * <p> A StorageClient can only compare paths within its own storage provider, and cannot detect paths
   * that reach the same files by another route, such as a symlink or an S3 access point alias.
   */
  private void validateNonOverlappingPaths() {
    for (int i = 0; i < storageURIs.size(); i++) {
      for (int j = i + 1; j < storageURIs.size(); j++) {
        URI first = storageURIs.get(i);
        URI second = storageURIs.get(j);
        StorageClient client = StorageClient.clientFor(first, storageClientMap, config);

        // paths handled by different storage providers cannot overlap
        if (client == null || client != StorageClient.clientFor(second, storageClientMap, config)) {
          continue;
        }

        if (client.containsPath(first, second) || client.containsPath(second, first)) {
          throw new IllegalArgumentException("FileConnector cannot traverse overlapping paths concurrently: '"
              + first + "' and '" + second + "'.");
        }
      }
    }
  }

  /**
   * Rejects a fileOptions move folder that the traversal itself would visit. Files moved there during a run would be
   * found again by the same traversal and published twice (an S3 flat listing finds them on later pages).
   */
  private void validateMoveTargetOutsidePath(String option) {
    String key = "fileOptions." + option;
    if (!config.hasPath(key) || storageURIs.size() != 1) {
      return;
    }

    URI path = storageURIs.get(0);
    URI target;
    try {
      target = TraversalParams.parsePathOrURI(config.getString(key));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid " + key + ": " + config.getString(key), e);
    }

    StorageClient client = StorageClient.clientFor(path, storageClientMap, config);

    // a folder on another provider can't sit under the path
    if (client == null || client != StorageClient.clientFor(target, storageClientMap, config)) {
      return;
    }

    if (client.containsPath(path, target)) {
      throw new IllegalArgumentException(key + " (" + target + ") is inside the path being traversed (" + path
          + "), so moved files would be traversed again. Choose a folder outside it.");
    }
  }

  private static String clientKeyFor(URI pathToTraverse) {
    return pathToTraverse.getScheme() != null ? pathToTraverse.getScheme() : "file";
  }

  private void traverseStoragePath(Publisher publisher, URI pathToTraverse) throws ConnectorException {
    StorageClient storageClient = StorageClient.clientFor(pathToTraverse, storageClientMap, config);

    if (storageClient == null) {
      throw new ConnectorException("No StorageClient was available for (" + pathToTraverse +
          "). Did you include the necessary configuration?");
    }

    TraversalParams params = buildTraversalParams(pathToTraverse);

    try {
      storageClient.traverse(publisher, params, stateManager);
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while traversing " + pathToTraverse + ".", e);
    }
  }

  private TraversalParams buildTraversalParams(URI pathToTraverse) {
    return new TraversalParams(config, pathToTraverse, getDocIdPrefix());
  }

  /**
   * Builds a tombstone Document for the given file. The document is marked as expired and skipped,
   * so it bypasses pipeline stages and signals downstream indexers to delete the corresponding entry.
   *
   * @param fullPath the full path of the file that is no longer present in storage, as stored in state
   * @param params the parameters supplying the docIdPrefix
   * @return a Document with {@link #EXPIRED} set to {@code true} and marked as skipped
   */
  private static Document buildTombstoneDoc(String fullPath, TraversalParams params) {
    Instant now = Instant.now();
    Document doc = BaseFileReference.buildBaseDoc(fullPath, now, 0L, now, params);
    doc.setField(EXPIRED, true);
    doc.setSkipped(true);
    return doc;
  }

}
