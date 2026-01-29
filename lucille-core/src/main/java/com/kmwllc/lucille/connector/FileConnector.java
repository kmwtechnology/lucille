package com.kmwllc.lucille.connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.connector.storageclient.StorageClient;
import com.kmwllc.lucille.connector.storageclient.TraversalParams;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Traverses local and cloud storage (S3, GCP, Azure) from one or more roots and publishes a Document for each file encountered.
 * Supports include/exclude regex filters, recency cutoffs, optional content fetching, archive/compressed file handling, file moves
 * after processing or on error, and optional JDBC-backed state to avoid republishing recently handled files. Only files matching
 * all filter criteria are processed. Durations use HOCON-style strings like "1h", "2d", "3s".
 *
 * For archive/compressed files, modification/publish cutoffs apply to both the container and its entries. When state is enabled,
 * traversal may be slower. Files that are moved/renamed are always  republished regardless of lastPublishedCutoff. You can enable
 * state without specifying lastPublishedCutoff to keep publish times updated.
 *
 * State tracks file paths and last publish timestamps to support filterOptions.lastPublishedCutoff and to avoid duplicate publications
 * across runs. You can connect to your own JDBC database by providing driver, connectionString, jdbcUser, and tableName. If
 * connectionString is omitted, an embedded database is created at ./state/{CONNECTOR_NAME}. With state enabled, traversal may be
 * slower, and files that are moved or renamed are always republished. The lastPublishedCutoff setting has no effect unless state is
 * configured. You may enable state without lastPublishCutoff and publish times will still be updated.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>paths (List&lt;String&gt;, Required) : Paths or URIs to traverse (local paths or cloud storage URIs). s3 URIs must be
 *   percent-encoded; unencoded spaces or special characters will not be recognized. For example, use s3://test/folder%20with%20spaces.</li>
 *   <li>filterOptions.includes (List&lt;String&gt;, Optional) : Regex patterns to include files.</li>
 *   <li>filterOptions.excludes (List&lt;String&gt;, Optional) : Regex patterns to exclude files.</li>
 *   <li>filterOptions.lastModifiedCutoff (String, Optional) : Duration string to include only files modified within this period (e.g., "1h").</li>
 *   <li>filterOptions.lastPublishedCutoff (String, Optional) : Duration string to include only files not published within this period.</li>
 *   <li>fileOptions.getFileContent (Boolean, Optional) : Fetch file content during traversal. Defaults to true.</li>
 *   <li>fileOptions.handleArchivedFiles (Boolean, Optional) : Process archive files. Defaults to false.</li>
 *   <li>fileOptions.handleCompressedFiles (Boolean, Optional) : Process compressed files. Defaults to false.</li>
 *   <li>fileOptions.moveToAfterProcessing (String, Optional) : URI to move files after successful processing (single input path only).</li>
 *   <li>fileOptions.moveToErrorFolder (String, Optional) : URI to move files if processing fails (single input path only).</li>
 *   <li>state.driver (String, Optional) : JDBC driver class. Defaults to "org.h2.Driver".</li>
 *   <li>state.connectionString (String, Optional) : JDBC connection string. Defaults to "jdbc:h2:./state/{CONNECTOR_NAME}".</li>
 *   <li>state.jdbcUser (String, Optional) : Database username. Defaults to "".</li>
 *   <li>state.jdbcPassword (String, Optional) : Database password. Defaults to "".</li>
 *   <li>state.tableName (String, Optional) : Table name for state. Defaults to the connector name.</li>
 *   <li>state.performDeletions (Boolean, Optional) : Delete rows for files removed from storage. Defaults to true.</li>
 *   <li>state.pathLength (Int, Optional) : Max length for stored file paths when Lucille creates the table. Defaults to 200.</li>
 *   <li>gcp.pathToServiceKey (String, Required) : Path to the Google Cloud service key JSON.</li>
 *   <li>gcp.maxNumOfPages (Int, Optional) : Maximum number of file references to hold in memory. Defaults to 100.</li>
 *   <li>s3.accessKeyId (String, Optional) : AWS access key ID (omit to use default credentials).</li>
 *   <li>s3.secretAccessKey (String, Optional) : AWS secret access key (omit to use default credentials).</li>
 *   <li>s3.region (String, Optional) : AWS region for S3.</li>
 *   <li>s3.maxNumOfPages (Int, Optional) : Maximum number of file references to hold in memory. Defaults to 100.</li>
 *   <li>azure.connectionString (String, Optional) : Azure connection string.</li>
 *   <li>azure.accountName (String, Optional) : Azure account name.</li>
 *   <li>azure.accountKey (String, Optional) : Azure account key.</li>
 *   <li>azure.maxNumOfPages (Int, Optional) : Maximum number of file references to hold in memory. Defaults to 100.</li>
 *   <li>fileHandlers (Map&lt;String, Map&lt;String, Object&gt;&gt;, Optional) : Per-type FileHandler configuration (e.g., csv, json, xml).
 *   Supply a class to override the default handler. Otherwise the built-in handler for csv/json/xml is used. Configure docIdPrefix inside
 *   each handler's config as needed.</li>
 * </ul>
 */
public class FileConnector extends AbstractConnector {

  public static final String FILE_PATH = "file_path";
  public static final String MODIFIED = "file_modification_date";
  public static final String CREATED = "file_creation_date";
  public static final String SIZE = "file_size_bytes";
  public static final String CONTENT = "file_content";
  public static final String ARCHIVE_FILE_SEPARATOR = "!";

  // cloudOption Keys
  public static final String AZURE_CONNECTION_STRING = "connectionString";
  public static final String AZURE_ACCOUNT_NAME = "accountName";
  public static final String AZURE_ACCOUNT_KEY = "accountKey";
  public static final String S3_REGION = "region";
  public static final String S3_ACCESS_KEY_ID = "accessKeyId";
  public static final String S3_SECRET_ACCESS_KEY = "secretAccessKey";
  public static final String GOOGLE_SERVICE_KEY = "pathToServiceKey";
  public static final String MAX_NUM_OF_PAGES = "maxNumOfPages";

  // fileOption Config Options
  public static final String GET_FILE_CONTENT = "getFileContent";
  public static final String HANDLE_ARCHIVED_FILES = "handleArchivedFiles";
  public static final String HANDLE_COMPRESSED_FILES = "handleCompressedFiles";
  public static final String MOVE_TO_AFTER_PROCESSING = "moveToAfterProcessing";
  public static final String MOVE_TO_ERROR_FOLDER = "moveToErrorFolder";

  // parent specs for cloud provider configs
  public static final Spec GCP_PARENT_SPEC = SpecBuilder.parent("gcp")
      .requiredString("pathToServiceKey")
      .optionalNumber("maxNumOfPages").build();
  public static final Spec S3_PARENT_SPEC = SpecBuilder.parent("s3")
      .optionalString("accessKeyId", "secretAccessKey", "region")
      .optionalNumber("maxNumOfPages").build();
  public static final Spec AZURE_PARENT_SPEC = SpecBuilder.parent("azure")
      .optionalString("connectionString", "accountName", "accountKey")
      .optionalNumber("maxNumOfPages").build();

  private static final Logger log = LoggerFactory.getLogger(FileConnector.class);

  public static final Spec SPEC = SpecBuilder.connector()
      .requiredList("paths", new TypeReference<List<String>>(){})
      .optionalParent(
          SpecBuilder.parent("filterOptions")
              .optionalList("includes", new TypeReference<List<String>>(){})
              .optionalList("excludes", new TypeReference<List<String>>(){})
              // durations are strings.
              .optionalString("lastModifiedCutoff", "lastPublishedCutoff").build(),
          SpecBuilder.parent("fileOptions")
              .optionalBoolean("getFileContent", "handleArchivedFiles", "handleCompressedFiles")
              .optionalString("moveToAfterProcessing", "moveToErrorFolder").build(),
          SpecBuilder.parent("state")
              .optionalString("driver", "connectionString", "jdbcUser", "jdbcPassword", "tableName")
              .optionalBoolean("performDeletions")
              .optionalNumber("pathLength").build(),
          GCP_PARENT_SPEC,
          AZURE_PARENT_SPEC,
          S3_PARENT_SPEC)
      .optionalParent("fileHandlers", new TypeReference<Map<String, Map<String, Object>>>(){}).build();

  private final List<URI> storageURIs;
  private final Map<String, StorageClient> storageClientMap;

  private final FileConnectorStateManager stateManager;

  public FileConnector(Config config) throws ConnectorException {
    super(config);

    List<String> paths = config.getStringList("paths");
    this.storageURIs = new ArrayList<>();

    for (String path : paths) {
      try {
        URI newStorageURI = new URI(path);
        storageURIs.add(newStorageURI);
        log.debug("FileConnector to use path {} with scheme {}", path, newStorageURI.getScheme());
      } catch (URISyntaxException e) {
        throw new ConnectorException("Invalid path to storage: " + path, e);
      }
    }

    this.stateManager = config.hasPath("state") ? new FileConnectorStateManager(config.getConfig("state"), getName()) : null;

    this.storageClientMap = StorageClient.createClients(config);

    // Cannot specify multiple storage paths and a moveTo of some kind
    if (storageURIs.size() > 1 && (config.hasPath("fileOptions.moveToAfterProcessing") || config.hasPath("fileOptions.moveToErrorFolder"))) {
      throw new IllegalArgumentException("FileConnector does not support multiple paths and moveToAfterProcessing / moveToErrorFolder. Create individual FileConnectors.");
    }

    if (config.hasPath("filterOptions.lastPublishedCutoff") && !config.hasPath("state")) {
      log.warn("filterOptions.lastPublishedCutoff was specified, but no state configuration was provided. It will not be enforced.");
    }
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    try {
      try {
        for (StorageClient client : storageClientMap.values()) {
          client.init();
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

      for (URI pathToTraverse : storageURIs) {
        String clientKey = pathToTraverse.getScheme() != null ? pathToTraverse.getScheme() : "file";
        StorageClient storageClient = storageClientMap.get(clientKey);

        if (storageClient == null) {
          throw new ConnectorException("No StorageClient was available for (" + pathToTraverse + "). Did you include the necessary configuration?");
        }

        // creating a new traversal params for each path, which includes rereading file/filterOptions and
        // creating the FileHandlers map. FileHandlers are lightweight, so this is not an intensive operation.
        TraversalParams params = new TraversalParams(config, pathToTraverse, getDocIdPrefix());

        try {
          storageClient.traverse(publisher, params, stateManager);
        } catch (Exception e) {
          throw new ConnectorException("Error occurred while traversing " + pathToTraverse + ".", e);
        }
      }
    } finally {
      for (StorageClient client : storageClientMap.values()) {
        try {
          client.shutdown();
        } catch (IOException e) {
          log.warn("Error shutting down StorageClient.", e);
        }
      }

      if (stateManager != null) {
        try {
          stateManager.shutdown();
        } catch (SQLException e) {
          log.warn("Error occurred while shutting down FileConnectorStateManager.", e);
        }
      }
    }
  }
}
