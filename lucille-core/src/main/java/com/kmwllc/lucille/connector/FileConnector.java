package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.connector.storageclient.StorageClient;
import com.kmwllc.lucille.connector.storageclient.TraversalParams;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.Spec.ParentSpec;
import com.kmwllc.lucille.core.fileHandler.CSVFileHandler;
import com.kmwllc.lucille.core.fileHandler.JsonFileHandler;
import com.kmwllc.lucille.core.fileHandler.XMLFileHandler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Config parameters:
 *  docIdPrefix (string, Optional): prefix to add to the docId when not handled by a file handler, defaults to empty string. To configure docIdPrefix for CSV, JSON or XML files, configure it in its respective file handler config in fileOptions
 *  pathToStorage (string): path to storage, can be local file system or cloud bucket/container
 *  e.g.
 *    /path/to/storage/in/local/filesystem
 *    gs://bucket-name/folder/
 *    s3://bucket-name/folder/
 *    https://accountName.blob.core.windows.net/containerName/prefix/
 *  filterOptions (Map, Optional): configuration for <i>which</i> files should/shouldn't be processed in your traversal. Example of filterOptions below.
 *  fileOptions (Map, Optional): Options for <i>how</i> you handle/process certain types of files in your traversal. Example of fileOptions below.
 *  state (Map, Optional): options for tracking when files were published and processed by Lucille. See example configuration and some important notes below.
 *  gcp (Map, Optional): options for handling Google Cloud files. See example below.
 *  s3 (Map, Optional): options for handling S3 files. See example below.
 *  azure (Map, Optional): options for handling Azure files. See example below.
 *
 * FilterOptions:
 *  includes (list of strings, Optional): list of regex patterns to include files.
 *  excludes (list of strings, Optional): list of regex patterns to exclude files.
 *  lastModifiedCutoff (Duration, Optional): Filter files that haven't been modified since a certain amount of time.
 *  Specify "1h" to only include / publish files that were modified within the last hour, for example.
 *  lastPublishedCutoff (Duration, Optional): Filter files that haven't been published by Lucille since a certain amount of time.
 *  Relies on your state configuration to determine when files were last published. If you do not specify configuration for state, this
 *  wil have no effect. Specify "1h" to only include / publish files that were last published <b>more</b> than an hour ago, for example.
 *  (State is <b>only</b> maintained / tracked for the archive file itself.)
 * <br> Only files that comply with <b>all</b> of your specified FilterOptions will be processed and published in a traversal.
 * <br> For archived / compressed files, filter options are applied to the archive / compressed file itself as well as its contents.
 * <br> See the HOCON documentation for examples of a Duration - strings like "1h", "2d" and "3s" are accepted, for example.
 *
 * FileOptions:
 *  getFileContent (boolean, Optional): option to fetch the file content or not, defaults to true. Setting this to false would speed up traversal significantly. Note that if you are traversing the cloud, setting this to true would download the file content. Ensure that you have enough resources if you expect file contents to be large.
 *  handleArchivedFiles (boolean, Optional): whether to handle archived files or not, defaults to false. Recurring not supported. Note: If this is enabled while traversing the cloud, it will force to fetch the file contents of the compressed file before processing. The file path field of extracted file will be in the format of "{path/to/archive/archive.zip}:{extractedFileName}" unless handled by fileHandler in which in that case will follow the id creation of that fileHandler
 *  handleCompressedFiles (boolean, Optional): whether to handle compressed files or not, defaults to false. Recurring not supported.Note: If this is enabled while traversing the cloud, it will force to fetch the file contents of the compressed file before processing.The file path field of decompressed file will be in the format of "{path/to/compressed/compressedFileName.gz}:{compressedFileName}" unless handled by fileHandler in which in that case will follow the id creation of that fileHandler
 *  moveToAfterProcessing (string, Optional): path to move files to after processing, currently only supported for local file system
 *  moveToErrorFolder (string, Optional): path to move files to if an error occurs during processing, currently only supported for local file system
 *  csv (Map, Optional): csv config options for handling csv type files. Config will be passed to CSVFileHandler
 *  json (Map, Optional): json config options for handling json/jsonl type files. Config will be passed to JsonFileHandler
 *  xml (Map, Optional): xml config options for handling xml type files. Config will be passed to XMLFileHandler
 *
 * <p> <b>State</b>: FileConnector allows you to avoid publishing files that were recently published (using FilterOptions.lastPublishedCutoff). In order to keep track of
 * this information, you'll need to specify a connection to a JDBC-compatible database which will be used to track file paths and
 * when they were last published by Lucille. For more information about the database / its schema, see {@link FileConnectorStateManager}
 * <br> Config Parameters:
 * <br> - driver (String): The driver to use for creating the connection.
 * <br> - connectionString (String): A String for a connection to your state database.
 * <br> - jdbcUser (String): The username for accessing the database.
 * <br> - jdbcPassword (String): The password for accessing the database.
 * <br> - tableName (String, Optional): The name of the table in your database that holds the relevant state information. Defaults to the connector name.
 * <br> - performDeletions (Boolean, Optional): Whether you want to delete rows in your database corresponding to files that appear to have been deleted
 * in the file system. Defaults to true.
 * <br> - pathLength (Int, Optional): The maximum length of file paths allowed in the table (VARCHAR length). Only affects the creation of tables
 * that did not already exist. Defaults to 200.
 *
 * <br> <b>Some notes on state:</b>
 * <ul>
 *   <li>The FileConnector will be slower when running with state.</li>
 *   <li>Files that get moved or renamed will be treated like deletions - they will always be published, regardless of lastPublishedCutoff.</li>
 *   <li>You can provide state configuration, but not a lastPublishedCutoff, and Lucille will keep your state updated.</li>
 * </ul>
 *
 * <br> gcp:
 * "pathToServiceKey" : "path/To/Service/Key.json"
 * "maxNumOfPages" : number of references of the files loaded into memory in a single fetch request. Optional, defaults to 100
 *
 * <br> s3:
 * "accessKeyId" : s3 key id. Not needed if secretAccessKey is not specified (using default credentials).
 * "secretAccessKey" : secret access key. Not needed if accessKeyId is not specified (using default credentials).
 * "region" : s3 storage region
 * "maxNumOfPages" : number of references of the files loaded into memory in a single fetch request. Optional, defaults to 100
 *
 * <br> azure:
 * "connectionString" : azure connection string
 * <b>Or</b>
 * "accountName" : azure account name
 * "accountKey" : azure account key
 * "maxNumOfPages" : number of references of the files loaded into memory in a single fetch request. Optional, defaults to 100
 */

public class FileConnector extends AbstractConnector {

  private static final Set<String> CLOUD_STORAGE_CLIENT_KEYS = Set.of("s3", "azure", "gcp");

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
  public static final ParentSpec GCP_PARENT_SPEC = Spec.parent("gcp")
      .withRequiredProperties("pathToServiceKey")
      .withOptionalProperties("maxNumOfPages");
  public static final ParentSpec S3_PARENT_SPEC = Spec.parent("s3")
      .withOptionalProperties("accessKeyId", "secretAccessKey", "region", "maxNumOfPages");
  public static final ParentSpec AZURE_PARENT_SPEC = Spec.parent("azure")
      .withOptionalProperties("connectionString", "accountName", "accountKey", "maxNumOfPages");

  private static final Logger log = LoggerFactory.getLogger(FileConnector.class);

  private final String pathToStorage;
  private final Config fileOptions;
  private final Config filterOptions;
  private StorageClient storageClient;
  private final URI storageURI;

  private final FileConnectorStateManager stateManager;

  public FileConnector(Config config) throws ConnectorException {
    super(config, Spec.connector()
        .withRequiredProperties("pathToStorage")
        .withOptionalParents(
            Spec.parent("filterOptions").withOptionalProperties("includes", "excludes", "lastModifiedCutoff", "lastPublishedCutoff"),
            Spec.parent("fileOptions")
                .withOptionalProperties("getFileContent", "handleArchivedFiles", "handleCompressedFiles", "moveToAfterProcessing",
                    "moveToErrorFolder")
                .withOptionalParents(CSVFileHandler.PARENT_SPEC, JsonFileHandler.PARENT_SPEC, XMLFileHandler.PARENT_SPEC),
            Spec.parent("state")
                .withRequiredProperties("driver", "connectionString", "jdbcUser", "jdbcPassword")
                .withOptionalProperties("tableName", "performDeletions", "pathLength"),
            GCP_PARENT_SPEC,
            AZURE_PARENT_SPEC,
            S3_PARENT_SPEC
        ));

    this.pathToStorage = config.getString("pathToStorage");
    this.fileOptions = config.hasPath("fileOptions") ? config.getConfig("fileOptions") : ConfigFactory.empty();
    this.filterOptions = config.hasPath("filterOptions") ? config.getConfig("filterOptions") : ConfigFactory.empty();

    try {
      this.storageURI = new URI(pathToStorage);
      log.debug("using path {} with scheme {}", pathToStorage, storageURI.getScheme());
    } catch (URISyntaxException e) {
      throw new ConnectorException("Invalid path to storage: " + pathToStorage, e);
    }

    this.stateManager = config.hasPath("state") ? new FileConnectorStateManager(config.getConfig("state"), getName()) : null;

    if (CLOUD_STORAGE_CLIENT_KEYS.stream().filter(config::hasPath).count() > 1) {
      log.warn("Config for FileConnector contains options for more than one cloud provider.");
    }

    if (filterOptions.hasPath("lastPublishedCutoff") && !config.hasPath("state")) {
      log.warn("FilterOptions.lastPublishedCutoff was specified, but no state configuration was provided. It will not be enforced.");
    }
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    try {
      storageClient = StorageClient.create(storageURI, config);
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while creating storage client.", e);
    }

    try {
      storageClient.init();
      TraversalParams params = new TraversalParams(storageURI, getDocIdPrefix(), fileOptions, filterOptions);

      if (stateManager != null) {
        stateManager.init();
        storageClient.traverse(publisher, params, stateManager);
      } else {
        storageClient.traverse(publisher, params);
      }
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while initializing client/state or publishing files.", e);
    } finally {
      try {
        // closes clients and clears file handlers if any
        storageClient.shutdown();
      } catch (IOException e) {
        throw new ConnectorException("Error occurred while shutting down client.", e);
      }

      if (stateManager != null) {
        try {
          stateManager.shutdown();
        } catch (SQLException e) {
          throw new ConnectorException("Error occurred while shutting down FileConnectorStateManager.", e);
        }
      }
    }
  }
}
