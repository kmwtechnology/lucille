package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.connector.storageclient.StorageClient;
import com.kmwllc.lucille.connector.storageclient.TraversalParams;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.Spec;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
 *  gcp (Map, Optional): options for handling Google Cloud files. See example below.
 *  s3 (Map, Optional): options for handling S3 files. See example below.
 *  azure (Map, Optional): options for handling Azure files. See example below.
 *
 * FilterOptions:
 *  includes (list of strings, Optional): list of regex patterns to include files.
 *  excludes (list of strings, Optional): list of regex patterns to exclude files.
 *  modificationCutoff (Duration, Optional): Filter files that haven't been modified since a certain amount of time.
 *  See the HOCON documentation for examples of a Duration - strings like "1h", "2d" and "3s" are accepted, for example.
 *  Note that, for archive files, this cutoff applies to both the archive file itself and its individual contents.
 *  lastPublishedCutoff (Duration, Optional): Filter files that haven't been published by Lucille since a certain amount of time.
 *  Relies on your state configuration to determine when files were last published, see ** STATE ** for more.
 *  TODO: Note that, for archive files, this cutoff applies to both the archive file itself and its individual contents?
 *  <b>Note:</b> Only files that comply with <b>all</b> of your specified FilterOptions will be processed and published in a traversal.
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

  private static final Logger log = LoggerFactory.getLogger(FileConnector.class);

  private final String pathToStorage;
  private final Config fileOptions;
  private final Config filterOptions;
  private StorageClient storageClient;
  private final URI storageURI;

  public FileConnector(Config config) throws ConnectorException {
    super(config, Spec.connector()
        .withRequiredProperties("pathToStorage")
        .withOptionalParents("fileOptions", "filterOptions", "s3", "gcp", "azure"));

    this.pathToStorage = config.getString("pathToStorage");
    this.fileOptions = config.hasPath("fileOptions") ? config.getConfig("fileOptions") : ConfigFactory.empty();
    this.filterOptions = config.hasPath("filterOptions") ? config.getConfig("filterOptions") : ConfigFactory.empty();

    try {
      this.storageURI = new URI(pathToStorage);
      log.debug("using path {} with scheme {}", pathToStorage, storageURI.getScheme());
    } catch (URISyntaxException e) {
      throw new ConnectorException("Invalid path to storage: " + pathToStorage, e);
    }

    if (CLOUD_STORAGE_CLIENT_KEYS.stream().filter(config::hasPath).count() > 1) {
      log.warn("Config for FileConnector contains options for more than one cloud provider.");
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
      // TODO: Pull the strings on StorageClientStateManager to get a StorageClientState. (Perhaps it is now a FileConnectorState...)
      storageClient.init();
      TraversalParams params = new TraversalParams(storageURI, getDocIdPrefix(), fileOptions, filterOptions);
      storageClient.traverse(publisher, params);
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while initializing client or publishing files.", e);
    } finally {
      try {
        // closes clients and clears file handlers if any
        storageClient.shutdown();
      } catch (IOException e) {
        throw new ConnectorException("Error occurred while shutting down client.", e);
      }
    }
  }
}
