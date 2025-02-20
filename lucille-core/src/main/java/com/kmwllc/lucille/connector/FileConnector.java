package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.connector.storageclient.AzureStorageClient;
import com.kmwllc.lucille.connector.storageclient.StorageClient;
import com.kmwllc.lucille.connector.storageclient.TraversalParams;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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
 *  includes (list of strings, Optional): list of regex patterns to include files
 *  excludes (list of strings, Optional): list of regex patterns to exclude files
 *  cloudOptions (Map, Optional): cloud storage options, required if using cloud storage. Example of cloudOptions below
 *  fileOptions (Map, Optional): file options for handling of files and file types. Example of fileOptions below
 *
 * CloudOptions:
 *  If using GoogleStorageClient:
 *    "pathToServiceKey" : "path/To/Service/Key.json"
 *  If using AzureStorageClient:
 *    "connectionString" : azure connection string
 *      Or
 *    "accountName" : azure account name
 *    "accountKey" : azure account key
 *  If using S3StorageClient:
 *    "accessKeyId" : s3 key id
 *    "secretAccessKey" : secret access key
 *    "region" : s3 storage region
 *  Optional:
 *    "maxNumOfPages" : number of references of the files loaded into memory in a single fetch request, defaults to 100
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

  private static final Logger log = LoggerFactory.getLogger(FileConnector.class);

  private final String pathToStorage;
  private final Map<String, Object> cloudOptions;
  private final Config fileOptions;
  private final List<Pattern> includes;
  private final List<Pattern> excludes;
  private StorageClient storageClient;
  private final URI storageURI;

  public FileConnector(Config config) throws ConnectorException {
    super(config);
    this.pathToStorage = config.getString("pathToStorage");
    // compile include and exclude regex paths or set an empty list if none were provided (allow all files)
    List<String> includeRegex = config.hasPath("includes") ?
        config.getStringList("includes") : Collections.emptyList();
    this.includes = includeRegex.stream().map(Pattern::compile).collect(Collectors.toList());
    List<String> excludeRegex = config.hasPath("excludes") ?
        config.getStringList("excludes") : Collections.emptyList();
    this.excludes = excludeRegex.stream().map(Pattern::compile).collect(Collectors.toList());
    this.cloudOptions = config.hasPath("cloudOptions") ? config.getConfig("cloudOptions").root().unwrapped() : Map.of();
    this.fileOptions = config.hasPath("fileOptions") ? config.getConfig("fileOptions") : ConfigFactory.empty();
    try {
      this.storageURI = new URI(pathToStorage);
      log.debug("using path {} with scheme {}", pathToStorage, storageURI.getScheme());
    } catch (URISyntaxException e) {
      throw new ConnectorException("Invalid path to storage: " + pathToStorage, e);
    }
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    try {
      storageClient = StorageClient.create(storageURI, cloudOptions);
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while creating storage client.", e);
    }

    try {
      storageClient.init();
      boolean usingAzure = storageClient instanceof AzureStorageClient;
      TraversalParams params = new TraversalParams(storageURI, getDocIdPrefix(), includes, excludes, fileOptions, usingAzure);
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
