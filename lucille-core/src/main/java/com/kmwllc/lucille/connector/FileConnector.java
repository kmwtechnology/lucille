package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.connector.storageclients.StorageClient;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
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
 *  getFileContent (boolean, Optional): whether to fetch the file content or not, defaults to true
 *  pathToStorage (string): path to storage, can be local file system or cloud bucket/container
 *  e.g.
 *    /path/to/storage/in/local/filesystem
 *    gs://bucket-name/folder/
 *    s3://bucket-name/folder/
 *    https://accountName.blob.core.windows.net/containerName/prefix/
 *  includes (list of strings, Optional): list of regex patterns to include files
 *  excludes (list of strings, Optional): list of regex patterns to exclude files
 *  cloudOptions (Map, Optional): cloud storage options, required if using cloud storage
 *  fileOptions (Map, Optional): file options for handling of files
 *
 * Cloud Options based on providers:
 *  Google:
 *    "pathToServiceKey" : "path/To/Service/Key.json"
 *  Azure:
 *    connectionString" : azure connection string
 *      Or:
 *    "accountName" : azure account name
 *    "accountKey" : azure account key
 *  Amazon:
 *    "accessKeyId" : s3 key id
 *    "secretAccessKey" : secret access key
 *    "region" : s3 storage region
 *  Optional:
 *    "maxNumOfPages" : number of references of the files loaded into memory in a single fetch request, defaults to 100
 *
 *  File Options:
 *    getFileContent (boolean, Optional): whether to fetch the file content or not, defaults to true
 *    handleArchivedFiles (boolean, Optional): whether to handle archived files or not, defaults to false
 *    handleCompressedFiles (boolean, Optional): whether to handle compressed files or not, defaults to false
 *    csv (Map, Optional): csv config options for handling csv type files. Config will be passed to FileTypeHandler
 *    json (Map, Optional): json config options for handling json type files. Config will be passed to FileTypeHandler
 *    jsonl (Map, Optional): jsonl config options for handling jsonl type files. Config will be passed to FileTypeHandler
 */
public class FileConnector extends AbstractConnector {

  public static final String FILE_PATH = "file_path";
  public static final String MODIFIED = "file_modification_date";
  public static final String CREATED = "file_creation_date";
  public static final String SIZE = "file_size_bytes";
  public static final String CONTENT = "file_content";

  // cloudOption Keys
  public static final String AZURE_CONNECTION_STRING = "connectionString";
  public static final String AZURE_ACCOUNT_NAME = "accountName";
  public static final String AZURE_ACCOUNT_KEY = "accountKey";
  public static final String S3_REGION = "region";
  public static final String S3_ACCESS_KEY_ID = "accessKeyId";
  public static final String S3_SECRET_ACCESS_KEY = "secretAccessKey";
  public static final String GOOGLE_SERVICE_KEY = "pathToServiceKey";

  // fileOption Keys
  public static final String GET_FILE_CONTENT = "getFileContent";
  public static final String HANDLE_ARCHIVED_FILES = "handleArchivedFiles";
  public static final String HANDLE_COMPRESSED_FILES = "handleCompressedFiles";

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
      log.info("using path {} with scheme {}", pathToStorage, storageURI.getScheme());
    } catch (URISyntaxException e) {
      throw new ConnectorException("Invalid path to storage: " + pathToStorage, e);
    }
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    storageClient = StorageClient.getClient(storageURI, publisher, getDocIdPrefix(), excludes, includes,
        cloudOptions, fileOptions);
    try {
      storageClient.init();
      storageClient.publishFiles();
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while initializing client or publishing files.", e);
    } finally {
      try {
        // closes clients and file handlers if any
        storageClient.shutdown();
      } catch (Exception e) {
        throw new ConnectorException("Error occurred while shutting down client.", e);
      }
    }
  }
}
