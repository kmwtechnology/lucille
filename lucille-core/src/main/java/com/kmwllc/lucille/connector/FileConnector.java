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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Config parameters:
 *  docIdPrefix (string, Optional): prefix to add to the docId when not handled by a file handler, defaults to empty string. To configure docIdPrefix for CSV, JSON or XML files, configure it in its respective file handler config in fileOptions
 *  pathsToStorage (List&lt;String&gt;): The paths to storage you want to traverse. Can be local file paths or cloud storage URIs. Make sure to include the necessary configuration for cloud providers as they are included in your pathsToStorage.
 *  e.g.
 *    /path/to/storage/in/local/filesystem
 *    gs://bucket-name/folder/
 *    s3://bucket-name/folder/
 *    https://accountName.blob.core.windows.net/containerName/prefix/
 *  filterOptions (Map, Optional): configuration for <i>which</i> files should/shouldn't be processed in your traversal. Example of filterOptions below.
 *  fileOptions (Map, Optional): Options for <i>how</i> you handle/process certain types of files in your traversal. Example of fileOptions below.
 *  gcp (Map, Optional): options for handling GoogleCloud files. See example below.
 *  s3 (Map, Optional): options for handling S3 files. See example below.
 *  azure (Map, Optional): options for handling Azure files. See example below.
 *
 * FilterOptions:
 *  includes (list of strings, Optional): list of regex patterns to include files.
 *  excludes (list of strings, Optional): list of regex patterns to exclude files.
 *  modificationCutoff (Duration, Optional): Filter files that haven't been modified since a certain amount of time.
 *  See the HOCON documentation for examples of a Duration - strings like "1h", "2d" and "3s" are accepted, for example.
 *  Note that, for archive files, this cutoff applies to both the archive file itself and its individual contents.
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

  // parent specs for cloud provider configs
  public static final ParentSpec GCP_PARENT_SPEC = Spec.parent("gcp")
      .withRequiredProperties("pathToServiceKey")
      .withOptionalProperties("maxNumOfPages");
  public static final ParentSpec S3_PARENT_SPEC = Spec.parent("s3")
      .withOptionalProperties("accessKeyId", "secretAccessKey", "region", "maxNumOfPages");
  public static final ParentSpec AZURE_PARENT_SPEC = Spec.parent("azure")
      .withOptionalProperties("connectionString", "accountName", "accountKey", "maxNumOfPages");

  private static final Logger log = LoggerFactory.getLogger(FileConnector.class);

  private final Config fileOptions;
  private final Config filterOptions;
  private final List<URI> storageURIs;
  private final Map<String, StorageClient> storageClientMap;

  public FileConnector(Config config) throws ConnectorException {
    super(config, Spec.connector()
        .withRequiredProperties("pathsToStorage")
        .withOptionalParents(
            Spec.parent("filterOptions").withOptionalProperties("includes", "excludes", "modificationCutoff"),
            Spec.parent("fileOptions")
                .withOptionalProperties("getFileContent", "handleArchivedFiles", "handleCompressedFiles", "moveToAfterProcessing",
                    "moveToErrorFolder")
                .withOptionalParents(CSVFileHandler.PARENT_SPEC, JsonFileHandler.PARENT_SPEC, XMLFileHandler.PARENT_SPEC),
            GCP_PARENT_SPEC,
            AZURE_PARENT_SPEC,
            S3_PARENT_SPEC
        ));

    this.fileOptions = config.hasPath("fileOptions") ? config.getConfig("fileOptions") : ConfigFactory.empty();
    this.filterOptions = config.hasPath("filterOptions") ? config.getConfig("filterOptions") : ConfigFactory.empty();
    this.storageClientMap = StorageClient.createClients(config);

    List<String> pathsToStorage = config.getStringList("pathsToStorage");
    this.storageURIs = new ArrayList<>();

    for (String path : pathsToStorage) {
      try {
        URI newStorageURI = new URI(path);
        storageURIs.add(newStorageURI);
        log.debug("using path {} with scheme {}", path, newStorageURI.getScheme());
      } catch (URISyntaxException e) {
        throw new ConnectorException("Invalid path to storage: " + path, e);
      }
    }
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    try {
      for (StorageClient client : storageClientMap.values()) {
        client.init();
      }
    } catch (IOException e) {
      throw new ConnectorException("Error initializing StorageClient.", e);
    }

    try {
      for (URI pathToTraverse : storageURIs) {
        try {
          String clientKey = pathToTraverse.getScheme() != null ? pathToTraverse.getScheme() : "file";
          StorageClient storageClient = storageClientMap.get(clientKey);

          if (storageClient == null) {
            log.warn("No StorageClient was available for {}, the path will be skipped. Did you include the necessary configuration?", pathToTraverse);
            continue;
          }

          TraversalParams params = new TraversalParams(pathToTraverse, getDocIdPrefix(), fileOptions, filterOptions);
          storageClient.traverse(publisher, params);
        } catch (Exception e) {
          throw new ConnectorException("Error occurred while initializing client or publishing files.", e);
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
    }
  }
}
