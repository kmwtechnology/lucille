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
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <code>FileConnector</code> traverses through a file system, starting at a given directory, and publishes a Document for each
 * file it encounters. It can traverse through the local file system, Azure Blob Storage, Google Cloud, and S3.
 *
 * <br> Config Parameters:
 * <ul>
 *   <li>docIdPrefix (string, Optional): prefix to add to the docId when not handled by a file handler, defaults to empty string. To configure docIdPrefix for CSV, JSON or XML files, configure it in its respective file handler config in fileOptions</li>
 *   <li>pathToStorage (string): path to storage, can be local file system or cloud bucket/container. Examples:
 *    <ul>
 *       <li>/path/to/storage/in/local/filesystem</li>
 *       <li>gs://bucket-name/folder/</li>
 *       <li>s3://bucket-name/folder/</li>
 *      <li>https://accountName.blob.core.windows.net/containerName/prefix/</li>
 *    </ul>
 *   </li>
 *   <li>filterOptions (Map, Optional): configuration for <i>which</i> files should/shouldn't be processed in your traversal. Example of filterOptions below.</li>
 *   <li>fileOptions (Map, Optional): configuratino for <i>how</i> you handle/process certain types of files in your traversal. Example of fileOptions below.</li>
 *   <li>gcp (Map, Optional): options for handling GoogleCloud files. See example below.</li>
 *   <li>s3 (Map, Optional): options for handling S3 files. See example below.</li>
 *   <li>azure (Map, Optional): options for handling Azure files. See example below.</li>
 * </ul>
 *
 * <br>
 *
 * <code>filterOptions</code>:
 * <ul>
 *   <li>includes (list of strings, Optional): list of regex patterns to include files.</li>
 *   <li>excludes (list of strings, Optional): list of regex patterns to exclude files.</li>
 *   <li>modificationCutoff (Duration, Optional): Filter files that haven't been modified since a certain amount of time.</li>
 * </ul>
 *
 * See the HOCON documentation for examples of a Duration - strings like "1h", "2d" and "3s" are accepted, for example.
 * <br> Note that, for archive files, this cutoff applies to both the archive file itself and its individual contents.
 *
 * <p> <code>fileOptions</code>:
 * <ul>
 *   <li>getFileContent (boolean, Optional): option to fetch the file content or not, defaults to true. Setting this to false would speed up traversal significantly. Note that if you are traversing the cloud, setting this to true would download the file content. Ensure that you have enough resources if you expect file contents to be large.</li>
 *   <li>handleArchivedFiles (boolean, Optional): whether to handle archived files or not, defaults to false. Recurring not supported. Note: If this is enabled while traversing the cloud, it will force to fetch the file contents of the compressed file before processing. The file path field of extracted file will be in the format of "{path/to/archive/archive.zip}:{extractedFileName}" unless handled by fileHandler in which in that case will follow the id creation of that fileHandler</li>
 *   <li>handleCompressedFiles (boolean, Optional): whether to handle compressed files or not, defaults to false. Recurring not supported.Note: If this is enabled while traversing the cloud, it will force to fetch the file contents of the compressed file before processing.The file path field of decompressed file will be in the format of "{path/to/compressed/compressedFileName.gz}:{compressedFileName}" unless handled by fileHandler in which in that case will follow the id creation of that fileHandler</li>
 *   <li>moveToAfterProcessing (string, Optional): path to move files to after processing, currently only supported for local file system</li>
 *   <li>moveToErrorFolder (string, Optional): path to move files to if an error occurs during processing, currently only supported for local file system</li>
 *   <li>csv (Map, Optional): csv config options for handling csv type files. Config will be passed to CSVFileHandler</li>
 *   <li>json (Map, Optional): json config options for handling json/jsonl type files. Config will be passed to JsonFileHandler</li>
 *   <li>xml (Map, Optional): xml config options for handling xml type files. Config will be passed to XMLFileHandler</li>
 * </ul>
 *
 * <code>gcp</code>:
 * <ul>
 *   <li>"pathToServiceKey" : "path/To/Service/Key.json"</li>
 *   <li>"maxNumOfPages" (Int, Optional): The maximum number of file references to hold in memory at once. Defaults to 100.</li>
 * </ul>
 *
 * <code>s3</code>:
 * <ul>
 *   <li>"accessKeyId" : s3 key id. Not needed if secretAccessKey is not specified (using default credentials).</li>
 *   <li>"secretAccessKey" : secret access key. Not needed if accessKeyId is not specified (using default credentials).</li>
 *   <li>"region" : s3 storage region</li>
 *   <li>"maxNumOfPages" (Int, Optional): The maximum number of file references to hold in memory at once. Defaults to 100.</li>
 * </ul>
 *
 * <code>azure</code>:
 * <ul>
 *   <li>"connectionString" : azure connection string</li>
 * </ul>
 * <b>Or</b>
 * <ul>
 *   <li>"accountName" : azure account name</li>
 *   <li>"accountKey" : azure account key</li>
 *   <li>"maxNumOfPages" (Int, Optional): The maximum number of file references to hold in memory at once. Defaults to 100.</li>
 * </ul>
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

  public FileConnector(Config config) throws ConnectorException {
    super(config, Spec.connector()
        .withRequiredProperties("pathToStorage")
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
