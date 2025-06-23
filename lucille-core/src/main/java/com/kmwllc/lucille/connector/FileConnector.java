package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.connector.storageclient.StorageClient;
import com.kmwllc.lucille.connector.storageclient.TraversalParams;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.Spec.ParentSpec;
import com.typesafe.config.Config;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <code>FileConnector</code> traverses through a file system, starting at a given directory (or directories), and publishes a Document for each
 * file it encounters. It can traverse through the local file system, Azure Blob Storage, Google Cloud, and S3.
 *
 * <br> Config Parameters:
 * <ul>
 *   <li>pathsToStorage (List &lt;String&gt;): The paths to storage you want to traverse. Can be local file paths or cloud storage URIs. Make sure to include the necessary configuration for cloud providers as they are included in your pathsToStorage. Examples:
 *    <ul>
 *       <li>/path/to/storage/in/local/filesystem</li>
 *       <li>gs://bucket-name/folder/</li>
 *       <li>s3://bucket-name/folder/</li>
 *      <li>https://accountName.blob.core.windows.net/containerName/prefix/</li>
 *    </ul>
 *   </li>
 *   <li>filterOptions (Map, Optional): configuration for <i>which</i> files should/shouldn't be processed in your traversal. Example of filterOptions below.</li>
 *   <li>fileOptions (Map, Optional): configuration for <i>how</i> you handle/process certain types of files in your traversal. Example of fileOptions below.</li>
 *   <li>fileHandlers (Map, Optional): Options for extracting additional Documents from files of a certain type. Example of fileHandlers below.</li>
 *   <li>gcp (Map, Optional): options for handling Google Cloud files. See example below.</li>
 *   <li>s3 (Map, Optional): options for handling S3 files. See example below.</li>
 *   <li>azure (Map, Optional): options for handling Azure files. See example below.</li>
 * </ul>
 *
 * <code>filterOptions</code>:
 * <ul>
 *   <li>includes (List&lt;String&gt;, Optional): list of regex patterns to include files.</li>
 *   <li>excludes (List&lt;String&gt;, Optional): list of regex patterns to exclude files.</li>
 *   <li>modificationCutoff (Duration, Optional): Filter files that haven't been modified since a certain amount of time. For example, specify "1h", and only files that were modified more than an hour ago will be published.</li>
 * </ul>
 *
 * See the HOCON documentation for examples of a Duration - strings like "1h", "2d" and "3s" are accepted, for example.
 * <br> Note that, for archive files, this cutoff applies to both the archive file itself and its individual contents.
 *
 * <p> <code>fileOptions</code>:
 * <ul>
 *   <li>getFileContent (boolean, Optional): option to fetch the file content or not, defaults to true. Setting this to false would speed up traversal significantly. Note that if you are traversing the cloud, setting this to true would download the file content. Ensure that you have enough resources if you expect file contents to be large.</li>
 *   <li>handleArchivedFiles (boolean, Optional): whether to handle archived files or not, defaults to false. See important notes below.</li>
 *   <li>handleCompressedFiles (boolean, Optional): whether to handle compressed files or not, defaults to false. See important notes below.</li>
 *   <li>moveToAfterProcessing (String, Optional): path to move files to after processing, currently only supported for local file system. If using, you can only specify one path in <code>pathsToStorage</code>.</li>
 *   <li>moveToErrorFolder (String, Optional): path to move files to if an error occurs during processing, currently only supported for local file system. If using, you can only specify one path in <code>pathsToStorage</code>.</li>
 *   <li><b>Notes</b> on archive / compressed files:
 *      <ul>
 *         <li>Recurring is not supported.</li>
 *         <li>If enabled during a cloud traversal, the file's contents <b>will</b> be downloaded before processing.</li>
 *         <li>For archive files, the file path field of the extracted file's Document will be in the format of "{path/to/archive/archive.zip}!{extractedFileName}".</li>
 *         <li>For compressed files, the file path follows the format of "{path/to/compressed/compressedFileName.gz}!{compressedFileName}".</li>
 *       </ul>
 *    </li>
 * </ul>
 *
 * <code>fileHandlers</code>:
 * <ul>
 *   <li>csv (Map, Optional): csv config options for handling csv type files. Config will be passed to CSVFileHandler (if no alternate <code>class</code> is provided).</li>
 *   <li>json (Map, Optional): json config options for handling json/jsonl type files. Config will be passed to JsonFileHandler (if no alternate <code>class</code> is provided).</li>
 *   <li>xml (Map, Optional): xml config options for handling xml type files. Config will be passed to XMLFileHandler (if no alternate <code>class</code> is provided).</li>
 *   <li>Include your custom FileHandler implementations here (if any). Remember to include the <code>class</code> in their Configuration.</li>
 *   <li>To configure the docIdPrefix for a certain file type, configure it in its respective config (under <code>docIdPrefix</code>).</li>
 * </ul>
 *
 * <code>gcp</code>:
 * <ul>
 *   <li>"pathToServiceKey": "path/To/Service/Key.json"</li>
 *   <li>"maxNumOfPages" (Int, Optional): The maximum number of file references to hold in memory at once. Defaults to 100.</li>
 * </ul>
 *
 * <code>s3</code>:
 * <ul>
 *   <li>"accessKeyId": s3 key id. Not needed if secretAccessKey is not specified (using default credentials).</li>
 *   <li>"secretAccessKey": secret access key. Not needed if accessKeyId is not specified (using default credentials).</li>
 *   <li>"region": s3 storage region</li>
 *   <li>"maxNumOfPages" (Int, Optional): The maximum number of file references to hold in memory at once. Defaults to 100.</li>
 * </ul>
 *
 * <code>azure</code>:
 * <ul>
 *   <li>"connectionString": azure connection string</li>
 * </ul>
 * <b>Or</b>
 * <ul>
 *   <li>"accountName": azure account name</li>
 *   <li>"accountKey": azure account key</li>
 *   <li>"maxNumOfPages" (Int, Optional): The maximum number of file references to hold in memory at once. Defaults to 100.</li>
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
  public static final ParentSpec GCP_PARENT_SPEC = Spec.parent("gcp")
      .withRequiredProperties("pathToServiceKey")
      .withOptionalProperties("maxNumOfPages");
  public static final ParentSpec S3_PARENT_SPEC = Spec.parent("s3")
      .withOptionalProperties("accessKeyId", "secretAccessKey", "region", "maxNumOfPages");
  public static final ParentSpec AZURE_PARENT_SPEC = Spec.parent("azure")
      .withOptionalProperties("connectionString", "accountName", "accountKey", "maxNumOfPages");

  private static final Logger log = LoggerFactory.getLogger(FileConnector.class);

  private final List<URI> storageURIs;
  private final Map<String, StorageClient> storageClientMap;

  public FileConnector(Config config) throws ConnectorException {
    super(config, Spec.connector()
        .withRequiredProperties("pathsToStorage")
        .withOptionalParents(
            Spec.parent("filterOptions").withOptionalProperties("includes", "excludes", "modificationCutoff"),
            Spec.parent("fileOptions").withOptionalProperties("getFileContent", "handleArchivedFiles",
                "handleCompressedFiles", "moveToAfterProcessing", "moveToErrorFolder"),
            GCP_PARENT_SPEC, AZURE_PARENT_SPEC, S3_PARENT_SPEC)
        .withOptionalParentNames("fileHandlers"));

    List<String> pathsToStorage = config.getStringList("pathsToStorage");
    this.storageURIs = new ArrayList<>();

    for (String path : pathsToStorage) {
      try {
        URI newStorageURI = new URI(path);
        storageURIs.add(newStorageURI);
        log.debug("FileConnector to use path {} with scheme {}", path, newStorageURI.getScheme());
      } catch (URISyntaxException e) {
        throw new ConnectorException("Invalid path to storage: " + path, e);
      }
    }

    this.storageClientMap = StorageClient.createClients(config);

    // Cannot specify multiple storage paths and a moveTo of some kind
    if (storageURIs.size() > 1 && (config.hasPath("fileOptions.moveToAfterProcessing") || config.hasPath("fileOptions.moveToErrorFolder"))) {
      throw new IllegalArgumentException("FileConnector does not support multiple pathsToStorage and moveToAfterProcessing / moveToErrorFolder. Create individual FileConnectors.");
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
          storageClient.traverse(publisher, params);
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
    }
  }
}
