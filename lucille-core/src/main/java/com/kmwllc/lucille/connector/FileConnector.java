package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.connector.cloudstorageclients.CloudStorageClient;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.FileHandler;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FilenameUtils;
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
 *    "maxNumOfPages" : number of reference to the files loaded into memory in a single fetch request, defaults to 100
 */
public class FileConnector extends AbstractConnector {

  public static final String FILE_PATH = "file_path";
  public static final String MODIFIED = "file_modification_date";
  public static final String CREATED = "file_creation_date";
  public static final String SIZE = "file_size_bytes";
  public static final String CONTENT = "file_content";

  // cloudOption Keys
  public static final String GET_FILE_CONTENT = "getFileContent";
  public static final String AZURE_CONNECTION_STRING = "connectionString";
  public static final String AZURE_ACCOUNT_NAME = "accountName";
  public static final String AZURE_ACCOUNT_KEY = "accountKey";
  public static final String S3_REGION = "region";
  public static final String S3_ACCESS_KEY_ID = "accessKeyId";
  public static final String S3_SECRET_ACCESS_KEY = "secretAccessKey";
  public static final String GOOGLE_SERVICE_KEY = "pathToServiceKey";

  private static final Logger log = LoggerFactory.getLogger(FileConnector.class);

  private final String pathToStorage;
  private final Map<String, Object> cloudOptions;
  private final List<Pattern> includes;
  private final List<Pattern> excludes;
  private CloudStorageClient cloudStorageClient;
  private final URI storageURI;
  private final boolean getFileContent;
  private final Map<String, Object> fileOptions;
  private final boolean handleCompressedFiles;

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
    this.getFileContent = config.hasPath("getFileContent") ? config.getBoolean("getFileContent") : true;
    this.cloudOptions = config.hasPath("cloudOptions") ? config.getConfig("cloudOptions").root().unwrapped() : Map.of();
    this.fileOptions = config.hasPath("fileOptions") ? config.getConfig("fileOptions").root().unwrapped() : Map.of();
    this.handleCompressedFiles = config.hasPath("handleCompressedFiles") ? config.getBoolean("handleCompressedFiles") : false;
    try {
      this.storageURI = new URI(pathToStorage);
      log.info("using path {} with scheme {}", pathToStorage, storageURI.getScheme());
    } catch (URISyntaxException e) {
      throw new ConnectorException("Invalid path to storage: " + pathToStorage, e);
    }
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    if (storageURI.getScheme() != null) {
      validateCloudOptions(storageURI, cloudOptions);
      cloudOptions.put(GET_FILE_CONTENT, getFileContent);
      cloudStorageClient = CloudStorageClient.getClient(storageURI, publisher, getDocIdPrefix(), excludes, includes, cloudOptions);
      try {
        cloudStorageClient.init();
        cloudStorageClient.publishFiles();
      } catch (Exception e) {
        throw new ConnectorException("Error occurred while initializing client or publishing files.", e);
      } finally {
        try {
          cloudStorageClient.shutdown();
        } catch (Exception e) {
          throw new ConnectorException("Error occurred while shutting down client.", e);
        }
      }
      return;
    }

    FileSystem fs = FileSystems.getDefault();
    // get current working directory
    Path startingDirectory = fs.getPath(pathToStorage);

    try (Stream<Path> paths = Files.walk(startingDirectory)) {
      paths.filter(this::isValidPath)
          .forEachOrdered(path -> {
            String fileExtension = FilenameUtils.getExtension(path.toString());
            try {
              // handle compressed files and after decompression, regardless if archived or not
              if (handleCompressedFiles && isSupportedCompressedFileType(path)) {
                // unzip the file
                try (CompressorInputStream compressorStream = new CompressorStreamFactory().createCompressorInputStream(
                    new BufferedInputStream(Files.newInputStream(path)))) {
                  if (!isArchivedAfterDecompression(path)) {
                    handleStreamExtensionFiles(publisher, compressorStream, fileExtension, path.toString());
                  } else {
                    handleArchiveFiles(publisher, compressorStream, fileExtension);
                  }
                }
                return;
              }

              // handle archived files that are not zipped
              if (isSupportedArchiveFileType(path)) {
                handleArchiveFiles(publisher, Files.newInputStream(path), fileExtension);
              }

              // not archived nor zip, handling supported file types if fileOptions are provided
              if (!fileOptions.isEmpty() && FileHandler.supportsFileType(fileExtension, fileOptions)) {
                // instantiate the right FileHandler based on path
                FileHandler handler = FileHandler.getFileHandler(fileExtension, fileOptions);
                Iterator<Document> docIterator = handler.processFile(path);
                // once docIterator.hasNext() is false, it will close its resources in handler and return
                while (docIterator.hasNext()) {
                  publisher.publish(docIterator.next());
                }
                return;
              }

              // default handling of files
              Document doc = pathToDoc(path);
              publisher.publish(doc);
            } catch (Exception e) {
              log.error("Unable to publish document '{}', SKIPPING", path, e);
            }
          });
    } catch (InvalidPathException e) {
      throw new ConnectorException("Path string provided cannot be converted to a Path.", e);
    } catch (SecurityException | IOException e) {
      throw new ConnectorException("Error while traversing file system.", e);
    } finally {
      if (fs != null) {
        try {
          fs.close();
        } catch (UnsupportedOperationException e) {
          // Some file systems may not need closing
        } catch (IOException e) {
          throw new ConnectorException("Failed to close file system.", e);
        }
      }
    }
  }

  private void handleArchiveFiles(Publisher publisher, InputStream inputStream, String fileExtension) throws ArchiveException, IOException, ConnectorException {
    try (ArchiveInputStream<? extends ArchiveEntry> in =
        new ArchiveStreamFactory().createArchiveInputStream(new BufferedInputStream(inputStream))) {
      ArchiveEntry entry = null;
      while ((entry = in.getNextEntry()) != null) {
        if (shouldIncludeFile(entry.getName(), includes, excludes) && !entry.isDirectory()) {
          handleStreamExtensionFiles(publisher, in, fileExtension, entry.getName());
        }
      }
    }
  }

  private void handleStreamExtensionFiles(Publisher publisher, InputStream in, String fileExtension, String fileName)
      throws ConnectorException {
    try {
      FileHandler handler = FileHandler.getFileHandler(fileExtension, fileOptions);
      Iterator<Document> docIterator = handler.processFile(in.readAllBytes());
      while (docIterator.hasNext()) {
        publisher.publish(docIterator.next());
      }
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while handling file: " + fileName, e);
    }
  }

  private void validateCloudOptions(URI storageURI, Map<String, Object> cloudOptions) {
    if (storageURI.getScheme().equals("gs")) {
      if (!cloudOptions.containsKey(GOOGLE_SERVICE_KEY)) {
        throw new IllegalArgumentException("Missing 'pathToServiceKey' in cloudOptions for Google Cloud storage.");
      }
    } else if (storageURI.getScheme().equals("s3")) {
      if (!cloudOptions.containsKey(S3_ACCESS_KEY_ID) || !cloudOptions.containsKey(S3_SECRET_ACCESS_KEY) || !cloudOptions.containsKey(S3_REGION)) {
        throw new IllegalArgumentException("Missing '" + S3_ACCESS_KEY_ID + "' or '" + S3_SECRET_ACCESS_KEY
            + "' or '" + S3_REGION + "' in cloudOptions for s3 storage.");
      }
    } else if (storageURI.getScheme().equals("https") && storageURI.getAuthority().contains("blob.core.windows.net")) {
      if (!cloudOptions.containsKey(AZURE_CONNECTION_STRING) &&
          !(cloudOptions.containsKey(AZURE_ACCOUNT_NAME) && cloudOptions.containsKey(AZURE_ACCOUNT_KEY))) {
        throw new IllegalArgumentException("Either '" + AZURE_CONNECTION_STRING + "' or '" + AZURE_ACCOUNT_NAME
            + "' & '" + AZURE_ACCOUNT_KEY + "' has to be in cloudOptions for Azure storage.");
      }
    } else {
      throw new IllegalArgumentException("Unsupported client type: " + storageURI.getScheme());
    }
  }

  private boolean isValidPath(Path path) {
    if (!Files.isRegularFile(path)) {
      return false;
    }

    return shouldIncludeFile(path.toString(), includes, excludes);
  }

  public static boolean shouldIncludeFile(String filePath, List<Pattern> includes, List<Pattern> excludes) {
    return excludes.stream().noneMatch(pattern -> pattern.matcher(filePath).matches())
        && (includes.isEmpty() || includes.stream().anyMatch(pattern -> pattern.matcher(filePath).matches()));
  }

  private Document pathToDoc(Path path) throws ConnectorException {
    final String docId = DigestUtils.md5Hex(path.toString());
    final Document doc = Document.create(createDocId(docId));

    try {
      // get file attributes
      BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);

      // setting fields on document
      doc.setField(FILE_PATH, path.toAbsolutePath().toString());
      doc.setField(MODIFIED, attrs.lastModifiedTime().toInstant());
      doc.setField(CREATED, attrs.creationTime().toInstant());
      doc.setField(SIZE, attrs.size());
      if (getFileContent) doc.setField(CONTENT, Files.readAllBytes(path));
    } catch (Exception e) {
      throw new ConnectorException("Error occurred getting/setting file attributes to document: " + path, e);
    }
    return doc;
  }

  private boolean isArchivedAfterDecompression(Path path) {
    String fileName = path.getFileName().toString();
    int firstDotIndex = fileName.indexOf('.');
    int lastDotIndex = fileName.lastIndexOf('.');

    if (firstDotIndex != -1 && lastDotIndex != -1 && firstDotIndex != lastDotIndex) {
      fileName = fileName.substring(0, lastDotIndex);
      return isSupportedArchiveFileType(Path.of(fileName));
    } else if (firstDotIndex != -1) {
      return false;
    } else {
      return false;
    }
  }

  private boolean isSupportedCompressedFileType(Path path) {
    String fileName = path.getFileName().toString();

    return fileName.endsWith(".gz") ||
        fileName.endsWith(".bz2") ||
        fileName.endsWith(".xz") ||
        fileName.endsWith(".lzma") ||
        fileName.endsWith(".br") ||
        fileName.endsWith(".pack") ||
        fileName.endsWith(".zst") ||
        fileName.endsWith(".Z");
  }

  private boolean isSupportedArchiveFileType(Path path) {
    String fileName = path.getFileName().toString();

    return fileName.endsWith(".7z") ||
        fileName.endsWith(".ar") ||
        fileName.endsWith(".arj") ||
        fileName.endsWith(".cpio") ||
        fileName.endsWith(".dump") ||
        fileName.endsWith(".dmp") ||
        fileName.endsWith(".tar") ||
        fileName.endsWith(".zip");
  }
}
