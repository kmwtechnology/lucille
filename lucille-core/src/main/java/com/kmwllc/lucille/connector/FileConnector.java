package com.kmwllc.lucille.connector;

import com.azure.storage.blob.nio.AzureFileSystem;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authentication:
 *
 * Google: https://github.com/googleapis/google-cloud-java#authentication
 * Azure: https://learn.microsoft.com/en-us/java/api/overview/azure/storage-blob-nio-readme?view=azure-java-preview#authenticate-the-client
 * Amazon: https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html
 *
 * Additional Cloud Options based on providers:
 *
 * Google:
 * - "workingDirectory":
 * - "permitEmptyPathComponents":
 * - "stripPrefixSlash":
 * - "usePseudoDirectories":
 * - "blockSize":
 * - "maxChannelReopens":
 * - "userProject":
 * - "useUserProjectOnlyForRequesterPaysBuckets":
 * - "retryableHttpCodes":
 * - "reopenableExceptions":
 *
 * Azure:
 * - "accountName" (Not optional)
 * - "accountKey" (Not optional)
 * - "AzureStorageUploadBlockSize"
 * - "AzureStoragePutBlobThreshold"
 * - "AzureStorageMaxConcurrencyPerRequest"
 * - "AzureStorageDownloadResumeRetries"
 * - "AzureStorageFileStores"
 * - "AzureStorageSkipInitialContainerCheck"
 *
 * Amazon:
 * - "locationConstraint"
 * - "acl"
 * - "grantFullControl"
 * - "grantRead"
 * - "grantReadACP"
 * - "grantWrite"
 * - "grantWriteACP"
 */

public class FileConnector extends AbstractConnector {

  public static final String FILE_PATH = "file_path";
  public static final String MODIFIED = "file_modification_date";
  public static final String CREATED = "file_creation_date";
  public static final String SIZE = "file_size_bytes";
  public static final String CONTENT = "file_content";

  private static final Logger log = LoggerFactory.getLogger(FileConnector.class);

  private final String pathToStorage;
  private final Map<String, Object> cloudOptions;
  private final List<Pattern> includes;
  private final List<Pattern> excludes;

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
    if (pathToStorage.startsWith("azb://") && (!cloudOptions.containsKey("accountName") || !cloudOptions.containsKey("accountKey"))) {
      throw new ConnectorException("Missing 'accountName' or 'accountKey' in cloudOptions for Azure storage.");
    }
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    FileSystem fs = retrieveFileSystem(pathToStorage);

    try {
      // get current working directory
      Path startingDirectory = fs.provider().getScheme().equals("file")
          ? fs.getPath(pathToStorage)
          : fs.getPath(parseCloudPath(pathToStorage));

      try (Stream<Path> paths = Files.walk(startingDirectory)) {
        paths.filter(this::isValidPath)
            .forEachOrdered(path -> {
              try {
                Document doc = pathToDoc(path);
                publisher.publish(doc);
              } catch (Exception e) {
                log.error("Unable to publish document '{}', SKIPPING", path, e);
              }
            });
      }
    } catch (InvalidPathException | URISyntaxException e) {
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

  private String parseCloudPath(String pathToStorage) throws URISyntaxException {
    URI uri = new URI(pathToStorage);
    if (uri.getPath().length() > 1) {
      return uri.getPath();
    } else {
      return ".";
    }
  }

  private FileSystem retrieveFileSystem(String pathToStorage) throws ConnectorException {
    try {
      if (pathToStorage.startsWith("gs://")) {
        return initGoogleFs();
      } else if (pathToStorage.startsWith("azb://")) {
        return initAzureFs();
      } else if (pathToStorage.startsWith("s3://")) {
        return initAmazonFs();
      }
    } catch (IOException e) {
      throw new ConnectorException("Failed to retrieve file system.", e);
    }

    return FileSystems.getDefault();
  }

  private boolean isValidPath(Path path) {
    if (!Files.isRegularFile(path)) {
      return false;
    }

    if (excludes.stream().anyMatch(pattern -> pattern.matcher(path.toString()).matches())
        || (!includes.isEmpty() && includes.stream().noneMatch(pattern -> pattern.matcher(path.toString()).matches()))) {
      log.debug("Skipping file because of include or exclude regex: " + path);
      return false;
    }

    return true;
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
      doc.setField(CONTENT, Files.readAllBytes(path));
    } catch (Exception e) {
      throw new ConnectorException("Error occurred getting/setting file attributes to document: " + path, e);
    }
    return doc;
  }

  private FileSystem initGoogleFs() throws IOException {
    try {
      return FileSystems.newFileSystem(URI.create(pathToStorage), cloudOptions);
    } catch (Exception e) {
      throw new IOException("Failed to initialize Google Cloud Storage filesystem", e);
    }
  }

  private FileSystem initAzureFs() throws IOException {
    try {
      StorageSharedKeyCredential credential = new StorageSharedKeyCredential(
          (String) cloudOptions.get("accountName"), (String) cloudOptions.get("accountKey"));
      cloudOptions.put(AzureFileSystem.AZURE_STORAGE_SHARED_KEY_CREDENTIAL, credential);
      return FileSystems.newFileSystem(URI.create(pathToStorage), cloudOptions);
    } catch (Exception e) {
      throw new IOException("Failed to initialize Azure Blob Storage filesystem", e);
    }
  }

  private FileSystem initAmazonFs() throws IOException {
    try {
      return FileSystems.newFileSystem(URI.create(pathToStorage), cloudOptions);
    } catch (Exception e) {
      throw new IOException("Failed to initialize Amazon S3 filesystem", e);
    }
  }
}
