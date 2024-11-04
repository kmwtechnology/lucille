package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.connector.cloudstorageclients.CloudStorageClient;
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
 *
 * Optional:
 *  "maxNumOfPages" : number of reference to the files loaded into memory in a single fetch request, defaults to 100
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
  private CloudStorageClient cloudStorageClient;
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
      cloudStorageClient = CloudStorageClient.getClient(storageURI, publisher, getDocIdPrefix(), excludes, includes, cloudOptions);
      cloudStorageClient.init();
      cloudStorageClient.publishFiles();
      try {
        cloudStorageClient.shutdown();
      } catch (Exception e) {
        throw new ConnectorException("Failed to shutdown client.", e);
      }
    } else {
      FileSystem fs = null;
      try {
        fs = FileSystems.getDefault();
        // get current working directory
        Path startingDirectory = fs.getPath(pathToStorage);

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
  }

  private void validateCloudOptions(URI storageURI, Map<String, Object> cloudOptions) {
    if (storageURI.getScheme().equals("gs")) {
      if (!cloudOptions.containsKey("pathToServiceKey")) {
        throw new IllegalArgumentException("Missing 'pathToServiceKey' in cloudOptions for Google Cloud storage.");
      }
    } else if (storageURI.getScheme().equals("s3")) {
      if (!cloudOptions.containsKey("accessKeyId") || !cloudOptions.containsKey("secretAccessKey") || !cloudOptions.containsKey("region")) {
        throw new IllegalArgumentException("Missing 'accountName' or 'accountKey' or 'region' in cloudOptions for s3 storage.");
      }
    } else if (storageURI.getScheme().equals("https") && storageURI.getAuthority().contains("blob.core.windows.net")) {
      if (!cloudOptions.containsKey("connectionString") &&
          !(cloudOptions.containsKey("accountName") && cloudOptions.containsKey("accountKey"))) {
        throw new IllegalArgumentException("Either 'connectionString' or 'accountName' & 'accountKey' has to be in cloudOptions"
            + "for Azure storage.");
      }
    } else {
      throw new IllegalArgumentException("Unsupported client type: " + storageURI.getScheme());
    }
  }

  private boolean isValidPath(Path path) {
    if (!Files.isRegularFile(path)) {
      return false;
    }

    if (excludes.stream().anyMatch(pattern -> pattern.matcher(path.toString()).matches())
        || (!includes.isEmpty() && includes.stream().noneMatch(pattern -> pattern.matcher(path.toString()).matches()))) {
      log.debug("Skipping file because of include or exclude regex: {}", path);
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
}
