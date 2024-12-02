package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.connector.cloudstorageclients.CloudStorageClient;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.FileHandler;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
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
  private final Config fileOptions;
  private final boolean handleCompressedFiles;
  private final boolean handleArchivedFiles;

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
    this.fileOptions = config.hasPath("fileOptions") ? config.getConfig("fileOptions") : ConfigFactory.empty();
    this.handleCompressedFiles = config.hasPath("handleCompressedFiles") ? config.getBoolean("handleCompressedFiles") : false;
    this.handleArchivedFiles = config.hasPath("handleArchivedFiles") ? config.getBoolean("handleArchivedFiles") : false;
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
    // paths will be close via the try resources block
    try (Stream<Path> paths = Files.walk(startingDirectory)) {
      paths.filter(this::isValidPath)
          .forEachOrdered(path -> {
            String fileExtension = FilenameUtils.getExtension(path.toString());
            try {
              //TODO: investigate resource usage for handling compressed and archived files before enabling them
//              // handle compressed files and after decompression, handle regardless if archived or not
//              if (handleCompressedFiles && isSupportedCompressedFileType(path)) {
//                // unzip the file, compressorStream will be closed when try block is exited
//                try (BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(path));
//                    CompressorInputStream compressorStream = new CompressorStreamFactory().createCompressorInputStream(bis)) {
//                  // we can remove the last extension from path knowing before we confirmed that it has a compressed extension
//                  String unzippedFileName = path.getFileName().toString().replaceFirst("[.][^.]+$", "");
//                  if (handleArchivedFiles && isSupportedArchiveFileType(unzippedFileName)) {
//                    handleArchiveFiles(publisher, compressorStream);
//                  } else if (!fileOptions.isEmpty() && FileHandler.supportsFileType(FilenameUtils.getExtension(unzippedFileName), fileOptions)) {
//                    handleStreamExtensionFiles(publisher, compressorStream, FilenameUtils.getExtension(unzippedFileName), path.toString());
//                  } else {
//                    Document doc = pathToDoc(path, compressorStream);
//                    publisher.publish(doc);
//                  }
//                }
//                return;
//              }
//
//              // handle archived files that are not zipped
//              if (handleArchivedFiles && isSupportedArchiveFileType(path)) {
//                try (InputStream in = Files.newInputStream(path)) {
//                  handleArchiveFiles(publisher, in);
//                }
//                return;
//              }

              // not archived nor zip, handling supported file types if fileOptions are provided
              if (!fileOptions.isEmpty() && FileHandler.supportsFileType(fileExtension, fileOptions)) {
                // instantiate the right FileHandler based on path
                FileHandler handler = FileHandler.getFileHandler(fileExtension, fileOptions);
                Iterator<Document> docIterator = handler.processFile(path);
                // once docIterator.hasNext() is false, it will close its resources in handler and return
                while (docIterator.hasNext()) {
                  try {
                    Document doc = docIterator.next();
                    if (doc != null) {
                      publisher.publish(doc);
                    }
                    // TODO: need to handle CSV post processing options like move to processed/error folders
                  } catch (Exception e) {
                    // if we fail to publish a document, we log the error and continue to the next document
                    // to "finish" the iterator and close its resources
                    log.error("Unable to publish document '{}', SKIPPING", path, e);
                  }
                }
                return;
              }

              // default handling of files
              Document doc = pathToDoc(path);
              publisher.publish(doc);
            } catch (Exception e) {
              // TODO: add error folders if csv is in fileConfigs and moveToErrorFolder exists
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
          FileHandler.closeAllHandlers();
        } catch (UnsupportedOperationException e) {
          // Some file systems may not need closing
        } catch (IOException e) {
          throw new ConnectorException("Failed to close file system.", e);
        }
      }
    }
  }

  // inputStream parameter will be closed outside of this method as well
  private void handleArchiveFiles(Publisher publisher, InputStream inputStream) throws ArchiveException, IOException, ConnectorException {
    try (BufferedInputStream bis = new BufferedInputStream(inputStream);
        ArchiveInputStream<? extends ArchiveEntry> in = new ArchiveStreamFactory().createArchiveInputStream(bis)) {
      ArchiveEntry entry = null;
      while ((entry = in.getNextEntry()) != null) {
        if (shouldIncludeFile(entry.getName(), includes, excludes) && !entry.isDirectory()) {
          String entryExtension = FilenameUtils.getExtension(entry.getName());
          if (FileHandler.supportsFileType(entryExtension, fileOptions)) {
            handleStreamExtensionFiles(publisher, in, entryExtension, entry.getName());
          } else {
            // handle entry to be published as a normal document
            Document doc = Document.create(createDocId(DigestUtils.md5Hex(entry.getName())));
            doc.setField(FILE_PATH, entry.getName());
            doc.setField(MODIFIED, entry.getLastModifiedDate().toInstant());
            // entry does not have creation date
            // some cases where entry.getSize() returns -1, so we use in.readAllBytes instead...
            // cannot use in.available as it shows the remaining bytes including the rest of the files in the archive
            byte[] content = in.readAllBytes();
            doc.setField(SIZE, content.length);
            if (getFileContent) {
              doc.setField(CONTENT, content);
            }
            try {
              publisher.publish(doc);
            } catch (Exception e) {
              throw new ConnectorException("Error occurred while publishing archive entry: " + entry.getName(), e);
            }
          }
        }
      }
    }
  }

  // inputStream parameter will be closed outside of this method as well
  private void handleStreamExtensionFiles(Publisher publisher, InputStream in, String fileExtension, String fileName)
      throws ConnectorException {
    try {
      FileHandler handler = FileHandler.getFileHandler(fileExtension, fileOptions);
      Iterator<Document> docIterator = handler.processFile(in.readAllBytes(), fileName);
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
      doc.setField(FILE_PATH, path.toAbsolutePath().normalize().toString());
      doc.setField(MODIFIED, attrs.lastModifiedTime().toInstant());
      doc.setField(CREATED, attrs.creationTime().toInstant());
      doc.setField(SIZE, attrs.size());
      if (getFileContent) doc.setField(CONTENT, Files.readAllBytes(path));
    } catch (Exception e) {
      throw new ConnectorException("Error occurred getting/setting file attributes to document: " + path, e);
    }
    return doc;
  }

  // InputStream parameter will be closed outside of this method as well
  private Document pathToDoc(Path path, InputStream in) throws ConnectorException {
    final String docId = DigestUtils.md5Hex(path.toString());
    final Document doc = Document.create(createDocId(docId));

    try {
      // get file attributes
      BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);

      // setting fields on document
      doc.setField(FILE_PATH, path.toAbsolutePath().normalize().toString());
      doc.setField(MODIFIED, attrs.lastModifiedTime().toInstant());
      doc.setField(CREATED, attrs.creationTime().toInstant());
      // wont be able to get the size unless we read from the stream :/ so have to readBytes even though we set getFileContent to false
      byte[] content = in.readAllBytes();
      doc.setField(SIZE, content.length);
      if (getFileContent) doc.setField(CONTENT, content);
    } catch (Exception e) {
      throw new ConnectorException("Error occurred getting/setting file attributes to document: " + path, e);
    }
    return doc;
  }

  private boolean isSupportedCompressedFileType(Path path) {
    String fileName = path.getFileName().toString();

    return fileName.endsWith(".gz");
      // note that the following are supported by apache-commons-compress, but have yet to been tested, so commented out for now
      // fileName.endsWith(".bz2") ||
      // fileName.endsWith(".xz") ||
      // fileName.endsWith(".lzma") ||
      // fileName.endsWith(".br") ||
      // fileName.endsWith(".pack") ||
      // fileName.endsWith(".zst") ||
      // fileName.endsWith(".Z");
  }

  private boolean isSupportedArchiveFileType(Path path) {
    String fileName = path.getFileName().toString();
    return isSupportedArchiveFileType(fileName);
  }

  private boolean isSupportedArchiveFileType(String string) {
    return string.endsWith(".tar") ||
       string.endsWith(".zip");
      // note that the following are supported by apache-commons compress, but have yet to been tested, so commented out for now
      // string.endsWith(".7z") ||
      // string.endsWith(".ar") ||
      // string.endsWith(".arj") ||
      // string.endsWith(".cpio") ||
      // string.endsWith(".dump") ||
      // string.endsWith(".dmp");
  }
}
