package com.kmwllc.lucille.connector.storageclients;

import static com.kmwllc.lucille.connector.FileConnector.GET_FILE_CONTENT;

import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

public abstract class BaseStorageClient implements StorageClient {

  protected Publisher publisher;
  protected String docIdPrefix;
  protected URI pathToStorageURI;
  protected String bucketOrContainerName;
  protected String startingDirectory;
  List<Pattern> excludes;
  List<Pattern> includes;
  Map<String, Object> cloudOptions;
  Config fileOptions;
  public Integer maxNumOfPages;
  protected boolean getFileContent;

  public BaseStorageClient(URI pathToStorageURI, Publisher publisher, String docIdPrefix, List<Pattern> excludes, List<Pattern> includes,
      Map<String, Object> cloudOptions, Config fileOptions) {
    this.publisher = publisher;
    this.docIdPrefix = docIdPrefix;
    this.pathToStorageURI = pathToStorageURI;
    this.bucketOrContainerName = getContainerOrBucketName();
    this.startingDirectory = getStartingDirectory();
    this.excludes = excludes;
    this.includes = includes;
    this.cloudOptions = cloudOptions;
    this.fileOptions = fileOptions;
    this.getFileContent = !fileOptions.hasPath(GET_FILE_CONTENT) || fileOptions.getBoolean(GET_FILE_CONTENT);
    this.maxNumOfPages = cloudOptions.containsKey("maxNumOfPages") ? (Integer) cloudOptions.get("maxNumOfPages") : 100;
  }

  public String getContainerOrBucketName() {
    return pathToStorageURI.getAuthority();
  }

  public String getStartingDirectory() {
    String startingDirectory = Objects.equals(pathToStorageURI.getPath(), "/") ? "" : pathToStorageURI.getPath();
    if (startingDirectory.startsWith("/")) return startingDirectory.substring(1);
    return startingDirectory;
  }

  public static boolean shouldIncludeFile(String filePath, List<Pattern> includes, List<Pattern> excludes) {
    return excludes.stream().noneMatch(pattern -> pattern.matcher(filePath).matches())
        && (includes.isEmpty() || includes.stream().anyMatch(pattern -> pattern.matcher(filePath).matches()));
  }

  //  // inputStream parameter will be closed outside of this method as well
//  private void handleArchiveFiles(Publisher publisher, InputStream inputStream) throws ArchiveException, IOException, ConnectorException {
//    try (BufferedInputStream bis = new BufferedInputStream(inputStream);
//        ArchiveInputStream<? extends ArchiveEntry> in = new ArchiveStreamFactory().createArchiveInputStream(bis)) {
//      ArchiveEntry entry = null;
//      while ((entry = in.getNextEntry()) != null) {
//        if (shouldIncludeFile(entry.getName(), includes, excludes) && !entry.isDirectory()) {
//          String entryExtension = FilenameUtils.getExtension(entry.getName());
//          if (FileHandler.supportsFileType(entryExtension, fileOptions)) {
//            handleStreamExtensionFiles(publisher, in, entryExtension, entry.getName());
//          } else {
//            // handle entry to be published as a normal document
//            Document doc = Document.create(createDocId(DigestUtils.md5Hex(entry.getName())));
//            doc.setField(FILE_PATH, entry.getName());
//            doc.setField(MODIFIED, entry.getLastModifiedDate().toInstant());
//            // entry does not have creation date
//            // some cases where entry.getSize() returns -1, so we use in.readAllBytes instead...
//            // cannot use in.available as it shows the remaining bytes including the rest of the files in the archive
//            byte[] content = in.readAllBytes();
//            doc.setField(SIZE, content.length);
//            if (getFileContent) {
//              doc.setField(CONTENT, content);
//            }
//            try {
//              publisher.publish(doc);
//            } catch (Exception e) {
//              throw new ConnectorException("Error occurred while publishing archive entry: " + entry.getName(), e);
//            }
//          }
//        }
//      }
//    }
//  }
//
//  // inputStream parameter will be closed outside of this method as well
//  private void handleStreamExtensionFiles(Publisher publisher, InputStream in, String fileExtension, String fileName)
//      throws ConnectorException {
//    try {
//      FileHandler handler = FileHandler.getFileHandler(fileExtension, fileOptions);
//      Iterator<Document> docIterator = handler.processFile(in.readAllBytes(), fileName);
//      while (docIterator.hasNext()) {
//        publisher.publish(docIterator.next());
//      }
//    } catch (Exception e) {
//      throw new ConnectorException("Error occurred while handling file: " + fileName, e);
//    }
//  }
//
//  // InputStream parameter will be closed outside of this method as well
//  private Document pathToDoc(Path path, InputStream in) throws ConnectorException {
//    final String docId = DigestUtils.md5Hex(path.toString());
//    final Document doc = Document.create(createDocId(docId));
//
//    try {
//      // get file attributes
//      BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
//
//      // setting fields on document
//      doc.setField(FILE_PATH, path.toAbsolutePath().normalize().toString());
//      doc.setField(MODIFIED, attrs.lastModifiedTime().toInstant());
//      doc.setField(CREATED, attrs.creationTime().toInstant());
//      // wont be able to get the size unless we read from the stream :/ so have to readBytes even though we set getFileContent to false
//      byte[] content = in.readAllBytes();
//      doc.setField(SIZE, content.length);
//      if (getFileContent) doc.setField(CONTENT, content);
//    } catch (Exception e) {
//      throw new ConnectorException("Error occurred getting/setting file attributes to document: " + path, e);
//    }
//    return doc;
//  }
//
//  private boolean isSupportedCompressedFileType(Path path) {
//    String fileName = path.getFileName().toString();
//
//    return fileName.endsWith(".gz");
//    // note that the following are supported by apache-commons-compress, but have yet to been tested, so commented out for now
//    // fileName.endsWith(".bz2") ||
//    // fileName.endsWith(".xz") ||
//    // fileName.endsWith(".lzma") ||
//    // fileName.endsWith(".br") ||
//    // fileName.endsWith(".pack") ||
//    // fileName.endsWith(".zst") ||
//    // fileName.endsWith(".Z");
//  }
//
//  private boolean isSupportedArchiveFileType(Path path) {
//    String fileName = path.getFileName().toString();
//    return isSupportedArchiveFileType(fileName);
//  }
//
//  private boolean isSupportedArchiveFileType(String string) {
//    return string.endsWith(".tar") ||
//        string.endsWith(".zip");
//    // note that the following are supported by apache-commons compress, but have yet to been tested, so commented out for now
//    // string.endsWith(".7z") ||
//    // string.endsWith(".ar") ||
//    // string.endsWith(".arj") ||
//    // string.endsWith(".cpio") ||
//    // string.endsWith(".dump") ||
//    // string.endsWith(".dmp");
//  }
}
