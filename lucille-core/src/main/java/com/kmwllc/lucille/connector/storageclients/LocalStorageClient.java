package com.kmwllc.lucille.connector.storageclients;

import static com.kmwllc.lucille.connector.FileConnector.CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.CREATED;
import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static com.kmwllc.lucille.connector.FileConnector.MODIFIED;
import static com.kmwllc.lucille.connector.FileConnector.SIZE;

import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.FileHandler;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalStorageClient extends BaseStorageClient {
  private static final Logger log = LoggerFactory.getLogger(FileConnector.class);
  private FileSystem fs;
  private Path startingDirectoryPath;

  public LocalStorageClient(URI pathToStorageURI, Publisher publisher, String docIdPrefix, List<Pattern> excludes, List<Pattern> includes,
      Map<String, Object> cloudOptions, Config fileOptions) {
    super(pathToStorageURI, publisher, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
  }

  @Override
  public void init() {
    fs = FileSystems.getDefault();
    // get current working directory path
    startingDirectoryPath = fs.getPath(startingDirectory);
  }

  @Override
  public String getStartingDirectory() {
    return pathToStorageURI.getPath();
  }

  @Override
  public void shutdown() throws Exception {
    if (fs != null) {
      try {
        fs.close();
        FileHandler.closeAllHandlers();
      } catch (UnsupportedOperationException e) {
        // Some file systems may not need closing
        fs = null;
      } catch (IOException e) {
        throw new IOException("Failed to close file system.", e);
      }
    }
  }

  @Override
  public void publishFiles() throws Exception {
    try (Stream<Path> paths = Files.walk(startingDirectoryPath)) {
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
                publishUsingFileHandler(fileExtension, path);
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
    }
  }

  private boolean isValidPath(Path path) {
    if (!Files.isRegularFile(path)) {
      return false;
    }

    return shouldIncludeFile(path.toString(), includes, excludes);
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
}
