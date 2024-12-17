package com.kmwllc.lucille.connector.storageclient;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleStorageClient extends BaseStorageClient {

  private static final Logger log = LoggerFactory.getLogger(GoogleStorageClient.class);
  private Storage storage;

  public GoogleStorageClient(URI pathToStorage, Publisher publisher, String docIdPrefix, List<Pattern> excludes, List<Pattern> includes,
      Map<String, Object> cloudOptions, Config fileOptions) {
    super(pathToStorage, publisher, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
  }

  @Override
  public void init() {
    try (FileInputStream serviceAccountStream = new FileInputStream((String) cloudOptions.get(FileConnector.GOOGLE_SERVICE_KEY))) {
      storage = StorageOptions.newBuilder()
          .setCredentials(ServiceAccountCredentials.fromStream(serviceAccountStream))
          .build()
          .getService();
    } catch (IOException e) {
      throw new IllegalArgumentException("Error occurred getting/using credentials from pathToServiceKey", e);
    }

    try {
      initializeFileHandlers();
    } catch (ConnectorException e) {
      throw new IllegalArgumentException("Error occurred initializing FileHandlers", e);
    }
  }

  @Override
  public void shutdown() throws Exception {
    if (storage != null) {
      storage.close();
    }
    // clear all FileHandlers if any
    clearFileHandlers();
  }

  @Override
  public void publishFiles() throws Exception {
    Page<Blob> page = storage.list(bucketOrContainerName, BlobListOption.prefix(startingDirectory), BlobListOption.pageSize(maxNumOfPages));
    do {
      page.streamAll()
          .forEachOrdered(blob -> {
            if (isValid(blob)) {
              try {
                String filePath = blob.getName();
                String fileExtension = FilenameUtils.getExtension(filePath);

                // handle compressed files if needed
                if (handleCompressedFiles && isSupportedCompressedFileType(filePath)) {
                  byte[] content = blob.getContent();
                  // unzip the file, compressorStream will be closed when try block is exited
                  try (BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream(content));
                      CompressorInputStream compressorStream = new CompressorStreamFactory().createCompressorInputStream(bis)) {
                    // we can remove the last extension from path knowing before we confirmed that it has a compressed extension
                    String unzippedFileName = FilenameUtils.removeExtension(filePath);
                    if (handleArchivedFiles && isSupportedArchiveFileType(unzippedFileName)) {
                      handleArchiveFiles(publisher, compressorStream);
                    } else if (!fileOptions.isEmpty() && FileHandler.supportAndContainFileType(FilenameUtils.getExtension(unzippedFileName), fileOptions)) {
                      handleStreamExtensionFiles(publisher, compressorStream, FilenameUtils.getExtension(unzippedFileName), filePath);
                    } else {
                      Document doc = blobToDoc(blob, bucketOrContainerName, compressorStream, unzippedFileName);
                      publisher.publish(doc);
                    }
                  }
                  return;
                }

                // handle archived files if needed
                if (handleArchivedFiles && isSupportedArchiveFileType(filePath)) {
                  byte[] content = blob.getContent();
                  try (InputStream is = new ByteArrayInputStream(content)) {
                    handleArchiveFiles(publisher, is);
                  }
                  return;
                }

                // handle file types if needed
                if (!fileOptions.isEmpty() && FileHandler.supportAndContainFileType(fileExtension, fileOptions)) {
                  // get the file content
                  byte[] content = blob.getContent();
                  // instantiate the right FileHandler and publish based on content
                  publishUsingFileHandler(fileExtension, content, filePath);
                  return;
                }

                Document doc = blobToDoc(blob, bucketOrContainerName);
                publisher.publish(doc);
              } catch (Exception e) {
                log.error("Unable to publish document '{}', SKIPPING", blob.getName(), e);
              }
            }
          });
      page = page.hasNextPage() ? page.getNextPage() : null;
    } while (page != null);
  }

  private boolean isValid(Blob blob) {
    if (blob.isDirectory()) return false;

    return shouldIncludeFile(blob.getName(), includes, excludes);
  }

  private Document blobToDoc(Blob blob, String bucketName) throws IOException {
    final String docId = DigestUtils.md5Hex(blob.getName());
    final Document doc = Document.create(docIdPrefix + docId);
    try {
      doc.setField(FileConnector.FILE_PATH, pathToStorageURI.getScheme() + "://" + bucketName + "/" + blob.getName());
      if (blob.getUpdateTimeOffsetDateTime() != null) {
        doc.setField(FileConnector.MODIFIED, blob.getUpdateTimeOffsetDateTime().toInstant());
      }
      if (blob.getCreateTimeOffsetDateTime() != null) {
        doc.setField(FileConnector.CREATED, blob.getCreateTimeOffsetDateTime().toInstant());
      }
      doc.setField(FileConnector.SIZE, blob.getSize());
      if (getFileContent) {
        doc.setField(FileConnector.CONTENT, blob.getContent());
      }
    } catch (Exception e) {
      throw new IOException("Error occurred getting/setting file attributes to document: " + blob.getName(), e);
    }
    return doc;
  }

  private Document blobToDoc(Blob blob, String bucketName, InputStream content, String nameWithoutExtension) throws IOException {
    final String docId = DigestUtils.md5Hex(nameWithoutExtension);
    final Document doc = Document.create(docIdPrefix + docId);
    try {
      doc.setField(FileConnector.FILE_PATH, pathToStorageURI.getScheme() + "://" + bucketName + "/" + nameWithoutExtension);
      if (blob.getUpdateTimeOffsetDateTime() != null) {
        doc.setField(FileConnector.MODIFIED, blob.getUpdateTimeOffsetDateTime().toInstant());
      }
      if (blob.getCreateTimeOffsetDateTime() != null) {
        doc.setField(FileConnector.CREATED, blob.getCreateTimeOffsetDateTime().toInstant());
      }
      // unable to get decompressed file size
      if (getFileContent) {
        doc.setField(FileConnector.CONTENT, content.readAllBytes());
      }
    } catch (Exception e) {
      throw new IOException("Error occurred getting/setting file attributes to document: " + blob.getName(), e);
    }
    return doc;
  }

  // Only for testing
  void setStorageForTesting(Storage storage) {
    this.storage = storage;
  }
}
