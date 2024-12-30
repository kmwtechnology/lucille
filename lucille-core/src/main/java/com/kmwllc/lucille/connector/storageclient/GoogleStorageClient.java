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
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleStorageClient extends BaseStorageClient {

  private static final Logger log = LoggerFactory.getLogger(GoogleStorageClient.class);
  private Storage storage;

  public GoogleStorageClient(URI pathToStorage, String docIdPrefix, List<Pattern> excludes, List<Pattern> includes,
      Map<String, Object> cloudOptions, Config fileOptions) {
    super(pathToStorage, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
  }

  @Override
  public void init() throws ConnectorException{
    try (FileInputStream serviceAccountStream = new FileInputStream((String) cloudOptions.get(FileConnector.GOOGLE_SERVICE_KEY))) {
      storage = StorageOptions.newBuilder()
          .setCredentials(ServiceAccountCredentials.fromStream(serviceAccountStream))
          .build()
          .getService();
    } catch (IOException e) {
      throw new ConnectorException("Error occurred building storage client", e);
    }

    initializeFileHandlers();
  }

  @Override
  public void shutdown() throws IOException {
    if (storage != null) {
      try {
        storage.close();
      } catch (Exception e) {
        throw new IOException("Error occurred closing storage", e);
      }
    }
    // clear all FileHandlers if any
    clearFileHandlers();
  }

  @Override
  public void traverse(Publisher publisher) throws Exception {
    Page<Blob> page = storage.list(bucketOrContainerName, BlobListOption.prefix(startingDirectory), BlobListOption.pageSize(maxNumOfPages));
    do {
      page.streamAll()
          .forEachOrdered(blob -> {
            if (isValid(blob)) {
              String fullPathStr = getFullPath(blob);
              String fileExtension = FilenameUtils.getExtension(fullPathStr);
              tryProcessAndPublishFile(publisher, fullPathStr, fileExtension, new FileReference(blob));
            }
          });
      page = page.hasNextPage() ? page.getNextPage() : null;
    } while (page != null);
  }

  @Override
  protected Document convertFileReferenceToDoc(FileReference fileReference) {
    Blob blob = fileReference.getBlob();

    try {
      return blobToDoc(blob);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to convert blob '" + blob.getName() + "' to Document", e);
    }
  }

  @Override
  protected Document convertFileReferenceToDoc(FileReference fileReference, InputStream in, String decompressedFullPathStr) {
    Blob blob = fileReference.getBlob();

    try {
      return blobToDoc(blob, in, decompressedFullPathStr);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to convert blob '" + blob.getName() + "' to Document", e);
    }
  }

  @Override
  protected byte[] getFileReferenceContent(FileReference fileReference) {
    Blob blob = fileReference.getBlob();
    return blob.getContent();
  }

  @Override
  protected InputStream getFileReferenceContentStream(FileReference fileReference) {
    byte[] content = getFileReferenceContent(fileReference);
    return new ByteArrayInputStream(content);
  }

  private boolean isValid(Blob blob) {
    if (blob.isDirectory()) return false;

    return shouldIncludeFile(blob.getName(), includes, excludes);
  }

  private String getFullPath(Blob blob) {
    return pathToStorageURI.getScheme() + "://" + bucketOrContainerName + "/" + blob.getName();
  }

  private Document blobToDoc(Blob blob) throws IOException {
    String fullPath = getFullPath(blob);
    String docId = DigestUtils.md5Hex(fullPath);
    Document doc = Document.create(docIdPrefix + docId);

    doc.setField(FileConnector.FILE_PATH, fullPath);

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

    return doc;
  }

  private Document blobToDoc(Blob blob, InputStream content, String decompressedFullPathStr) throws IOException {
    final String docId = DigestUtils.md5Hex(decompressedFullPathStr);
    final Document doc = Document.create(docIdPrefix + docId);

    doc.setField(FileConnector.FILE_PATH, decompressedFullPathStr);

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

    return doc;
  }

  // Only for testing
  void setStorageForTesting(Storage storage) {
    this.storage = storage;
  }
}
