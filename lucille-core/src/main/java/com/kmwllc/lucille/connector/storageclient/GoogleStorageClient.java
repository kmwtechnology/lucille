package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.GOOGLE_SERVICE_KEY;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
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
import java.nio.channels.Channels;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleStorageClient extends BaseStorageClient {

  private static final Logger log = LoggerFactory.getLogger(GoogleStorageClient.class);
  private Storage storage;

  public GoogleStorageClient(Config googleCloudOptions) {
    super(googleCloudOptions);
  }

  @Override
  public void init() throws IOException {
    try (FileInputStream serviceAccountStream = new FileInputStream(cloudOptions.getString(GOOGLE_SERVICE_KEY))) {
      storage = StorageOptions.newBuilder()
          .setCredentials(ServiceAccountCredentials.fromStream(serviceAccountStream))
          .build()
          .getService();
    }
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
  }

  @Override
  public void traverse(Publisher publisher, TraversalParams params) throws Exception {
    try {
      initializeFileHandlers(params);

      Page<Blob> page = storage.list(getBucketOrContainerName(params), BlobListOption.prefix(getStartingDirectory(params)), BlobListOption.pageSize(maxNumOfPages));
      do {
        page.streamAll()
            .forEachOrdered(blob -> {
              if (isValid(blob, params)) {
                String fullPathStr = getFullPath(blob, params);
                String fileExtension = FilenameUtils.getExtension(fullPathStr);
                tryProcessAndPublishFile(publisher, fullPathStr, fileExtension, new FileReference(blob), params);
              }
            });
        page = page.hasNextPage() ? page.getNextPage() : null;
      } while (page != null);
    } finally {
      clearFileHandlers();
    }
  }

  @Override
  public InputStream getFileContentStream(URI uri) throws IOException {
    String bucketName = uri.getAuthority();
    String objectKey = uri.getPath().substring(1);

    Blob blob = storage.get(BlobId.of(bucketName, objectKey));
    ReadChannel blobReadChannel = blob.reader();
    return Channels.newInputStream(blobReadChannel);
  }

  @Override
  protected Document convertFileReferenceToDoc(FileReference fileReference, TraversalParams params) {
    Blob blob = fileReference.getBlob();

    try {
      return blobToDoc(blob, params);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to convert blob '" + blob.getName() + "' to Document", e);
    }
  }

  @Override
  protected Document convertFileReferenceToDoc(FileReference fileReference, InputStream in, String decompressedFullPathStr, TraversalParams params) {
    Blob blob = fileReference.getBlob();

    try {
      return blobToDoc(blob, in, decompressedFullPathStr, params);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to convert blob '" + blob.getName() + "' to Document", e);
    }
  }

  @Override
  protected byte[] getFileReferenceContent(FileReference fileReference, TraversalParams params) {
    Blob blob = fileReference.getBlob();
    return blob.getContent();
  }

  @Override
  protected InputStream getFileReferenceContentStream(FileReference fileReference, TraversalParams params) {
    byte[] content = getFileReferenceContent(fileReference, params);
    return new ByteArrayInputStream(content);
  }

  private boolean isValid(Blob blob, TraversalParams params) {
    if (blob.isDirectory()) return false;

    return params.shouldIncludeFile(blob.getName());
  }

  private String getFullPath(Blob blob, TraversalParams params) {
    return params.getPathToStorageURI().getScheme() + "://" + getBucketOrContainerName(params) + "/" + blob.getName();
  }

  private Document blobToDoc(Blob blob, TraversalParams params) throws IOException {
    String fullPath = getFullPath(blob, params);
    String docId = DigestUtils.md5Hex(fullPath);
    Document doc = Document.create(params.getDocIdPrefix() + docId);

    doc.setField(FileConnector.FILE_PATH, fullPath);

    if (blob.getUpdateTimeOffsetDateTime() != null) {
      doc.setField(FileConnector.MODIFIED, blob.getUpdateTimeOffsetDateTime().toInstant());
    }

    if (blob.getCreateTimeOffsetDateTime() != null) {
      doc.setField(FileConnector.CREATED, blob.getCreateTimeOffsetDateTime().toInstant());
    }

    doc.setField(FileConnector.SIZE, blob.getSize());

    if (params.shouldGetFileContent()) {
      doc.setField(FileConnector.CONTENT, blob.getContent());
    }

    return doc;
  }

  private Document blobToDoc(Blob blob, InputStream content, String decompressedFullPathStr, TraversalParams params) throws IOException {
    final String docId = DigestUtils.md5Hex(decompressedFullPathStr);
    final Document doc = Document.create(params.getDocIdPrefix() + docId);

    doc.setField(FileConnector.FILE_PATH, decompressedFullPathStr);

    if (blob.getUpdateTimeOffsetDateTime() != null) {
      doc.setField(FileConnector.MODIFIED, blob.getUpdateTimeOffsetDateTime().toInstant());
    }

    if (blob.getCreateTimeOffsetDateTime() != null) {
      doc.setField(FileConnector.CREATED, blob.getCreateTimeOffsetDateTime().toInstant());
    }

    // unable to get decompressed file size
    if (params.shouldGetFileContent()) {
      doc.setField(FileConnector.CONTENT, content.readAllBytes());
    }

    return doc;
  }

  public static void validateOptions(Config cloudOptions) {
    if (!validOptions(cloudOptions)) {
      throw new IllegalArgumentException("Missing " + GOOGLE_SERVICE_KEY + " in cloudOptions for GoogleStorageClient.");
    }
  }

  public static boolean validOptions(Config cloudOptions) {
    return cloudOptions.hasPath(GOOGLE_SERVICE_KEY);
  }

  // Only for testing
  void setStorageForTesting(Storage storage) {
    this.storage = storage;
  }
}
