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
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.util.Objects;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleStorageClient extends BaseStorageClient {

  public class GoogleFileReference extends BaseFileReference {

    private final Blob blob;

    public GoogleFileReference(Blob blob) {
      super(blob.getUpdateTimeOffsetDateTime().toInstant());

      this.blob = blob;
    }

    @Override
    public String getName() {
      return blob.getName();
    }

    @Override
    public String getFullPath(TraversalParams params) {
      URI paramsURI = params.getURI();
      return paramsURI.getScheme() + "://" + paramsURI.getAuthority() + "/" + blob.getName();
    }

    @Override
    public boolean isCloudFileReference() {
      return true;
    }

    @Override
    public boolean isValidFile() {
      return !blob.isDirectory();
    }

    @Override
    public InputStream getContentStream(TraversalParams params) {
      ReadChannel readChannel = blob.reader();
      return Channels.newInputStream(readChannel);
    }

    @Override
    public Document toDoc(TraversalParams params) {
      String fullPath = getFullPath(params);
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

    @Override
    public Document toDoc(InputStream in, String decompressedFullPathStr, TraversalParams params) throws IOException {
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
        doc.setField(FileConnector.CONTENT, in.readAllBytes());
      }

      return doc;
    }
  }

  private static final Logger log = LoggerFactory.getLogger(GoogleStorageClient.class);
  private Storage storage;

  public GoogleStorageClient(Config googleCloudOptions) {
    super(googleCloudOptions);
  }

  @Override
  protected void validateOptions(Config config) {
    if (!config.hasPath(GOOGLE_SERVICE_KEY)) {
      throw new IllegalArgumentException("Missing " + GOOGLE_SERVICE_KEY + " in Config for GoogleStorageClient.");
    }
  }

  @Override
  protected void initializeStorageClient() throws IOException {
    try (FileInputStream serviceAccountStream = new FileInputStream(config.getString(GOOGLE_SERVICE_KEY))) {
      storage = StorageOptions.newBuilder()
          .setCredentials(ServiceAccountCredentials.fromStream(serviceAccountStream))
          .build()
          .getService();
    }
  }

  @Override
  protected void shutdownStorageClient() throws IOException {
    if (storage != null) {
      try {
        storage.close();
      } catch (Exception e) {
        throw new IOException("Error occurred closing storage", e);
      }
    }
  }

  @Override
  protected void traverseStorageClient(Publisher publisher, TraversalParams params) throws Exception {
    Page<Blob> page = storage.list(getBucketOrContainerName(params), BlobListOption.prefix(getStartingDirectory(params)),
        BlobListOption.pageSize(maxNumOfPages));
    do {
      page.streamAll()
          .forEachOrdered(blob -> {
            GoogleFileReference fileRef = new GoogleFileReference(blob);
            String fullPathStr = fileRef.getFullPath(params);
            String fileExtension = FilenameUtils.getExtension(fullPathStr);
            processAndPublishFileIfValid(publisher, fullPathStr, fileExtension, fileRef, params);
          });
      page = page.hasNextPage() ? page.getNextPage() : null;
    } while (page != null);
  }

  @Override
  protected InputStream getFileContentStreamFromStorage(URI uri) throws IOException {
    String bucketName = uri.getAuthority();
    String objectKey = uri.getPath().substring(1);

    Blob blob = storage.get(BlobId.of(bucketName, objectKey));
    ReadChannel blobReadChannel = blob.reader();
    return Channels.newInputStream(blobReadChannel);
  }

  @Override
  protected String getStartingDirectory(TraversalParams params) {
    URI pathURI = params.getURI();
    String startingDirectory = Objects.equals(pathURI.getPath(), "/") ? "" : pathURI.getPath();
    if (startingDirectory.startsWith("/")) {
      return startingDirectory.substring(1);
    }
    return startingDirectory;
  }

  @Override
  protected String getBucketOrContainerName(TraversalParams params) {
    return params.getURI().getAuthority();
  }

  // Only for testing
  void setStorageForTesting(Storage storage) {
    this.storage = storage;
  }
}
