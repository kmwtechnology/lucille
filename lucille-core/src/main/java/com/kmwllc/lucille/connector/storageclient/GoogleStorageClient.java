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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleStorageClient extends BaseStorageClient {

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
            // TODO: need to make sure this will give us directories.
            GoogleFileReference fileRef = new GoogleFileReference(blob, params);
            processAndPublishFileIfValid(publisher, fileRef, params);
          });
      page = page.hasNextPage() ? page.getNextPage() : null;
    } while (page != null);
  }

  @Override
  protected String getStateTableName(URI pathToStorage) {
    return "gs_" + pathToStorage.getHost();
  }

  @Override
  protected InputStream getFileContentStreamFromStorage(URI uri) throws IOException {
    String bucketName = uri.getAuthority();
    String objectKey = uri.getPath().substring(1);

    Blob blob = storage.get(BlobId.of(bucketName, objectKey));
    ReadChannel blobReadChannel = blob.reader();
    return Channels.newInputStream(blobReadChannel);
  }

  private String getStartingDirectory(TraversalParams params) {
    URI pathURI = params.getURI();
    String startingDirectory = Objects.equals(pathURI.getPath(), "/") ? "" : pathURI.getPath();
    if (startingDirectory.startsWith("/")) {
      return startingDirectory.substring(1);
    }
    return startingDirectory;
  }

  private String getBucketOrContainerName(TraversalParams params) {
    return params.getURI().getAuthority();
  }

  // Only for testing
  void setStorageForTesting(Storage storage) {
    this.storage = storage;
  }


  private class GoogleFileReference extends BaseFileReference {

    private final Blob blob;

    public GoogleFileReference(Blob blob, TraversalParams params) {
      // This is an inexpensive call that doesn't involve networking / RPC.
      super(getFullPathHelper(blob, params), blob.getUpdateTimeOffsetDateTime().toInstant());

      this.blob = blob;
    }

    @Override
    public String getName() {
      return blob.getName();
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
    public Document asDoc(TraversalParams params) {
      Document doc = createEmptyDocument(params);

      doc.setField(FileConnector.FILE_PATH, getFullPath());
      doc.setField(FileConnector.MODIFIED, getLastModified());

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
    public Document asDoc(InputStream in, String decompressedFullPathStr, TraversalParams params) throws IOException {
      Document doc = createEmptyDocument(params, decompressedFullPathStr);

      doc.setField(FileConnector.FILE_PATH, decompressedFullPathStr);

      if (getLastModified() != null) {
        doc.setField(FileConnector.MODIFIED, getLastModified());
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

    // Just here to simplify the call to
    private static String getFullPathHelper(Blob blob, TraversalParams params) {
      URI paramsURI = params.getURI();
      return paramsURI.getScheme() + "://" + paramsURI.getAuthority() + "/" + blob.getName();
    }
  }
}
