package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.GOOGLE_SERVICE_KEY;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.CopyRequest;
import com.google.cloud.storage.StorageOptions;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A storage client for Google Cloud. Create using a configuration (commonly mapped to <b>gcp</b>) which contains "pathToServiceKey".
 */
public class GoogleStorageClient extends BaseStorageClient {

  private static final Logger log = LoggerFactory.getLogger(GoogleStorageClient.class);
  protected Storage storage;

  public GoogleStorageClient(Config googleCloudOptions) {
    super(googleCloudOptions);
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
  protected void traverseStorageClient(Publisher publisher, TraversalParams params, FileStateTracker stateMgr) throws Exception {
    traversePrefix(publisher, params, stateMgr, getStartingDirectory(params));
  }

  private void traversePrefix(Publisher publisher, TraversalParams params, FileStateTracker stateMgr, String prefix) {
    storage.list(getBucketOrContainerName(params),
            BlobListOption.prefix(prefix),
            BlobListOption.currentDirectory(),
            BlobListOption.pageSize(maxResultsPerPage))
        .streamAll()
        .forEachOrdered(blob -> {
          if (blob.isDirectory()) {
            URI dirUri = uriForDirectory(blob.getName(), params);

            if (!isSkippedDirectory(dirUri, params)) {
              traversePrefix(publisher, params, stateMgr, blob.getName());
            }
          } else {
            FileReference fileRef = buildReferenceOrSkip(blob.getName(), () -> new GoogleFileReference(blob, params));
            if (fileRef != null) {
              processAndPublishFileIfValid(publisher, fileRef, params, stateMgr);
            }
          }
        });
  }

  private URI uriForDirectory(String prefix, TraversalParams params) {
    return buildObjectURI(params.getURI(), prefix);
  }

  // Builds the gs:// URI for an object name or prefix, percent-encoding any characters that can't appear in a URI
  // (space, '#', '%', '[', ...), so the name round-trips through URI.getPath().
  private static URI buildObjectURI(URI paramsUri, String name) {
    try {
      return new URI(paramsUri.getScheme(), paramsUri.getAuthority(), "/" + name, null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Unable to build Google Cloud URI for object: " + name, e);
    }
  }

  @Override
  protected InputStream getFileContentStreamFromStorage(URI uri) throws IOException {
    String bucketName = uri.getAuthority();
    String objectKey = uri.getPath().substring(1);

    Blob blob = storage.get(BlobId.of(bucketName, objectKey));
    ReadChannel blobReadChannel = blob.reader();
    return Channels.newInputStream(blobReadChannel);
  }

  // the folder's key: its path without the leading '/'. Empty for the bucket itself, with or without a trailing '/'.
  private static String folderKey(URI folder) {
    String path = folder.getPath();
    return path == null || path.length() <= 1 ? "" : path.substring(1);
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

  @Override
  public void moveFile(URI filePath, URI folder) throws IOException {
    String sourceBucket = filePath.getAuthority();
    String sourceKey = filePath.getPath().substring(1);

    String destBucket = folder.getAuthority();
    String destKey = destinationKey(folderKey(folder), sourceKey);

    BlobId source = BlobId.of(sourceBucket, sourceKey);
    BlobId target = BlobId.of(destBucket, destKey);

    // copy source to target, and then delete source.
    storage.copy(CopyRequest.newBuilder().setSource(source).setTarget(target).build());
    storage.delete(source);
  }


  private class GoogleFileReference extends BaseFileReference {

    private final Blob blob;

    public GoogleFileReference(Blob blob, TraversalParams params) {
      // This is an inexpensive call that doesn't involve networking / RPC.
      super(getFullPathHelper(blob, params),
          blob.getUpdateTimeOffsetDateTime() == null ? null : blob.getUpdateTimeOffsetDateTime().toInstant(),
          blob.getSize(),
          blob.getCreateTimeOffsetDateTime() == null ? null : blob.getCreateTimeOffsetDateTime().toInstant());

      this.blob = blob;
    }

    @Override
    public String getName() {
      return lastSegment(blob.getName());
    }

    @Override
    public String getFilterName() {
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
    protected byte[] getFileContent(TraversalParams params) {
      return blob.getContent();
    }

    private static URI getFullPathHelper(Blob blob, TraversalParams params) {
      return buildObjectURI(params.getURI(), blob.getName());
    }
  }
}
