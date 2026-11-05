package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.AZURE_ACCOUNT_KEY;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.AZURE_ACCOUNT_NAME;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.AZURE_CONNECTION_STRING;

import com.azure.core.util.polling.SyncPoller;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Objects;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A storage client for Microsoft Azure. Create using a configuration (commonly mapped to <b>azure</b>) which contains either
 * "connectionString" <i>or</i> contains both "accountName" and "accountKey".
 */
public class AzureStorageClient extends BaseStorageClient {

  private static final Logger log = LoggerFactory.getLogger(AzureStorageClient.class);
  protected BlobServiceClient serviceClient;

  /**
   * Creates an AzureStorageClient from the given azureCloudOptions, which must contain either "connectionString" OR
   * contains both "accountName" and "accountKey".
   * @param azureCloudOptions Configuration for the AzureStorageClient.
   */
  public AzureStorageClient(Config azureCloudOptions) {
    super(azureCloudOptions);
  }

  @Override
  protected void validateOptions(Config config) {
    if (!config.hasPath(AZURE_CONNECTION_STRING)
        && (!config.hasPath(AZURE_ACCOUNT_NAME) || !config.hasPath(AZURE_ACCOUNT_KEY))) {
      throw new IllegalArgumentException("Either '" + AZURE_CONNECTION_STRING + "' or '" + AZURE_ACCOUNT_NAME + "' & '" + AZURE_ACCOUNT_KEY + "' has to be in Config for AzureStorageClient.");
    }
  }

  @Override
  protected void initializeStorageClient() throws IOException {
    try {
      BlobServiceClientBuilder builder = new BlobServiceClientBuilder();

      if (config.hasPath(AZURE_CONNECTION_STRING)) {
        builder = builder.connectionString(config.getString(AZURE_CONNECTION_STRING));
      } else {
        String accountName = config.getString(AZURE_ACCOUNT_NAME);
        String accountKey = config.getString(AZURE_ACCOUNT_KEY);

        builder = builder.credential(new StorageSharedKeyCredential(accountName, accountKey));
      }

      serviceClient = configureClientBuilder(builder).buildClient();
    } catch (Exception e) {
      throw new IOException("Error occurred building AzureStorageClient", e);
    }
  }

  /**
   * Called with the builder for the BlobServiceClient when this client starts, whether it was given a connection string or
   * an account name and key, and after those are applied. Override this to set anything else, such as an HTTP client.
   *
   * @param builder The builder.
   * @return The builder to build the BlobServiceClient from.
   */
  protected BlobServiceClientBuilder configureClientBuilder(BlobServiceClientBuilder builder) {
    return builder;
  }

  /**
   * Called with the options of each listing before it is sent, flat or hierarchical. Override this to set further options,
   * such as what details to retrieve. Called from several threads when <code>traversalThreads</code> is above 1.
   *
   * @param options The options, with the prefix and page size set.
   * @return The options to list with.
   */
  protected ListBlobsOptions customizeListOptions(ListBlobsOptions options) {
    return options;
  }

  /**
   * Opens a stream to a blob's content, including for {@link #getFileContentStream(URI)}. Override this to read it another
   * way, such as with request conditions. Called from several threads when <code>traversalThreads</code> is above 1.
   *
   * @param blobClient The client for the blob.
   * @return A stream to the blob's content.
   */
  protected InputStream openBlobStream(BlobClient blobClient) {
    return blobClient.openInputStream();
  }

  /**
   * Downloads a blob's whole content, which a traversal does when it reads a file's content into a Document with no
   * <code>maxSizeBytes</code>. Override this along with {@link #openBlobStream(BlobClient)}.
   *
   * @param blobClient The client for the blob.
   * @return The blob's content.
   */
  protected byte[] downloadBlobContent(BlobClient blobClient) {
    return blobClient.downloadContent().toBytes();
  }

  /**
   * Builds the FileReference for one listed blob. Override this to return a subclass of {@link AzureFileReference}. If this
   * throws, the traversal logs the error and skips only this blob. Called from several threads when
   * <code>traversalThreads</code> is above 1.
   *
   * @param blobItem The blob, as listed.
   * @param params Parameters for the traversal.
   * @return The blob's FileReference.
   */
  protected FileReference createFileReference(BlobItem blobItem, TraversalParams params) {
    return new AzureFileReference(blobItem, params);
  }

  @Override
  protected void shutdownStorageClient() throws IOException {
    // azure service client is not closable
    serviceClient = null;
  }

  @Override
  protected void traverseStorageClient(Publisher publisher, TraversalParams params, FileStateTracker stateMgr) throws ConnectorException {
    traverseByListing(publisher, params, stateMgr);
  }

  @Override
  protected Stream<ListingEntry> listDirectory(String prefix, TraversalParams params) {
    return serviceClient.getBlobContainerClient(getBucketOrContainerName(params))
        .listBlobsByHierarchy("/", listOptions(prefix), Duration.ofSeconds(10))
        .stream()
        .map(blob -> isPrefix(blob) ? ListingEntry.directory(blob.getName()) : fileEntry(blob, params));
  }

  @Override
  protected Stream<ListingEntry> listFlat(String prefix, TraversalParams params) {
    return serviceClient.getBlobContainerClient(getBucketOrContainerName(params))
        .listBlobs(listOptions(prefix), Duration.ofSeconds(10))
        .stream()
        .map(blob -> fileEntry(blob, params));
  }

  private ListBlobsOptions listOptions(String prefix) {
    return customizeListOptions(new ListBlobsOptions().setPrefix(prefix).setMaxResultsPerPage(maxResultsPerPage));
  }

  private ListingEntry fileEntry(BlobItem blob, TraversalParams params) {
    return ListingEntry.file(blob.getName(), () -> createFileReference(blob, params));
  }

  // a flat listing doesn't set this
  private static boolean isPrefix(BlobItem blob) {
    return Boolean.TRUE.equals(blob.isPrefix());
  }

  @Override
  protected URI directoryURI(String prefix, TraversalParams params) {
    return uriForDirectory(prefix, params);
  }

  private URI uriForDirectory(String prefix, TraversalParams params) {
    return buildBlobURI(params.getURI(), getBucketOrContainerName(params), prefix);
  }

  /**
   * {@inheritDoc}
   * <p> The container is the first segment of the path and must match exactly. Within it, a blob prefix is a raw
   * prefix, so <code>.../container/data</code> contains <code>.../container/data2</code>.
   */
  @Override
  public boolean containsPath(URI parent, URI child) {
    if (!Objects.equals(parent.getScheme(), child.getScheme()) || !Objects.equals(parent.getAuthority(), child.getAuthority())) {
      return false;
    }

    BlobLocation parentLocation = BlobLocation.fromURI(parent.normalize());
    BlobLocation childLocation = BlobLocation.fromURI(child.normalize());

    return parentLocation.containerName.equals(childLocation.containerName)
        && childLocation.blobName.startsWith(parentLocation.blobName);
  }

  @Override
  public boolean moveTargetInsidePath(URI path, URI folder) {
    if (!Objects.equals(path.getScheme(), folder.getScheme()) || !Objects.equals(path.getAuthority(), folder.getAuthority())) {
      return false;
    }

    BlobLocation pathLocation = BlobLocation.fromURI(path.normalize());
    BlobLocation folderLocation = BlobLocation.fromURI(folder.normalize());

    return pathLocation.containerName.equals(folderLocation.containerName)
        && movedKeyInsidePrefix(pathLocation.blobName, folderLocation.blobName);
  }

  @Override
  protected InputStream getFileContentStreamFromStorage(URI uri) throws IOException {
    BlobLocation location = BlobLocation.fromURI(uri);

    BlobContainerClient containerClient = serviceClient.getBlobContainerClient(location.containerName);
    return openBlobStream(containerClient.getBlobClient(location.blobName));
  }

  @Override
  public void moveFile(URI filePath, URI folder) throws IOException {
    BlobLocation source = BlobLocation.fromURI(filePath);
    BlobLocation dest = BlobLocation.fromURI(folder);

    String destBlob = destinationKey(dest.blobName, source.blobName);

    BlobClient sourceBlobClient = serviceClient.getBlobContainerClient(source.containerName).getBlobClient(source.blobName);
    BlobClient destBlobClient = serviceClient.getBlobContainerClient(dest.containerName).getBlobClient(destBlob);

    // Start the copy, then we wait for it to complete
    SyncPoller<BlobCopyInfo, Void> poller = sourceBlobClient.beginCopy(destBlobClient.getBlobUrl(), null);
    poller.waitForCompletion();

    // then delete the source file, completing the move
    sourceBlobClient.delete();
  }

  @Override
  protected String getStartingDirectory(TraversalParams params) {
    String path = params.getURI().getPath();
    // path is in the format /containerName/folder1/folder2/... so need to return folder1/folder2/...
    String[] subPaths = path.split("/", 3);
    return subPaths.length > 2 ? subPaths[2] : "";
  }

  /**
   * Returns the container a traversal lists and reads from: by default, the first segment of the path of the traversal's URI.
   *
   * @param params Parameters for the traversal.
   * @return The container's name.
   */
  protected String getBucketOrContainerName(TraversalParams params) {
    return BlobLocation.fromURI(params.getURI()).containerName;
  }

  /**
   * Builds a URI for the given blob in the given container, using the scheme and authority of baseURI. Characters in
   * the blob name that aren't legal in a URI path (such as spaces, '%' and '#') are percent-encoded, so the full blob
   * name is returned by {@link URI#getPath()}.
   */
  private static URI buildBlobURI(URI baseURI, String containerName, String blobName) {
    try {
      return new URI(baseURI.getScheme(), baseURI.getAuthority(), "/" + containerName + "/" + blobName, null, null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Unable to build Azure URI for blob: " + blobName, e);
    }
  }

  /**
   * The container name and full blob name referenced by an Azure URI's path, which is in the format
   * /containerName/folder1/folder2/blobName.
   */
  private static class BlobLocation {

    private final String containerName;
    private final String blobName;

    private BlobLocation(String containerName, String blobName) {
      this.containerName = containerName;
      this.blobName = blobName;
    }

    private static BlobLocation fromURI(URI uri) {
      // getPath() has already percent-decoded the path, so it must not be decoded again.
      String path = uri.getPath() == null ? "" : uri.getPath();
      String[] parts = (path.startsWith("/") ? path.substring(1) : path).split("/", 2);

      if (parts[0].isBlank()) {
        throw new IllegalArgumentException("Azure URI does not contain a container name: " + uri);
      }

      return new BlobLocation(parts[0], parts.length > 1 ? parts[1] : "");
    }
  }

  private URI fullPathFor(BlobItem blobItem, TraversalParams params) {
    return buildBlobURI(params.getURI(), getBucketOrContainerName(params), blobItem.getName());
  }

  // Only for testing
  void setServiceClientForTesting(BlobServiceClient serviceClient) {
    this.serviceClient = serviceClient;
  }


  /**
   * The FileReference for a listed Azure blob. Extend this and return it from {@link #createFileReference} to change, for
   * example, the Document built for a blob.
   */
  protected class AzureFileReference extends BaseFileReference {

    private final BlobItem blobItem;

    public AzureFileReference(BlobItem blobItem, TraversalParams params) {
      // These are inexpensive calls - information is stored in the BlobItem.
      super(fullPathFor(blobItem, params),
          blobItem.getProperties().getLastModified().toInstant(),
          blobItem.getProperties().getContentLength(),
          blobItem.getProperties().getCreationTime().toInstant());

      this.blobItem = blobItem;
    }

    @Override
    public String getName() {
      return lastSegment(blobItem.getName());
    }

    @Override
    public String getFilterName() {
      return blobItem.getName();
    }

    @Override
    public boolean isValidFile() {
      return !isPrefix(blobItem);
    }

    @Override
    public InputStream getContentStream(TraversalParams params) {
      return openBlobStream(serviceClient
          .getBlobContainerClient(getBucketOrContainerName(params))
          .getBlobClient(blobItem.getName()));
    }

    @Override
    protected byte[] getFileContent(TraversalParams params) {
      return downloadBlobContent(serviceClient
          .getBlobContainerClient(getBucketOrContainerName(params))
          .getBlobClient(blobItem.getName()));
    }
  }
}
