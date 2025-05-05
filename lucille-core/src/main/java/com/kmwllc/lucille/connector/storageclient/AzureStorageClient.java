package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.AZURE_ACCOUNT_KEY;
import static com.kmwllc.lucille.connector.FileConnector.AZURE_ACCOUNT_NAME;
import static com.kmwllc.lucille.connector.FileConnector.AZURE_CONNECTION_STRING;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A storage client for Microsoft Azure. Create using a configuration (commonly mapped to <b>azure</b>) which contains either
 * "connectionString" <i>or</i> contains both "accountName" and "accountKey".
 */
public class AzureStorageClient extends BaseStorageClient {

  private BlobServiceClient serviceClient;
  private static final Logger log = LoggerFactory.getLogger(AzureStorageClient.class);

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
      if (config.hasPath(AZURE_CONNECTION_STRING)) {
        serviceClient = new BlobServiceClientBuilder()
            .connectionString(config.getString(AZURE_CONNECTION_STRING))
            .buildClient();
      } else {
        String accountName = config.getString(AZURE_ACCOUNT_NAME);
        String accountKey = config.getString(AZURE_ACCOUNT_KEY);

        serviceClient = new BlobServiceClientBuilder()
            .credential(new StorageSharedKeyCredential(accountName, accountKey))
            .buildClient();
      }
    } catch (Exception e) {
      throw new IOException("Error occurred building AzureStorageClient", e);
    }
  }

  @Override
  protected void shutdownStorageClient() throws IOException {
    // azure service client is not closable
    serviceClient = null;
  }

  @Override
  protected void traverseStorageClient(Publisher publisher, TraversalParams params) throws Exception {
    serviceClient.getBlobContainerClient(getBucketOrContainerName(params))
        .listBlobs(new ListBlobsOptions().setPrefix(getStartingDirectory(params)).setMaxResultsPerPage(maxNumOfPages),
            Duration.ofSeconds(10)).stream()
        .forEachOrdered(blob -> {
          AzureFileReference fileRef = new AzureFileReference(blob, params);
          processAndPublishFileIfValid(publisher, fileRef, params);
        });
  }

  @Override
  protected InputStream getFileContentStreamFromStorage(URI uri) throws IOException {
    String containerName = uri.getPath().split("/")[1];
    String blobName = uri.getPath().split("/")[2];

    BlobContainerClient containerClient = serviceClient.getBlobContainerClient(containerName);
    return containerClient.getBlobClient(blobName).openInputStream();
  }

  @Override
  public void moveFile(URI filePath, URI folder) throws IOException {
    String sourceContainer = filePath.getPath().split("/")[1];
    String sourceBlob = filePath.getPath().split("/")[2];

    String destContainer = folder.getPath().split("/")[1];

    String destBlob;
    if (folder.getPath().split("/").length > 2) {
      destBlob = folder.getPath().split("/")[2] + "/" + sourceBlob;
    } else {
      destBlob = sourceBlob;
    }

    BlobClient fileClient = serviceClient.getBlobContainerClient(sourceContainer).getBlobClient(sourceBlob);
    BlobClient targetFileClient = serviceClient.getBlobContainerClient(destContainer).getBlobClient(destBlob);

    // Starts the copy in the background
    fileClient.beginCopy(targetFileClient.getBlobUrl(), null);
  }

  private String getStartingDirectory(TraversalParams params) {
    String path = params.getURI().getPath();
    // path is in the format /containerName/folder1/folder2/... so need to return folder1/folder2/...
    String[] subPaths = path.split("/", 3);
    return subPaths.length > 2 ? subPaths[2] : "";
  }

  private String getBucketOrContainerName(TraversalParams params) {
    return params.getURI().getPath().split("/")[1];
  }

  // Only for testing
  void setServiceClientForTesting(BlobServiceClient serviceClient) {
    this.serviceClient = serviceClient;
  }


  private class AzureFileReference extends BaseFileReference {

    private final BlobItem blobItem;

    public AzureFileReference(BlobItem blobItem, TraversalParams params) {
      // These are inexpensive calls - information is stored in the BlobItem.
      super(getFullPathHelper(blobItem, params),
          blobItem.getProperties().getLastModified().toInstant(),
          blobItem.getProperties().getContentLength(),
          blobItem.getProperties().getCreationTime().toInstant());

      this.blobItem = blobItem;
    }

    @Override
    public String getName() {
      return blobItem.getName();
    }

    @Override
    public boolean isValidFile() {
      return !blobItem.isPrefix();
    }

    @Override
    public InputStream getContentStream(TraversalParams params) {
      return serviceClient
          .getBlobContainerClient(getBucketOrContainerName(params))
          .getBlobClient(blobItem.getName()).openInputStream();
    }

    @Override
    protected byte[] getFileContent(TraversalParams params) {
      return serviceClient
          .getBlobContainerClient(getBucketOrContainerName(params))
          .getBlobClient(blobItem.getName())
          .downloadContent().toBytes();
    }

    private static URI getFullPathHelper(BlobItem blobItem, TraversalParams params) {
      URI pathURI = params.getURI();

      String fulLURIStr = String.format("%s://%s/%s/%s", pathURI.getScheme(), pathURI.getAuthority(),
          pathURI.getPath().split("/")[1], blobItem.getName());

      return URI.create(fulLURIStr);
    }
  }
}
