package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.AZURE_ACCOUNT_KEY;
import static com.kmwllc.lucille.connector.FileConnector.AZURE_ACCOUNT_NAME;
import static com.kmwllc.lucille.connector.FileConnector.AZURE_CONNECTION_STRING;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.connector.storageclient.filereference.AzureFileReference;
import com.kmwllc.lucille.connector.storageclient.filereference.FileReference;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureStorageClient extends BaseStorageClient {

  private BlobServiceClient serviceClient;
  private static final Logger log = LoggerFactory.getLogger(AzureStorageClient.class);

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
          AzureFileReference fileRef = new AzureFileReference(blob);

          if (validFile(fileRef)) {
            String fullPathStr = getFullPath(blob, params);
            String fileExtension = FilenameUtils.getExtension(fullPathStr);
            tryProcessAndPublishFile(publisher, fullPathStr, fileExtension, fileRef, params);
          }
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
  protected Document convertFileReferenceToDoc(FileReference fileReference, TraversalParams params) {
    Document fileReferenceDoc = fileReference.toDoc(params);

  }

  @Override
  protected InputStream getFileReferenceContentStream(FileReference fileReference, TraversalParams params) {
    BlobItem blobItem = fileReference.getBlobItem();
    return serviceClient
        .getBlobContainerClient(getBucketOrContainerName(params))
        .getBlobClient(blobItem.getName()).openInputStream();
  }

  @Override
  protected String getStartingDirectory(TraversalParams params) {
    String path = params.getURI().getPath();
    // path is in the format /containerName/folder1/folder2/... so need to return folder1/folder2/...
    String[] subPaths = path.split("/", 3);
    return subPaths.length > 2 ? subPaths[2] : "";
  }

  @Override
  protected String getBucketOrContainerName(TraversalParams params) {
    return params.getURI().getPath().split("/")[1];
  }

  private String getFullPath(BlobItem blobItem, TraversalParams params) {
    URI pathURI = params.getURI();

    return String.format("%s://%s/%s/%s", pathURI.getScheme(), pathURI.getAuthority(),
        getBucketOrContainerName(params), blobItem.getName());
  }

  // Only for testing
  void setServiceClientForTesting(BlobServiceClient serviceClient) {
    this.serviceClient = serviceClient;
  }
}
