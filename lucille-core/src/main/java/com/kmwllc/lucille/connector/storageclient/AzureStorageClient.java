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
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureStorageClient extends BaseStorageClient {

  private BlobServiceClient serviceClient;
  private static final Logger log = LoggerFactory.getLogger(AzureStorageClient.class);

  public AzureStorageClient(Map<String, Object> cloudOptions) {
    super(cloudOptions);
  }

  @Override
  public void init() throws ConnectorException {
    try {
      if (cloudOptions.containsKey(AZURE_CONNECTION_STRING)) {
        serviceClient = new BlobServiceClientBuilder()
            .connectionString((String) cloudOptions.get(AZURE_CONNECTION_STRING))
            .buildClient();
      } else {
        String accountName = (String) cloudOptions.get(AZURE_ACCOUNT_NAME);
        String accountKey = (String) cloudOptions.get(AZURE_ACCOUNT_KEY);

        serviceClient = new BlobServiceClientBuilder()
            .credential(new StorageSharedKeyCredential(accountName, accountKey))
            .buildClient();
      }
    } catch (Exception e) {
      throw new ConnectorException("Error occurred building AzureStorageClient", e);
    }
  }

  @Override
  public void shutdown() throws IOException {
    // azure service client is not closable
    serviceClient = null;
  }

  @Override
  public void traverse(Publisher publisher, TraversalParams params) throws Exception {
    try {
      initializeFileHandlers(params);

      serviceClient.getBlobContainerClient(params.bucketOrContainerName).listBlobs(new ListBlobsOptions().setPrefix(params.startingDirectory).setMaxResultsPerPage(maxNumOfPages), Duration.ofSeconds(10)).stream()
          .forEachOrdered(blob -> {
            if (isValid(blob, params)) {
              String fullPathStr = getFullPath(blob, params);
              String fileExtension = FilenameUtils.getExtension(fullPathStr);
              tryProcessAndPublishFile(publisher, fullPathStr, fileExtension, new FileReference(blob), params);
            }
          });
    } finally {
      clearFileHandlers();
    }
  }

  @Override
  public InputStream getFileContentStream(URI uri) throws IOException {
    String containerName = uri.getPath().split("/")[1];
    String blobName = uri.getPath().split("/")[2];

    BlobContainerClient containerClient = serviceClient.getBlobContainerClient(containerName);
    return containerClient.getBlobClient(blobName).openInputStream();
  }

  @Override
  protected Document convertFileReferenceToDoc(FileReference fileReference, TraversalParams params) {
    BlobItem blobItem = fileReference.getBlobItem();
    return blobItemToDoc(blobItem, params);
  }

  @Override
  protected Document convertFileReferenceToDoc(FileReference fileReference, InputStream in, String decompressedFullPathStr, TraversalParams params) {
    BlobItem blobItem = fileReference.getBlobItem();

    try {
      return blobItemToDoc(blobItem, in, decompressedFullPathStr, params);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to convert BlobItem '" + blobItem.getName() + "' to Document", e);
    }
  }

  @Override
  protected byte[] getFileReferenceContent(FileReference fileReference, TraversalParams params) {
    BlobItem blobItem = fileReference.getBlobItem();
    return serviceClient
        .getBlobContainerClient(params.bucketOrContainerName)
        .getBlobClient(blobItem.getName()).downloadContent().toBytes();
  }

  @Override
  protected InputStream getFileReferenceContentStream(FileReference fileReference, TraversalParams params) {
    byte[] content = getFileReferenceContent(fileReference, params);
    return new ByteArrayInputStream(content);
  }

  private String getFullPath(BlobItem blobItem, TraversalParams params) {
    URI pathURI = params.pathToStorageURI;

    return String.format("%s://%s/%s/%s", pathURI.getScheme(), pathURI.getAuthority(),
        params.bucketOrContainerName, blobItem.getName());
  }

  private Document blobItemToDoc(BlobItem blob, TraversalParams params) {
    String fullPath = getFullPath(blob, params);
    String docId = DigestUtils.md5Hex(fullPath);
    Document doc = Document.create(params.docIdPrefix + docId);

    BlobItemProperties properties = blob.getProperties();
    doc.setField(FileConnector.FILE_PATH, fullPath);

    if (properties.getLastModified() != null) {
      doc.setField(FileConnector.MODIFIED, properties.getLastModified().toInstant());
    }

    if (properties.getCreationTime() != null) {
      doc.setField(FileConnector.CREATED, properties.getCreationTime().toInstant());
    }

    doc.setField(FileConnector.SIZE, properties.getContentLength());

    if (params.getFileContent) {
      doc.setField(FileConnector.CONTENT, serviceClient.getBlobContainerClient(params.bucketOrContainerName).getBlobClient(blob.getName()).downloadContent().toBytes());
    }

    return doc;
  }

  private Document blobItemToDoc(BlobItem blob, InputStream is, String decompressedFullPathStr, TraversalParams params)
      throws IOException {
    String docId = DigestUtils.md5Hex(decompressedFullPathStr);
    Document doc = Document.create(params.docIdPrefix + docId);

    BlobItemProperties properties = blob.getProperties();
    doc.setField(FileConnector.FILE_PATH, decompressedFullPathStr);

    if (properties.getLastModified() != null) {
      doc.setField(FileConnector.MODIFIED, properties.getLastModified().toInstant());
    }

    if (properties.getCreationTime() != null) {
      doc.setField(FileConnector.CREATED, properties.getCreationTime().toInstant());
    }

    // unable to get the decompressed size via inputStream
    if (params.getFileContent) {
      doc.setField(FileConnector.CONTENT, is.readAllBytes());
    }

    return doc;
  }

  private boolean isValid(BlobItem blob, TraversalParams params) {
    if (blob.isPrefix()) return false;

    return params.shouldIncludeFile(blob.getName());
  }

  public static void validateOptions(Map<String, Object> cloudOptions) {
    if (!validOptions(cloudOptions)) {
      throw new IllegalArgumentException("Either '" + AZURE_CONNECTION_STRING + "' or '" + AZURE_ACCOUNT_NAME + "' & '" + AZURE_ACCOUNT_KEY + "' has to be in cloudOptions for AzureStorageClient.");
    }
  }

  public static boolean validOptions(Map<String, Object> cloudOptions) {
    return cloudOptions.containsKey(AZURE_CONNECTION_STRING)
        || (cloudOptions.containsKey(AZURE_ACCOUNT_NAME) && cloudOptions.containsKey(AZURE_ACCOUNT_KEY));
  }

  // Only for testing
  void setServiceClientForTesting(BlobServiceClient serviceClient) {
    this.serviceClient = serviceClient;
  }
}
