package com.kmwllc.lucille.connector.storageclient;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureStorageClient extends BaseStorageClient {

  private BlobContainerClient containerClient;
  private static final Logger log = LoggerFactory.getLogger(AzureStorageClient.class);

  public AzureStorageClient(URI pathToStorage, String docIdPrefix, List<Pattern> excludes, List<Pattern> includes,
      Map<String, Object> cloudOptions, Config fileOptions) {
    super(pathToStorage, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
  }

  @Override
  protected String getContainerOrBucketName() {
    return pathToStorageURI.getPath().split("/")[1];
  }

  @Override
  protected String getStartingDirectory() {
    String path = pathToStorageURI.getPath();
    String[] subPaths = path.split("/", 3);
    return subPaths.length > 2 ? subPaths[2] : "";
  }

  @Override
  public void init() throws ConnectorException {
    try {
      if (cloudOptions.containsKey(FileConnector.AZURE_CONNECTION_STRING)) {
        containerClient = new BlobContainerClientBuilder()
            .connectionString((String) cloudOptions.get(FileConnector.AZURE_CONNECTION_STRING))
            .containerName(bucketOrContainerName)
            .buildClient();
      } else {
        String accountName = (String) cloudOptions.get(FileConnector.AZURE_ACCOUNT_NAME);
        String accountKey = (String) cloudOptions.get(FileConnector.AZURE_ACCOUNT_KEY);

        containerClient = new BlobContainerClientBuilder()
            .credential(new StorageSharedKeyCredential(accountName, accountKey))
            .containerName(bucketOrContainerName)
            .buildClient();
      }
    } catch (Exception e) {
      throw new ConnectorException("Error occurred building AzureStorageClient", e);
    }

    initializeFileHandlers();
  }

  @Override
  public void shutdown() throws IOException {
    // azure container client is not closable
    containerClient = null;
    // clear all file handlers if any
    clearFileHandlers();
  }

  @Override
  public void traverse(Publisher publisher) throws Exception{
    containerClient.listBlobs(new ListBlobsOptions().setPrefix(startingDirectory).setMaxResultsPerPage(maxNumOfPages), Duration.ofSeconds(10)).stream()
        .forEach(blob -> {
          if (isValid(blob)) {
            String fullPathStr = getFullPath(blob);
            String fileExtension = FilenameUtils.getExtension(fullPathStr);
            tryProcessAndPublishFile(publisher, fullPathStr, fileExtension, new FileReference(blob));
          }
        });
  }

  @Override
  protected Document convertFileReferenceToDoc(FileReference fileReference, String bucketOrContainerName) {
    BlobItem blobItem = fileReference.getBlobItem();
    return blobItemToDoc(blobItem, bucketOrContainerName);
  }

  @Override
  protected Document convertFileReferenceToDoc(FileReference fileReference, String bucketOrContainerName, InputStream in, String fullPathStr) {
    BlobItem blobItem = fileReference.getBlobItem();

    try {
      return blobItemToDoc(blobItem, bucketOrContainerName, in, fullPathStr);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to convert BlobItem '" + blobItem.getName() + "' to Document", e);
    }
  }

  @Override
  protected byte[] getFileReferenceContent(FileReference fileReference) {
    BlobItem blobItem = fileReference.getBlobItem();
    return containerClient.getBlobClient(blobItem.getName()).downloadContent().toBytes();
  }

  @Override
  protected InputStream getFileReferenceContentStream(FileReference fileReference) {
    byte[] content = getFileReferenceContent(fileReference);
    return new ByteArrayInputStream(content);
  }

  private String getFullPath(BlobItem blobItem) {
    return String.format("%s://%s/%s/%s", pathToStorageURI.getScheme(), pathToStorageURI.getAuthority(),
        bucketOrContainerName, blobItem.getName());
  }

  private Document blobItemToDoc(BlobItem blob, String container) {
    String docId = DigestUtils.md5Hex(blob.getName());
    Document doc = Document.create(docIdPrefix + docId);
    BlobItemProperties properties = blob.getProperties();
    String path = String.format("%s://%s/%s/%s", pathToStorageURI.getScheme(), pathToStorageURI.getAuthority(),
        container, blob.getName());
    doc.setField(FileConnector.FILE_PATH, path);
    doc.setField(FileConnector.MODIFIED, properties.getLastModified().toInstant());
    doc.setField(FileConnector.CREATED, properties.getCreationTime().toInstant());
    doc.setField(FileConnector.SIZE, properties.getContentLength());

    if (getFileContent) {
      doc.setField(FileConnector.CONTENT, containerClient.getBlobClient(blob.getName()).downloadContent().toBytes());
    }

    return doc;
  }

  private Document blobItemToDoc(BlobItem blob, String container, InputStream is, String unzippedFileName)
      throws IOException {
    String docId = DigestUtils.md5Hex(unzippedFileName);
    Document doc = Document.create(docIdPrefix + docId);
    BlobItemProperties properties = blob.getProperties();
    String path = String.format("%s://%s/%s/%s", pathToStorageURI.getScheme(), pathToStorageURI.getAuthority(),
        container, unzippedFileName);
    doc.setField(FileConnector.FILE_PATH, path);
    doc.setField(FileConnector.MODIFIED, properties.getLastModified().toInstant());
    doc.setField(FileConnector.CREATED, properties.getCreationTime().toInstant());
    // unable to get the decompressed size
    if (getFileContent) {
      doc.setField(FileConnector.CONTENT, is.readAllBytes());
    }

    return doc;
  }

  private boolean isValid(BlobItem blob) {
    if (blob.isPrefix()) return false;

    return shouldIncludeFile(blob.getName(), includes, excludes);
  }

  // Only for testing
  void setContainerClientForTesting(BlobContainerClient containerClient) {
    this.containerClient = containerClient;
  }
}
