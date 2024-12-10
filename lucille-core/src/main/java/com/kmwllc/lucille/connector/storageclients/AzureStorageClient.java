package com.kmwllc.lucille.connector.storageclients;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.fileHandlers.FileHandler;
import com.kmwllc.lucille.core.fileHandlers.FileHandlerManager;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
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
  private static final Logger log = LoggerFactory.getLogger(FileConnector.class);

  public AzureStorageClient(URI pathToStorage, Publisher publisher, String docIdPrefix, List<Pattern> excludes, List<Pattern> includes,
      Map<String, Object> cloudOptions, Config fileOptions) {
    super(pathToStorage, publisher, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
  }

  @Override
  public String getContainerOrBucketName() {
    return pathToStorageURI.getPath().split("/")[1];
  }

  @Override
  public String getStartingDirectory() {
    String path = pathToStorageURI.getPath();
    String[] subPaths = path.split("/", 3);
    return subPaths.length > 2 ? subPaths[2] : "";
  }

  @Override
  public void init() {
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
  }

  @Override
  public void shutdown() throws Exception {
    // azure container client is not closable
    containerClient = null;
    // close all file handlers if any
    FileHandlerManager.closeAllHandlers();
  }

  @Override
  public void publishFiles() throws Exception{
    containerClient.listBlobs(new ListBlobsOptions().setPrefix(startingDirectory).setMaxResultsPerPage(maxNumOfPages), Duration.ofSeconds(10)).stream()
        .forEach(blob -> {
          if (isValid(blob)) {
            try {
              String filePath = blob.getName();
              String fileExtension = FilenameUtils.getExtension(filePath);

              // handle file types if needed
              if (!fileOptions.isEmpty() && FileHandler.supportsFileType(fileExtension, fileOptions)) {
                // get the file content
                byte[] content = containerClient.getBlobClient(blob.getName()).downloadContent().toBytes();
                // instantiate the right FileHandler and publish based on content
                publishUsingFileHandler(fileExtension, content, filePath);
                return;
              }

              Document doc = blobItemToDoc(blob);
              publisher.publish(doc);
            } catch (Exception e) {
              log.error("Error publishing blob: {}", blob.getName(), e);
            }
          }
        });
  }

  private Document blobItemToDoc(BlobItem blob) {
    String docId = DigestUtils.md5Hex(blob.getName());
    Document doc = Document.create(docIdPrefix + docId);
    BlobItemProperties properties = blob.getProperties();
    String path = String.format("%s://%s/%s/%s", pathToStorageURI.getScheme(), pathToStorageURI.getAuthority(),
        bucketOrContainerName, blob.getName());
    doc.setField(FileConnector.FILE_PATH, path);
    doc.setField(FileConnector.MODIFIED, properties.getLastModified().toInstant());
    doc.setField(FileConnector.CREATED, properties.getCreationTime().toInstant());
    doc.setField(FileConnector.SIZE, properties.getContentLength());

    if (getFileContent) {
      doc.setField(FileConnector.CONTENT, containerClient.getBlobClient(blob.getName()).downloadContent().toBytes());
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
