package com.kmwllc.lucille.connector.cloudstorageclients;


import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureStorageClient extends BaseStorageClient {

  BlobContainerClient containerClient;
  private static final Logger log = LoggerFactory.getLogger(FileConnector.class);

  public AzureStorageClient(URI pathToStorage, Publisher publisher, String docIdPrefix, List<Pattern> excludes, List<Pattern> includes,
      Map<String, Object> cloudOptions) {
    super(pathToStorage, publisher, docIdPrefix, excludes, includes, cloudOptions);
  }

  @Override
  public void init() {
    if (!cloudOptions.containsKey("connectionString")) {
      containerClient = new BlobContainerClientBuilder()
          .connectionString((String) cloudOptions.get("connectionString"))
          .containerName(bucketName)
          .buildClient();
    } else {
      String accountName = (String) cloudOptions.get("accountName");
      String accountKey = (String) cloudOptions.get("accountKey");

      containerClient = new BlobContainerClientBuilder()
          .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
          .credential(new StorageSharedKeyCredential(accountName, accountKey))
          .containerName(bucketName)
          .buildClient();
    }
  }

  @Override
  public void shutdown() throws Exception {
    containerClient = null;
  }

  @Override
  public void publishFiles() {
    containerClient.listBlobs(new ListBlobsOptions().setPrefix(startingDirectory).setMaxResultsPerPage(100), Duration.ofSeconds(10)).stream()
        .forEach(blob -> {
          if (isNotValid(blob)) {
            return;
          }
          try {
            Document doc = blobItemToDoc(blob, bucketName);
            publisher.publish(doc);
          } catch (Exception e) {
            log.error("Error publishing blob: {}", blob.getName(), e);
          }
        });
  }

  private Document blobItemToDoc(BlobItem blob, String bucketName) {
    String docId = DigestUtils.md5Hex(blob.getName());
    Document doc = Document.create(docIdPrefix + docId);
    BlobItemProperties properties = blob.getProperties();
    doc.setField(FileConnector.FILE_PATH, "azb://" + bucketName + "/" + blob.getName());
    doc.setField(FileConnector.MODIFIED, properties.getLastModified());
    doc.setField(FileConnector.CREATED, properties.getCreationTime());
    doc.setField(FileConnector.SIZE, properties.getContentLength());
    doc.setField(FileConnector.CONTENT, properties.getContentMd5());

    return doc;
  }

  private boolean isNotValid(BlobItem blob) {
    if (blob.isPrefix()) return true;

    String blobName = blob.getName();
    if (excludes.stream().anyMatch(pattern -> pattern.matcher(blobName).matches())
        || (!includes.isEmpty() && includes.stream().noneMatch(pattern -> pattern.matcher(blobName).matches()))) {
      log.debug("Skipping file because of include or exclude regex: {}", blobName);
      return true;
    }

    return false;
  }
}
