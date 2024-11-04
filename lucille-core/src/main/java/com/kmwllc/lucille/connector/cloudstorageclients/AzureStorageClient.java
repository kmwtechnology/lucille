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
  public String getContainerOrBucketName() {
    String subPath = pathToStorageURI.getPath().substring(1);
    return subPath.split("/")[0];
  }

  @Override
  public String getStartingDirectory() {
    String path = pathToStorageURI.getPath();
    int secondSlashIndex = path.indexOf("/", 1);
    return (secondSlashIndex + 1 < path.length()) ? path.substring(secondSlashIndex + 1) : "";
  }

  @Override
  public void init() {
    if (cloudOptions.containsKey("connectionString")) {
      containerClient = new BlobContainerClientBuilder()
          .connectionString((String) cloudOptions.get("connectionString"))
          .containerName(bucketOrContainerName)
          .buildClient();
    } else {
      String accountName = (String) cloudOptions.get("accountName");
      String accountKey = (String) cloudOptions.get("accountKey");

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
  }

  @Override
  public void publishFiles() {
    containerClient.listBlobsByHierarchy("/", new ListBlobsOptions().setPrefix(startingDirectory).setMaxResultsPerPage(maxNumOfPages), Duration.ofSeconds(10)).stream()
        .forEach(blob -> {
          if (isNotValid(blob)) {
            return;
          }
          try {
            Document doc = blobItemToDoc(blob);
            publisher.publish(doc);
          } catch (Exception e) {
            log.error("Error publishing blob: {}", blob.getName(), e);
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
    doc.setField(FileConnector.CONTENT, properties.getContentMd5());

    return doc;
  }

  private boolean isNotValid(BlobItem blob) {
    if (blob.isPrefix()) return true;

    return shouldSkipBasedOnRegex(blob.getName());
  }
}
