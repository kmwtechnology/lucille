package com.kmwllc.lucille.connector.cloudstorageclients;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleStorageClient extends BaseStorageClient {

  private static final Logger log = LoggerFactory.getLogger(FileConnector.class);
  private Storage storage;

  public GoogleStorageClient(URI pathToStorage, Publisher publisher, String docIdPrefix, List<Pattern> excludes, List<Pattern> includes,
      Map<String, Object> cloudOptions) {
    super(pathToStorage, publisher, docIdPrefix, excludes, includes, cloudOptions);
  }

  @Override
  public void init() {
    try (FileInputStream serviceAccountStream = new FileInputStream((String) cloudOptions.get(FileConnector.GOOGLE_SERVICE_KEY))) {
      storage = StorageOptions.newBuilder()
          .setCredentials(ServiceAccountCredentials.fromStream(serviceAccountStream))
          .build()
          .getService();
    } catch (IOException e) {
      throw new IllegalArgumentException("Error occurred getting/using credentials from pathToServiceKey", e);
    }
  }

  @Override
  public void shutdown() throws Exception {
    if (storage != null) {
      storage.close();
    }
  }

  @Override
  public void publishFiles() {
    Page<Blob> page = storage.list(bucketOrContainerName, BlobListOption.prefix(startingDirectory), Storage.BlobListOption.pageSize(maxNumOfPages));

    do {
      page.streamAll()
          .forEachOrdered(blob -> {
            if (isNotValid(blob)) {
              return;
            }
            try {
              Document doc = blobToDoc(blob, bucketOrContainerName);
              publisher.publish(doc);
            } catch (Exception e) {
              log.error("Unable to publish document '{}', SKIPPING", blob.getName(), e);
            }
          });
      page = page.hasNextPage() ? page.getNextPage() : null;
    } while (page != null);
  }

  private boolean isNotValid(Blob blob) {
    if (blob.isDirectory()) return true;

    return shouldSkipBasedOnRegex(blob.getName());
  }

  private Document blobToDoc(Blob blob, String bucketName) throws IOException {
    BlobInfo blobInfo = blob.asBlobInfo();
    final String docId = DigestUtils.md5Hex(blobInfo.getName());
    final Document doc = Document.create(docIdPrefix + docId);
    try {
      doc.setField(FileConnector.FILE_PATH, pathToStorageURI.getScheme() + "://" + bucketName + "/" + blobInfo.getName());
      doc.setField(FileConnector.MODIFIED, blobInfo.getUpdateTimeOffsetDateTime().toInstant());
      doc.setField(FileConnector.CREATED, blobInfo.getCreateTimeOffsetDateTime().toInstant());
      doc.setField(FileConnector.SIZE, blobInfo.getSize());

      if ((boolean) cloudOptions.get(FileConnector.GET_FILE_CONTENT)) {
        doc.setField(FileConnector.CONTENT, blob.getContent());
      }

    } catch (Exception e) {
      throw new IOException("Error occurred getting/setting file attributes to document: " + blobInfo.getName(), e);
    }
    return doc;
  }

  // Only for testing
  public void setStorageForTesting(Storage storage) {
    this.storage = storage;
  }
}
