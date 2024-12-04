package com.kmwllc.lucille.connector.storageclients;

import static com.kmwllc.lucille.connector.FileConnector.AZURE_ACCOUNT_KEY;
import static com.kmwllc.lucille.connector.FileConnector.GET_FILE_CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.GOOGLE_SERVICE_KEY;
import static com.kmwllc.lucille.connector.FileConnector.S3_ACCESS_KEY_ID;
import static com.kmwllc.lucille.connector.FileConnector.S3_SECRET_ACCESS_KEY;
import static com.kmwllc.lucille.connector.FileConnector.S3_REGION;
import static com.kmwllc.lucille.connector.FileConnector.AZURE_CONNECTION_STRING;
import static com.kmwllc.lucille.connector.FileConnector.AZURE_ACCOUNT_NAME;

import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface for cloud storage clients. Implementations of this interface should be able to connect to a cloud storage service
 * and publish files to Lucille.
 *
 *  - getClient() - Get the appropriate client based on the URI scheme with authentication/settings from cloudOptions
 *  - init() - Initialize the client
 *  - shutdown() - Shutdown the client
 *  - publishFiles() - Publish files to Lucille
 *
 */
public interface StorageClient {
  Logger log = LoggerFactory.getLogger(FileConnector.class);

  static StorageClient getClient(URI pathToStorage, Publisher publisher, String docIdPrefix, List<Pattern> excludes, List<Pattern> includes,
      Map<String, Object> cloudOptions, Config fileOptions) {
    String activeClient = pathToStorage.getScheme() != null ? pathToStorage.getScheme() : "file";
    switch (activeClient) {
      case "gs" -> {
        validateCloudOptions(pathToStorage, cloudOptions);
        return new GoogleStorageClient(pathToStorage, publisher, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
      }
      case "s3" -> {
        validateCloudOptions(pathToStorage, cloudOptions);
        return new S3StorageClient(pathToStorage, publisher, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
      }
      case "https" -> {
        if (pathToStorage.getAuthority().contains("blob.core.windows.net")) {
          validateCloudOptions(pathToStorage, cloudOptions);
          return new AzureStorageClient(pathToStorage, publisher, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
        } else {
          throw new RuntimeException("Unsupported client type: " + activeClient);
        }
      }
      case "file" -> {
        return new LocalStorageClient(pathToStorage, publisher, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
      }
      default -> throw new RuntimeException("Unsupported client type: " + activeClient);
    }
  }

  void init();

  void shutdown() throws Exception;

  void publishFiles() throws Exception;

  static void validateCloudOptions(URI storageURI, Map<String, Object> cloudOptions) {
    switch (storageURI.getScheme()) {
      case "gs":
        if (!cloudOptions.containsKey(GOOGLE_SERVICE_KEY)) {
          throw new IllegalArgumentException("Missing 'pathToServiceKey' in cloudOptions for Google Cloud storage.");
        }
        break;
      case "s3":
        if (!cloudOptions.containsKey(S3_ACCESS_KEY_ID) || !cloudOptions.containsKey(S3_SECRET_ACCESS_KEY) || !cloudOptions.containsKey(S3_REGION)) {
          throw new IllegalArgumentException("Missing '" + S3_ACCESS_KEY_ID + "' or '" + S3_SECRET_ACCESS_KEY + "' or '" + S3_REGION + "' in cloudOptions for s3 storage.");
        }
        break;
      case "https":
        if (!storageURI.getAuthority().contains("blob.core.windows.net")) {
          throw new IllegalArgumentException("Unsupported client type: " + storageURI.getScheme());
        } else if (!cloudOptions.containsKey(AZURE_CONNECTION_STRING) &&
            !(cloudOptions.containsKey(AZURE_ACCOUNT_NAME) && cloudOptions.containsKey(AZURE_ACCOUNT_KEY))) {
          throw new IllegalArgumentException("Either '" + AZURE_CONNECTION_STRING + "' or '" + AZURE_ACCOUNT_NAME + "' & '" + AZURE_ACCOUNT_KEY + "' has to be in cloudOptions for Azure storage.");
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported client type: " + storageURI.getScheme());
    }
  }
}
