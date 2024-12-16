package com.kmwllc.lucille.connector.storageclients;

import static com.kmwllc.lucille.connector.FileConnector.AZURE_ACCOUNT_KEY;
import static com.kmwllc.lucille.connector.FileConnector.GOOGLE_SERVICE_KEY;
import static com.kmwllc.lucille.connector.FileConnector.S3_ACCESS_KEY_ID;
import static com.kmwllc.lucille.connector.FileConnector.S3_SECRET_ACCESS_KEY;
import static com.kmwllc.lucille.connector.FileConnector.S3_REGION;
import static com.kmwllc.lucille.connector.FileConnector.AZURE_CONNECTION_STRING;
import static com.kmwllc.lucille.connector.FileConnector.AZURE_ACCOUNT_NAME;

import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

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

  /**
   * Initialize the client, create any connections or resources
   */
  void init();

  /**
   * Shutdown the client, closes any open connections or resources
   */
  void shutdown() throws Exception;

  /**
   * Publish files to the Lucille pipeline
   */
  void publishFiles() throws Exception;

  /**
   * Gets the appropriate client based on the URI scheme and validate with authentication/settings from cloudOptions
   */
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
        String authority = pathToStorage.getAuthority();
        if (authority != null && authority.contains("blob.core.windows.net")) {
          validateCloudOptions(pathToStorage, cloudOptions);
          return new AzureStorageClient(pathToStorage, publisher, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
        } else {
          throw new RuntimeException("Unsupported client type: " + activeClient + " with authority: " + authority);
        }
      }
      case "file" -> {
        return new LocalStorageClient(pathToStorage, publisher, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
      }
      default -> throw new RuntimeException("Unsupported client type: " + activeClient);
    }
  }

  /**
   * Validate that respective cloud authentication information is provided for the given cloud provider.
   * Only checks the presence of required fields.
   */
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
