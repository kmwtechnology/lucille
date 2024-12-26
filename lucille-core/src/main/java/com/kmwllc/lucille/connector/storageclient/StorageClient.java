package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.AZURE_ACCOUNT_KEY;
import static com.kmwllc.lucille.connector.FileConnector.GOOGLE_SERVICE_KEY;
import static com.kmwllc.lucille.connector.FileConnector.S3_ACCESS_KEY_ID;
import static com.kmwllc.lucille.connector.FileConnector.S3_SECRET_ACCESS_KEY;
import static com.kmwllc.lucille.connector.FileConnector.S3_REGION;
import static com.kmwllc.lucille.connector.FileConnector.AZURE_CONNECTION_STRING;
import static com.kmwllc.lucille.connector.FileConnector.AZURE_ACCOUNT_NAME;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Interface for storage clients. Implementations of this interface should be able to traverse a storage system
 * and publish files to the Lucille pipeline.
 *
 *  - create : create appropriate client based on the URI scheme with authentication/settings from cloudOptions
 *  - init : Initialize the client
 *  - shutdown : Shutdown the client
 *  - traverse : traverse through the storage client and publish files to Lucille pipeline
 *  - validateCloudOptions : Validate that respective cloud authentication information is present for the given cloud provider.
 *    Does not validate the correctness of the authentication information.
 */

public interface StorageClient {

  /**
   * Initialize the client and creates necessary connections and/or resources
   */
  void init() throws ConnectorException;

  /**
   * Shutdown the client, closes any open connections and/or resources
   */
  void shutdown() throws IOException;

  /**
   * Traverses through the storage client and publish files to Lucille pipeline
   */
  void traverse(Publisher publisher) throws Exception;

  /**
   * Gets the appropriate client based on the URI scheme and validate with authentication/settings from cloudOptions
   */
  static StorageClient create(URI pathToStorage, String docIdPrefix, List<Pattern> excludes, List<Pattern> includes,
      Map<String, Object> cloudOptions, Config fileOptions) {
    String activeClient = pathToStorage.getScheme() != null ? pathToStorage.getScheme() : "file";
    switch (activeClient) {
      case "gs" -> {
        validateCloudOptions(pathToStorage, cloudOptions);
        return new GoogleStorageClient(pathToStorage, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
      }
      case "s3" -> {
        validateCloudOptions(pathToStorage, cloudOptions);
        return new S3StorageClient(pathToStorage, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
      }
      case "https" -> {
        String authority = pathToStorage.getAuthority();
        if (authority != null && authority.contains("blob.core.windows.net")) {
          validateCloudOptions(pathToStorage, cloudOptions);
          return new AzureStorageClient(pathToStorage, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
        } else {
          throw new IllegalArgumentException("Unsupported client type: " + activeClient + " with authority: " + authority);
        }
      }
      case "file" -> {
        return new LocalStorageClient(pathToStorage, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
      }
      default -> throw new IllegalArgumentException("Unsupported client type: " + activeClient);
    }
  }

  /**
   * Validate that respective cloud authentication information is present for the given cloud provider.
   * Does not validate the correctness of the authentication information.
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
