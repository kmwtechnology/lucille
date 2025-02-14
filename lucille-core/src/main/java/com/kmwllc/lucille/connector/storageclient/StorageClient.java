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
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Interface for storage clients. Implementations of this interface should be able to traverse a storage system
 * and publish files to the Lucille pipeline.
 *
 *  - create : create appropriate client based on the URI scheme with authentication/settings from cloudOptions. Authentication only checks that required information is present
 *  - init : Initialize the client
 *  - shutdown : Shutdown the client
 *  - traverse : traverse through the storage client and publish files to Lucille pipeline
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
   * Opens and returns an InputStream for a file's contents, located at the given URI.
   */
  InputStream getFileContentStream(URI uri) throws Exception;

  /**
   * Gets the appropriate client based on the URI scheme and validate with authentication/settings from cloudOptions
   */
  static StorageClient create(URI pathToStorage, String docIdPrefix, List<Pattern> excludes, List<Pattern> includes,
      Map<String, Object> cloudOptions, Config fileOptions) {
    String activeClient = pathToStorage.getScheme() != null ? pathToStorage.getScheme() : "file";
    switch (activeClient) {
      case "gs" -> {
        GoogleStorageClient.validateOptions(cloudOptions);
        return new GoogleStorageClient(pathToStorage, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
      }
      case "s3" -> {
        S3StorageClient.validateOptions(cloudOptions);
        return new S3StorageClient(pathToStorage, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
      }
      case "https" -> {
        String authority = pathToStorage.getAuthority();
        if (authority != null && authority.contains("blob.core.windows.net")) {
          AzureStorageClient.validateOptions(cloudOptions);
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
   * Based on the provided map of URI scheme Strings to StorageClients, returns the appropriate StorageClient for
   * handling the given URI. The return may be null, indicating there is not a StorageClient available to handle the
   * URI appropriately.
   */
  static StorageClient clientForGettingURIStream(URI pathToFile, Map<String, StorageClient> availableClients) {
    String activeClient = pathToFile.getScheme() != null ? pathToFile.getScheme() : "file";
    return availableClients.get(activeClient);
  }

  /**
   * Builds a list of all StorageClients which can be built from the given cloudOptions. Always returns at least
   * a LocalStorageClient.
   *
   * To build clients for the cloud providers, these arguments must be provided:
   * <br> If using GoogleStorageClient:
   * <br> "pathToServiceKey" : "path/To/Service/Key.json"
   * <br> If using AzureStorageClient:
   * <br> "connectionString" : azure connection string
   * <br> <b>or</b>
   * <br> "accountName" : azure account name
   * <br> "accountKey" : azure account key
   * <br> If using S3StorageClient:
   * <br> "accessKeyId" : s3 key id
   * <br> "secretAccessKey" : secret access key
   * <br> "region" : s3 storage region
   */
  static Map<String, StorageClient> clientsFromCloudOptions(Map<String, Object> cloudOptions) {
    Map<String, StorageClient> results = new HashMap<>();

    results.put("file", new LocalStorageClient());

    if (GoogleStorageClient.validOptions(cloudOptions)) {
      results.put("gs", new GoogleStorageClient(cloudOptions));
    }

    if (AzureStorageClient.validOptions(cloudOptions)) {
      results.put("https", new AzureStorageClient(cloudOptions));
    }

    if (S3StorageClient.validOptions(cloudOptions)) {
      results.put("s3", new S3StorageClient(cloudOptions));
    }

    return results;
  }
}
