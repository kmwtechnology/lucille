package com.kmwllc.lucille.connector.storageclient;

import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Interface for storage clients. Implementations of this interface should be able to traverse a storage system
 * and publish files to the Lucille pipeline.
 *
 *  - create : create appropriate client based on the URI scheme with authentication/settings from Config. Authentication only checks that required information is present
 *  - init : Initialize the client
 *  - shutdown : Shutdown the client
 *  - validateOptions: Whether the storage client's Config is sufficient for it to connect to its file system. See below for necessary keys / arguments.
 *  - traverse : traverse through the storage client and publish files to Lucille pipeline
 *  - getFileContentStream : Given a URI to a file in the storage client's system, return an InputStream for the file's contents
 *  - createClients : Given a Config, build a map of StorageClients that can be created from the supplied options, using the URI schemes as keys for the map.
 */

public interface StorageClient {

  /**
   * Initialize the client and creates necessary connections and/or resources
   */
  void init() throws IOException;

  /**
   * Shutdown the client, closes any open connections and/or resources
   */
  void shutdown() throws IOException;

  /**
   * Traverses through the storage client and publish files to Lucille pipeline
   */
  void traverse(Publisher publisher, TraversalParams params) throws Exception;

  /**
   * Opens and returns an InputStream for a file's contents, located at the given URI.
   */
  InputStream getFileContentStream(URI uri) throws IOException;

  /**
   * Gets the appropriate client based on the URI scheme and validate with authentication/settings from the Config.
   */
  static StorageClient create(URI pathToStorage, Config connectorConfig) {
    String activeClient = pathToStorage.getScheme() != null ? pathToStorage.getScheme() : "file";
    switch (activeClient) {
      case "gs" -> {
        if (!connectorConfig.hasPath("gcp")) {
          throw new IllegalArgumentException("Path is to Google Cloud but no options provided.");
        }
        Config gcpOptions = connectorConfig.getConfig("gcp");
        return new GoogleStorageClient(gcpOptions);
      }
      case "s3" -> {
        if (!connectorConfig.hasPath("s3")) {
          throw new IllegalArgumentException("Path is to S3 but no options provided.");
        }
        Config s3Options = connectorConfig.getConfig("s3");
        return new S3StorageClient(s3Options);
      }
      case "https" -> {
        String authority = pathToStorage.getAuthority();
        if (authority != null && authority.contains("blob.core.windows.net")) {
          if (!connectorConfig.hasPath("azure")) {
            throw new IllegalArgumentException("Path is to Azure but no options provided.");
          }
          Config azureOptions = connectorConfig.getConfig("azure");
          return new AzureStorageClient(azureOptions);
        } else {
          throw new IllegalArgumentException("Unsupported client type: " + activeClient + " with authority: " + authority);
        }
      }
      case "file" -> {
        return new LocalStorageClient();
      }
      default -> throw new IllegalArgumentException("Unsupported client type: " + activeClient);
    }
  }

  /**
   * Builds a map of all StorageClients which can be built from the given Config. Always returns at least
   * a LocalStorageClient (mapped to "file"). The map uses the cloud provider's URI schemes as keys (gs, https,
   * s3, and file).
   *
   * To build clients for the cloud providers, these arguments must be provided in separate maps:
   * <br> gcp:
   *   "pathToServiceKey" : "path/To/Service/Key.json"
   *   "maxNumOfPages" : number of references of the files loaded into memory in a single fetch request. Optional, defaults to 100
   *
   * <br> s3:
   *   "accessKeyId" : s3 key id
   *   "secretAccessKey" : secret access key
   *   "region" : s3 storage region
   *   "maxNumOfPages" : number of references of the files loaded into memory in a single fetch request. Optional, defaults to 100
   *
   * <br> azure:
   *   "connectionString" : azure connection string
   * <b>Or</b>
   *   "accountName" : azure account name
   *   "accountKey" : azure account key
   *   "maxNumOfPages" : number of references of the files loaded into memory in a single fetch request. Optional, defaults to 100
   */
  static Map<String, StorageClient> createClients(Config config) {
    Map<String, StorageClient> results = new HashMap<>();

    results.put("file", new LocalStorageClient());

    if (config.hasPath("gcp")) {
      Config gCloudOptions = config.getConfig("gcp");
      results.put("gs", new GoogleStorageClient(gCloudOptions));
    }

    if (config.hasPath("azure")) {
      Config azureOptions = config.getConfig("azure");
      results.put("https", new AzureStorageClient(azureOptions));
    }

    if (config.hasPath("s3")) {
      Config s3Options = config.getConfig("s3");
      results.put("s3", new S3StorageClient(s3Options));
    }

    return results;
  }

  /**
   * Creates a full document ID from the initial docId and the given params. Adds the parameters' ID prefix
   * to the beginning of the initial ID.
   *
   * @param docId The initial docId for your document.
   * @param params Parameters for your storage traversal.
   * @return A full document ID to use, adding the parameters' ID prefix to the beginning of the initial ID.
   */
  static String createDocId(String docId, TraversalParams params) {
    return params.getDocIdPrefix() + docId;
  }
}
