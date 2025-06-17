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
 */
public interface StorageClient {

  /**
   * Initialize the client and creates necessary connections and/or resources.
   * @throws IOException If an error occurs during initialization.
   */
  void init() throws IOException;

  /**
   * Shutdown the client, closes any open connections and/or resources.
   * @throws IOException If an error occurs during shutdown.
   */
  void shutdown() throws IOException;

  /**
   * Traverses through the storage client and publish files to Lucille pipeline
   * @param publisher The Publisher you want to publish documents to.
   * @param params Parameters / options regarding the traversal of the client's file system.
   * @throws Exception If an error occurs during traversal.
   */
  void traverse(Publisher publisher, TraversalParams params) throws Exception;

  /**
   * Opens and returns an InputStream for a file's contents, located at the given URI.
   * @param uri A URI to the file whose contents you want to extract.
   * @return An InputStream for the file's contents.
   * @throws IOException If an error occurs while getting the file's contents.
   */
  InputStream getFileContentStream(URI uri) throws IOException;

  /**
   * Moves the file at the given String to the folder at the given URI.
   * @param filePath The full path to the file that you want to move.
   * @param folder A URI to the folder that you want to move the file to.
   * @throws IOException If an error occurs moving the file.
   */
  void moveFile(URI filePath, URI folder) throws IOException;

  /**
   * Gets the appropriate client based on the URI scheme and validate with authentication/settings from the Config.
   * @param pathToStorage A URI to storage - either local or cloud - that you need a StorageClient for.
   * @param connectorConfig Configuration for your connector, which should contain configuration for cloud storage clients
   *                        as needed.
   * @return The appropriate, configured storage client to use for your traversal.
   *
   * @throws IllegalArgumentException If your path to storage requires support for a cloud storage client that you did not provide,
   * or you provided a URI with an unsupported scheme.
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
   * a LocalStorageClient (keyed by "file"). The map uses the cloud provider's URI schemes as keys (gs, https,
   * s3, and file).
   *
   * To build clients for the cloud providers, these arguments must be provided in separate maps:
   * <br> gcp:
   * "pathToServiceKey" : "path/To/Service/Key.json"
   * "maxNumOfPages" : number of references of the files loaded into memory in a single fetch request. Optional, defaults to 100
   *
   * <br> s3:
   * "accessKeyId" : s3 key id. Not needed if secretAccessKey is not specified (using default credentials).
   * "secretAccessKey" : secret access key. Not needed if accessKeyId is not specified (using default credentials).
   * "region" : s3 storage region
   * "maxNumOfPages" : number of references of the files loaded into memory in a single fetch request. Optional, defaults to 100
   *
   * <br> azure:
   * "connectionString" : azure connection string
   * <b>Or</b>
   * "accountName" : azure account name
   * "accountKey" : azure account key
   * "maxNumOfPages" : number of references of the files loaded into memory in a single fetch request. Optional, defaults to 100
   *
   * @param config The configuration which potentially contains cloud options that you want to use to build storage clients.
   * @return A map of Strings to StorageClients. StorageClients are keyed by their URI scheme (gs, https, s3, file). Always
   * includes the LocalStorageClient, keyed by "file".
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
