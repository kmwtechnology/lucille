package com.kmwllc.lucille.connector.cloudstorageclients;

import com.kmwllc.lucille.core.Publisher;
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

public interface CloudStorageClient {

  static CloudStorageClient getClient(URI pathToStorage, Publisher publisher, String docIdPrefix, List<Pattern> excludes, List<Pattern> includes,
      Map<String, Object> cloudOptions) {
    String activeClient = pathToStorage.getScheme();
    switch (activeClient) {
      case "gs" -> {
        return new GoogleStorageClient(pathToStorage, publisher, docIdPrefix, excludes, includes, cloudOptions);
      }
      case "s3" -> {
        return new S3StorageClient(pathToStorage, publisher, docIdPrefix, excludes, includes, cloudOptions);
      }
      case "https" -> {
        if (pathToStorage.getAuthority().contains("blob.core.windows.net")) {
          return new AzureStorageClient(pathToStorage, publisher, docIdPrefix, excludes, includes, cloudOptions);
        } else {
          throw new RuntimeException("Unsupported client type: " + activeClient);
        }
      }
      default -> throw new RuntimeException("Unsupported client type: " + activeClient);
    }
  }

  void init();

  void shutdown() throws Exception;

  void publishFiles();
}
