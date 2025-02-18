package com.kmwllc.lucille.util;

import com.kmwllc.lucille.connector.storageclient.StorageClient;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.StageException;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for working with StorageClients to repeatedly fetch the contents of String paths, determining the appropriate
 * storage provider.
 */
public class FileContentFetcher {
  private static final Logger log = LoggerFactory.getLogger(FileContentFetcher.class);

  private final Map<String, StorageClient> availableClients;

  /**
   * Creates a FileContentFetcher that will only fetch files from the classpath or the local file system
   */
  public FileContentFetcher() {
    this.availableClients = StorageClient.clientsFromCloudOptions(Map.of());
  }

  public FileContentFetcher(Map<String, Object> cloudOptions) {
    this.availableClients = StorageClient.clientsFromCloudOptions(cloudOptions);
  }

  /**
   * Initialize the storage clients associated with this FileContentFetcher. Calls shutdown on all clients and throws an Exception
   * if an error occurs while initializing any of the StorageClients.
   */
  public void startup() throws StageException {
    for (StorageClient client : availableClients.values()) {
      try {
        client.init();
      } catch (ConnectorException e) {
        shutdown();

        throw new StageException("Unable to initialize StorageClient.", e);
      }
    }
  }

  /**
   * Shutdown each of the storage clients associated with this FileContentFetcher. If an error occurs, a
   * warning is logged.
   */
  public void shutdown() {
    for (StorageClient client : availableClients.values()) {
      try {
        client.shutdown();
      } catch (IOException e) {
        log.warn("Error shutting down StorageClient", e);
      }
    }
  }

  /**
   * Attempts to return an InputStream for the file at the given path. If the path begins with "classpath:" the prefix will be removed
   * and the file will be read from the classpath. Otherwise, if the path is a URI, a corresponding storage client will be used;
   * if not, the local file system will be used. Will not return a null InputStream - instead, an exception will be thrown.
   */
  public InputStream getInputStream(String path) throws IOException {
    InputStream is;

    if (path.startsWith("classpath:")) {
      is = FileContentFetcher.class.getClassLoader().getResourceAsStream(path.substring(path.indexOf(":") + 1));
    } else if (FileUtils.isValidURI(path)) {
      URI pathURI = URI.create(path);
      String activeClient = pathURI.getScheme() != null ? pathURI.getScheme() : "file";
      StorageClient storageClient = availableClients.get(activeClient);
      try {
        is = storageClient.getFileContentStream(pathURI);
      } catch (Exception e) {
        throw new IOException("Error getting InputStream on StorageClient.", e);
      }

    } else {
      is = new FileInputStream(path);
    }

    if (is == null) {
      throw new IOException("No InputStream could be created for the path " + path + ".");
    }

    return is;
  }

  /**
   * Returns a Reader for the file at the given path. If the path begins with "classpath:" the prefix will be removed
   * and the file will be read from the classpath. Otherwise, it will be read from the local file system. Will use UTF-8
   * encoding.
   */
  public Reader getReader(String path) throws IOException {
    return getReader(path, "utf-8");
  }

  /**
   * Returns a Reader for the file at the given path using the given encoding. If the path begins with "classpath:"
   * the prefix will be removed and the file will be read from the classpath. Otherwise, it will be read from the local file system.
   */
  public Reader getReader(String path, String encoding) throws IOException {
    InputStream stream = getInputStream(path);

    return new BufferedReader(new InputStreamReader(stream, encoding));
  }

  /**
   * Counts the number of lines in the file at the given path.
   */
  public int countLines(String path) throws IOException {
    try (BufferedReader reader = new BufferedReader(getReader(path))) {
      int lines = 0;

      while (reader.readLine() != null) {
        lines++;
      }

      return lines;
    }
  }
}
