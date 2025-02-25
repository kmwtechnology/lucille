package com.kmwllc.lucille.util;

import com.kmwllc.lucille.connector.storageclient.StorageClient;
import com.typesafe.config.Config;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
   * Creates a FileContentFetcher that will fetch files from the classpath, the local file system, and any cloud file systems
   * (Azure, S3, Google) which have the necessary options provided in the given options map. Be sure to call startup and shutdown
   * as appropriate.
   */
  public FileContentFetcher(Config cloudOptions) {
    this.availableClients = StorageClient.createClients(cloudOptions);
  }

  /**
   * Initialize the storage clients associated with this FileContentFetcher. Calls shutdown on all clients and throws an Exception
   * if an error occurs while initializing any of the StorageClients.
   */
  public void startup() throws IOException {
    for (StorageClient client : availableClients.values()) {
      try {
        client.init();
      } catch (IOException e) {
        shutdown();

        throw new IOException("Unable to initialize StorageClient.", e);
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

      if (storageClient == null) {
        throw new IOException("No Storage Client available to get content for path " + path + ".");
      }

      is = storageClient.getFileContentStream(pathURI);
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
  public BufferedReader getReader(String path) throws IOException {
    return getReader(path, "utf-8");
  }

  /**
   * Returns a Reader for the file at the given path using the given encoding. If the path begins with "classpath:"
   * the prefix will be removed and the file will be read from the classpath. Otherwise, it will be read from the local file system.
   */
  public BufferedReader getReader(String path, String encoding) throws IOException {
    InputStream stream = getInputStream(path);

    // This method of creating the Reader is used because it handles non-UTF-8 characters by replacing them with UTF
    // chars, rather than throwing an Exception.
    // https://stackoverflow.com/questions/26268132/all-inclusive-charset-to-avoid-java-nio-charset-malformedinputexception-input
    return new BufferedReader(new InputStreamReader(stream, encoding));
  }

  /**
   * Counts the number of lines in the file at the given path.
   */
  public int countLines(String path) throws IOException {
    try (BufferedReader reader = getReader(path)) {
      int lines = 0;

      while (reader.readLine() != null) {
        lines++;
      }

      return lines;
    }
  }

  // -- Convenience / Static Methods --
  /**
   * Returns an InputStream for the file at the given path, which could be in the classpath, a local file path, or a URI
   * to a supported Storage service. An exception may occur - in this case, the StorageClient(s) created will be shutdown.
   * If an InputStream is returned successfully, its close() method will shutdown the StorageClient(s) this method temporarily
   * creates. Be sure to close the returned stream!
   *
   * If an object is making repeated calls to this function, it should instead manage its own instance of a FileContentFetcher.
   */
  public static InputStream getOneTimeInputStream(String path, Config cloudOptions) throws IOException {
    // Before attempting to use a storage client, handle classpath files as a special case.
    if (path.startsWith("classpath:")) {
      return FileContentFetcher.class.getClassLoader().getResourceAsStream(path.substring(path.indexOf(":") + 1));
    }

    URI pathURI;
    StorageClient clientForFile;

    try {
      pathURI = URI.create(path);
      clientForFile = StorageClient.create(pathURI, cloudOptions);
    } catch (IllegalArgumentException e) {
      throw new IOException("Error setting up to fetch file content.", e);
    }

    try {
      clientForFile.init();
      InputStream result = clientForFile.getFileContentStream(pathURI);

      return new InputStream() {
        @Override
        public int read() throws IOException {
          return result.read();
        }

        @Override
        public void close() throws IOException {
          try {
            result.close();
          } finally {
            // shutdown if an IOException is thrown
            clientForFile.shutdown();
          }
        }
      };
    } catch (IOException e) {
      clientForFile.shutdown();
      throw e;
    }
  }

  /**
   * Returns a reader for the file at the given path using UTF-8 encoding. The file could be in the classpath, a local file path, or a URI
   * to a supported Storage service. An exception may occur - in this case, the StorageClient(s) created will be shutdown.
   * If an Reader is returned successfully, its close() method will shutdown the StorageClient(s) this method temporarily
   * creates. Be sure to close the returned Reader!
   *
   * If an object is making repeated calls to this function, it should instead manage its own instance of a FileContentFetcher.
   */
  public static BufferedReader getOneTimeReader(String path, Config cloudOptions) throws IOException {
    return getOneTimeReader(path, "utf-8", cloudOptions);
  }

  /**
   * Returns a reader for the file at the given path using the given encoding. The file could be in the classpath, a local file path, or a URI
   * to a supported Storage service. An exception may occur - in this case, the StorageClient(s) created will be shutdown.
   * If an Reader is returned successfully, its close() method will shutdown the StorageClient(s) this method temporarily
   * creates. Be sure to close the returned Reader!
   *
   * If an object is making repeated calls to this function, it should instead manage its own instance of a FileContentFetcher.
   */
  public static BufferedReader getOneTimeReader(String path, String encoding, Config cloudOptions) throws IOException {
    InputStream result = getOneTimeInputStream(path, cloudOptions);

    return new BufferedReader(new InputStreamReader(result, encoding));
  }
}
