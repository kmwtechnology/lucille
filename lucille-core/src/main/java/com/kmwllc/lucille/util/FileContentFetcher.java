package com.kmwllc.lucille.util;

import com.kmwllc.lucille.connector.storageclient.StorageClient;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
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
  private boolean started = false;

  /**
   * Creates a FileContentFetcher that will fetch files from the classpath, the local file system, and any cloud file systems
   * (Azure, S3, Google) which have the necessary options provided in the given Config. Be sure to call startup and shutdown
   * as appropriate.
   *
   * @param config Configuration for whatever is using a FileContentFetcher, and potentially has "https", "s3", and "gcp".
   */
  public FileContentFetcher(Config config) {
    this.availableClients = StorageClient.createClients(config);
  }

  /**
   * Initialize the storage clients associated with this FileContentFetcher. Calls shutdown on all clients and throws an Exception
   * if an error occurs while initializing any of the StorageClients.
   *
   * @throws IOException If an error occurs initializing this FileContentFetcher and starting up the related StorageClients.
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

    started = true;
  }

  /**
   * Shutdown each of the storage clients associated with this FileContentFetcher. If an error occurs while shutting down a
   * StorageClient, a warning will be logged with the error.
   */
  public void shutdown() {
    started = false;

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
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Can be a classpath / local file path,
   *             or a URI to a cloud storage file.
   * @return An InputStream for the file's contents.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public InputStream getInputStream(String path) throws IOException {
    if (!started) {
      throw new IOException("FileContentFetcher has not had startup called or has been shutdown.");
    }

    InputStream is;

    if (path.startsWith("classpath:")) {
      is = FileContentFetcher.class.getClassLoader().getResourceAsStream(path.substring(path.indexOf(":") + 1));
    } else if (FileUtils.isValidURI(path)) {
      URI pathURI = URI.create(path);
      String activeClient = pathURI.getScheme() != null ? pathURI.getScheme() : "file";
      StorageClient storageClient = availableClients.get(activeClient);

      if (storageClient == null) {
        throw new IOException("No Storage Client available for path " + path + ".");
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
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Can be a classpath / local file path,
   *             or a URI to a cloud storage file.
   * @return A Reader for the file's contents, using UTF-8 encoding.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public BufferedReader getReader(String path) throws IOException {
    return getReader(path, "utf-8");
  }

  /**
   * Returns a Reader for the file at the given path using the given encoding. If the path begins with "classpath:"
   * the prefix will be removed and the file will be read from the classpath. Otherwise, it will be read from the local file system.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Can be a classpath / local file path,
   *             or a URI to a cloud storage file.
   * @param encoding The encoding you want the Reader to use.
   * @return A Reader for the file's contents, using the given encoding.
   * @throws IOException If an error occurs getting the file's contents.
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
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Can be a classpath / local file path,
   *             or a URI to a cloud storage file.
   * @return The number of lines in the given file.
   * @throws IOException If an error occurs getting the file's contents.
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

  // -- Convenience / Static Methods - For One-Time Use to Avoid Managing an Instance --
  /**
   * Returns an InputStream for the file at the given path, which could be in the classpath, a local file path, or a URI
   * to a supported Storage service. An exception may occur - in this case, the StorageClient(s) created will be shutdown.
   * If an InputStream is returned successfully, its close() method will shutdown the StorageClient(s) this method temporarily
   * creates. Be sure to close the returned stream!
   *
   * If an object is making repeated calls to this function, it should instead manage its own instance of a FileContentFetcher.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Can be a classpath / local file path,
   *             or a URI to a cloud storage file.
   * @param config Configuration for the object calling this method, which potentially has "https", "s3", and "gcp".
   * @return An InputStream for the file's contents.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public static InputStream getOneTimeInputStream(String path, Config config) throws IOException {
    // Before attempting to use a storage client, handle classpath / non URI files as a special case.
    if (path.startsWith("classpath:")) {
      return FileContentFetcher.class.getClassLoader().getResourceAsStream(path.substring(path.indexOf(":") + 1));
    } else if (!FileUtils.isValidURI(path)) {
      return new FileInputStream(path);
    }

    URI pathURI;
    StorageClient clientForFile;

    try {
      pathURI = URI.create(path);
      clientForFile = StorageClient.create(pathURI, config);
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
   * If a Reader is returned successfully, its close() method will shutdown the StorageClient(s) this method temporarily
   * creates. Be sure to close the returned Reader!
   *
   * If an object is making repeated calls to this function, it should instead manage its own instance of a FileContentFetcher.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Can be a classpath / local file path,
   *             or a URI to a cloud storage file.
   * @param config Configuration for the object calling this method, which potentially has "https", "s3", and "gcp".
   * @return A Reader for the file's contents, using UTF-8 encoding.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public static BufferedReader getOneTimeReader(String path, Config config) throws IOException {
    return getOneTimeReader(path, "utf-8", config);
  }

  /**
   * Returns a reader for the file at the given path using the given encoding. The file could be in the classpath, a local file path, or a URI
   * to a supported Storage service. An exception may occur - in this case, the StorageClient(s) created will be shutdown.
   * If a Reader is returned successfully, its close() method will shutdown the StorageClient(s) this method temporarily
   * creates. Be sure to close the returned Reader!
   *
   * If an object is making repeated calls to this function, it should instead manage its own instance of a FileContentFetcher.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Can be a classpath / local file path,
   *             or a URI to a cloud storage file.
   * @param encoding The encoding you want the Reader to use.
   * @param config Configuration for the object calling this method, which potentially has "https", "s3", and "gcp".
   * @return A Reader for the file's contents, using the given encoding.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public static BufferedReader getOneTimeReader(String path, String encoding, Config config) throws IOException {
    InputStream result = getOneTimeInputStream(path, config);

    return new BufferedReader(new InputStreamReader(result, encoding));
  }

  // -- Extra Convenience - No CloudOptions --> Only classpath / local file paths / local file URIs --
  /**
   * Returns an InputStream for the file at the given path, which could be in the classpath, a local file path, or a URI
   * to a local file. An exception may occur - in this case, the StorageClient(s) created will be shutdown.
   * If an InputStream is returned successfully, its close() method will shutdown the StorageClient(s) this method temporarily
   * creates. Be sure to close the returned stream!
   *
   * If an object is making repeated calls to this function, it should instead manage its own instance of a FileContentFetcher.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Should be a local or classpath file.
   * @return An InputStream for the file's contents.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public static InputStream getOneTimeInputStream(String path) throws IOException {
    return getOneTimeInputStream(path, ConfigFactory.empty());
  }

  /**
   * Returns a reader for the file at the given path using UTF-8 encoding. The file could be in the classpath, a local file path, or a URI
   * to a local file. An exception may occur - in this case, the StorageClient(s) created will be shutdown.
   * If a Reader is returned successfully, its close() method will shutdown the StorageClient(s) this method temporarily
   * creates. Be sure to close the returned Reader!
   *
   * If an object is making repeated calls to this function, it should instead manage its own instance of a FileContentFetcher.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Should be a local or classpath file.
   * @return A Reader for the file's contents, using UTF-8 encoding.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public static BufferedReader getOneTimeReader(String path) throws IOException {
    return getOneTimeReader(path, ConfigFactory.empty());
  }

  /**
   * Returns a reader for the file at the given path using the given encoding. The file could be in the classpath, a local file path, or a URI
   * to a local file. An exception may occur - in this case, the StorageClient(s) created will be shutdown.
   * If a Reader is returned successfully, its close() method will shutdown the StorageClient(s) this method temporarily
   * creates. Be sure to close the returned Reader!
   *
   * If an object is making repeated calls to this function, it should instead manage its own instance of a FileContentFetcher.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Should be a local or classpath file.
   * @param encoding The encoding you want the Reader to use.
   * @return A Reader for the file's contents, using the given encoding.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public static BufferedReader getOneTimeReader(String path, String encoding) throws IOException {
    return getOneTimeReader(path, encoding, ConfigFactory.empty());
  }
}
