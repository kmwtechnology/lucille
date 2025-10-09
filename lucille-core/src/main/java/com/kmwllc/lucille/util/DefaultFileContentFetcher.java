package com.kmwllc.lucille.util;

import com.kmwllc.lucille.connector.storageclient.StorageClient;
import com.kmwllc.lucille.core.FileContentFetcher;
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
public class DefaultFileContentFetcher implements FileContentFetcher {
  private static final Logger log = LoggerFactory.getLogger(DefaultFileContentFetcher.class);

  private final Map<String, StorageClient> availableClients;
  private boolean started = false;

  /**
   * Creates a FileContentFetcher that will fetch files from the classpath, the local file system, and any cloud file systems
   * (Azure, S3, Google) which have the necessary options provided in the given Config. Be sure to call startup and shutdown
   * as appropriate.
   *
   * @param config Configuration for whatever is using a FileContentFetcher, and potentially has "https", "s3", and "gcp".
   */
  public DefaultFileContentFetcher(Config config) {
    this.availableClients = StorageClient.createClients(config);
  }

  /**
   * Initialize the storage clients associated with this FileContentFetcher. Calls shutdown on all clients and throws an Exception
   * if an error occurs while initializing any of the StorageClients.
   *
   * @throws IOException If an error occurs initializing this FileContentFetcher and starting up the related StorageClients.
   */
  @Override
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
  @Override
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
  @Override
  public InputStream getInputStream(String path) throws IOException {
    if (!started) {
      throw new IOException("FileContentFetcher has not had startup called or has been shutdown.");
    }

    InputStream is;

    if (path.startsWith("classpath:")) {
      is = DefaultFileContentFetcher.class.getClassLoader().getResourceAsStream(path.substring(path.indexOf(":") + 1));
    } else if (FileUtils.isValidURI(path)) {
      URI pathURI = URI.create(path);

      // scheme won't be null / empty, as we make sure the URI is valid.
      String activeClient = pathURI.getScheme();
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
  @Override
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
  @Override
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
  @Override
  public int countLines(String path) throws IOException {
    try (BufferedReader reader = getReader(path)) {
      int lines = 0;

      while (reader.readLine() != null) {
        lines++;
      }

      return lines;
    }
  }

}
