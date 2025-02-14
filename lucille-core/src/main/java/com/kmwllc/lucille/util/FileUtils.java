package com.kmwllc.lucille.util;

import com.kmwllc.lucille.connector.storageclient.StorageClient;
import com.kmwllc.lucille.core.StageException;
import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtils {

  private static final Logger log = LoggerFactory.getLogger(FileUtils.class);

  /**
   * Attempts to return an InputStream for the file at the given path. If the path begins with "classpath:" the prefix will be removed
   * and the file will be read from the classpath. Otherwise, if the path is a URI, one of the storage clients will be used;
   * if not, the local file system will be used.
   */
  public static InputStream getInputStream(String path, Map<String, StorageClient> availableClients) throws IOException {
    InputStream is;

    if (path.startsWith("classpath:")) {
      is = FileUtils.class.getClassLoader().getResourceAsStream(path.substring(path.indexOf(":") + 1));
    } else if (isValidURI(path)) {
      URI pathURI = URI.create(path);
      StorageClient storageClient = StorageClient.clientForGettingURIStream(pathURI, availableClients);
      try {
        is = storageClient.getFileContentStream(pathURI);
      } catch (Exception e) {
        throw new IOException("Error getting InputStream on StorageClient.", e);
      }

    } else {
      is = new FileInputStream(path);
    }

    return is;
  }

  /**
   * Returns a Reader for the file at the given path. If the path begins with "classpath:" the prefix will be removed
   * and the file will be read from the classpath. Otherwise, it will be read from the local file system.
   */
  public static Reader getReader(String path) throws IOException {
    return getReader(path, "utf-8");
  }

  public static Reader getReader(String path, String encoding) throws IOException {
    InputStream inputStreamForPath = getInputStream(path, null);
    // This method of creating the Reader is used because it handles non-UTF-8 characters by replacing them with UTF
    // chars, rather than throwing an Exception.
    // https://stackoverflow.com/questions/26268132/all-inclusive-charset-to-avoid-java-nio-charset-malformedinputexception-input
    // return Files.newBufferedReader(Paths.get(path), StandardCharsets.UTF_8);
    return new BufferedReader(new InputStreamReader(inputStreamForPath, encoding));
  }

  // TODO : Potentially support setting home dir in config
  public static String getLucilleHomeDirectory() {
    String homeDir = System.getProperty("LUCILLE_HOME");

    if (homeDir != null) {
      return Path.of(homeDir).toAbsolutePath().toString();
    }

    return System.getProperty("user.dir");
  }

  public static boolean isValidURI(String uriString) {
    try {
      URI rawURI = URI.create(uriString);
      return rawURI.getScheme() != null && !rawURI.getScheme().trim().isEmpty();
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Get a Reader for the given path.
   *
   * @param path file path
   * @return Reader object
   * @throws StageException if the file does not exist or cannot be read
   */
  public static Reader getFileReader(String path) throws StageException {
    try {
      return getReader(path);
    } catch (Exception e) {
      throw new StageException("File does not exist: " + path);
    }
  }

  /**
   * Count the number of lines in a file.
   *
   * @param filename file path
   * @return number of lines
   */
  public static int countLines(String filename) throws StageException {
    try (BufferedReader reader = new BufferedReader(getFileReader(filename))) {
      int lines = 0;
      while (reader.readLine() != null) {
        lines++;
      }
      return lines;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
