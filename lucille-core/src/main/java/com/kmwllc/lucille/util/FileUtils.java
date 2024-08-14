package com.kmwllc.lucille.util;


import com.kmwllc.lucille.core.StageException;
import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileUtils {

  private static final Logger log = LogManager.getLogger(FileUtils.class);
  /**
   * Returns a Reader for the file at the given path. If the path begins with "classpath:" the prefix will be removed
   * and the file will be read from the classpath. If the path appears to be a URI, it will be accessed using VFS.
   * Otherwise, it will be read from the filesystem.
   */
  public static Reader getReader(String path) throws IOException {
    return getReader(path, "utf-8");
  }

  public static Reader getReader(String path, String encoding) throws IOException {
    InputStream is;
    if (!path.startsWith("classpath:")) {
      if (isValidURI(path)) {
        is = VFSInputStream.open(path);
      } else {
        is = new FileInputStream(path);
      }
    } else {
      is = FileUtils.class.getClassLoader().getResourceAsStream(path.substring(path.indexOf(":") + 1));
    }
    // This method of creating the Reader is used because it handles non-UTF-8 characters by replacing them with UTF
    // chars, rather than throwing an Exception.
    // https://stackoverflow.com/questions/26268132/all-inclusive-charset-to-avoid-java-nio-charset-malformedinputexception-input
    // return Files.newBufferedReader(Paths.get(path), StandardCharsets.UTF_8);
    return new BufferedReader(new InputStreamReader(is, encoding));
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
   * Takes a path and fsManager to retrieve the FileObject
   *
   * @param path the path to file as a String
   * @param fsManager the fileSystem manager instance
   * @return the FileObject to retrieve content from
   */
  public static FileObject getFileObject(String path, FileSystemManager fsManager) {
    try {
      // transforming non-absolute path to absolute path if path is no absolute
      if (!isValidURI(path)) {
        path = Paths.get(path).toAbsolutePath().toString();
      }
      return fsManager.resolveFile(path);
    } catch (FileSystemException e) {
      log.warn("Error retrieving file contents: {}", path, e);
      return null;
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
    } catch (NullPointerException | IOException e) {
      throw new StageException("File does not exist: " + path);
    }
  }

  /**
   * Count the number of lines in a file.
   *
   * @param filename file path
   * @return number of lines
   * @throws StageException if the file does not exist or cannot be read
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
