package com.kmwllc.lucille.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtils {

  private static final Logger log = LoggerFactory.getLogger(FileUtils.class);

  private FileUtils() {
  }

  // TODO : Potentially support setting home dir in config
  public static String getLucilleHomeDirectory() {
    String homeDir = System.getProperty("LUCILLE_HOME");

    if (homeDir != null) {
      return Path.of(homeDir).toAbsolutePath().toString();
    }

    return System.getProperty("user.dir");
  }

  /**
   * Returns whether the given String represents a valid URI, meaning a URI can be created from it, and the URI
   * has a non-null scheme.
   */
  public static boolean isValidURI(String uriString) {
    try {
      URI rawURI = URI.create(uriString);
      return rawURI.getScheme() != null;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Parses a configured path or URI into a URI.
   * <p>
   * Strings that are valid URIs are used as-is. Local file system paths that are not valid URIs, such as Windows paths
   * ("C:\data", "C:/data", "\\server\share"), are converted to an absolute {@code file://} URI.
   * <p>
   * Throws a URISyntaxException if the string is neither a valid URI nor a local file system path.
   */
  public static URI parsePathOrURI(String s) throws URISyntaxException {
    try {
      URI uri = new URI(s);

      if (uri.getScheme() == null || uri.getScheme().length() > 1) {
        return uri;
      }
    } catch (URISyntaxException e) {
      if (s.contains("://")) {
        throw e;
      }
    }

    try {
      return Paths.get(s).toUri();
    } catch (InvalidPathException e) {
      throw new URISyntaxException(s, e.getReason());
    }
  }

  /**
   * Returns the URI a storage client should fetch the given path from, or null if it should be opened as a local file
   * path. Parsed like FileConnector's paths (see {@link #parsePathOrURI(String)}), so a Windows drive
   * path such as <code>C:/data/a.csv</code> is a local file, not a URI with the scheme "C".
   */
  static URI storageURIFor(String path) {
    try {
      URI uri = parsePathOrURI(path);
      return uri.getScheme() == null ? null : uri;
    } catch (URISyntaxException e) {
      // neither a URI nor a valid local path: opening it as a local path reports the error
      return null;
    }
  }

  /**
   * <p> Appends the given String to the name of the file in the given path. Returns the path with this information appended.
   * <p> Example:
   * <p> ("/Users/Desktop/hello.txt", "-FILE-1") --> "/Users/Desktop/hello-FILE-1.txt"
   *
   * @param filePath A path to a file.
   * @param append The String you want to append to the file's name.
   * @return The path to the file with the given String appended to the file's name.
   */
  public static String appendStringToFileName(String filePath, String append) {
    // the path to the file's parent
    String parentPath = FilenameUtils.getFullPath(filePath);

    String fileName = FilenameUtils.getBaseName(filePath);
    String extension = FilenameUtils.getExtension(filePath);

    String newFileName = fileName + append;
    String newFileNameWithExtension = newFileName + (extension.isEmpty() ? "" : "." + extension);

    return parentPath + newFileNameWithExtension;
  }

  /**
   * Reads the remaining contents of the given InputStream into a byte[], failing instead of reading more than
   * <code>maxBytes</code>. Does not close the InputStream.
   *
   * @param in The InputStream to read.
   * @param maxBytes The maximum number of bytes to read, or null for no limit. Must fit in a byte[], i.e. be at most
   *                 Integer.MAX_VALUE - 9.
   * @return The contents of the InputStream.
   * @throws ContentTooLargeException If the InputStream contains more than <code>maxBytes</code> bytes.
   * @throws IOException If an error occurs reading the InputStream.
   */
  public static byte[] readAllBytes(InputStream in, Long maxBytes) throws IOException {
    if (maxBytes == null) {
      return in.readAllBytes();
    }

    byte[] bytes = in.readNBytes(Math.toIntExact(maxBytes));
    if (in.read() != -1) {
      throw new ContentTooLargeException("Content exceeds the maximum size of " + maxBytes + " bytes.");
    }
    return bytes;
  }

  /**
   * Thrown when content exceeds a configured maximum size.
   */
  public static class ContentTooLargeException extends IOException {

    public ContentTooLargeException(String message) {
      super(message);
    }
  }
}
