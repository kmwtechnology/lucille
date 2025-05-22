package com.kmwllc.lucille.util;

import java.net.URI;
import java.nio.file.Path;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtils {

  private static final Logger log = LoggerFactory.getLogger(FileUtils.class);

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
}
