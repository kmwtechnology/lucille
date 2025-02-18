package com.kmwllc.lucille.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
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
   * Returns a reader to the file at the given path, which should be either a local file path or in the classpath
   * (starts with "classpath:").
   */
  public static Reader getLocalFileReader(String path) throws IOException {
    InputStream is;

    if (path.startsWith("classpath:")) {
      is = FileUtils.class.getClassLoader().getResourceAsStream(path.substring(path.indexOf(":") + 1));
    } else {
      is = new FileInputStream(path);
    }

    return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
  }
}
