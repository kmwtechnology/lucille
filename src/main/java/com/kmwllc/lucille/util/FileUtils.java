package com.kmwllc.lucille.util;

import java.io.*;
import java.nio.file.Path;

public class FileUtils {

  /**
   * Returns a Reader for the file at the given path. If the path begins with "classpath:" the prefix will be removed
   * and the file will be read from the classpath; otherwise it will be read from the filesystem.
   *
   * Could possibly do this with URLStreamHandlers but we don't want to require that filesystem paths begin with
   * a protocol prefix and we don't want more complexity than necessary. See:
   * https://stackoverflow.com/questions/861500/url-to-load-resources-from-the-classpath-in-java
   *
   */
  public static Reader getReader(String path) throws IOException {

    if (!path.startsWith("classpath:")) {
      // This method of creating the Reader is used because it handles none UTF-8 characters by replacing them with UTF
      // chars, rather than throwing an Exception.
      // https://stackoverflow.com/questions/26268132/all-inclusive-charset-to-avoid-java-nio-charset-malformedinputexception-input
      // return Files.newBufferedReader(Paths.get(path), StandardCharsets.UTF_8);
      return new BufferedReader(new InputStreamReader(new FileInputStream(path),"utf-8"));
    } else {
      InputStream is = FileUtils.class.getClassLoader().getResourceAsStream(path.substring(path.indexOf(":")+1));
      return new BufferedReader(new InputStreamReader(is));
    }
  }

  // TODO : Potentially support setting home dir in config
  public static String getLucilleHomeDirectory() {
    String homeDir = System.getProperty("LUCILLE_HOME");

    if (homeDir != null) {
      return Path.of(homeDir).toAbsolutePath().toString();
    }

    return System.getProperty("user.dir");
  }
}
