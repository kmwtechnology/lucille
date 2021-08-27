package com.kmwllc.lucille.util;

import java.io.*;
import java.nio.file.Files;
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
      return Files.newBufferedReader(Path.of(path));
    } else {
      InputStream is = FileUtils.class.getClassLoader().getResourceAsStream(path.substring(path.indexOf(":")+1));
      return new BufferedReader(new InputStreamReader(is));
    }
  }


}
