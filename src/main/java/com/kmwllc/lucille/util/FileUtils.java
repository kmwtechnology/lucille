package com.kmwllc.lucille.util;


import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Path;

public class FileUtils {

  /**
   * Returns a Reader for the file at the given path. If the path begins with "classpath:" the prefix will be removed
   * and the file will be read from the classpath; otherwise it will be read from the filesystem.
   * <p>
   * Could possibly do this with URLStreamHandlers but we don't want to require that filesystem paths begin with
   * a protocol prefix and we don't want more complexity than necessary. See:
   * https://stackoverflow.com/questions/861500/url-to-load-resources-from-the-classpath-in-java
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
      is = FileUtils.class.getClassLoader().getResourceAsStream(path.substring(path.indexOf(":")+1));
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

  /**
   * Takes a path and returns the correct InputStream depending on whether the path is a URI or URL.
   *
   * @param path the path as a String
   * @return the correct InputStream
   */
  public static InputStream getInputStream(String path) {
    try {
      URI uri = new URI(path);
      URL url = uri.toURL(); //get URL from your uri object
      InputStream stream = url.openStream();
      return stream;

    } catch (Exception e) {
      File f = new File(path);
      try {
        return new FileInputStream(f);
      } catch (FileNotFoundException fileNotFoundException) {
        fileNotFoundException.printStackTrace();
      }
    }
    return null;
  }


  public static boolean isValidURI(String uriString) {
    try {
      URI rawURI = URI.create(uriString);
      return rawURI.getScheme() != null && !rawURI.getScheme().trim().isEmpty();
    } catch (Exception e) {
      return false;
    }
  }
}
