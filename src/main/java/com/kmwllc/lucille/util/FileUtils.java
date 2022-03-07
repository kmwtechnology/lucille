package com.kmwllc.lucille.util;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileUtils {

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

  public static boolean isValidURI(String uriString) {
    try {
      URI rawURI = URI.create(uriString);
      return rawURI.getScheme() != null && !rawURI.getScheme().trim().isEmpty();
    } catch (Exception e) {
      return false;
    }
  }

  static public final String toString(File file) throws IOException {
    InputStream is = new FileInputStream(file);
    byte[] bytes = IOUtils.toByteArray(is);
    if (bytes == null) {
      return null;
    }
    return new String(bytes);
  }

  /**
   * simple file to byte array
   *
   * @param file - file to read
   * @return byte array of contents
   * @throws IOException
   */
  static public final byte[] toByteArray(File file) throws IOException {

    FileInputStream fis = null;
    byte[] data = null;

    fis = new FileInputStream(file);
    data = toByteArray(fis);

    fis.close();

    return data;
  }

  /**
   * IntputStream to byte array
   *
   * @param is
   * @return
   */
  static public final byte[] toByteArray(InputStream is) {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      int nRead;
      byte[] data = new byte[16384];
      while ((nRead = is.read(data, 0, data.length)) != -1) {
        baos.write(data, 0, nRead);
      }

      baos.flush();
      baos.close();
      return baos.toByteArray();
    } catch (Exception e) {
      // TODO: proper log messages
      e.printStackTrace();
    }

    return null;
  }
}
