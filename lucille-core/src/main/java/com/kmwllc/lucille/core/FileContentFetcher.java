package com.kmwllc.lucille.core;

import com.kmwllc.lucille.connector.storageclient.StorageClient;
import com.kmwllc.lucille.util.DefaultFileContentFetcher;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

public interface FileContentFetcher {
  public void startup() throws IOException;
  public void shutdown() throws IOException;
  public InputStream getInputStream(String path) throws IOException;
  public BufferedReader getReader(String path) throws IOException;
  public BufferedReader getReader(String path, String encoding) throws IOException;
  public int countLines(String path) throws IOException;

  // -- Convenience / Static Methods - For One-Time Use to Avoid Managing an Instance --
  /**
   * Returns an InputStream for the file at the given path, which could be in the classpath, a local file path, or a URI
   * to a supported Storage service. An exception may occur - in this case, the StorageClient(s) created will be shutdown.
   * If an InputStream is returned successfully, its close() method will shutdown the StorageClient(s) this method temporarily
   * creates. Be sure to close the returned stream!
   *
   * If an object is making repeated calls to this function, it should instead manage its own instance of a FileContentFetcher.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Can be a classpath / local file path,
   *             or a URI to a cloud storage file.
   * @param config Configuration for the object calling this method, which potentially has "https", "s3", and "gcp".
   * @return An InputStream for the file's contents.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public static InputStream getOneTimeInputStream(String path, Config config) throws IOException {
    // Before attempting to use a storage client, handle classpath / non URI files as a special case.
    if (path.startsWith("classpath:")) {
      return DefaultFileContentFetcher.class.getClassLoader().getResourceAsStream(path.substring(path.indexOf(":") + 1));
    } else if (!FileUtils.isValidURI(path)) {
      return new FileInputStream(path);
    }

    URI pathURI;
    StorageClient clientForFile;

    try {
      pathURI = URI.create(path);
      clientForFile = StorageClient.create(pathURI, config);
    } catch (IllegalArgumentException e) {
      throw new IOException("Error setting up to fetch file content.", e);
    }

    try {
      clientForFile.init();
      InputStream result = clientForFile.getFileContentStream(pathURI);

      return new InputStream() {
        @Override
        public int read() throws IOException {
          return result.read();
        }

        @Override
        public void close() throws IOException {
          try {
            result.close();
          } finally {
            // shutdown if an IOException is thrown
            clientForFile.shutdown();
          }
        }
      };
    } catch (IOException e) {
      clientForFile.shutdown();
      throw e;
    }
  }

  /**
   * Returns a reader for the file at the given path using UTF-8 encoding. The file could be in the classpath, a local file path, or a URI
   * to a supported Storage service. An exception may occur - in this case, the StorageClient(s) created will be shutdown.
   * If a Reader is returned successfully, its close() method will shutdown the StorageClient(s) this method temporarily
   * creates. Be sure to close the returned Reader!
   *
   * If an object is making repeated calls to this function, it should instead manage its own instance of a FileContentFetcher.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Can be a classpath / local file path,
   *             or a URI to a cloud storage file.
   * @param config Configuration for the object calling this method, which potentially has "https", "s3", and "gcp".
   * @return A Reader for the file's contents, using UTF-8 encoding.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public static BufferedReader getOneTimeReader(String path, Config config) throws IOException {
    return getOneTimeReader(path, "utf-8", config);
  }

  /**
   * Returns a reader for the file at the given path using the given encoding. The file could be in the classpath, a local file path, or a URI
   * to a supported Storage service. An exception may occur - in this case, the StorageClient(s) created will be shutdown.
   * If a Reader is returned successfully, its close() method will shutdown the StorageClient(s) this method temporarily
   * creates. Be sure to close the returned Reader!
   *
   * If an object is making repeated calls to this function, it should instead manage its own instance of a FileContentFetcher.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Can be a classpath / local file path,
   *             or a URI to a cloud storage file.
   * @param encoding The encoding you want the Reader to use.
   * @param config Configuration for the object calling this method, which potentially has "https", "s3", and "gcp".
   * @return A Reader for the file's contents, using the given encoding.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public static BufferedReader getOneTimeReader(String path, String encoding, Config config) throws IOException {
    InputStream result = getOneTimeInputStream(path, config);

    return new BufferedReader(new InputStreamReader(result, encoding));
  }

  // -- Extra Convenience - No CloudOptions --> Only classpath / local file paths / local file URIs --
  /**
   * Returns an InputStream for the file at the given path, which could be in the classpath, a local file path, or a URI
   * to a local file. An exception may occur - in this case, the StorageClient(s) created will be shutdown.
   * If an InputStream is returned successfully, its close() method will shutdown the StorageClient(s) this method temporarily
   * creates. Be sure to close the returned stream!
   *
   * If an object is making repeated calls to this function, it should instead manage its own instance of a FileContentFetcher.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Should be a local or classpath file.
   * @return An InputStream for the file's contents.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public static InputStream getOneTimeInputStream(String path) throws IOException {
    return getOneTimeInputStream(path, ConfigFactory.empty());
  }

  /**
   * Returns a reader for the file at the given path using UTF-8 encoding. The file could be in the classpath, a local file path, or a URI
   * to a local file. An exception may occur - in this case, the StorageClient(s) created will be shutdown.
   * If a Reader is returned successfully, its close() method will shutdown the StorageClient(s) this method temporarily
   * creates. Be sure to close the returned Reader!
   *
   * If an object is making repeated calls to this function, it should instead manage its own instance of a FileContentFetcher.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Should be a local or classpath file.
   * @return A Reader for the file's contents, using UTF-8 encoding.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public static BufferedReader getOneTimeReader(String path) throws IOException {
    return getOneTimeReader(path, ConfigFactory.empty());
  }

  /**
   * Returns a reader for the file at the given path using the given encoding. The file could be in the classpath, a local file path, or a URI
   * to a local file. An exception may occur - in this case, the StorageClient(s) created will be shutdown.
   * If a Reader is returned successfully, its close() method will shutdown the StorageClient(s) this method temporarily
   * creates. Be sure to close the returned Reader!
   *
   * If an object is making repeated calls to this function, it should instead manage its own instance of a FileContentFetcher.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Should be a local or classpath file.
   * @param encoding The encoding you want the Reader to use.
   * @return A Reader for the file's contents, using the given encoding.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public static BufferedReader getOneTimeReader(String path, String encoding) throws IOException {
    return getOneTimeReader(path, encoding, ConfigFactory.empty());
  }
}
