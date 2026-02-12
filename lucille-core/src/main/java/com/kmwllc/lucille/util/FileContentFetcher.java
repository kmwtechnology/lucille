package com.kmwllc.lucille.util;

import com.kmwllc.lucille.connector.storageclient.StorageClient;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.net.URI;


/**
 * Responsible for working with StorageClients to repeatedly fetch the contents of String paths, determining the appropriate
 * storage provider.
 */
public interface FileContentFetcher {

  public static final Spec SPEC = SpecBuilder.withoutDefaults()
      .optionalString("fetcherClass")
      .build();

  /**
   * Initialize the underlying resources associated with this FileContentFetcher.
   *
   * @throws IOException If an error occurs initializing this FileContentFetcher.
   */
  public void startup() throws IOException;

  /**
   * Shutdown all the underlying resources associated with this FileContentFetcher.
   */
  public void shutdown();

  /**
   * Attempts to return an InputStream for the file at the given path. If the path begins with "classpath:" the prefix will be removed
   * and the file will be read from the classpath. Otherwise, if the path is a URI, a corresponding storage client will be used;
   * if not, the local file system will be used. Will not return a null InputStream - instead, an exception will be thrown.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Can be a classpath / local file path,
   *             or a URI to a cloud storage file.
   * @return An InputStream for the file's contents.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public InputStream getInputStream(String path) throws IOException;

  /**
   * Attempts to return an InputStream for the file at the given path. If the path begins with "classpath:" the prefix will be removed
   * and the file will be read from the classpath. Otherwise, if the path is a URI, a corresponding storage client will be used;
   * if not, the local file system will be used. Will not return a null InputStream - instead, an exception will be thrown.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Can be a classpath / local file path,
   *             or a URI to a cloud storage file.
   * @param doc A document containing additional information about the file. This can be used by the fetcher to determine how the path
   *            is interpreted to retrieve an input stream.
   * @return An InputStream for the file's contents.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public InputStream getInputStream(String path, Document doc) throws IOException;

  /**
   * Returns a Reader for the file at the given path. If the path begins with "classpath:" the prefix will be removed
   * and the file will be read from the classpath. Otherwise, it will be read from the local file system. Will use UTF-8
   * encoding.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Can be a classpath / local file path,
   *             or a URI to a cloud storage file.
   * @return A Reader for the file's contents, using UTF-8 encoding.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public BufferedReader getReader(String path) throws IOException;

  /**
   * Returns a Reader for the file at the given path. If the path begins with "classpath:" the prefix will be removed
   * and the file will be read from the classpath. Otherwise, it will be read from the local file system. Will use UTF-8
   * encoding.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Can be a classpath / local file path,
   *             or a URI to a cloud storage file.
   * @param doc A document containing additional information about the file. This can be used by the fetcher to determine how the path
   *            is interpreted to retrieve a reader.
   * @return A Reader for the file's contents, using UTF-8 encoding.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public BufferedReader getReader(String path, Document doc) throws IOException;

  /**
   * Returns a Reader for the file at the given path using the given encoding. If the path begins with "classpath:"
   * the prefix will be removed and the file will be read from the classpath. Otherwise, it will be read from the local file system.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Can be a classpath / local file path,
   *             or a URI to a cloud storage file.
   * @param encoding The encoding you want the Reader to use.
   * @return A Reader for the file's contents, using the given encoding.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public BufferedReader getReader(String path, String encoding) throws IOException;

  /**
   * Returns a Reader for the file at the given path using the given encoding. If the path begins with "classpath:"
   * the prefix will be removed and the file will be read from the classpath. Otherwise, it will be read from the local file system.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Can be a classpath / local file path,
   *             or a URI to a cloud storage file.
   * @param encoding The encoding you want the Reader to use.
   * @param doc A document containing additional information about the file. This can be used by the fetcher to determine how the path
   *            is interpreted to retrieve a reader.   *
   * @return A Reader for the file's contents, using the given encoding.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public BufferedReader getReader(String path, String encoding, Document doc) throws IOException;

  /**
   * Counts the number of lines in the file at the given path.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Can be a classpath / local file path,
   *             or a URI to a cloud storage file.
   * @return The number of lines in the given file.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public int countLines(String path) throws IOException;

  /**
   * Counts the number of lines in the file at the given path.
   *
   * @param path A String representation of a path to a file whose contents you want to receive. Can be a classpath / local file path,
   *             or a URI to a cloud storage file.
   * @param doc A document containing additional information about the file. This can be used by the fetcher to determine how the path
   *            is interpreted to retrieve the file's lines.
   * @return The number of lines in the given file.
   * @throws IOException If an error occurs getting the file's contents.
   */
  public int countLines(String path, Document doc) throws IOException;

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

  /**
   * Returns a FileContentFetcher from the given config. A custom FileContentFetcher can be specified by setting the "fetcherClass".
   * If the provided config does not specify a custom FileContentFetcher, the default FileContentFetcher implementation will be used.
   * @param config A Config object containing the fetcherClass setting if a custom implementation is desired.
   * @return A FileContentFetcher instance.
   */
  public static FileContentFetcher create(Config config) {
    if (config == null || !config.hasPath("fetcherClass")) {
      return new DefaultFileContentFetcher(config);
    }

    String fetcherClass = config.getString("fetcherClass");
    try {
      Class<?> clazz = Class.forName(fetcherClass);
      Constructor<?> constructor = clazz.getConstructor(Config.class);
      return (FileContentFetcher) constructor.newInstance(config);
    } catch (Exception e) {
      throw new RuntimeException("Could not instantiate FileContentFetcher of type " + fetcherClass, e);
    }
  }
}
