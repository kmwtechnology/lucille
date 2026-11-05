package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.AZURE_ALLOWED_HOSTS;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.DEFAULT_AZURE_ALLOWED_HOSTS;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.STORAGE_CLIENTS;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.STORAGE_CLIENT_CLASS;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Interface for storage clients. Implementations of this interface should be able to traverse a storage system
 * and publish files to the Lucille pipeline.
 */
public interface StorageClient {

  /**
   * Starts the client, creating any necessary connections and/or resources. Must be called before traversing or getting
   * a file's contents. Calling it again on a started client has no effect.
   * @throws IOException If an error occurs while starting.
   */
  void start() throws IOException;

  /**
   * Stops the client, closing any open connections and/or resources. Calling it on a client that isn't started has
   * no effect.
   * @throws IOException If an error occurs while stopping.
   */
  void stop() throws IOException;

  /**
   * Traverses through the storage client and publish files to Lucille pipeline. Does not update or use state, and
   * FilterOptions.lastPublishedCutoff will not be enforced, if specified.
   * @param publisher The Publisher you want to publish documents to.
   * @param params Parameters / options regarding the traversal of the client's file system.
   * @throws IOException If an error occurs reading from storage during traversal.
   * @throws ConnectorException If any other error stops the traversal.
   */
  void traverse(Publisher publisher, TraversalParams params) throws IOException, ConnectorException;

  /**
   * Traverses through the storage client and publish files to Lucille pipeline. Updates the given state as files are encountered
   * and published. Uses the {@link FileStateTracker#getLastPublished(String)} method to enforce lastPublishedCutoff, if specified.
   * @param publisher The Publisher you want to publish documents to.
   * @param params Parameters / options regarding the traversal of the client's file system.
   * @param stateMgr The state to read and update for each file, keyed by its full path. May be null, in which case state is
   *                 not used.
   * @throws IOException If an error occurs reading from storage during traversal.
   * @throws ConnectorException If any other error stops the traversal.
   */
  void traverse(Publisher publisher, TraversalParams params, FileStateTracker stateMgr) throws IOException, ConnectorException;

  /**
   * Rejects a traversal that this client's settings can't run correctly. FileConnector calls this at startup for each
   * path, so that a bad combination fails before any file is published. Does not require this client to be initialized,
   * and does not contact the provider. By default, accepts every traversal.
   * @param params Parameters for the traversal. Its URI may not be set.
   * @param stateEnabled Whether the traversal will update state.
   * @param collapsing Whether the Publisher will collapse documents, which is not safe for concurrent publish() calls.
   * @throws IllegalArgumentException If the traversal can't run as configured.
   */
  default void validateTraversal(TraversalParams params, boolean stateEnabled, boolean collapsing) { }

  /**
   * Returns whether a traversal of the given parent would also visit the files under the given child, based on this
   * provider's addressing rules. Does not require this client to be initialized, and does not contact the provider.
   * @param parent A URI to a path in storage.
   * @param child A URI to a path in storage, which may or may not sit under the parent.
   * @return Whether traversing parent would also visit the files under child.
   */
  boolean containsPath(URI parent, URI child);

  /**
   * Returns whether files that {@link #moveFile(URI, URI)} moves to the given folder during a traversal of the given path
   * would land back inside that path, where the traversal could find them again or a file could overwrite itself. Used
   * to validate move folders at startup. Does not require this client to be initialized, and does not contact the provider.
   * <p> By default, whether the path contains the folder. Override this if your moveFile keeps part of the source path
   * under the folder, so that a file can land inside the path even when the folder itself is outside it.
   * @param path A URI to the path being traversed.
   * @param folder A URI to the folder that files are moved to.
   * @return Whether a moved file would land inside the path.
   */
  default boolean moveTargetInsidePath(URI path, URI folder) {
    return containsPath(path, folder);
  }

  /**
   * Opens and returns an InputStream for a file's contents, located at the given URI.
   * @param uri A URI to the file whose contents you want to extract.
   * @return An InputStream for the file's contents.
   * @throws IOException If an error occurs while getting the file's contents.
   */
  InputStream getFileContentStream(URI uri) throws IOException;

  /**
   * Moves the file at the given String to the folder at the given URI.
   * @param filePath The full path to the file that you want to move.
   * @param folder A URI to the folder that you want to move the file to.
   * @throws IOException If an error occurs moving the file.
   */
  void moveFile(URI filePath, URI folder) throws IOException;

  /**
   * Gets the appropriate client based on the URI scheme and validate with authentication/settings from the Config. If
   * "storageClients" configures a client for the URI's scheme, that client is created. Otherwise, the built-in client for
   * the scheme is created from the gcp (gs), s3 (s3), or azure (https) config, or a LocalStorageClient for a file URI.
   * An https URI is only read with the built-in Azure client if its authority contains one of azure.allowedHosts (a
   * substring match), which defaults to "blob.core.windows.net". A client configured for https in "storageClients" handles
   * every https URI, and is responsible for any host rule of its own.
   * @param pathToStorage A URI to storage - either local or cloud - that you need a StorageClient for.
   * @param connectorConfig Configuration for your connector, which should contain configuration for cloud storage clients
   *                        as needed.
   * @return The appropriate, configured storage client to use for your traversal.
   *
   * @throws IllegalArgumentException If your path to storage requires support for a cloud storage client that you did not provide,
   * you provided a URI with an unsupported scheme or host, "storageClients" is invalid, or a configured "class" could not be created.
   */
  static StorageClient create(URI pathToStorage, Config connectorConfig) {
    String activeClient = pathToStorage.getScheme() != null ? pathToStorage.getScheme() : "file";
    Map<String, Config> customClientOptions = getCustomClientOptions(connectorConfig);

    if (customClientOptions.containsKey(activeClient)) {
      return createClientFromClass(customClientOptions.get(activeClient));
    }

    switch (activeClient) {
      case "gs" -> {
        if (!connectorConfig.hasPath("gcp")) {
          throw new IllegalArgumentException("Path is to Google Cloud but no options provided.");
        }
        return new GoogleStorageClient(connectorConfig.getConfig("gcp"));
      }
      case "s3" -> {
        if (!connectorConfig.hasPath("s3")) {
          throw new IllegalArgumentException("Path is to S3 but no options provided.");
        }
        return new S3StorageClient(connectorConfig.getConfig("s3"));
      }
      case "https" -> {
        if (!isAllowedAzureHost(pathToStorage, connectorConfig)) {
          throw new IllegalArgumentException("Unsupported client type: " + activeClient + " with authority: "
              + pathToStorage.getAuthority() + ". https URIs are read with the azure client, and the host must contain one of azure."
              + AZURE_ALLOWED_HOSTS + ".");
        }
        if (!connectorConfig.hasPath("azure")) {
          throw new IllegalArgumentException("Path is to Azure but no options provided.");
        }
        return new AzureStorageClient(connectorConfig.getConfig("azure"));
      }
      case "file" -> {
        return new LocalStorageClient();
      }
      default -> throw new IllegalArgumentException("Unsupported client type: " + activeClient);
    }
  }

  /**
   * Builds a map of all StorageClients which can be built from the given Config. Always returns at least
   * a LocalStorageClient (keyed by "file"). The map is keyed by URI scheme. Use {@link #clientFor(URI, Map, Config)} to
   * look up the client for a URI in the map, so that azure.allowedHosts is applied to https URIs.
   *
   * To build the built-in clients for the cloud providers, these arguments must be provided in separate maps:
   * <br> gcp (keyed by "gs"):
   * "pathToServiceKey" : "path/To/Service/Key.json"
   * "maxResultsPerPage" : number of results returned per listing request (page size). Optional, defaults to 100
   *
   * <br> s3 (keyed by "s3"):
   * "accessKeyId" : s3 key id. Not needed if secretAccessKey is not specified (using default credentials).
   * "secretAccessKey" : secret access key. Not needed if accessKeyId is not specified (using default credentials).
   * "region" : s3 storage region
   * "anonymous" : send unsigned requests, for public buckets that need no credentials. Cannot be combined with
   * accessKeyId / secretAccessKey. Defaults the region to us-east-1 when no region is given. Optional, defaults to false.
   * "maxResultsPerPage" : number of results returned per LIST request (page size). Optional, defaults to 1000 (S3's per-request maximum)
   *
   * <br> azure (keyed by "https"):
   * "connectionString" : azure connection string
   * <b>Or</b>
   * "accountName" : azure account name
   * "accountKey" : azure account key
   * "maxResultsPerPage" : number of results returned per listing request (page size). Optional, defaults to 100
   * "allowedHosts" : hosts whose https URIs are read with the azure client (a substring match on the URI authority).
   * Applied by {@link #create(URI, Config)} and {@link #clientFor(URI, Map, Config)}. Optional, defaults to ["blob.core.windows.net"].
   *
   * <br> storageClients:
   * A map from a URI scheme (e.g. "sftp", or "s3") to the config for a StorageClient for that scheme. Each config must
   * contain "class", the fully qualified name of a StorageClient implementation with a public constructor that takes a
   * Config. The client is created with that config, which may contain any other settings it needs. A client for a built-in
   * scheme (s3, gs, https) replaces the built-in client, and the scheme's built-in config (s3, gcp, azure) can't also be
   * given. The scheme must be at least two characters long and can't be "file".
   *
   * @param config The configuration which potentially contains cloud options that you want to use to build storage clients.
   * @return A map of Strings to StorageClients, keyed by URI scheme. Always includes the LocalStorageClient, keyed by "file".
   * @throws IllegalArgumentException If "storageClients" is invalid or a configured "class" could not be created.
   */
  static Map<String, StorageClient> createClients(Config config) {
    Map<String, Config> customClientOptions = getCustomClientOptions(config);
    Map<String, StorageClient> results = new HashMap<>();

    results.put("file", new LocalStorageClient());

    if (config.hasPath("gcp")) {
      results.put("gs", new GoogleStorageClient(config.getConfig("gcp")));
    }

    if (config.hasPath("azure")) {
      results.put("https", new AzureStorageClient(config.getConfig("azure")));
    }

    if (config.hasPath("s3")) {
      results.put("s3", new S3StorageClient(config.getConfig("s3")));
    }

    // a scheme can't be in both storageClients and its built-in config, so this never replaces a built-in client
    for (Map.Entry<String, Config> entry : customClientOptions.entrySet()) {
      results.put(entry.getKey(), createClientFromClass(entry.getValue()));
    }

    return results;
  }

  /**
   * Returns the client in the given map, built by {@link #createClients(Config)}, that handles the given URI. The client
   * is looked up by the URI's scheme ("file" if it has none). An https URI is only handled by the built-in Azure client
   * if its authority contains one of azure.allowedHosts, which defaults to "blob.core.windows.net". A client configured for
   * https in "storageClients" handles every https URI.
   *
   * @param uri The URI you need a StorageClient for.
   * @param clients The clients built from the given config by {@link #createClients(Config)}.
   * @param config The config the clients were built from.
   * @return The client for the URI, or null if no client in the map handles it.
   */
  static StorageClient clientFor(URI uri, Map<String, StorageClient> clients, Config config) {
    String scheme = uri.getScheme() != null ? uri.getScheme() : "file";

    if ("https".equals(scheme) && !config.hasPath(STORAGE_CLIENTS + ".https") && !isAllowedAzureHost(uri, config)) {
      return null;
    }

    return clients.get(scheme);
  }

  /**
   * Returns whether the built-in Azure client should read the given https URI: whether its authority contains one of
   * azure.allowedHosts (a substring match), which defaults to "blob.core.windows.net".
   */
  private static boolean isAllowedAzureHost(URI uri, Config config) {
    String authority = uri.getAuthority();
    List<String> allowedHosts = config.hasPath("azure." + AZURE_ALLOWED_HOSTS)
        ? config.getStringList("azure." + AZURE_ALLOWED_HOSTS)
        : DEFAULT_AZURE_ALLOWED_HOSTS;

    return authority != null && allowedHosts.stream().anyMatch(authority::contains);
  }

  /**
   * Returns the config for each StorageClient in the given config's "storageClients", keyed by URI scheme.
   *
   * @param config Configuration which may contain "storageClients".
   * @return A map of URI schemes to the config for their StorageClient. Empty if "storageClients" is not present.
   * @throws IllegalArgumentException If a scheme is invalid or "file", a built-in scheme's config (s3, gcp, azure) is also
   * given, or a client's config is not a map containing "class".
   */
  private static Map<String, Config> getCustomClientOptions(Config config) {
    Map<String, Config> results = new HashMap<>();

    if (!config.hasPath(STORAGE_CLIENTS)) {
      return results;
    }

    for (Map.Entry<String, ConfigValue> entry : config.getObject(STORAGE_CLIENTS).entrySet()) {
      String scheme = entry.getKey();

      // single-letter schemes are treated as Windows drive letters, so they can't be used
      if (!scheme.matches("[a-zA-Z][a-zA-Z0-9+.-]+")) {
        throw new IllegalArgumentException(STORAGE_CLIENTS + " contains an invalid URI scheme: " + scheme + ".");
      }

      if (scheme.equals("file")) {
        throw new IllegalArgumentException(STORAGE_CLIENTS + " can't configure the file scheme. Local files are always "
            + "read with LocalStorageClient.");
      }

      String builtInConfig = switch (scheme) {
        case "gs" -> "gcp";
        case "s3" -> "s3";
        case "https" -> "azure";
        default -> null;
      };

      if (builtInConfig != null && config.hasPath(builtInConfig)) {
        throw new IllegalArgumentException(STORAGE_CLIENTS + "." + scheme + " and " + builtInConfig + " both configure a client for the "
            + scheme + " scheme. Use only one: " + STORAGE_CLIENTS + "." + scheme + " replaces the built-in client, so put all of its "
            + "settings there.");
      }

      if (!(entry.getValue() instanceof ConfigObject clientOptions) || !clientOptions.toConfig().hasPath(STORAGE_CLIENT_CLASS)) {
        throw new IllegalArgumentException(STORAGE_CLIENTS + "." + scheme + " must be a map containing \"" + STORAGE_CLIENT_CLASS + "\".");
      }

      results.put(scheme, clientOptions.toConfig());
    }

    return results;
  }

  /**
   * Creates the StorageClient named by "class" in the given options, using its (Config) constructor.
   *
   * @param options Configuration for the storage client, containing "class".
   * @return The configured storage client.
   * @throws IllegalArgumentException If the class can't be found, doesn't implement StorageClient, is abstract, or can't be created.
   */
  private static StorageClient createClientFromClass(Config options) {
    String className = options.getString(STORAGE_CLIENT_CLASS);
    Class<?> clazz;

    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Couldn't find storage client class " + className + ".", e);
    }

    if (!StorageClient.class.isAssignableFrom(clazz)) {
      throw new IllegalArgumentException("Storage client class " + className + " must implement " + StorageClient.class.getName() + ".");
    }

    if (Modifier.isAbstract(clazz.getModifiers())) {
      throw new IllegalArgumentException("Storage client class " + className + " must not be abstract.");
    }

    try {
      return (StorageClient) clazz.getConstructor(Config.class).newInstance(options);
    } catch (InvocationTargetException e) {
      // surface errors from the constructor, such as invalid options, the same way as for the built-in clients
      if (e.getCause() instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      throw new IllegalArgumentException("Couldn't create storage client " + className + ".", e.getCause());
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException("Couldn't create storage client " + className + ". It must have a public (Config) constructor.", e);
    }
  }

  /**
   * Creates a full document ID from the initial docId and the given params. Adds the parameters' ID prefix
   * to the beginning of the initial ID.
   *
   * @param docId The initial docId for your document.
   * @param params Parameters for your storage traversal.
   * @return A full document ID to use, adding the parameters' ID prefix to the beginning of the initial ID.
   */
  static String createDocId(String docId, TraversalParams params) {
    return params.getDocIdPrefix() + docId;
  }
}
