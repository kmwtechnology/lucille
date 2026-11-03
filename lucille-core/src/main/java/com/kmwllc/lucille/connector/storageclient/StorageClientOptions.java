package com.kmwllc.lucille.connector.storageclient;

import java.util.List;

/**
 * Config keys read by StorageClients, their factories in {@link StorageClient}, and {@link TraversalParams}. The cloud
 * keys are read from a client's own config (for example, the <code>s3</code> block), and the file option keys from
 * <code>fileOptions</code>.
 */
public final class StorageClientOptions {

  // -- Built-in cloud client keys --

  /** azure: the Azure Storage connection string. */
  public static final String AZURE_CONNECTION_STRING = "connectionString";
  /** azure: the Azure Storage account name, used with {@link #AZURE_ACCOUNT_KEY}. */
  public static final String AZURE_ACCOUNT_NAME = "accountName";
  /** azure: the Azure Storage account key, used with {@link #AZURE_ACCOUNT_NAME}. */
  public static final String AZURE_ACCOUNT_KEY = "accountKey";
  /** azure: the hosts whose https URIs are read with the built-in Azure client (a substring match on the authority). */
  public static final String AZURE_ALLOWED_HOSTS = "allowedHosts";
  /** The value of {@link #AZURE_ALLOWED_HOSTS} when it is not set. */
  public static final List<String> DEFAULT_AZURE_ALLOWED_HOSTS = List.of("blob.core.windows.net");
  /** s3: the AWS region. */
  public static final String S3_REGION = "region";
  /** s3: the AWS access key ID. */
  public static final String S3_ACCESS_KEY_ID = "accessKeyId";
  /** s3: the AWS secret access key. */
  public static final String S3_SECRET_ACCESS_KEY = "secretAccessKey";
  /** s3: whether to send unsigned requests, for public buckets. */
  public static final String S3_ANONYMOUS = "anonymous";
  /** gcp: the path to the Google Cloud service key JSON. */
  public static final String GOOGLE_SERVICE_KEY = "pathToServiceKey";
  /** Any cloud client: the number of results returned per listing request (page size). */
  public static final String MAX_RESULTS_PER_PAGE = "maxResultsPerPage";

  // -- Custom storage clients --

  /** The config key holding custom StorageClients, keyed by URI scheme. */
  public static final String STORAGE_CLIENTS = "storageClients";
  /** In a <code>storageClients</code> entry: the fully qualified name of the StorageClient implementation. */
  public static final String STORAGE_CLIENT_CLASS = "class";

  // -- fileOptions keys --

  /** fileOptions: whether to put each file's contents in {@link StorageClientFields#CONTENT}. Defaults to true. */
  public static final String GET_FILE_CONTENT = "getFileContent";
  /** fileOptions: the maximum size, in bytes, of a file whose contents are read. */
  public static final String MAX_SIZE_BYTES = "maxSizeBytes";
  /** fileOptions: whether to publish the entries of supported archive files (.tar, .zip). */
  public static final String HANDLE_ARCHIVED_FILES = "handleArchivedFiles";
  /** fileOptions: whether to decompress supported compressed files (.gz). */
  public static final String HANDLE_COMPRESSED_FILES = "handleCompressedFiles";
  /** fileOptions: a folder to move each file to after it is processed. */
  public static final String MOVE_TO_AFTER_PROCESSING = "moveToAfterProcessing";
  /** fileOptions: a folder to move each file to if an error occurs processing it. */
  public static final String MOVE_TO_ERROR_FOLDER = "moveToErrorFolder";

  private StorageClientOptions() { }
}
