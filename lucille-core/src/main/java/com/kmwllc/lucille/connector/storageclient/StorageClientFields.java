package com.kmwllc.lucille.connector.storageclient;

/**
 * The names of the Document fields a StorageClient emits for each file it publishes, and the separator used in the path
 * of an entry inside an archive or compressed file. These names are part of the StorageClient contract: stages such as
 * FetchFileContent and ApplyFileHandlers read <code>file_path</code> and <code>file_content</code> by default.
 */
public final class StorageClientFields {

  /** The full path (URI) of the file, as a String. Always set. */
  public static final String FILE_PATH = "file_path";

  /** The time the file was last modified, as an Instant. Set when the storage provider reports it. */
  public static final String MODIFIED = "file_modification_date";

  /** The time the file was created, as an Instant. Set when the storage provider reports it. */
  public static final String CREATED = "file_creation_date";

  /** The size of the file in bytes, as a Long. Set when the storage provider reports it. */
  public static final String SIZE = "file_size_bytes";

  /** The file's contents, as a byte[]. Set only when fileOptions.getFileContent is true (the default). */
  public static final String CONTENT = "file_content";

  /** Set to true on the tombstone Document FileConnector sends for a file that has been deleted, when sendTombstones is set. */
  public static final String EXPIRED = "file_expired";

  /**
   * Separates the path to an archive or compressed file from the name of an entry inside it, e.g.
   * <code>s3://bucket/data.zip!entry.json</code>.
   */
  public static final String ARCHIVE_FILE_SEPARATOR = "!";

  private StorageClientFields() { }
}
