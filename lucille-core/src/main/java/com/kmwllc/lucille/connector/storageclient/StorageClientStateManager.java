package com.kmwllc.lucille.connector.storageclient;

import com.typesafe.config.Config;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p> Holds / manages a connection to a JDBC database used to maintain state regarding StorageClient traversals and the last
 * time files were processed and published by Lucille.
 * <p> The database can either be embedded in the JVM (H2 database), or you can build a connection to any JDBC-compatible
 * database which has the appropriate schema and holds your state information.
 *
 * <p> Call {@link #init()} and {@link #shutdown()} to connect / disconnect to the database.
 * <p> Call {@link #getStateForTraversal(URI, String)} to retrieve the appropriate / relevant state information for your traversal,
 * holding it in memory. Invoke the appropriate methods on the returned {@link StorageClientState} as you progress throughout your
 * traversal.
 * <p> After your traversal completes, call {@link #updateState(StorageClientState, String)} to update the database based on the
 * results of your traversal. This method performs UPDATE, INSERT, and DELETE operations, based on the status of {@link StorageClientState}.
 *
 * <p> The database should have the following schema:
 * <ul>
 *   <li>Columns:</li>
 *   <ul>
 *     <li>name (VARCHAR): The full path to the file. When using embedded H2, Lucille defaults to a 200 character maximum, and the field is a PRIMARY KEY (recommended).</li>
 *     <li>last_published (TIMESTAMP): The last time the file was known to be published by Lucille.</li>
 *     <li>is_directory (BOOLEAN): Whether the path is a directory.</li>
 *     <li>parent (VARCHAR): The full path to the file's parent folder. When using embedded H2, Lucille defaults to a 200 character maximum. It is <b>strongly recommended</b> you index this field.</li>
 *   </ul>
 *   <li>Tables: The "root" of each file system is its own table. You'll have to delete the table manually if you want to remove the data.</li>
 *   <ul>
 *     <li>The local file system will all be held under one table, "file", representing the "root" of the file system. TODO: Lucille makes sure you have an entry for '/'.</li>
 *     <li>Google Cloud and S3 files will be held under their own tables, with the name combining the URI scheme and the host bucket/container's name. (s3_bucket or gcp_bucket, for example)</li>
 *     <li>Azure files will be held under tables based on the storage name in the connection string in your URI. ("https://storagename.blob.core.windows.net/folder" becomes azure_storagename) (TODO: Make sure this doesn't get changed / is sufficient)</li>
 *   </ul>
 *   <li>Indices: it is <b>strongly recommended</b> that you index the "parent" column for each table. This will make your state load much faster.</li>
 * </ul>
 *
 * <p> <b>Note:</b> This class is operating under two key assumptions about FileConnector / Connectors:
 * <p> 1. Connectors run sequentially.
 * <p> 2. FileConnector does not support individual multithreading.
 */
public class StorageClientStateManager {

  private static final Logger log = LoggerFactory.getLogger(StorageClientStateManager.class);

  private final String driver;
  private final String connectionString;
  private final String jdbcUser;
  private final String jdbcPassword;

  private Connection jdbcConnection;

  /**
   * Creates a StorageClientStateManager from the given config.
   * @param config Configuration for the StorageClientStateManager.
   */
  public StorageClientStateManager(Config config) {
    this.driver = config.getString("driver");
    this.connectionString = config.getString("connectionString");
    this.jdbcUser = config.getString("jdbcUser");
    this.jdbcPassword = config.getString("jdbcPassword");
  }

  // Builds a connection to the database where state is stored - either embedded or a specified JDBC connection / config
  public void init() throws ClassNotFoundException, SQLException {
    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      log.error("Driver class {} not found. Is the jar file in your classpath?", driver);
      throw e;
    }

    jdbcConnection = DriverManager.getConnection(connectionString, jdbcUser, jdbcPassword);
  }

  // Closes the connection to the database where state is stored - either embedded or a specified JDBC connection / config
  public void shutdown() throws SQLException {
    if (jdbcConnection != null) {
      try {
        jdbcConnection.close();
      } catch (SQLException e) {
        log.warn("Couldn't close connection to database.", e);
      }
    }
  }

  // Gets State information relevant for a traversal which starts at the given directory, and has the given table name
  // (determined by the Storage Client's handling of the URI)
  // TODO: How do tables get setup / initialized? How does it persist?
  public StorageClientState getStateForTraversal(URI pathToStorage, String traversalTableName) throws SQLException {
    Set<String> knownDirectories = new HashSet<>();
    Map<String, Instant> fileStateEntries = new HashMap<>();
    Queue<String> pathsToProcess = new LinkedList<>();

    // We start by processing the starting directory / path to storage.
    pathsToProcess.add(pathToStorage.toString());

    // If the starting path is a directory, the entry in the database will end with the slash.
    // If it is a file, there won't be a corresponding entry in the database for this... not a problem
    if (!pathToStorage.toString().endsWith("/")) {
      pathsToProcess.add(pathToStorage + "/");
    }

    // Rinse and repeat the process (described below) until the queue is drained.
    while (!pathsToProcess.isEmpty()) {
      String currentPath = pathsToProcess.poll();

      // Get the SQL entry, regardless of whether it is a directory or not. Have to know what we actually have state for.
      String entryQuery = "SELECT * FROM " + traversalTableName + " WHERE name = '" + currentPath + "'";
      try (Statement statement = jdbcConnection.createStatement();
          ResultSet rs = statement.executeQuery(entryQuery)) {

        if (rs.next()) {
          if (rs.getBoolean("is_directory")) {
            // adds directory to knownDirectories and updates Queue with paths which are indexed to the directory.
            handleDirectory(currentPath, knownDirectories, pathsToProcess, traversalTableName);
          } else {
            Timestamp lastPublished = rs.getTimestamp("last_published");
            log.debug("Adding file: ({}, {})", currentPath, lastPublished.toInstant());
            fileStateEntries.put(currentPath, lastPublished.toInstant());
          }
        } else {
          log.debug("None for {}", currentPath);
        }

      }
    }

    return new StorageClientState(knownDirectories, fileStateEntries);
  }

  private void handleDirectory(String directoryPath, Set<String> directorySet, Queue<String> pathsQueue, String tableName) throws SQLException {
    log.debug("Adding directory: {}", directoryPath);
    // if it is a directory, then we want to see what has this directory as an index, add it to a Set<String> directories
    directorySet.add(directoryPath);

    String directoryIndexQuery = "SELECT * FROM " + tableName + " WHERE parent = '" + directoryPath + "'";

    try (Statement directoryStatement = jdbcConnection.createStatement();
        ResultSet directoryChildrenResults = directoryStatement.executeQuery(directoryIndexQuery)) {
      while (directoryChildrenResults.next()) {
        log.debug("Adding directory {} child: {}", directoryPath, directoryChildrenResults.getString("name"));
        pathsQueue.add(directoryChildrenResults.getString("name"));
      }
    }
  }

  // Updates the database to reflect the given state, which should have been updated as files were encountered and published.
  public void updateState(StorageClientState state, String traversalTableName) throws SQLException {
    // TODO: Performance testing (on enron) w/ and w/out the index... curious how long it would take
    Timestamp sharedTimestamp = Timestamp.from(Instant.now());

    // 1. update the state entries for published files
    String updateKnownSQL = "UPDATE " + traversalTableName + " SET last_published = '" + sharedTimestamp + "' WHERE name = ?";
    try (PreparedStatement updateStatement = jdbcConnection.prepareStatement(updateKnownSQL)) {
      for (String knownPublishedFilePath : state.getKnownAndPublishedFilePaths()) {
        updateStatement.setString(1, knownPublishedFilePath);
        updateStatement.addBatch();
      }

      updateStatement.executeBatch();
    }

    // 2. insert state entries for any "new" files encountered in this run
    String insertNewFileSQL = "INSERT INTO " + traversalTableName + " VALUES (?, '" + sharedTimestamp + "', FALSE, ?)";
    try (PreparedStatement insertStatement = jdbcConnection.prepareStatement(insertNewFileSQL)) {
      for (String newlyPublishedFilePath : state.getNewlyPublishedFilePaths()) {
        // an INSERT SQL statement
        String parent = newlyPublishedFilePath.substring(0, newlyPublishedFilePath.lastIndexOf("/") + 1);
        insertStatement.setString(1, newlyPublishedFilePath);
        insertStatement.setString(2, parent);
        insertStatement.addBatch();
      }

      insertStatement.executeBatch();
    }

    // 3. add the new directories to the database
    String insertNewDirectorySQL = "INSERT INTO " + traversalTableName + " VALUES (?, NULL, TRUE, ?)";
    try (PreparedStatement insertStatement = jdbcConnection.prepareStatement(insertNewDirectorySQL)) {
      for (String newDirectoryPath : state.getNewDirectoryPaths()) {
        // TODO: Some improved handling here would be nice...
        // an INSERT, with is_directory = false, timestamp = NULL
        insertStatement.setString(1, newDirectoryPath);
        insertStatement.setString(2, getDirectoryParent(newDirectoryPath));
        insertStatement.addBatch();
      }

      insertStatement.executeBatch();
    }

    // 4. delete all of the paths in pathsToDelete()
    String deletePathSQL = "DELETE FROM " + traversalTableName + " WHERE name = ?";
    try (PreparedStatement deleteStatement = jdbcConnection.prepareStatement(deletePathSQL)) {
      for (String pathToDelete : state.getPathsToDelete()) {
        deleteStatement.setString(1, pathToDelete);
        deleteStatement.addBatch();
      }

      deleteStatement.executeBatch();
    }
  }

  // s3://lucille-bucket/ would ideally return null, but it's okay for it to return s3://
  // / should return null
  // /Users/ should return /
  // /Users/Desktop should return
  private static String getDirectoryParent(String directoryPath) {
    int lastSlash = directoryPath.lastIndexOf("/");

    if (lastSlash == 0) {
      return null;
    }

    String excludingLastSlash = directoryPath.substring(0, lastSlash);
    return excludingLastSlash.substring(0, excludingLastSlash.lastIndexOf("/")) + "/";
  }
}
