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
 * database which holds your state information.
 * <p> Call {@link #init()} and {@link #shutdown()} to connect / disconnect to the database.
 * <p> Call {@link #getStateForTraversal(URI, String)} to build the appropriate / relevant state information for your traversal. Invoke the
 * appropriate methods on the returned {@link StorageClientState} as you progress throughout your traversal. This object allows
 * you to lookup when a file was last known to be published by Lucille, based on the information in the state database.
 * <p> After your traversal completes, call {@link #updateState(StorageClientState)} method to update the database based on the
 * results of your traversal. This will update the database to reflect the results of your traversal - like which files were
 * successfully published, which files were not encountered (deleted), etc.
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

    // Rinse and repeat the process (described below) until the queue is drained.
    while (!pathsToProcess.isEmpty()) {
      String currentPath = pathsToProcess.poll();

      // Get the SQL entry, regardless of whether it is a directory or not. Have to know what we actually have state for.
      String entryQuery = "SELECT * FROM " + traversalTableName + " WHERE name = '" + currentPath + "';";
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

    String directoryIndexQuery = "SELECT * FROM " + tableName + " WHERE parent = '" + directoryPath + "';";

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
    String insertNewFileSQL = "INSERT INTO " + traversalTableName + " VALUES (?, '" + sharedTimestamp + "', 0, ?);";
    try (PreparedStatement insertStatement = jdbcConnection.prepareStatement(insertNewFileSQL)) {
      for (String newlyPublishedFilePath : state.getNewlyPublishedFilePaths()) {
        // an INSERT SQL statement
        String parent = newlyPublishedFilePath.substring(0, newlyPublishedFilePath.lastIndexOf("/"));
        insertStatement.setString(1, newlyPublishedFilePath);
        insertStatement.setString(2, parent);
        insertStatement.addBatch();
      }

      insertStatement.executeBatch();
    }

    // 3. add the new directories to the database
    String insertNewDirectorySQL = "INSERT INTO " + traversalTableName + " VALUES (?, NULL, 1, ?)";
    try (PreparedStatement insertStatement = jdbcConnection.prepareStatement(insertNewDirectorySQL)) {
      for (String newDirectoryPath : state.getNewDirectoryPaths()) {
        // an INSERT, with is_directory = false, timestamp = NULL
        String parent = newDirectoryPath.substring(0, newDirectoryPath.lastIndexOf("/") + 1);
        insertStatement.setString(1, newDirectoryPath);
        insertStatement.setString(2, parent);
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
}
