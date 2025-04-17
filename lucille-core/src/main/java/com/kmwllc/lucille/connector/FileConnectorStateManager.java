package com.kmwllc.lucille.connector;

import com.typesafe.config.Config;
import java.net.URI;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
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
 * <p> Holds / manages a connection to a JDBC database used to maintain state regarding FileConnector traversals and the last
 * time files were processed and published by Lucille.
 * <p> Build a connection to a JDBC-compatible database (can be embedded or not)
 *
 * <p> Call {@link #init()} and {@link #shutdown()} to connect / disconnect to the database.
 * <p> Call {@link #getStateForTraversal(URI, String)} to retrieve the appropriate / relevant state information for your traversal,
 * holding it in memory. Invoke the appropriate methods on the returned {@link FileConnectorState} as you progress throughout your
 * traversal.
 * <p> After your traversal completes, call {@link #updateStateDatabase(FileConnectorState, String)} to update the database based on the
 * results of your traversal. This method performs UPDATE, INSERT, and DELETE operations, based on the status of {@link FileConnectorState}.
 *
 * <p> The database should/will have the following schema:
 * <ul>
 *   <li>Tables: The "root" of each file system is its own table. Table names are always fully capitalized by Lucille.</li>
 *     <ul>
 *       <li>The local file system will all be held under one table, "FILE", representing the "root" of the file system.</li>
 *       <li>Google Cloud and S3 table names combine their URI schemes (gs, s3) and the host bucket/container's name. (S3_BUCKETNAME or GS_BUCKETNAME, for example)</li>
 *       <li>Azure table names are based on the connection string's storage name and the container's name. ("https://storagename.blob.core.windows.net/myContainer" becomes AZURE_STORAGENAME_MYCONTAINER) </li>
 *       <li>If you want to drop a table, removing the data, you'll have to do so manually. (Lucille can't detect when an S3 bucket is deleted, for example!)</li>
 *       <li>As table names are always fully capitalized by Lucille, it is important that you do not use state to traverse through directories with the same names, but different letters capitalized.</li>
 *     </ul>
 *   <li>Columns:</li>
 *   <ul>
 *     <li>name (VARCHAR(200) PRIMARY KEY): The full path (for cloud, the full URI) to the file.</li>
 *       <ul>
 *         <li>Directory names <b>must</b> end with '/'. File names <b>must not</b> end with '/'.</li>
 *         <li>Names are case-sensitive - it is very important to consistently capitalize directory names in your Config when using state.</li>
 *       </ul>
 *     <li>last_published (TIMESTAMP): The last time the file was known to be published by Lucille. Is NULL for directories.</li>
 *     <li>is_directory (BOOLEAN): Whether the path is a directory.</li>
 *     <li>parent (VARCHAR(200)): The full path to the file's parent folder.</li>
 *       <ul>
 *         <li>This name must end with '/'.</li>
 *       </ul>
 *   </ul>
 *   <li>Indices: The "parent" column will be indexed for each table, allowing state to load much faster.</li>
 *   <ul>
 *     <li>The name of the index will be "{tableName}_parent".</li>
 *     <li>You are free to remove the index, if you find it necessary/beneficial to do so.</li>
 *   </ul>
 *   <li>If you need more characters for name or parent, you're free to create the table yourself.</li>
 * </ul>
 *
 * <p> <b>Note:</b> This class is operating under two key assumptions about FileConnector / Connectors:
 * <ol>
 *   <li>Connectors run sequentially.</li>
 *   <li>FileConnector does not support individual multithreading.</li>
 * </ol>
 */
public class FileConnectorStateManager {

  private static final Logger log = LoggerFactory.getLogger(FileConnectorStateManager.class);

  private final String driver;
  private final String connectionString;
  private final String jdbcUser;
  private final String jdbcPassword;

  private Connection jdbcConnection;

  /**
   * Creates a FileConnectorStateManager from the given config.
   * @param config Configuration for the FileConnectorStateManager.
   */
  public FileConnectorStateManager(Config config) {
    this.driver = config.getString("driver");
    this.connectionString = config.getString("connectionString");
    this.jdbcUser = config.getString("jdbcUser");
    this.jdbcPassword = config.getString("jdbcPassword");
  }

  /**
   * Connect to the database specified by your Config. Throws an exception if an error occurs.
   */
  public void init() throws ClassNotFoundException, SQLException {
    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      log.error("Driver class {} not found. Is the jar file in your classpath?", driver);
      throw e;
    }

    jdbcConnection = DriverManager.getConnection(connectionString, jdbcUser, jdbcPassword);
  }

  /**
   * Disconnect from the database specified by your Config. Throws an exception if an error occurs.
   */
  public void shutdown() throws SQLException {
    if (jdbcConnection != null) {
      try {
        jdbcConnection.close();
      } catch (SQLException e) {
        log.warn("Couldn't close connection to database.", e);
      }
    }
  }

  /**
   * Returns a {@link FileConnectorState} populated with relevant information for a traversal that starts at the given URI. This
   * allows you to see when files were last known to be published by Lucille, and then later call {@link #updateStateDatabase(FileConnectorState, String)}
   * to update the results of your traversal.
   * <br>
   * The {@link FileConnectorState} returned will only contain information (last published timestamps)
   * for relevant files. For example, building state for a traversal starting at /Users/gwashington/Desktop will <b>not</b> include information
   * for any files found in /Users/gwashington.
   * <br>
   * Information will be read from the table with the given name, which should be provided by a storage client, and does
   * not need to be capitalized. If the table does not exist in the database already, it will be created automatically
   * (including an index for "parent").
   * <br>
   *
   * @param pathToStorage A URI to the starting point of your traversal. If it is a directory, it is recommended that it ends with '/'.
   * @param traversalTableName The name for the table with relevant state information for your traversal. Should be determined by a storage client.
   * @return a FileConnectorState with information for your traversal.
   * @throws SQLException If a SQL-related error occurs.
   */
  public FileConnectorState getStateForTraversal(URI pathToStorage, String traversalTableName) throws SQLException {
    traversalTableName = traversalTableName.toUpperCase();
    ensureTableExists(traversalTableName);

    Set<String> knownDirectories = new HashSet<>();
    Map<String, Instant> fileStateEntries = new HashMap<>();
    Queue<String> pathsToProcess = new LinkedList<>();

    // We start by processing the starting directory / path to storage.
    pathsToProcess.add(pathToStorage.toString());

    // If the starting path is a directory, the entry in the database (and FileReference.getFullPath()) will end with the slash.
    // pathToStorage is the path as the user input it - and sometimes they might not include the final slash.
    // If the pathToStorage is a file, there won't be a corresponding entry in the database for this... that isn't a problem
    if (!pathToStorage.toString().endsWith("/")) {
      pathsToProcess.add(pathToStorage + "/");
    }

    Instant startInstant = Instant.now();
    int numQueries = 0;

    log.info("Building State for File Connector...");
    while (!pathsToProcess.isEmpty()) {
      String currentPath = pathsToProcess.poll();

      // Get a potential SQL entry for this path.
      String entryQuery = "SELECT * FROM \"" + traversalTableName + "\" WHERE name = '" + currentPath + "'";
      try (Statement statement = jdbcConnection.createStatement();
          ResultSet rs = statement.executeQuery(entryQuery)) {

        numQueries++;

        if (rs.next()) {
          if (rs.getBoolean("is_directory")) {
            numQueries++;
            knownDirectories.add(currentPath);
            // adds directory to knownDirectories and updates Queue with paths which are indexed to the directory.
            handleDirectory(currentPath, fileStateEntries, pathsToProcess, traversalTableName);
          } else {
            Timestamp lastPublished = rs.getTimestamp("last_published");
            fileStateEntries.put(currentPath, lastPublished.toInstant());
          }
        }
      }
    }

    log.info("State was built. {} SQL Queries run. Ran from {} to {}.", numQueries, startInstant, Instant.now());
    return new FileConnectorState(knownDirectories, fileStateEntries);
  }

  // Ensures a table with the given name exists in the database. If it does not, it will be created with the appropriate
  // columns + an index on "parent". Note that, if it does exist, the schema of the database is not validated.
  // We can't always use "CREATE TABLE IF NOT EXISTS" - have to check the metadata instead.
  private void ensureTableExists(String tableName) throws SQLException {
    DatabaseMetaData metadata = jdbcConnection.getMetaData();

    // checking if we have a table under the given name. All table names get stored in uppercase.
    try (ResultSet rs = metadata.getTables(null, null, tableName.toUpperCase(), null)) {
      if (!rs.next()) {
        try (Statement statement = jdbcConnection.createStatement()) {
          statement.executeUpdate("CREATE TABLE \"" + tableName + "\" (name VARCHAR(200) PRIMARY KEY, last_published TIMESTAMP, is_directory BOOLEAN, parent VARCHAR(200))");
          statement.executeUpdate("CREATE INDEX \"" + tableName + "_parent\" ON \"" + tableName + "\"(parent)");
        }
      }
    }
  }

  private void handleDirectory(String directoryPath, Map<String, Instant> fileEntries, Queue<String> pathsQueue, String tableName) throws SQLException {
    String directoryIndexQuery = "SELECT * FROM \"" + tableName + "\" WHERE parent = '" + directoryPath + "'";

    try (Statement directoryStatement = jdbcConnection.createStatement();
        ResultSet directoryChildrenResults = directoryStatement.executeQuery(directoryIndexQuery)) {
      while (directoryChildrenResults.next()) {
        String childName = directoryChildrenResults.getString("name");

        if (directoryChildrenResults.getBoolean("is_directory")) {
          pathsQueue.add(childName);
        } else {
          fileEntries.put(childName, directoryChildrenResults.getTimestamp("last_published").toInstant());
        }
      }
    }
  }

  /**
   * Updates the state database to reflect the information held by the given {@link FileConnectorState} under the given table name.
   * <br>
   * Executes UPDATE, INSERT, and DELETE statements based on {@link FileConnectorState#getKnownAndPublishedFilePaths()},
   * {@link FileConnectorState#getNewlyPublishedFilePaths()}, {@link FileConnectorState#getNewDirectoryPaths()}, and
   * {@link FileConnectorState#getPathsToDelete()}.
   * <br>
   * All files that got published (according to the state) will receive the same "last_published" Timestamp in the database.
   *
   * @param state FileConnectorState that has been used in a traversal.
   * @param traversalTableName The table name for your traversal. Should be produced by a storage client. Doesn't need to be capitalized.
   * @throws SQLException If a SQL-related error occurs.
   */
  public void updateStateDatabase(FileConnectorState state, String traversalTableName) throws SQLException {
    traversalTableName = traversalTableName.toUpperCase();
    Timestamp sharedTimestamp = Timestamp.from(Instant.now());

    // 1. update the state entries for published files
    String updateKnownSQL = "UPDATE \"" + traversalTableName + "\" SET last_published = '" + sharedTimestamp + "' WHERE name = ?";
    try (PreparedStatement updateStatement = jdbcConnection.prepareStatement(updateKnownSQL)) {
      for (String knownPublishedFilePath : state.getKnownAndPublishedFilePaths()) {
        updateStatement.setString(1, knownPublishedFilePath);
        updateStatement.addBatch();
      }

      updateStatement.executeBatch();
    }

    // 2. insert state entries for any "new" files encountered in this run
    String insertNewFileSQL = "INSERT INTO \"" + traversalTableName + "\" VALUES (?, '" + sharedTimestamp + "', FALSE, ?)";
    try (PreparedStatement insertStatement = jdbcConnection.prepareStatement(insertNewFileSQL)) {
      for (String newlyPublishedFilePath : state.getNewlyPublishedFilePaths()) {
        String parent = newlyPublishedFilePath.substring(0, newlyPublishedFilePath.lastIndexOf("/") + 1);
        insertStatement.setString(1, newlyPublishedFilePath);
        insertStatement.setString(2, parent);
        insertStatement.addBatch();
      }

      insertStatement.executeBatch();
    }

    // 3. add the new directories to the database
    String insertNewDirectorySQL = "INSERT INTO \"" + traversalTableName + "\" VALUES (?, NULL, TRUE, ?)";
    try (PreparedStatement insertStatement = jdbcConnection.prepareStatement(insertNewDirectorySQL)) {
      for (String newDirectoryPath : state.getNewDirectoryPaths()) {
        insertStatement.setString(1, newDirectoryPath);
        insertStatement.setString(2, getDirectoryParent(newDirectoryPath));
        insertStatement.addBatch();
      }

      insertStatement.executeBatch();
    }

    // 4. delete all of the paths in pathsToDelete()
    String deletePathSQL = "DELETE FROM \"" + traversalTableName + "\" WHERE name = ?";
    try (PreparedStatement deleteStatement = jdbcConnection.prepareStatement(deletePathSQL)) {
      for (String pathToDelete : state.getPathsToDelete()) {
        deleteStatement.setString(1, pathToDelete);
        deleteStatement.addBatch();
      }

      deleteStatement.executeBatch();
    }
  }

  // s3://lucille-bucket/ would ideally return null, but it's okay for it to return s3://, since we are never seeing what is
  // indexed to "s3://" as a parent, for example (that would be the start of a traversal)
  // / should return null
  // /Users/ should return /
  // /Users/Desktop should return /Users/
  private static String getDirectoryParent(String directoryPath) {
    int lastSlash = directoryPath.lastIndexOf("/");

    if (lastSlash == -1) {
      return null;
    }

    String pathWithoutFinalSlash = directoryPath.substring(0, lastSlash);
    return pathWithoutFinalSlash.substring(0, pathWithoutFinalSlash.lastIndexOf("/") + 1);
  }
}
