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
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p> Holds / manages a connection to a JDBC database used to maintain state regarding FileConnector traversals and the last
 * time files were processed and published by Lucille.
 * <p> Build a connection to a JDBC-compatible database (can be embedded or not)
 *
 * <p> Call {@link #init()} and {@link #shutdown()} to connect / disconnect to the database.
 * <p> Call {@link #getStateForTraversal(String)}to retrieve the appropriate / relevant state information for your traversal,
 * holding it in memory. Invoke the appropriate methods on the returned {@link FileConnectorState} as you progress throughout your
 * traversal.
 * <p> After your traversal completes, call {@link #updateDatabaseAfterTraversal(URI, String)} to update the database based on the
 * results of your traversal. This method deletes rows that weren't encountered and are relevant to the traversal.
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
 *     <li>encountered (BOOLEAN): Used internally to track files which have been encountered in a traversal, enabling deletions.</li>
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
   * allows you to see when files were last known to be published by Lucille, and then later call {@link #updateDatabaseAfterTraversal(URI, String)}
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
   * @param traversalTableName The name for the table with relevant state information for your traversal. Should be determined by a storage client.
   * @return a FileConnectorState with information for your traversal.
   * @throws SQLException If a SQL-related error occurs.
   */
  public FileConnectorState getStateForTraversal(String traversalTableName) throws SQLException {
    // All table names get stored in uppercase.
    traversalTableName = traversalTableName.toUpperCase();

    // Start by making sure the table exists, and creating it if it doesn't.
    DatabaseMetaData metadata = jdbcConnection.getMetaData();

    // checking if we have a table under the given name.
    try (ResultSet rs = metadata.getTables(null, null, traversalTableName, null)) {
      if (!rs.next()) {
        try (Statement statement = jdbcConnection.createStatement()) {
          statement.executeUpdate("CREATE TABLE \"" + traversalTableName + "\" (name VARCHAR(200) PRIMARY KEY, last_published TIMESTAMP, is_directory BOOLEAN, encountered BOOLEAN, parent VARCHAR(200))");
          statement.executeUpdate("CREATE INDEX \"" + traversalTableName + "_parent\" ON \"" + traversalTableName + "\"(parent)");
        }
      }
    }

    return new FileConnectorState(traversalTableName);
  }

  /**
   * Deletes any rows that appear to represent files that were deleted from storage, based on the ENCOUNTERED column. Uses the given
   * URI to determine which files are "relevant" to the traversal, and should be deleted, if ENCOUNTERED = false.
   * @param pathToStorage The full path to storage used for the traversal.
   * @throws SQLException If a SQL-related error occurs.
   */
  public void updateDatabaseAfterTraversal(URI pathToStorage, String tableName) throws SQLException {
    tableName = tableName.toUpperCase();

    // if path ends with /, we certainly have a directory, and can just start recurring.
    if (pathToStorage.toString().endsWith("/")) {
      updateDatabaseRecursive(pathToStorage.toString(), tableName, false);
    } else {
      // Otherwise, we have to handle potentially adding the "/" for the user, in case they didn't.

      if (pathNameInTable(pathToStorage.toString(), tableName)) {
        updateDatabaseRecursive(pathToStorage.toString(), tableName, false);
      } else if (pathNameInTable(pathToStorage + "/", tableName)) {
        updateDatabaseRecursive(pathToStorage + "/", tableName, false);
      } else {
        log.warn("There wasn't an entry for {} or {} in the State Database - deletions won't take place.", pathToStorage, pathToStorage + "/");
      }

    }

    // At the end, set ENCOUNTERED=FALSE for the whole database.
    String allFalseSQL = "UPDATE \"" + tableName + "\" SET encountered=FALSE";
    try (Statement statement = jdbcConnection.createStatement()) {
      statement.executeUpdate(allFalseSQL);
    }
  }

  private void updateDatabaseRecursive(String fullPathToProcess, String tableName, boolean deletedParent) throws SQLException {
    // Get a StateEntry for the path, then call updateDatabaseRecursive using it.
    String selectEntrySQL = "SELECT * FROM \"" + tableName + "\" WHERE name='" + fullPathToProcess + "'";

    StateEntry entry;
    try (Statement statement = jdbcConnection.createStatement();
        ResultSet rs = statement.executeQuery(selectEntrySQL)) {

      if (!rs.next()) {
        log.warn("Couldn't get a row for {}. Deletions will not continue along this path / its children.", fullPathToProcess);
        return;
      }

      entry = StateEntry.fromResultSet(rs);
    }

    updateDatabaseRecursive(entry, tableName, deletedParent);
  }

  private void updateDatabaseRecursive(StateEntry currentEntry, String tableName, boolean deletedParent) throws SQLException {
    // 1. Delete this entry itself, if needed.
    if (!currentEntry.encountered() || deletedParent) {
      String deleteSQL = "DELETE FROM \"" + tableName + "\" WHERE name='" + currentEntry.fullPathStr() + "'";

      try (Statement statement = jdbcConnection.createStatement()) {
        int rowsChanged = statement.executeUpdate(deleteSQL);

        if (rowsChanged != 1) {
          log.warn("Deleting {} caused {} rows to be affected (expected 1).", currentEntry.fullPathStr(), rowsChanged);
        }
      }
    }

    // 2. If it is a directory, call recursive on each of the children, with deletedParent = !encountered || deletedParent
    if (currentEntry.isDirectory()) {
      // Run a query for the children. Build a StateEntry for each. Close the connection.
      String childrenSQL = "SELECT * FROM \"" + tableName + "\" WHERE parent='" + currentEntry.fullPathStr() + "'";

      Set<StateEntry> entries = new HashSet<>();

      try (Statement statement = jdbcConnection.createStatement();
          ResultSet rs = statement.executeQuery(childrenSQL)) {
        while (rs.next()) {
          try {
            entries.add(StateEntry.fromResultSet(rs));
          } catch (SQLException e) {
            log.warn("Error occurred adding a StateEntry, some deletions may not occur.", e);
          }
        }
      }

      for (StateEntry childEntry : entries) {
        updateDatabaseRecursive(childEntry, tableName, (!currentEntry.encountered() || deletedParent));
      }
    }
  }

  // Returns whether there is an entry in the database for the given full path, in the given table.
  private boolean pathNameInTable(String fullPathStr, String tableName) throws SQLException {
    String existenceSQL = "SELECT 1 FROM \"" + tableName + "\" WHERE name='" + fullPathStr + "' LIMIT 1";

    try (Statement statement = jdbcConnection.createStatement();
        ResultSet rs = statement.executeQuery(existenceSQL)) {
      return rs.next();
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

  /**
   * Mainly just a convenient way to use a single table name / timestamp for files associated with a traversal.
   * Usually gets passed into storage clients - limits the scope of available actions for them to take, allows
   * FileConnector to still "pull the strings" for the database.
   */
  public class FileConnectorState {
    private final String tableName;
    private final Timestamp traversalTimestamp;

    private FileConnectorState(String tableName) {
      this.tableName = tableName;
      this.traversalTimestamp = Timestamp.from(Instant.now());
    }

    /**
     * Update the database to reflect that the given file or directory was encountered during a FileConnector traversal. Directories
     * must end with the path separator.
     * @param fullPathStr The full path to the file or directory you encountered during a FileConnector traversal. Directories must
     *                    end with the path separator.
     */
    public void markFileOrDirectoryEncountered(String fullPathStr) {
      // First, we try an update statement, see if it updates an existing file.
      String updateSQL = "UPDATE \"" + tableName + "\" SET encountered=true WHERE name='" + fullPathStr + "'";

      try (Statement statement = jdbcConnection.createStatement()) {
        int rowsChanged = statement.executeUpdate(updateSQL);

        // if it doesn't change any rows, then we need to insert this file - it is "new".
        if (rowsChanged == 0) {
          boolean isDirectory = fullPathStr.endsWith("/");

          if (isDirectory) {
            insertDirectory(fullPathStr);
          } else {
            insertFile(fullPathStr);
          }
        }
      } catch (SQLException e) {
        log.warn("An error occurred trying to mark a file or directory as encountered in your state database.", e);
      }
    }

    /**
     * Retrieves the instant at which this file was last known to be published by Lucille. If the State Database has
     * no record of publishing this file, a null Instant is returned.
     *
     * @param fullPathStr The full path to the file you want to get this information for.
     * @return The instant at which this file was last known to be published by Lucille; null if there is no information
     * on this file.
     */
    public Instant getLastPublished(String fullPathStr) {
      String querySQL = "SELECT last_published FROM \"" + tableName + "\" WHERE name='" + fullPathStr + "'";

      try (Statement statement = jdbcConnection.createStatement();
          ResultSet rs = statement.executeQuery(querySQL)) {
        if (rs.next()) {
          Timestamp timestamp = rs.getTimestamp("last_published");

          if (timestamp == null) {
            return null;
          } else {
            return timestamp.toInstant();
          }
        } else {
          return null;
        }
      } catch (SQLException e) {
        log.warn("Error occurred trying to get file's last published time. Returning null, so the file will be published.", e);
        return null;
      }
    }

    /**
     * Updates the state database to reflect that the given file was successfully published during a FileConnector traversal.
     * @param fullPathStr The full path to the file that was successfully published.
     */
    public void successfullyPublishedFile(String fullPathStr) {
      String updateSQL = "UPDATE \"" + tableName + "\" SET last_published = ? WHERE name = ?";

      try (PreparedStatement statement = jdbcConnection.prepareStatement(updateSQL)) {
        statement.setTimestamp(1, traversalTimestamp);
        statement.setString(2, fullPathStr);

        int rowsChanged = statement.executeUpdate();

        if (rowsChanged != 1) {
          log.warn("Updating a file's last published timestamp changed {} rows.", rowsChanged);
        }
      } catch (SQLException e) {
        log.warn("Couldn't update a file's last published timestamp.", e);
      }
    }

    // Insert a directory with the given name into the database. It will be marked as encountered.
    private void insertDirectory(String fullPathStr) throws SQLException {
      String insertNewDirectorySQL = "INSERT INTO \"" + tableName + "\" VALUES (?, NULL, TRUE, TRUE, ?)";

      try (PreparedStatement insertStatement = jdbcConnection.prepareStatement(insertNewDirectorySQL)) {
        insertStatement.setString(1, fullPathStr);
        insertStatement.setString(2, getDirectoryParent(fullPathStr));
        insertStatement.execute();
      }
    }

    // Insert a file with the given name into the database. It will have no "last_published" time, but will
    // be marked as encountered.
    private void insertFile(String fullPathStr) throws SQLException {
      String insertNewFileSQL = "INSERT INTO \"" + tableName + "\" VALUES (?, NULL, FALSE, TRUE, ?)";

      try (PreparedStatement insertStatement = jdbcConnection.prepareStatement(insertNewFileSQL)) {
        String parent = fullPathStr.substring(0, fullPathStr.lastIndexOf("/") + 1);
        insertStatement.setString(1, fullPathStr);
        insertStatement.setString(2, parent);
        insertStatement.execute();
      }
    }
  }

  private record StateEntry(String fullPathStr, Timestamp lastPublished, boolean isDirectory, boolean encountered, String parent) {
    // create a StateEntry from the current value of the given ResultSet. You should already call rs.next() and make sure it's true
    // before calling this!
    private static StateEntry fromResultSet(ResultSet rs) throws SQLException {
      return new StateEntry(rs.getString("name"), rs.getTimestamp("last_published"), rs.getBoolean("is_directory"), rs.getBoolean("encountered"), rs.getString("parent"));
    }
  }
}
