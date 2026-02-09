package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConfigUtils;
import com.typesafe.config.Config;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p> Holds / manages a connection to a JDBC database used to maintain state regarding FileConnector traversals and the last
 * time files were processed and published by Lucille.
 * <p> Build a connection to a JDBC-compatible database (can be embedded or not)
 *
 * <p> Call {@link #init()} and {@link #shutdown()} to connect / disconnect to the database.
 *
 * <p> The database should/will have the following Columns:
 *   <ul>
 *     <li>name (VARCHAR PRIMARY KEY): The full path (for cloud, the full URI) to the file. Defaults to a length of 200.
 *       <ul>
 *         <li>Names are case-sensitive - it is very important to keep the paths consistent in your Config when using state.</li>
 *       </ul>
 *     </li>
 *     <li>last_published (TIMESTAMP): The last time the file was known to be published by Lucille. Is NULL for directories.</li>
 *     <li>encountered (BOOLEAN): Used internally to track files which have been encountered in a traversal, enabling deletions.</li>
 *   </ul>
 *
 * <p> Lucille includes H2 as a dependency. You are welcome to configure the FileConnectorStateManager to use an embedded
 * H2 instance - use the <code>org.h2.Driver</code>, and something like <code>jdbc:h2:{LOCAL_FILE_PATH}</code> as your connectionString.
 *
 * <p> <b>Note:</b> This class is operating under two key assumptions about FileConnector / Connectors:
 * <ol>
 *   <li>Connectors run sequentially.</li>
 *   <li>The FileConnector is not multithreaded.</li>
 * </ol>
 */
public class FileConnectorStateManager {

  private static final Logger log = LoggerFactory.getLogger(FileConnectorStateManager.class);

  private final String driver;
  private final String connectionString;
  private final String jdbcUser;
  private final String jdbcPassword;

  private final String tableName;
  private final boolean performDeletions;
  private final int pathLength;

  private final Timestamp traversalTimestamp = Timestamp.from(Instant.now());

  private Connection jdbcConnection;
  private PreparedStatement queryStatement;
  private PreparedStatement updateStatement;
  private PreparedStatement insertNewFileStatement;

  /**
   * Creates a FileConnectorStateManager from the given config.
   * @param config Configuration for the FileConnectorStateManager.
   * @param connectorName The name of the connector using this connection. Uses the name for tableName, if it is not specified.
   */
  public FileConnectorStateManager(Config config, String connectorName) {
    this.driver = ConfigUtils.getOrDefault(config, "driver", "org.h2.Driver");
    this.connectionString = ConfigUtils.getOrDefault(config, "connectionString", "jdbc:h2:./state/" + connectorName);
    this.jdbcUser = ConfigUtils.getOrDefault(config, "jdbcUser", "");
    this.jdbcPassword = ConfigUtils.getOrDefault(config, "jdbcPassword", "");

    this.tableName = ConfigUtils.getOrDefault(config, "tableName", connectorName).toUpperCase();
    this.performDeletions = ConfigUtils.getOrDefault(config, "performDeletions", true);
    this.pathLength = ConfigUtils.getOrDefault(config, "pathLength", 200);
  }

  /**
   * Connects to the database specified by your Config, and ensure a table with tableName exists, building it with the
   * appropriate schema if it doesn't. After connecting to the table, sets encountered=FALSE for all rows.
   */
  public void init() throws ClassNotFoundException, SQLException {
    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      log.error("Driver class {} not found. Is the jar file in your classpath?", driver);
      throw e;
    }

    jdbcConnection = DriverManager.getConnection(connectionString, jdbcUser, jdbcPassword);

    // After getting connection setup, make sure the table exists, create it if it doesn't.
    DatabaseMetaData metadata = jdbcConnection.getMetaData();

    // checking if we have a table under the given name.
    try (ResultSet rs = metadata.getTables(null, null, tableName, null)) {
      if (!rs.next()) {
        try (Statement statement = jdbcConnection.createStatement()) {
          statement.executeUpdate("CREATE TABLE \"" + tableName + "\" (name VARCHAR(" + pathLength
              + ") PRIMARY KEY, last_published TIMESTAMP, encountered BOOLEAN)");
        }
      }
    }

    // making sure encountered = false for all rows
    String allNotEncounteredSQL = "UPDATE \"" + tableName + "\" SET encountered=false";

    try (Statement statement = jdbcConnection.createStatement()) {
      int rowsAffected = statement.executeUpdate(allNotEncounteredSQL);
      log.debug("{} rows from the state database had encountered switched from TRUE to FALSE.", rowsAffected);
    }

    // create PreparedStatements to be used for sql queries that take input
    String querySQL = "SELECT last_published FROM \"" + tableName + "\" WHERE name=?";
    queryStatement = jdbcConnection.prepareStatement(querySQL);

    String updateSQL = "UPDATE \"" + tableName + "\" SET encountered=true WHERE name=?";
    updateStatement = jdbcConnection.prepareStatement(updateSQL);

    String insertNewFileSQL = "INSERT INTO \"" + tableName + "\" VALUES (?, NULL, TRUE)";
    insertNewFileStatement = jdbcConnection.prepareStatement(insertNewFileSQL);
  }

  /**
   * If configured to, delete any files that weren't encountered. Then, disconnect from the database specified by your Config.
   * Throws an exception if an error occurs.
   */
  public void shutdown() throws SQLException {
    if (performDeletions) {
      deleteFilesNotEncounteredAndResetTable();
    }

    if (jdbcConnection != null) {
      try {
        jdbcConnection.close();
      } catch (SQLException e) {
        log.warn("Couldn't close connection to database.", e);
      }
    }

    if (queryStatement != null) {
      try {
        queryStatement.close();
      } catch (SQLException e) {
        log.warn("Couldn't close query statement (PreparedStatement).", e);
      }
    }

    if (updateStatement != null) {
      try {
        updateStatement.close();
      } catch (SQLException e) {
        log.warn("Couldn't close update statement (PreparedStatement).", e);
      }
    }

    if (insertNewFileStatement != null) {
      try {
        insertNewFileStatement.close();
      } catch (SQLException e) {
        log.warn("Couldn't close insert statement (PreparedStatement).", e);
      }
    }
  }

  private void deleteFilesNotEncounteredAndResetTable() throws SQLException {
    String deleteSQL = "DELETE FROM \"" + tableName + "\" WHERE encountered=FALSE";

    try (Statement statement = jdbcConnection.createStatement()) {
      int rowsAffected = statement.executeUpdate(deleteSQL);
      log.info("{} rows from the state database were deleted.", rowsAffected);
    }
  }

  /**
   * Update the database to reflect that the given file was encountered during a FileConnector traversal.
   * @param fullPathStr The full path to the file you encountered during a FileConnector traversal.
   */
  public void markFileEncountered(String fullPathStr) {
    // First, we try an update statement, see if it updates an existing file.
    try {
      updateStatement.setString(1, fullPathStr);
      int rowsChanged = updateStatement.executeUpdate();

      // if it doesn't change any rows, then we need to insert this file - it is "new".
      if (rowsChanged == 0) {
        insertFile(fullPathStr);
      }
    } catch (SQLException e) {
      log.warn("Error marking file encountered in state database.", e);
    }
  }

  /**
   * Retrieves the instant at which this file was last known to be published by Lucille. If the State Database has
   * no record of publishing this file, a null Instant is returned.
   *
   * @param fullPathStr The full path to the file you want to get a last_published Timestamp for.
   * @return The instant at which this file was last known to be published by Lucille; null if there is no information
   * on this file.
   */
  public Instant getLastPublished(String fullPathStr) {
    try {
      queryStatement.setString(1, fullPathStr);
      try (ResultSet rs = queryStatement.executeQuery()) {
        if (rs.next()) {
          Timestamp timestamp = rs.getTimestamp("last_published");

          if (timestamp != null) {
            return timestamp.toInstant();
          }
        }
      }
    } catch (SQLException e) {
      log.warn("SQL error occurred getting last published for {}, lastPublishedCutoff won't apply.", fullPathStr, e);
    }

    return null;
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
        log.warn("Updating {} last published timestamp changed {} rows.", fullPathStr, rowsChanged);
      }
    } catch (SQLException e) {
      log.warn("Couldn't update a file's last published timestamp.", e);
    }
  }

  // Insert a file with the given name into the database. It will have no "last_published" time, but will
  // be marked as encountered.
  private void insertFile(String fullPathStr) throws SQLException {
    insertNewFileStatement.setString(1, fullPathStr);
    insertNewFileStatement.executeUpdate();
  }
}
