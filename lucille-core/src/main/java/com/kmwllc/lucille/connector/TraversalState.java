package com.kmwllc.lucille.connector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p> Holds a JDBC connection, and the PreparedStatements built on it, used to read and update state for the files
 * encountered during a single FileConnector traversal.
 *
 * <p> A JDBC Connection is not thread-safe, so each traversal thread uses its own TraversalState. Obtain one from
 * {@link FileConnectorStateManager}, which owns the connection's lifetime.
 *
 * <p> Every operation here affects a single file (or, for {@link #markAllEntriesEncountered(String)}, the entries of a
 * single archive). Operations spanning the whole table belong to {@link FileConnectorStateManager}.
 */
class TraversalState {

  private static final Logger log = LoggerFactory.getLogger(TraversalState.class);

  private final String tableName;
  private final Instant traversalInstant;

  private final Connection jdbcConnection;
  private final PreparedStatement queryStatement;
  private final PreparedStatement updateStatement;
  private final PreparedStatement insertNewFileStatement;

  /**
   * Builds a TraversalState on the given connection. Does not take ownership of it.
   *
   * @param jdbcConnection An open connection to the state database.
   * @param tableName The name of the state table. Must already exist in the database with the correct schema.
   * @param traversalInstant The instant stamped onto files published during this run.
   */
  TraversalState(Connection jdbcConnection, String tableName, Instant traversalInstant) throws SQLException {
    this.jdbcConnection = jdbcConnection;
    this.tableName = tableName;
    this.traversalInstant = traversalInstant;

    String querySQL = "SELECT last_published FROM \"" + tableName + "\" WHERE name=?";
    this.queryStatement = jdbcConnection.prepareStatement(querySQL);

    String updateSQL = "UPDATE \"" + tableName + "\" SET encountered=true, runs_not_encountered = 0 WHERE name=?";
    this.updateStatement = jdbcConnection.prepareStatement(updateSQL);

    String insertNewFileSQL = "INSERT INTO \"" + tableName + "\" VALUES (?, NULL, TRUE, 0)";
    this.insertNewFileStatement = jdbcConnection.prepareStatement(insertNewFileSQL);
  }

  /**
   * Update the database to reflect that the given file was encountered during a FileConnector traversal.
   * @param fullPathStr The full path to the file you encountered during a FileConnector traversal.
   */
  void markFileEncountered(String fullPathStr) {
    // First, we try an update statement, see if it updates an existing file.
    try {
      updateStatement.setString(1, fullPathStr);
      int rowsChanged = updateStatement.executeUpdate();

      // if it doesn't change any rows, then we need to insert this file - it is "new".
      if (rowsChanged == 0) {
        insertNewFileStatement.setString(1, fullPathStr);
        insertNewFileStatement.executeUpdate();
      }
    } catch (SQLException e) {
      log.warn("Error marking file encountered in state database.", e);
    }
  }

  /**
   * Marks all state database entries whose name starts with the given prefix as encountered.
   * Used to mark archive/compressed file entries as encountered when their container file is visited
   * but not re-published (e.g., in incremental mode when the archive has not been modified since last publish).
   *
   * @param prefix The prefix to match against entry names (typically archivePath + ARCHIVE_FILE_SEPARATOR).
   */
  void markAllEntriesEncountered(String prefix) {
    // updates every entry whose name starts with the given prefix
    // this is useful for archive files, in which the paths look like:
    // file:///tmp/archive.zip!entry1.txt or file:///tmp/archive.zip!subdir/entry2.txt.
    // the LIKE operator allows us to target entries which don't match our parameter, but contain it.
    // So the parameter/prefix could be "file:///tmp/archive.zip!" and those aforementioned paths would get updated.
    String updateSQL = "UPDATE \"" + tableName + "\" SET encountered=true WHERE name LIKE ?";
    try (PreparedStatement ps = jdbcConnection.prepareStatement(updateSQL)) {
      // the "%" says anything can come after the prefix in a given DB entry and it will still match
      ps.setString(1, prefix + "%");
      int rowsChanged = ps.executeUpdate();
      log.debug("Marked {} archive entries as encountered for prefix '{}'", rowsChanged, prefix);
    } catch (SQLException e) {
      log.warn("Error marking archive entries as encountered for prefix '{}'.", prefix, e);
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
  Instant getLastPublished(String fullPathStr) {
    try {
      queryStatement.setString(1, fullPathStr);
      try (ResultSet rs = queryStatement.executeQuery()) {
        if (rs.next()) {
          // Get timestamp as OffsetDateTime, which is stored in UTC
          // Note: H2 may convert to local timezone on retrieval, but toInstant() returns the correct absolute point in time.
          OffsetDateTime odt = rs.getObject("last_published", OffsetDateTime.class);
          if (odt != null) {
            return odt.toInstant();
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
  void successfullyPublishedFile(String fullPathStr) {
    String updateSQL = "UPDATE \"" + tableName + "\" SET last_published = ? WHERE name = ?";

    try (PreparedStatement statement = jdbcConnection.prepareStatement(updateSQL)) {
      statement.setObject(1, traversalInstant);
      statement.setString(2, fullPathStr);

      int rowsChanged = statement.executeUpdate();

      if (rowsChanged != 1) {
        log.warn("Updating {} last published timestamp changed {} rows.", fullPathStr, rowsChanged);
      }
    } catch (SQLException e) {
      log.warn("Couldn't update a file's last published timestamp.", e);
    }
  }

  // The connection these PreparedStatements were prepared on.
  Connection connection() {
    return jdbcConnection;
  }

  // Closes the PreparedStatements. The connection may outlive this TraversalState, so they don't close with it.
  void closeStatements() {
    for (PreparedStatement statement : List.of(queryStatement, updateStatement, insertNewFileStatement)) {
      try {
        statement.close();
      } catch (SQLException e) {
        log.warn("Couldn't close a PreparedStatement.", e);
      }
    }
  }
}
