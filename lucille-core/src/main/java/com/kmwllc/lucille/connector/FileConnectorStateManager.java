package com.kmwllc.lucille.connector;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kmwllc.lucille.connector.storageclient.FileStateTracker;
import com.kmwllc.lucille.core.ConfigUtils;
import com.typesafe.config.Config;

/**
 * <p> Holds / manages a connection to a JDBC database used to maintain state regarding FileConnector traversals and the last
 * time files were processed and published by Lucille. StorageClients see it as a {@link FileStateTracker}.
 * <p> Build a connection to a JDBC-compatible database (can be embedded or not)
 *
 * <p> Call {@link #init()} and {@link #shutdown()} to connect / disconnect to the database.
 *
 * <p> The database should/will have the following Columns:
 *   <ul>
 *     <li>name (VARCHAR PRIMARY KEY): The full path (for cloud, the full URI) to the file. Defaults to a length of 768 (state.pathLength).
 *       <ul>
 *         <li>Names are case-sensitive - it is very important to keep the paths consistent in your Config when using state.</li>
 *       </ul>
 *     </li>
 *     <li>last_published (BIGINT epoch microseconds): The last time the file was known to be published by Lucille. Is NULL for directories.</li>
 *     <li>encountered (BOOLEAN): Used internally to track files which have been encountered in a traversal, enabling deletions.</li>
 *     <li>runs_not_encountered (INTEGER): The consecutive number of runs the state manager has seen where the file was not encountered.</li>
 *   </ul>
 *
 * <p> Lucille includes H2 as a dependency. You are welcome to configure the FileConnectorStateManager to use an embedded
 * H2 instance - use the <code>org.h2.Driver</code>, and something like <code>jdbc:h2:{LOCAL_FILE_PATH}</code> as your connectionString.
 *
 * <p> <b>Note:</b> This class is operating under these key assumptions about FileConnector / Connectors:
 * <ol>
 *   <li>Connectors run sequentially.</li>
 *   <li>A FileConnector traversal may be concurrent. The whole-table operations - {@link #init()},
 *   {@link #incrementRunsNotEncountered()}, {@link #listExpiredFiles()}, and {@link #shutdown()} - run only on the
 *   connector thread, before or after the traversal. Per-file operations are delegated to the calling thread's
 *   {@link TraversalState}.</li>
 *   <li>The paths a FileConnector traverses do not overlap.</li>
 * </ol>
 * <p> Regardless, only one FileConnectorStateManager within the same JVM may use a given <code>connectionString</code> and
 * <code>tableName</code> at a time. {@link #init()} fails if another FileConnectorStateManager is already using them.
 */
public class FileConnectorStateManager implements FileStateTracker {

  // connectionString:tableName keys of the state tables currently in use in this JVM.
  private static final Set<String> tablesInUse = ConcurrentHashMap.newKeySet();

  private static final Logger log = LoggerFactory.getLogger(FileConnectorStateManager.class);

  private final String driver;
  private final String connectionString;
  private final String jdbcUser;
  private final String jdbcPassword;

  private final String tableName;
  private final boolean performDeletions;
  private final int runsBeforeExpiration;
  private final int pathLength;

  // Works with the embedded H2 database Lucille ships with (and uses when no connectionString is set), and is the
  // largest VARCHAR primary key MySQL/InnoDB allows with utf8mb4 (3072-byte index limit / 4 bytes per character).
  static final int DEFAULT_PATH_LENGTH = 768;

  private Instant traversalInstant;

  // set when this run failed or its tombstones could not be listed, so shutdown() keeps the expired rows for the next run
  private volatile boolean retainExpiredRows = false;

  // Used for the whole-table operations. Kept separate from the per-thread connections because it has to outlive them.
  private Connection jdbcConnection;

  // Per-file operations are delegated to the calling thread's TraversalState, since a JDBC Connection is not thread-safe.
  // init() binds one for the thread that calls it, and each additional traversal thread binds its own with openStateForThread().
  private final ThreadLocal<TraversalState> threadState = new ThreadLocal<>();

  private final String tableKey;

  // Whether this instance has claimed tableKey in tablesInUse. shutdown() is also called when init() never ran or was
  // rejected, and must not release a table another instance is using. Atomic because init() and shutdown() can run
  // on different threads (Runner calls execute() on a ConnectorThread but close() on its own thread), and so the table
  // is released at most once.
  private final AtomicBoolean claimedTable = new AtomicBoolean(false);

  /**
   * Creates a FileConnectorStateManager from the given config.
   * @param config Configuration for the FileConnectorStateManager.
   * @param connectorName The name of the connector using this connection. Uses the name for tableName, if it is not specified.
   */
  FileConnectorStateManager(Config config, String connectorName) throws IllegalArgumentException {
    this.driver = ConfigUtils.getOrDefault(config, "driver", "org.h2.Driver");
    this.connectionString = ConfigUtils.getOrDefault(config, "connectionString", "jdbc:h2:./state/" + connectorName);
    this.jdbcUser = ConfigUtils.getOrDefault(config, "jdbcUser", "");
    this.jdbcPassword = ConfigUtils.getOrDefault(config, "jdbcPassword", "");

    // An explicit state.tableName is used as given (upper-cased) and must already be a valid identifier. Only a table name
    // generated from the connector name is mapped, which is what makes connector names with other characters work.
    this.tableName = config.hasPath("tableName")
        ? validateExplicitTableName(config.getString("tableName"))
        : toSqlTableName(connectorName);
    this.performDeletions = ConfigUtils.getOrDefault(config, "performDeletions", true);
    this.runsBeforeExpiration = ConfigUtils.getOrDefault(config, "runsBeforeExpiration", 1);
    if (this.runsBeforeExpiration < 1) {
      throw new IllegalArgumentException("state.runsBeforeExpiration must be >= 1");
    }
    this.pathLength = ConfigUtils.getOrDefault(config, "pathLength", DEFAULT_PATH_LENGTH);

    this.tableKey = connectionString + ":" + tableName;
  }

  // Longest table name we generate. Keeps us under the identifier limits of PostgreSQL (63 bytes) and MySQL (64 characters).
  static final int MAX_TABLE_NAME_LENGTH = 48;
  // Prefix on every generated table name. It is also a letter, so a generated name never starts with a digit, and it is
  // never a reserved word, so a connector name such as "order" still gives a valid unquoted identifier.
  static final String TABLE_NAME_PREFIX = "STATE_";
  private static final int HASH_SUFFIX_BYTES = 4;
  // Characters kept from a name that had to be rewritten: whatever the total budget leaves after the prefix, the "_"
  // before the hash, and the hash itself.
  private static final int MAX_TABLE_NAME_BASE_LENGTH = MAX_TABLE_NAME_LENGTH - TABLE_NAME_PREFIX.length() - 1 - 2 * HASH_SUFFIX_BYTES;

  /**
   * Converts a connector name into a valid, unquoted SQL table name. Every generated name starts with
   * {@link #TABLE_NAME_PREFIX}, so the table for connector "file" is STATE_FILE.
   * <ul>
   *   <li>A name that is already a valid identifier (letters, digits and underscores, starting with a letter) is only
   *   upper-cased and prefixed, provided the result is at most {@link #MAX_TABLE_NAME_LENGTH} characters.</li>
   *   <li>Any other name has each invalid character replaced with an underscore, is truncated, and gets an 8-character hash
   *   of the upper-cased name appended. The hash keeps names such as "my-conn" and "my_conn" from colliding.</li>
   * </ul>
   * @param name The connector name.
   * @return A table name safe to use unquoted on H2, PostgreSQL and MySQL.
   */
  static String toSqlTableName(String name) {
    String upper = name.toUpperCase(Locale.ROOT);
    int maxBodyLength = MAX_TABLE_NAME_LENGTH - TABLE_NAME_PREFIX.length();
    if (isValidIdentifier(upper, maxBodyLength)) {
      return TABLE_NAME_PREFIX + upper;
    }

    String base = upper.replaceAll("[^A-Z0-9_]", "_");
    if (base.length() > MAX_TABLE_NAME_BASE_LENGTH) {
      base = base.substring(0, MAX_TABLE_NAME_BASE_LENGTH);
    }
    String mapped = TABLE_NAME_PREFIX + base + "_" + hashSuffix(upper);
    // guaranteed by the base cap above; checked anyway so a future constant change cannot exceed the identifier limit
    if (mapped.length() > MAX_TABLE_NAME_LENGTH) {
      throw new IllegalStateException("Generated state table name is " + mapped.length() + " characters, over the limit of "
          + MAX_TABLE_NAME_LENGTH + ": " + mapped);
    }
    return mapped;
  }

  // The shape a table name must have to be used unquoted on H2, PostgreSQL and MySQL: a letter, then letters, digits or
  // underscores, within the given length. Used for generated names and for explicit names alike.
  private static boolean isValidIdentifier(String upper, int maxLength) {
    return upper.matches("[A-Z][A-Z0-9_]*") && upper.length() <= maxLength;
  }

  // An explicit state.tableName is not rewritten, so it has to be valid as written. Rejecting it here gives the user a
  // clear message at startup, rather than a "Table not found" error from the database.
  static String validateExplicitTableName(String name) {
    String upper = name.toUpperCase(Locale.ROOT);
    if (!isValidIdentifier(upper, MAX_TABLE_NAME_LENGTH)) {
      throw new IllegalArgumentException("state.tableName \"" + name + "\" is not a valid table name. Use letters, digits and "
          + "underscores, starting with a letter, at most " + MAX_TABLE_NAME_LENGTH + " characters. Hyphens and other characters "
          + "are only accepted in connector names, which are converted to generated table names; remove state.tableName to use "
          + "that.");
    }
    return upper;
  }

  // Escapes the wildcards in a name before it is used as a DatabaseMetaData search pattern. Otherwise "_" matches any
  // character, so the name "A_B" would also match a table called "AXB".
  private static String escapeSearchPattern(String name, DatabaseMetaData metadata) throws SQLException {
    String escape = metadata.getSearchStringEscape();
    if (escape == null || escape.isEmpty()) {
      return name;
    }
    StringBuilder escaped = new StringBuilder(name.length());
    for (char c : name.toCharArray()) {
      if (c == '_' || c == '%' || escape.indexOf(c) >= 0) {
        escaped.append(escape);
      }
      escaped.append(c);
    }
    return escaped.toString();
  }

  // Fails if the state table already exists in an older format. Such a table would otherwise fail every read and write
  // silently, because the failures are only logged as warnings.
  private void checkTimestampSchema(DatabaseMetaData metadata, String foldedTableName) throws SQLException {
    String tablePattern = escapeSearchPattern(foldedTableName, metadata);
    String columnPattern = escapeSearchPattern(foldIdentifier("last_published", metadata), metadata);
    try (ResultSet rs = metadata.getColumns(null, null, tablePattern, columnPattern)) {
      if (!rs.next()) {
        throw new IllegalStateException("State table " + tableName + " has no last_published column.");
      }
      int dataType = rs.getInt("DATA_TYPE");
      if (dataType != Types.BIGINT) {
        throw new IllegalStateException("State table " + tableName + " was created by an older version of Lucille (last_published"
            + " has SQL type " + dataType + ", expected BIGINT). Drop the table, or set state.tableName to a new name.");
      }
    }
  }

  // Hex of the first bytes of the SHA-256 of the upper-cased name, so the suffix is stable and matches the case-insensitive naming used before.
  private static String hashSuffix(String name) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(name.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().withUpperCase().formatHex(digest, 0, HASH_SUFFIX_BYTES);
    } catch (NoSuchAlgorithmException e) {
      // every JVM is required to provide SHA-256
      throw new IllegalStateException(e);
    }
  }

  // Applies the database's rule for unquoted identifiers, so lookups in the metadata match what CREATE TABLE stored.
  // H2 folds to upper case, PostgreSQL to lower case, and MySQL keeps the case it was given.
  private static String foldIdentifier(String identifier, DatabaseMetaData metadata) throws SQLException {
    if (metadata.storesUpperCaseIdentifiers()) {
      return identifier.toUpperCase(Locale.ROOT);
    }
    if (metadata.storesLowerCaseIdentifiers()) {
      return identifier.toLowerCase(Locale.ROOT);
    }
    return identifier;
  }

  /**
   * Connects to the database specified by your Config, and ensure a table with tableName exists, building it with the
   * appropriate schema if it doesn't. After connecting to the table, sets encountered=FALSE for all rows.
   * @throws IllegalStateException If another FileConnectorStateManager in this JVM is using the same table.
   */
  public void init() throws ClassNotFoundException, SQLException {
    if (!tablesInUse.add(tableKey)) {
      throw new IllegalStateException("State table " + tableKey + " is in use by another run in this JVM.");
    }
    // released in shutdown(), which is still called if the rest of init() fails
    claimedTable.set(true);

    // Microsecond precision: this is what the table stores, so the value written and the value read back agree.
    traversalInstant = Instant.now().truncatedTo(ChronoUnit.MICROS);
    retainExpiredRows = false;

    jdbcConnection = openConnection();

    // After getting connection setup, make sure the table exists, create it if it doesn't.
    DatabaseMetaData metadata = jdbcConnection.getMetaData();
    String foldedName = foldIdentifier(tableName, metadata);

    // checking if we have a table under the given name. The name is escaped, so "_" in it can't match another table.
    boolean exists;
    try (ResultSet rs = metadata.getTables(null, null, escapeSearchPattern(foldedName, metadata), null)) {
      exists = rs.next();
    }

    if (exists) {
      checkTimestampSchema(metadata, foldedName);
    } else {
      // Identifiers are unquoted so that one statement works on H2, PostgreSQL and MySQL. last_published is epoch
      // microseconds, because TIMESTAMP WITH TIME ZONE is not valid on MySQL.
      try (Statement statement = jdbcConnection.createStatement()) {
        statement.executeUpdate("CREATE TABLE " + tableName + " (name VARCHAR(" + pathLength
            + ") PRIMARY KEY, last_published BIGINT, encountered BOOLEAN, runs_not_encountered INTEGER)");
      }
    }

    // making sure encountered = false for all rows
    String allNotEncounteredSQL = "UPDATE " + tableName + " SET encountered=false";

    try (Statement statement = jdbcConnection.createStatement()) {
      int rowsAffected = statement.executeUpdate(allNotEncounteredSQL);
      log.debug("{} rows from the state database had encountered switched from TRUE to FALSE.", rowsAffected);
    }

    // Binds a state to this thread, sharing the connection above rather than opening a second one. A sequential
    // traversal runs on this thread; a concurrent one leaves this state unused.
    threadState.set(new TraversalState(jdbcConnection, tableName, traversalInstant));
  }

  /**
   * Binds a {@link TraversalState}, with its own connection to the state database, to the calling thread. The per-file
   * methods on this class then operate through it. {@link #init()} does this for the thread that calls it, so only
   * additional traversal threads need to call this. Every call must be paired with {@link #closeStateForThread()}.
   * @throws IllegalStateException If called before {@link #init()}, or if the calling thread already has a state bound.
   */
  public void openStateForThread() throws ClassNotFoundException, SQLException {
    if (jdbcConnection == null) {
      throw new IllegalStateException("openStateForThread() was called before init().");
    }

    if (threadState.get() != null) {
      throw new IllegalStateException("Thread " + Thread.currentThread().getName() + " already has an open state.");
    }

    Connection connection = openConnection();

    try {
      threadState.set(new TraversalState(connection, tableName, traversalInstant));
    } catch (SQLException e) {
      // the connection has no owner until the TraversalState is bound, so close it here
      try {
        connection.close();
      } catch (SQLException closeException) {
        e.addSuppressed(closeException);
      }
      throw e;
    }
  }

  /**
   * Closes the calling thread's {@link TraversalState} and unbinds it, closing its connection unless that connection is
   * this state manager's own. Does nothing if the thread has none.
   */
  public void closeStateForThread() {
    TraversalState state = threadState.get();

    if (state == null) {
      return;
    }

    threadState.remove();
    state.closeStatements();

    if (state.connection() != jdbcConnection) {
      closeConnection(state.connection());
    }
  }

  // Returns the calling thread's TraversalState. Every per-file operation goes through here.
  private TraversalState currentState() {
    TraversalState state = threadState.get();

    if (state == null) {
      throw new IllegalStateException("Thread " + Thread.currentThread().getName()
          + " has no open state. Call openStateForThread() before traversing on this thread.");
    }

    return state;
  }

  // Opens a connection to the database specified by your Config.
  private Connection openConnection() throws ClassNotFoundException, SQLException {
    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      log.error("Driver class {} not found. Is the jar file in your classpath?", driver);
      throw e;
    }

    return DriverManager.getConnection(connectionString, jdbcUser, jdbcPassword);
  }

  private static void closeConnection(Connection connection) {
    if (connection == null) {
      return;
    }

    try {
      connection.close();
    } catch (SQLException e) {
      log.warn("Couldn't close connection to database.", e);
    }
  }

  /**
   * Increments the runs_not_encountered counter for every file that was not seen during the current traversal. This method must
   * be called exactly once per run, after all markFileEncountered calls are complete.
   * Must be called before shutdown() and before listExpiredFiles(), since both rely on the updated counters.
   * Calling it before marking is finished will prematurely increment counters for files still being traversed.
   */
  public void incrementRunsNotEncountered() throws SQLException {
    String incrementSQL = "UPDATE " + tableName + " SET runs_not_encountered = runs_not_encountered + 1 WHERE encountered=FALSE";

    try (Statement statement = jdbcConnection.createStatement()) {
      statement.executeUpdate(incrementSQL);
    }
  }

  /**
   * If configured to, delete any files that weren't encountered. Then, disconnect from the database specified by your Config.
   * Throws an exception if an error occurs.
   */
  public void shutdown() throws SQLException {
    try {
      closeStateForThread();

      // jdbcConnection is null if init() never ran or failed before connecting - e.g. FileConnector.close() is
      // still called when preExecute fails
      if (performDeletions && jdbcConnection != null) {
        if (retainExpiredRows) {
          log.warn("This run did not finish or could not publish its tombstones, so expired files are kept in state for the next run.");
        } else {
          deleteExpiredFilesAndResetTable();
        }
      }
    } finally {
      // close even if the deletion failed, so the connection isn't leaked
      closeConnection(jdbcConnection);

      if (claimedTable.getAndSet(false)) {
        tablesInUse.remove(tableKey);
      }
    }
  }
  
  /**
   * Returns the names of the files that have not been encountered for <code>runsBeforeExpiration</code> runs. Names are
   * returned as stored: a file's full path, or for an archive entry the archive's full path, the archive separator and
   * the entry's name, which need not be a valid URI.
   */
  public List<String> listExpiredFiles() throws SQLException {
    String selectSQL = "SELECT name FROM " + tableName + " WHERE encountered=FALSE AND runs_not_encountered >= ?";
    List<String> names = new ArrayList<>();
    try (PreparedStatement ps = jdbcConnection.prepareStatement(selectSQL)) {
      ps.setInt(1, runsBeforeExpiration);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          names.add(rs.getString("name"));
        }
      }
    }
    return names;
  }

  /**
   * Keeps the expired rows when {@link #shutdown()} would otherwise delete them, because the run failed or their
   * tombstones could not be listed. They are listed again, and their tombstones published, on the next run that
   * finishes. Reset by {@link #init()}.
   */
  void retainExpiredRows() {
    retainExpiredRows = true;
  }

  private void deleteExpiredFilesAndResetTable() throws SQLException {
    String deleteSQL = "DELETE FROM " + tableName + " WHERE encountered=FALSE AND runs_not_encountered >= ?";

    try (PreparedStatement ps = jdbcConnection.prepareStatement(deleteSQL)) {
      ps.setInt(1, runsBeforeExpiration);
      int rowsAffected = ps.executeUpdate();
      log.info("{} rows from the state database were deleted.", rowsAffected);
    }
  }

  /**
   * Update the database to reflect that the given file was encountered during a FileConnector traversal.
   * @param fullPathStr The full path to the file you encountered during a FileConnector traversal.
   */
  @Override
  public void markFileEncountered(String fullPathStr) {
    currentState().markFileEncountered(fullPathStr);
  }

  /**
   * Marks all state database entries whose name starts with the given prefix as encountered.
   * Used to mark archive/compressed file entries as encountered when their container file is visited
   * but not re-published (e.g., in incremental mode when the archive has not been modified since last publish).
   *
   * @param prefix The prefix to match against entry names (typically archivePath + ARCHIVE_FILE_SEPARATOR).
   */
  @Override
  public void markAllEntriesEncountered(String prefix) {
    currentState().markAllEntriesEncountered(prefix);
  }

  /**
   * Retrieves the instant at which this file was last known to be published by Lucille. If the State Database has
   * no record of publishing this file, a null Instant is returned.
   *
   * @param fullPathStr The full path to the file you want to get a last_published Timestamp for.
   * @return The instant at which this file was last known to be published by Lucille; null if there is no information
   * on this file.
   */
  @Override
  public Instant getLastPublished(String fullPathStr) {
    return currentState().getLastPublished(fullPathStr);
  }

  /**
   * Updates the state database to reflect that the given file was successfully published during a FileConnector traversal.
   * @param fullPathStr The full path to the file that was successfully published.
   */
  @Override
  public void successfullyPublishedFile(String fullPathStr) {
    currentState().successfullyPublishedFile(fullPathStr);
  }
}
