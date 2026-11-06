package com.kmwllc.lucille.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.After;
import org.junit.Test;

/**
 * Integration test for {@link FileConnectorStateManager} against a live database other than H2. The H2 unit tests in
 * {@link FileConnectorStateManagerTest} cannot catch dialect or driver problems, so this runs the state lifecycle
 * (create table, mark, publish, expire, look up) against the server named in the environment.
 *
 * <p>Set these variables to run it:
 * <ul>
 *   <li>{@code STATE_DB_URL}: the JDBC URL, for example {@code jdbc:postgresql://localhost:5432/lucille} or
 *   {@code jdbc:mysql://localhost:3306/lucille}. When unset, the test is skipped.</li>
 *   <li>{@code STATE_DB_USER} and {@code STATE_DB_PASSWORD}: credentials, default empty.</li>
 *   <li>{@code STATE_DB_DRIVER}: optional. Derived from the URL when unset.</li>
 * </ul>
 * Each test is skipped (via JUnit {@code Assume}) when the server is unreachable, so {@code mvn verify} stays green
 * without a database. Every test uses its own table, which is dropped afterwards.
 * CI runs this against PostgreSQL and MySQL; see {@code .github/workflows/state-db-smoketest.yml}.
 */
public class FileConnectorStateManagerDatabaseIT {

  private static final String URL = System.getenv("STATE_DB_URL");
  private static final String USER = System.getenv().getOrDefault("STATE_DB_USER", "");
  private static final String PASSWORD = System.getenv().getOrDefault("STATE_DB_PASSWORD", "");

  private static final String KEEP = "s3://bucket/keep.txt";
  private static final String GONE = "s3://bucket/gone.txt";

  private final List<String> tablesToDrop = new ArrayList<>();

  @After
  public void dropTables() throws SQLException {
    // Nothing was created if the test was skipped because the server was unreachable. Connecting here would throw and
    // turn the skip into a failure.
    if (tablesToDrop.isEmpty() || !isReachable()) {
      return;
    }
    try (Connection connection = connect(); Statement statement = connection.createStatement()) {
      for (String table : tablesToDrop) {
        statement.executeUpdate("DROP TABLE IF EXISTS " + table);
      }
    }
  }

  @Test
  public void testInitCreatesTableWithExpectedSchema() throws Exception {
    String connectorName = newConnectorName();
    FileConnectorStateManager manager = newManager(connectorName, null);
    manager.init();
    manager.shutdown();

    Set<String> columns = new HashSet<>();
    try (Connection connection = connect()) {
      DatabaseMetaData metadata = connection.getMetaData();
      try (ResultSet rs = metadata.getColumns(null, null, folded(tableFor(connectorName), metadata), null)) {
        while (rs.next()) {
          columns.add(rs.getString("COLUMN_NAME").toLowerCase(Locale.ROOT));
          if ("last_published".equalsIgnoreCase(rs.getString("COLUMN_NAME"))) {
            // new tables store epoch microseconds, not a timezone-aware timestamp
            assertEquals(Types.BIGINT, rs.getInt("DATA_TYPE"));
          }
        }
      }
    }
    assertEquals(Set.of("name", "last_published", "encountered", "runs_not_encountered"), columns);
  }

  @Test
  public void testExpiredFilesAreDeletedAcrossRuns() throws Exception {
    String connectorName = newConnectorName();

    // Run 1: both files are seen and published.
    FileConnectorStateManager first = newManager(connectorName, null);
    first.init();
    first.markFileEncountered(KEEP);
    first.markFileEncountered(GONE);
    first.successfullyPublishedFile(KEEP);
    first.successfullyPublishedFile(GONE);
    first.incrementRunsNotEncountered();
    first.shutdown();

    // Run 2: only KEEP is seen, so GONE is not encountered and expires after one run.
    FileConnectorStateManager second = newManager(connectorName, null);
    second.init();
    second.markFileEncountered(KEEP);
    second.incrementRunsNotEncountered();
    assertEquals(List.of(GONE), second.listExpiredFiles());
    second.shutdown();

    assertEquals(Set.of(KEEP), namesInTable(connectorName));
  }

  @Test
  public void testLastPublishedRoundTrip() throws Exception {
    String connectorName = newConnectorName();

    FileConnectorStateManager first = newManager(connectorName, null);
    Instant before = Instant.now().truncatedTo(ChronoUnit.MICROS);
    first.init();
    first.markFileEncountered(KEEP);
    first.successfullyPublishedFile(KEEP);
    Instant after = Instant.now();
    first.shutdown();

    FileConnectorStateManager second = newManager(connectorName, null);
    second.init();
    Instant lastPublished = second.getLastPublished(KEEP);
    second.shutdown();

    assertNotNull(lastPublished);
    // the table stores microseconds, so compare at that precision
    assertTrue("last published " + lastPublished + " before " + before, !lastPublished.isBefore(before));
    assertTrue("last published " + lastPublished + " after " + after, !lastPublished.isAfter(after));
  }

  @Test
  public void testMarkAllEntriesEncounteredMatchesPrefix() throws Exception {
    String connectorName = newConnectorName();
    String archiveEntry = "s3://bucket/archive.zip!/a.txt";
    String otherEntry = "s3://bucket/other.txt";

    FileConnectorStateManager first = newManager(connectorName, null);
    first.init();
    first.markFileEncountered(archiveEntry);
    first.markFileEncountered(otherEntry);
    first.incrementRunsNotEncountered();
    first.shutdown();

    // Run 2 encounters only the archive, via its prefix. The other entry should be the one that expires.
    FileConnectorStateManager second = newManager(connectorName, null);
    second.init();
    second.markAllEntriesEncountered("s3://bucket/archive.zip!/");
    second.incrementRunsNotEncountered();
    assertEquals(List.of(otherEntry), second.listExpiredFiles());
    second.shutdown();
  }

  @Test
  public void testDefaultPathLengthFitsLongPaths() throws Exception {
    String connectorName = newConnectorName();
    String longPath = "s3://lucille-bucket/" + "a".repeat(FileConnectorStateManager.DEFAULT_PATH_LENGTH - 20);

    FileConnectorStateManager manager = newManager(connectorName, null);
    manager.init();
    manager.markFileEncountered(longPath);
    manager.successfullyPublishedFile(longPath);
    assertNotNull(manager.getLastPublished(longPath));
    manager.shutdown();
  }

  @Test
  public void testPathLongerThanColumnThrows() throws Exception {
    String connectorName = newConnectorName();
    Config overrides = ConfigFactory.empty().withValue("pathLength", ConfigValueFactory.fromAnyRef(20));
    String path = "s3://lucille-bucket/files/info.txt";

    FileConnectorStateManager manager = newManager(connectorName, overrides);
    manager.init();
    try {
      StatePathTooLongException e = assertThrows(StatePathTooLongException.class, () -> manager.markFileEncountered(path));
      assertTrue(e.getMessage().contains(path));
    } finally {
      manager.shutdown();
    }
  }

  // Paths are case-sensitive, so two paths that differ only by case are two files. MySQL's default collation ignores case,
  // so the name column must be binary-collated there, or the second file would update the first file's row.
  @Test
  public void testPathsDifferingOnlyByCaseAreDistinct() throws Exception {
    String upper = "s3://bucket/Readme.txt";
    String lower = "s3://bucket/README.txt";
    String connectorName = "case_paths";
    tablesToDrop.add(tableFor(connectorName));

    FileConnectorStateManager first = newManager(connectorName, null);
    first.init();
    first.markFileEncountered(upper);
    first.markFileEncountered(lower);
    first.successfullyPublishedFile(upper);
    first.successfullyPublishedFile(lower);
    first.incrementRunsNotEncountered();
    first.shutdown();
    assertEquals(Set.of(upper, lower), namesInTable(connectorName));

    // The second run encounters only the lower-case file, so the upper-case file must expire.
    FileConnectorStateManager second = newManager(connectorName, null);
    second.init();
    second.markFileEncountered(lower);
    second.incrementRunsNotEncountered();
    assertEquals(List.of(upper), second.listExpiredFiles());
    second.shutdown();
  }

  // "_" is a wildcard in a metadata search pattern. Connector "axb" creates STATE_AXB, and connector "a_b" creates STATE_A_B.
  // Looking up STATE_A_B must not match STATE_AXB, or the second connector would skip its CREATE TABLE and fail.
  @Test
  public void testUnderscoreInNameDoesNotMatchAnotherTable() throws Exception {
    tablesToDrop.add(tableFor("axb"));
    tablesToDrop.add(tableFor("a_b"));

    FileConnectorStateManager first = newManager("axb", null);
    first.init();
    first.markFileEncountered(KEEP);
    first.successfullyPublishedFile(KEEP);
    first.shutdown();

    FileConnectorStateManager second = newManager("a_b", null);
    second.init();
    second.markFileEncountered(GONE);
    second.successfullyPublishedFile(GONE);
    assertNotNull(second.getLastPublished(GONE));
    assertEquals(Set.of(GONE), namesInTable("a_b"));
    second.shutdown();
  }

  // Builds a manager pointed at the configured database. Skips the test only when STATE_DB_URL is unset. When it is set
  // but the database can't be reached, the test fails, so a misconfigured CI job can't pass without running anything.
  private FileConnectorStateManager newManager(String connectorName, Config overrides) {
    assumeTrue("STATE_DB_URL is not set", URL != null);
    if (!isReachable()) {
      fail("STATE_DB_URL is set, but the database is not reachable at " + URL);
    }

    Config base = ConfigFactory.parseMap(Map.of(
        "driver", driver(),
        "connectionString", URL,
        "jdbcUser", USER,
        "jdbcPassword", PASSWORD));
    Config config = overrides == null ? base : overrides.withFallback(base);
    return new FileConnectorStateManager(config, connectorName);
  }

  // Each test gets its own table, registered for cleanup.
  private String newConnectorName() {
    String connectorName = "it_state_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    tablesToDrop.add(tableFor(connectorName));
    return connectorName;
  }

  // The table the state manager uses for a connector: the same mapping the manager applies.
  private static String tableFor(String connectorName) {
    return FileConnectorStateManager.toSqlTableName(connectorName);
  }

  // The name an unquoted identifier has in the database's metadata, which differs by database.
  private static String folded(String identifier, DatabaseMetaData metadata) throws SQLException {
    if (metadata.storesUpperCaseIdentifiers()) {
      return identifier.toUpperCase(Locale.ROOT);
    }
    if (metadata.storesLowerCaseIdentifiers()) {
      return identifier.toLowerCase(Locale.ROOT);
    }
    return identifier;
  }

  private Set<String> namesInTable(String connectorName) throws SQLException {
    Set<String> names = new HashSet<>();
    try (Connection connection = connect();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT name FROM " + tableFor(connectorName))) {
      while (rs.next()) {
        names.add(rs.getString("name"));
      }
    }
    return names;
  }

  private static String driver() {
    String configured = System.getenv("STATE_DB_DRIVER");
    if (configured != null && !configured.isBlank()) {
      return configured;
    }
    if (URL.startsWith("jdbc:mysql:")) {
      return "com.mysql.cj.jdbc.Driver";
    }
    if (URL.startsWith("jdbc:postgresql:")) {
      return "org.postgresql.Driver";
    }
    throw new IllegalStateException("Set STATE_DB_DRIVER for " + URL);
  }

  private static boolean isReachable() {
    if (URL == null) {
      return false;
    }
    try (Connection connection = connect()) {
      return connection.isValid(2);
    } catch (SQLException e) {
      return false;
    }
  }

  private static Connection connect() throws SQLException {
    return DriverManager.getConnection(URL, USER, PASSWORD);
  }
}
